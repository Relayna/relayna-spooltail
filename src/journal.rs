use crate::{
    packing::{Manifest, PackedReader},
    partition_index,
    store::{Frame, index},
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, Transaction};
use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{BufReader, ErrorKind, IoSlice, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        mpsc::{Receiver, Sender, channel},
    },
};

pub(crate) const SEGMENT_BYTES: u64 = 8 * 1024 * 1024;
const SYNC_BATCH_LIMIT: usize = 16;
pub const MAX_FRAME: usize = 1024 * 1024;
pub const OFFSET_FIELD_BYTES: usize = r#","offset":"#.len();
pub const ACCEPTED_AT_FIELD_BYTES: usize = r#","accepted_at_ms":"#.len();
pub(crate) const MAX_STORED_FRAME: usize =
    MAX_FRAME + OFFSET_FIELD_BYTES + ACCEPTED_AT_FIELD_BYTES + 38;

#[derive(Clone, Copy)]
pub struct Address {
    pub segment: u64,
    pub pos: u64,
    pub len: usize,
}

pub struct Journal {
    dir: PathBuf,
    segment: u64,
    file: File,
    end: u64,
    index: Option<IndexWriter>,
    pending: Vec<PendingAppend>,
    failed_sync: bool,
}

struct PendingAppend {
    frame: Arc<Frame>,
    partition: u32,
    address: Address,
    synced: Sender<Address>,
}

pub struct StagedAppend {
    pub leader: bool,
    synced: Receiver<Address>,
}

impl StagedAppend {
    pub fn wait(self) -> Result<Address> {
        self.synced.recv().context("journal batch failed")
    }
}

enum IndexWriter {
    Inline(Connection),
    Background(partition_index::BackgroundIndex),
}

pub fn partition_dir(root: &Path, partition: u32) -> PathBuf {
    if partition == 0 {
        root.to_owned()
    } else {
        root.join("partitions").join(partition.to_string())
    }
}

pub(crate) fn segment_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:020}.seg"))
}

fn create_segment(dir: &Path, id: u64) -> Result<File> {
    let file = File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(segment_path(dir, id))?;
    File::open(dir)?.sync_all()?;
    Ok(file)
}

impl Journal {
    pub fn create(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let path = segment_path(dir, 0);
        let mut entries = fs::read_dir(dir)?;
        let file = match entries.next().transpose()? {
            None => create_segment(dir, 0)?,
            Some(first) => {
                // A crash before registry commit may have left an empty first segment.
                ensure!(
                    first.path() == path
                        && first.metadata()?.len() == 0
                        && entries.next().is_none(),
                    "unregistered partition directory is not empty"
                );
                File::options().read(true).write(true).open(path)?
            }
        };
        Ok(Self {
            dir: dir.to_owned(),
            segment: 0,
            file,
            end: 0,
            index: None,
            pending: Vec::new(),
            failed_sync: false,
        })
    }

    pub fn active_segment(&self) -> u64 {
        self.segment
    }

    pub fn open(
        dir: &Path,
        db: &mut Connection,
        partition: u32,
        packed: &Manifest,
    ) -> Result<(Self, usize)> {
        Self::open_with(dir, db, partition, (0, 0), packed, index)
    }

    pub fn open_partitioned(
        dir: &Path,
        mut db: Connection,
        partition: u32,
        legacy_checkpoint: (i64, i64),
        packed: &Manifest,
    ) -> Result<(Self, usize)> {
        let (mut journal, recovered) = Self::open_with(
            dir,
            &mut db,
            partition,
            legacy_checkpoint,
            packed,
            partition_index::index_frame,
        )?;
        journal.enable_index(db);
        Ok((journal, recovered))
    }

    pub fn enable_index(&mut self, db: Connection) {
        self.index = Some(IndexWriter::Inline(db));
    }

    pub fn batch_index(&mut self, size: usize) -> Result<()> {
        let Some(IndexWriter::Inline(db)) = self.index.take() else {
            anyhow::bail!("background batching requires an inline partition index");
        };
        self.index = Some(IndexWriter::Background(
            partition_index::BackgroundIndex::start(db, size)?,
        ));
        Ok(())
    }

    fn open_with(
        dir: &Path,
        db: &mut Connection,
        partition: u32,
        fallback: (i64, i64),
        packed: &Manifest,
        apply: fn(&Transaction<'_>, &Frame, u32, Address) -> Result<i64>,
    ) -> Result<(Self, usize)> {
        let mut checkpoint = db.query_row(
            "SELECT segment,pos+8+len FROM records WHERE partition=? ORDER BY offset DESC LIMIT 1",
            [partition],
            |r| Ok((r.get(0)?, r.get(1)?)),
        ).optional()?.unwrap_or(fallback);
        if checkpoint < fallback {
            checkpoint = fallback;
        }
        let (mut segment, mut end) = (u64::try_from(checkpoint.0)?, u64::try_from(checkpoint.1)?);
        let mut recovered_frames: usize = 0;
        let mut ids: BTreeSet<_> = packed
            .range((partition, 0)..=(partition, u64::MAX))
            .map(|(&(.., segment), _)| segment)
            .collect();
        for f in fs::read_dir(dir)? {
            let path = f?.path();
            if path.extension().and_then(|s| s.to_str()) == Some("seg") {
                let id: u64 = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .context("invalid segment name")?
                    .parse()?;
                ensure!(segment_path(dir, id) == path, "unexpected segment name");
                ids.insert(id);
            }
        }
        for (expected, actual) in ids.iter().enumerate() {
            ensure!(
                *actual == expected as u64,
                "missing journal segment {expected}"
            );
        }
        if ids.is_empty() {
            ensure!(end == 0 && segment == 0, "missing indexed journal files");
            create_segment(dir, 0)?;
            ids.insert(0);
        }
        let last = *ids.last().unwrap();
        ensure!(
            !packed.contains_key(&(partition, last)),
            "the active segment must stay raw"
        );
        ensure!(segment <= last, "indexed segment missing");
        loop {
            let packed_len = packed.get(&(partition, segment)).copied();
            if packed_len == Some(0) {
                segment += 1;
                end = 0;
                continue;
            }
            let (mut reader, size) = match packed_len {
                Some(len) => (open_segment(dir, segment, Some(len))?, len),
                None => {
                    let file = File::options()
                        .read(true)
                        .write(true)
                        .open(segment_path(dir, segment))?;
                    let size = file.metadata()?.len();
                    if size > end {
                        // Restore journal durability before indexing a recovered suffix.
                        file.sync_data()?;
                    }
                    (SegmentReader::Raw(BufReader::new(file)), size)
                }
            };
            ensure!(size >= end, "segment ends before durable index checkpoint");
            reader.seek(SeekFrom::Start(end))?;
            let mut tx = db.transaction()?;
            while end < size {
                let mut header = [0; 8];
                let complete_header = size - end >= 8;
                if complete_header {
                    reader.read_exact(&mut header)?;
                }
                let len = u32::from_le_bytes(header[..4].try_into()?) as usize;
                if complete_header {
                    ensure!(
                        (1..=MAX_STORED_FRAME).contains(&len),
                        "invalid frame length at {segment}:{end}"
                    );
                }
                if !complete_header || size - end - 8 < len as u64 {
                    ensure!(segment == last, "truncated sealed segment");
                    let SegmentReader::Raw(raw) = &reader else {
                        anyhow::bail!("truncated packed segment");
                    };
                    raw.get_ref().set_len(end)?;
                    raw.get_ref().sync_all()?;
                    break;
                }
                let mut body = vec![0; len];
                reader.read_exact(&mut body)?;
                ensure!(
                    crc32fast::hash(&body) == u32::from_le_bytes(header[4..].try_into()?),
                    "journal checksum mismatch at {segment}:{end}"
                );
                let frame: Frame = serde_json::from_slice(&body)?;
                apply(
                    &tx,
                    &frame,
                    partition,
                    Address {
                        segment,
                        pos: end,
                        len,
                    },
                )?;
                end += 8 + len as u64;
                recovered_frames += 1;
                if recovered_frames.is_multiple_of(2048) {
                    tx.commit()?;
                    tx = db.transaction()?;
                }
            }
            tx.commit()?;
            if segment == last {
                break;
            }
            segment += 1;
            end = 0;
        }
        let mut file = File::options()
            .read(true)
            .write(true)
            .open(segment_path(dir, segment))?;
        file.seek(SeekFrom::Start(end))?;
        Ok((
            Self {
                dir: dir.to_owned(),
                segment,
                file,
                end,
                index: None,
                pending: Vec::new(),
                failed_sync: false,
            },
            recovered_frames,
        ))
    }

    pub fn append(&mut self, frame: &Frame, partition: u32, body: &[u8]) -> Result<Address> {
        let address = self.write(body)?;
        self.index_append(frame, partition, address)?;
        Ok(address)
    }

    fn index_append(&mut self, frame: &Frame, partition: u32, address: Address) -> Result<()> {
        match &mut self.index {
            Some(IndexWriter::Inline(db)) => {
                let tx = db.transaction()?;
                partition_index::index_frame(&tx, frame, partition, address)?;
                tx.commit()?;
            }
            Some(IndexWriter::Background(worker)) => worker.enqueue(frame, partition, address)?,
            None => {}
        }
        Ok(())
    }

    pub fn stage_append(
        &mut self,
        frame: Arc<Frame>,
        partition: u32,
        body: &[u8],
    ) -> Result<StagedAppend> {
        ensure!(!self.failed_sync, "journal batch failed");
        // Seal the current segment only after every preceding frame is durable.
        if self.would_rotate(body) {
            self.flush_staged()?;
        }
        let address = match self.write_frame(body) {
            Ok(address) => address,
            Err(error) => {
                self.failed_sync = true;
                self.pending.clear();
                return Err(error);
            }
        };
        let leader = self.pending.is_empty();
        let (synced, receipt) = channel();
        self.pending.push(PendingAppend {
            frame,
            partition,
            address,
            synced,
        });
        if self.pending.len() == SYNC_BATCH_LIMIT {
            self.flush_staged()?;
        }
        Ok(StagedAppend {
            leader,
            synced: receipt,
        })
    }

    pub fn flush_staged(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let result = self.file.sync_data();
        let mut pending = std::mem::take(&mut self.pending);
        let result = (|| {
            result?;
            for entry in &pending {
                self.index_append(&entry.frame, entry.partition, entry.address)?;
            }
            for entry in pending.drain(..) {
                let _ = entry.synced.send(entry.address);
            }
            Ok(())
        })();
        self.pending = pending;
        self.pending.clear();
        self.failed_sync = result.is_err();
        result
    }

    pub fn write(&mut self, body: &[u8]) -> Result<Address> {
        let address = self.write_frame(body)?;
        self.file.sync_data()?;
        Ok(address)
    }

    fn would_rotate(&self, body: &[u8]) -> bool {
        self.end + 8 + body.len() as u64 > SEGMENT_BYTES
    }

    fn write_frame(&mut self, body: &[u8]) -> Result<Address> {
        if self.would_rotate(body) {
            self.file = create_segment(&self.dir, self.segment + 1)?;
            self.segment += 1;
            self.end = 0;
        }
        let address = Address {
            segment: self.segment,
            pos: self.end,
            len: body.len(),
        };
        let length = (body.len() as u32).to_le_bytes();
        let checksum = crc32fast::hash(body).to_le_bytes();
        let mut parts = [
            IoSlice::new(&length),
            IoSlice::new(&checksum),
            IoSlice::new(body),
        ];
        let mut remaining = parts.as_mut_slice();
        while !remaining.is_empty() {
            match self.file.write_vectored(remaining) {
                Ok(0) => return Err(std::io::Error::from(ErrorKind::WriteZero).into()),
                Ok(written) => IoSlice::advance_slices(&mut remaining, written),
                Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
        }
        self.end += 8 + body.len() as u64;
        Ok(address)
    }
}

pub fn read(dir: &Path, address: Address, packed_len: Option<u64>) -> Result<Frame> {
    if let Some(len) = packed_len {
        let mut reader = PackedReader::open(dir, address.segment, len)?;
        reader.seek(SeekFrom::Start(address.pos))?;
        return read_frame(&mut reader, address.len);
    }
    let mut file = File::open(segment_path(dir, address.segment))?;
    file.seek(SeekFrom::Start(address.pos))?;
    read_frame(&mut file, address.len)
}

pub(crate) enum SegmentReader {
    Raw(BufReader<File>),
    Packed(PackedReader),
}

impl Read for SegmentReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Raw(file) => file.read(buffer),
            Self::Packed(file) => file.read(buffer),
        }
    }
}

impl Seek for SegmentReader {
    fn seek(&mut self, to: SeekFrom) -> std::io::Result<u64> {
        match self {
            Self::Raw(file) => file.seek(to),
            Self::Packed(file) => file.seek(to),
        }
    }
}

pub(crate) fn open_segment(
    dir: &Path,
    segment: u64,
    packed_len: Option<u64>,
) -> Result<SegmentReader> {
    Ok(match packed_len {
        Some(len) => SegmentReader::Packed(PackedReader::open(dir, segment, len)?),
        None => SegmentReader::Raw(BufReader::new(File::open(segment_path(dir, segment))?)),
    })
}

pub(crate) fn read_frame(file: &mut impl Read, len: usize) -> Result<Frame> {
    ensure!(len <= MAX_STORED_FRAME, "invalid size in address index");
    let mut header = [0; 8];
    file.read_exact(&mut header)?;
    ensure!(
        u32::from_le_bytes(header[..4].try_into()?) as usize == len,
        "invalid indexed length"
    );
    let mut body = vec![0; len];
    file.read_exact(&mut body)?;
    ensure!(
        crc32fast::hash(&body) == u32::from_le_bytes(header[4..].try_into()?),
        "journal checksum mismatch"
    );
    Ok(serde_json::from_slice(&body)?)
}
