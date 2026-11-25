//! The coordinator manifest selects the authoritative copy of each sealed segment.
use crate::{
    Broker,
    journal::{MAX_STORED_FRAME, SEGMENT_BYTES, partition_dir, segment_path},
    store::Store,
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, params};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{self, BufReader, ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::Duration,
};

const MAGIC: &[u8; 8] = b"STPACK01";
const HEADER: u64 = 24;
const ENTRY: u64 = 20;
const BLOCK_BYTES: usize = 64 * 1024;
const MAX_BLOCK_BYTES: usize = MAX_STORED_FRAME + 8;
// Adjacent blocks together exceed the target size unless one is the only block.
const MAX_BLOCKS: u64 = SEGMENT_BYTES * 2 / BLOCK_BYTES as u64 + 1;
const MIN_PACK_INTERVAL: Duration = Duration::from_secs(1);
const IDLE_INTERVAL: Duration = Duration::from_secs(5);
pub type Manifest = BTreeMap<(u32, u64), u64>;

pub fn init(db: &Connection) -> Result<Manifest> {
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS packed_segments (
            partition INTEGER NOT NULL, segment INTEGER NOT NULL, raw_len INTEGER NOT NULL,
            PRIMARY KEY(partition,segment)
        );",
    )?;
    let mut stmt = db.prepare("SELECT partition,segment,raw_len FROM packed_segments")?;
    let mut rows = stmt.query([])?;
    let mut manifest = BTreeMap::new();
    while let Some(row) = rows.next()? {
        manifest.insert(
            (row.get(0)?, u64::try_from(row.get::<_, i64>(1)?)?),
            u64::try_from(row.get::<_, i64>(2)?)?,
        );
    }
    Ok(manifest)
}

pub fn configure(store: &mut Store) -> Result<Option<Duration>> {
    match std::env::var("SPOOLTAIL_PACK_SEALED") {
        Ok(value) if value == "1" => {}
        Ok(value) if value == "0" => return Ok(None),
        Err(std::env::VarError::NotPresent) => return Ok(None),
        _ => anyhow::bail!("SPOOLTAIL_PACK_SEALED must be 0 or 1"),
    }
    let seconds = match std::env::var("SPOOLTAIL_PACK_MIN_AGE_SECS") {
        Ok(value) => value
            .parse()
            .context("SPOOLTAIL_PACK_MIN_AGE_SECS must be a nonnegative integer")?,
        Err(std::env::VarError::NotPresent) => 3600,
        Err(error) => return Err(error).context("read SPOOLTAIL_PACK_MIN_AGE_SECS"),
    };
    store.ensure_segment_index()?;
    store
        .db
        .execute_batch("CREATE TEMP TABLE packed_archive_gc (id INTEGER PRIMARY KEY)")?;
    Ok(Some(Duration::from_secs(seconds)))
}

pub fn packed_path(dir: &Path, segment: u64) -> PathBuf {
    dir.join(format!("{segment:020}.seg.zst"))
}

#[derive(Clone, Copy)]
struct Block {
    start: u32,
    pos: u32,
    packed_len: u32,
    raw_len: u32,
    checksum: u32,
}

impl Block {
    fn read(reader: &mut impl Read) -> io::Result<Self> {
        Ok(Self {
            start: read_u32(reader)?,
            pos: read_u32(reader)?,
            packed_len: read_u32(reader)?,
            raw_len: read_u32(reader)?,
            checksum: read_u32(reader)?,
        })
    }

    fn write(self, mut writer: impl Write) -> io::Result<()> {
        for value in [
            self.start,
            self.pos,
            self.packed_len,
            self.raw_len,
            self.checksum,
        ] {
            writer.write_all(&value.to_le_bytes())?;
        }
        Ok(())
    }
}

fn read_u32(reader: &mut impl Read) -> io::Result<u32> {
    let mut bytes = [0; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn flush_block(file: &mut File, raw: &[u8], start: u32, blocks: &mut Vec<Block>) -> Result<()> {
    let packed = zstd::bulk::compress(raw, 1)?;
    ensure!(
        zstd::bulk::decompress(&packed, raw.len())? == raw,
        "packed bytes failed verification"
    );
    let pos = u32::try_from(file.stream_position()?)?;
    file.write_all(&packed)?;
    blocks.push(Block {
        start,
        pos,
        packed_len: u32::try_from(packed.len())?,
        raw_len: u32::try_from(raw.len())?,
        checksum: crc32fast::hash(raw),
    });
    Ok(())
}

struct TemporaryPack(PathBuf);

impl Drop for TemporaryPack {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn build(dir: &Path, segment: u64) -> Result<Option<u64>> {
    let raw = File::open(segment_path(dir, segment))?;
    let raw_len = raw.metadata()?.len();
    ensure!(
        (1..=SEGMENT_BYTES).contains(&raw_len),
        "invalid sealed segment size"
    );
    let mut reader = BufReader::new(raw);
    let final_path = packed_path(dir, segment);
    let temporary = TemporaryPack(final_path.with_extension("zst.writing"));
    let mut file = File::create(&temporary.0)?;
    stop_at("pack_opened");
    file.write_all(&[0; HEADER as usize])?;
    let mut raw_pos = 0_u32;
    let mut block_start = 0_u32;
    let mut chunk = Vec::new();
    let mut blocks = Vec::new();
    while u64::from(raw_pos) < raw_len {
        let length = read_u32(&mut reader)?;
        let checksum = read_u32(&mut reader)?;
        ensure!(
            (1..=MAX_STORED_FRAME).contains(&(length as usize)),
            "invalid journal frame"
        );
        let frame_len = length + 8;
        ensure!(
            u64::from(raw_pos) + u64::from(frame_len) <= raw_len,
            "torn sealed journal"
        );
        if !chunk.is_empty() && chunk.len() + frame_len as usize > BLOCK_BYTES {
            flush_block(&mut file, &chunk, block_start, &mut blocks)?;
            chunk.clear();
            block_start = raw_pos;
        }
        chunk.extend_from_slice(&length.to_le_bytes());
        chunk.extend_from_slice(&checksum.to_le_bytes());
        let start = chunk.len();
        chunk.resize(start + length as usize, 0);
        reader.read_exact(&mut chunk[start..])?;
        ensure!(
            crc32fast::hash(&chunk[start..]) == checksum,
            "journal checksum mismatch"
        );
        raw_pos += frame_len;
    }
    if !chunk.is_empty() {
        flush_block(&mut file, &chunk, block_start, &mut blocks)?;
    }
    let index_at = u32::try_from(file.stream_position()?)?;
    for block in &blocks {
        block.write(&mut file)?;
    }
    if file.stream_position()? >= raw_len {
        return Ok(None);
    }
    file.rewind()?;
    file.write_all(MAGIC)?;
    for value in [raw_pos, index_at, u32::try_from(blocks.len())?, 0] {
        file.write_all(&value.to_le_bytes())?;
    }
    file.sync_all()?;
    stop_at("pack_synced");
    fs::rename(&temporary.0, final_path)?;
    stop_at("pack_renamed");
    File::open(dir)?.sync_all()?;
    stop_at("pack_named");
    Ok(Some(raw_len))
}

pub struct PackedReader {
    file: File,
    raw_len: u64,
    pos: u64,
    blocks: Vec<Block>,
    loaded: Option<usize>,
    bytes: Vec<u8>,
}

impl PackedReader {
    pub fn open(dir: &Path, segment: u64, raw_len: u64) -> Result<Self> {
        let path = packed_path(dir, segment);
        let mut file =
            File::open(&path).with_context(|| format!("open packed segment {}", path.display()))?;
        let mut magic = [0; 8];
        file.read_exact(&mut magic)?;
        ensure!(&magic == MAGIC, "invalid packed segment format");
        ensure!(
            (1..=SEGMENT_BYTES).contains(&raw_len) && u64::from(read_u32(&mut file)?) == raw_len,
            "packed segment length differs from manifest"
        );
        let index_at = u64::from(read_u32(&mut file)?);
        let count = u64::from(read_u32(&mut file)?);
        ensure!(
            read_u32(&mut file)? == 0,
            "unsupported packed segment flags"
        );
        ensure!(
            index_at >= HEADER
                && (1..=MAX_BLOCKS).contains(&count)
                && index_at + count * ENTRY == file.metadata()?.len(),
            "invalid packed segment index size"
        );
        let mut input = BufReader::new(file);
        input.seek(SeekFrom::Start(index_at))?;
        let mut raw_end = 0_u64;
        let mut packed_end = HEADER;
        let mut blocks = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let block = Block::read(&mut input)?;
            ensure!(
                u64::from(block.start) == raw_end
                    && u64::from(block.pos) == packed_end
                    && (1..=MAX_BLOCK_BYTES).contains(&(block.raw_len as usize))
                    && (1..=zstd::zstd_safe::compress_bound(block.raw_len as usize))
                        .contains(&(block.packed_len as usize)),
                "invalid packed block bounds"
            );
            raw_end += u64::from(block.raw_len);
            packed_end += u64::from(block.packed_len);
            blocks.push(block);
        }
        ensure!(
            raw_end == raw_len && packed_end == index_at,
            "incomplete packed segment coverage"
        );
        Ok(Self {
            file: input.into_inner(),
            raw_len,
            pos: 0,
            blocks,
            loaded: None,
            bytes: Vec::new(),
        })
    }
}

impl Read for PackedReader {
    fn read(&mut self, into: &mut [u8]) -> io::Result<usize> {
        if into.is_empty() || self.pos >= self.raw_len {
            return Ok(0);
        }
        let id = self
            .blocks
            .partition_point(|block| u64::from(block.start) <= self.pos)
            - 1;
        let block = self.blocks[id];
        if self.loaded != Some(id) {
            self.file.seek(SeekFrom::Start(u64::from(block.pos)))?;
            let mut input = vec![0; block.packed_len as usize];
            self.file.read_exact(&mut input)?;
            let bytes = zstd::bulk::decompress(&input, block.raw_len as usize)?;
            if bytes.len() != block.raw_len as usize || crc32fast::hash(&bytes) != block.checksum {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "packed block checksum mismatch",
                ));
            }
            self.bytes = bytes;
            self.loaded = Some(id);
        }
        let start = (self.pos - u64::from(block.start)) as usize;
        let count = into.len().min(self.bytes.len() - start);
        into[..count].copy_from_slice(&self.bytes[start..start + count]);
        self.pos += count as u64;
        Ok(count)
    }
}

impl Seek for PackedReader {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        let next = match to {
            SeekFrom::Start(pos) => Some(pos),
            SeekFrom::End(delta) => self.raw_len.checked_add_signed(delta),
            SeekFrom::Current(delta) => self.pos.checked_add_signed(delta),
        };
        self.pos =
            next.ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "invalid packed seek"))?;
        Ok(self.pos)
    }
}

fn unlink_raw(dir: &Path, segment: u64) -> Result<()> {
    match fs::remove_file(segment_path(dir, segment)) {
        Ok(()) => stop_at("raw_unlinked"),
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    File::open(dir)?.sync_all()?;
    Ok(())
}

pub fn recover(root: &Path, partitions: &[u32], entries: &Manifest) -> Result<()> {
    for (&(partition, segment), &len) in entries {
        let dir = partition_dir(root, partition);
        if len == 0 {
            if (packed_path(&dir, segment).try_exists()?
                || segment_path(&dir, segment).try_exists()?)
                && let Err(error) = crate::retire::remove_retired(&dir, segment)
            {
                eprintln!("could not remove retired segment {partition}:{segment}: {error:#}");
            }
            continue;
        }
        let mut reader = PackedReader::open(&dir, segment, len)?;
        if segment_path(&dir, segment).try_exists()? {
            // Interrupted cleanup left both copies. Verify the packed payload before removing raw.
            ensure!(
                io::copy(&mut reader, &mut io::sink())? == len,
                "packed segment incomplete"
            );
            if let Err(error) = unlink_raw(&dir, segment) {
                eprintln!(
                    "could not remove redundant journal segment {partition}:{segment}: {error:#}"
                );
            }
        }
    }
    for &partition in partitions {
        let dir = partition_dir(root, partition);
        if let Err(error) = remove_unpublished(&dir, partition, entries) {
            eprintln!(
                "could not remove unpublished packed segment in partition {partition}: {error:#}"
            );
        }
    }
    Ok(())
}

fn remove_unpublished(dir: &Path, partition: u32, entries: &Manifest) -> Result<()> {
    let mut changed = false;
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(segment) = name
            .strip_suffix(".seg.zst")
            .or_else(|| name.strip_suffix(".seg.zst.writing"))
            .and_then(|id| id.parse::<u64>().ok())
        else {
            continue;
        };
        let published = packed_path(dir, segment);
        if path != published && path != published.with_extension("zst.writing") {
            continue;
        }
        if path == published && entries.contains_key(&(partition, segment)) {
            continue;
        }
        if segment_path(dir, segment).try_exists()? {
            fs::remove_file(path)?;
            changed = true;
        }
    }
    if changed {
        File::open(dir)?.sync_all()?;
    }
    Ok(())
}

#[cfg(feature = "pack-fault-injection")]
pub(crate) fn stop_at(stage: &str) {
    if std::env::var("SPOOLTAIL_PACK_STOP_AT").as_deref() == Ok(stage) {
        eprintln!("PACK_STOP {stage}");
        if let Some(path) = std::env::var_os("SPOOLTAIL_PACK_TEST_RESUME") {
            while !Path::new(&path).exists() {
                std::thread::sleep(Duration::from_millis(5));
            }
            return;
        }
        loop {
            std::thread::park();
        }
    }
}

#[cfg(not(feature = "pack-fault-injection"))]
pub(crate) fn stop_at(_: &str) {}

pub async fn run(broker: Broker, min_age: Duration) {
    let mut scan = Scan::default();
    loop {
        let delay = match scan.one(&broker, min_age).await {
            Ok(true) => MIN_PACK_INTERVAL,
            Ok(false) => IDLE_INTERVAL,
            Err(error) => {
                eprintln!("packing failed: {error:#}");
                IDLE_INTERVAL
            }
        };
        tokio::time::sleep(delay).await;
    }
}

#[derive(Default)]
struct Scan {
    next: BTreeMap<u32, u64>,
    last_partition: Option<u32>,
    pending_unlink: Option<(PathBuf, u64)>,
}

impl Scan {
    async fn one(&mut self, broker: &Broker, min_age: Duration) -> Result<bool> {
        self.finish_unlink().await?;
        let next = self.next.clone();
        let (root, mut journals) = broker
            .run(move |store| {
                let journals = store
                    .partition_ids()
                    .into_iter()
                    .map(|id| {
                        let mut segment = next.get(&id).copied().unwrap_or(0);
                        for (&(_, packed), _) in store.packed.range((id, segment)..=(id, u64::MAX))
                        {
                            if packed != segment {
                                break;
                            }
                            segment += 1;
                        }
                        Ok((id, store.journal(id)?, segment))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok((store.data_dir().to_owned(), journals))
            })
            .await?;
        if let Some(last) = self.last_partition {
            let split = journals.partition_point(|(id, ..)| *id <= last);
            journals.rotate_left(split);
        }
        for (partition, journal, segment) in journals {
            // Never wait for a journal writer while holding the coordinator lock.
            let active = journal.lock().await.active_segment();
            if segment >= active {
                continue;
            }
            let dir = partition_dir(&root, partition);
            let source = dir.clone();
            let metadata =
                tokio::task::spawn_blocking(move || fs::metadata(segment_path(&source, segment)))
                    .await??;
            if metadata.modified()?.elapsed().unwrap_or_default() < min_age {
                continue;
            }
            let budget = metadata.len();
            if !broker
                .run(move |store| store.reserve_pack_space(budget))
                .await?
            {
                return Ok(false);
            }
            let result = async {
                let source = dir.clone();
                if let Some(raw_len) =
                    tokio::task::spawn_blocking(move || build(&source, segment)).await??
                {
                    broker
                        .run(move |store| store.publish_packed(partition, segment, raw_len))
                        .await?;
                    self.pending_unlink = Some((dir, segment));
                }
                Ok::<_, anyhow::Error>(())
            }
            .await;
            broker
                .run(|store| {
                    store.release_pack_space();
                    Ok(())
                })
                .await?;
            result?;
            self.next.insert(partition, segment + 1);
            self.last_partition = Some(partition);
            self.finish_unlink().await?;
            return Ok(true);
        }
        Ok(false)
    }

    async fn finish_unlink(&mut self) -> Result<()> {
        if let Some((dir, segment)) = self.pending_unlink.clone() {
            tokio::task::spawn_blocking(move || {
                stop_at("manifest_committed");
                unlink_raw(&dir, segment)
            })
            .await??;
            self.pending_unlink = None;
        }
        Ok(())
    }
}

impl Store {
    pub fn ensure_segment_index(&self) -> Result<()> {
        self.db.execute_batch(if self.split.is_some() {
            "CREATE INDEX IF NOT EXISTS temp.by_segment ON records(partition,segment,offset)"
        } else {
            "CREATE INDEX IF NOT EXISTS by_segment ON records(partition,segment,offset)"
        })?;
        Ok(())
    }

    fn publish_packed(&mut self, partition: u32, segment: u64, raw_len: u64) -> Result<()> {
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO packed_segments VALUES (?,?,?)",
            params![partition, i64::try_from(segment)?, i64::try_from(raw_len)?],
        )?;
        // Convert existing audit rows to shared frame references in the same commit.
        // A BLOB shared with a still-raw segment remains until its last reference moves.
        tx.execute(
            "INSERT INTO packed_archive_gc
             SELECT DISTINCT block_id FROM archive_records AS a JOIN records AS r ON r.offset=a.offset
             WHERE r.partition=? AND r.segment=? AND a.block_id>0",
            params![partition, i64::try_from(segment)?],
        )?;
        tx.execute(
            "UPDATE archive_records SET block_id=0
             WHERE offset IN (SELECT offset FROM records WHERE partition=? AND segment=?)",
            params![partition, i64::try_from(segment)?],
        )?;
        tx.execute(
            "DELETE FROM archive_blocks AS b WHERE id IN (SELECT id FROM packed_archive_gc)
             AND NOT EXISTS (SELECT 1 FROM archive_records WHERE block_id=b.id)",
            [],
        )?;
        tx.execute("DELETE FROM packed_archive_gc", [])?;
        tx.commit()?;
        self.packed.insert((partition, segment), raw_len);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn framed(bodies: &[&[u8]]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for &body in bodies {
            bytes.extend((body.len() as u32).to_le_bytes());
            bytes.extend(crc32fast::hash(body).to_le_bytes());
            bytes.extend(body);
        }
        bytes
    }

    #[test]
    fn packed_blocks_preserve_logical_seeks_and_check_integrity() -> Result<()> {
        let dir = tempdir()?;
        let a = vec![b'a'; BLOCK_BYTES / 2];
        let b = vec![b'b'; BLOCK_BYTES];
        let raw = framed(&[&a, &b, &a]);
        fs::write(segment_path(dir.path(), 0), &raw)?;
        let len = build(dir.path(), 0)?.unwrap();
        let mut reader = PackedReader::open(dir.path(), 0, len)?;
        for start in [0, 1, a.len() + 6, a.len() + b.len() + 20] {
            reader.seek(SeekFrom::Start(start as u64))?;
            let mut decoded = vec![0; 32];
            reader.read_exact(&mut decoded)?;
            assert_eq!(decoded, raw[start..start + 32]);
        }
        let mut file = File::options()
            .write(true)
            .open(packed_path(dir.path(), 0))?;
        file.seek(SeekFrom::Start(HEADER))?;
        file.write_all(b"BAD!")?;
        let mut reader = PackedReader::open(dir.path(), 0, len)?;
        assert!(reader.read_exact(&mut [0; 1]).is_err());
        assert!(segment_path(dir.path(), 0).exists());
        Ok(())
    }

    #[test]
    fn leaves_a_segment_raw_if_the_pack_would_be_larger() -> Result<()> {
        let dir = tempdir()?;
        let raw = framed(&[b"{}"]);
        fs::write(segment_path(dir.path(), 0), &raw)?;
        assert!(build(dir.path(), 0)?.is_none());
        assert_eq!(fs::read(segment_path(dir.path(), 0))?, raw);
        assert!(!packed_path(dir.path(), 0).exists());
        assert!(
            !packed_path(dir.path(), 0)
                .with_extension("zst.writing")
                .exists()
        );
        Ok(())
    }
}
