use crate::subscriptions::WorkLease;
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const SEGMENT_BYTES: u64 = 8 * 1024 * 1024;
const MAX_FRAME: usize = 1024 * 1024;
pub const PAGE_SIZE: i64 = 64;

#[derive(Debug, thiserror::Error)]
pub enum Problem {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Conflict(String),
    #[error("cursor unavailable")]
    Gap { earliest: Option<String> },
    #[error("resource not found")]
    Missing,
}

pub fn identifier(s: &str) -> Result<()> {
    if s.is_empty() || s.len() > 256 || s.chars().any(char::is_control) {
        return Err(Problem::Invalid(
            "identifier must be 1..256 bytes without control characters".into(),
        )
        .into());
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct Frame {
    topic: String,
    event: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    completion: Option<WorkLease>,
}

pub fn terminal_status(event: &Value) -> bool {
    event["status"].as_str().is_some_and(|status| {
        status.eq_ignore_ascii_case("completed") || status.eq_ignore_ascii_case("failed")
    })
}

impl Frame {
    fn key(&self) -> Result<(&str, &str)> {
        identifier(&self.topic)?;
        let field = |name: &str| -> Result<&str> {
            let s = self
                .event
                .get(name)
                .and_then(Value::as_str)
                .ok_or_else(|| Problem::Invalid(format!("expected string field {name}")))?;
            identifier(s)?;
            Ok(s)
        };
        Ok((field("task_id")?, field("event_id")?))
    }
}

#[derive(Debug, Serialize)]
pub struct Record {
    pub offset: i64,
    pub event: Value,
}

pub struct Store {
    pub(crate) db: Connection,
    dir: PathBuf,
    _lock: File,
    segment: u64,
    file: File,
    end: u64,
    pub recovered_frames: usize,
    pub payload_reads: std::cell::Cell<u64>,
    failed_write: bool,
}

fn segment_path(dir: &Path, id: u64) -> PathBuf {
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

const SCHEMA: &str = "
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
CREATE TABLE IF NOT EXISTS records (
    offset INTEGER PRIMARY KEY, topic TEXT NOT NULL, task TEXT NOT NULL, event_id TEXT NOT NULL,
    segment INTEGER NOT NULL, pos INTEGER NOT NULL, len INTEGER NOT NULL, UNIQUE(topic,task,event_id)
);
CREATE INDEX IF NOT EXISTS by_task ON records(topic,task,offset);
CREATE INDEX IF NOT EXISTS by_topic ON records(topic,offset);
CREATE TABLE IF NOT EXISTS floors(topic TEXT PRIMARY KEY, first_offset INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS cursors(topic TEXT, name TEXT, offset INTEGER NOT NULL, PRIMARY KEY(topic,name));
CREATE TABLE IF NOT EXISTS leases(topic TEXT, name TEXT, offset INTEGER, generation INTEGER NOT NULL,
    visible_at INTEGER NOT NULL, state TEXT NOT NULL, PRIMARY KEY(topic,name,offset));
";

fn index(tx: &Transaction<'_>, frame: &Frame, segment: u64, pos: u64, len: usize) -> Result<i64> {
    let (task, event_id) = frame.key()?;
    tx.prepare_cached(
        "INSERT INTO records(topic,task,event_id,segment,pos,len) VALUES(?,?,?,?,?,?)",
    )?
    .execute(params![
        frame.topic,
        task,
        event_id,
        i64::try_from(segment)?,
        i64::try_from(pos)?,
        i64::try_from(len)?
    ])?;
    let offset = tx.last_insert_rowid();
    if let Some(lease) = &frame.completion {
        lease.apply_completion(tx)?;
    }
    Ok(offset)
}

impl Store {
    pub fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join("writer.lock"))?;
        lock.try_lock()
            .context("data directory already in use or locking unsupported")?;
        let mut db = Connection::open(dir.join("index.sqlite"))?;
        db.execute_batch(SCHEMA)?;
        // The last indexed frame is the checkpoint. Its address and indexes commit together.
        let (segment, end): (i64, i64) = db
            .query_row(
                "SELECT segment,pos+8+len FROM records ORDER BY offset DESC LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?
            .unwrap_or((0, 0));
        let (mut segment, mut end) = (u64::try_from(segment)?, u64::try_from(end)?);
        let mut recovered_frames: usize = 0;
        let mut ids = Vec::new();
        for f in fs::read_dir(dir)? {
            let path = f?.path();
            if path.extension().and_then(|s| s.to_str()) == Some("seg") {
                let id: u64 = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .context("invalid segment name")?
                    .parse()?;
                ensure!(segment_path(dir, id) == path, "unexpected segment name");
                ids.push(id);
            }
        }
        ids.sort_unstable();
        for (expected, actual) in ids.iter().enumerate() {
            ensure!(
                *actual == expected as u64,
                "missing journal segment {expected}"
            );
        }
        if ids.is_empty() {
            ensure!(end == 0 && segment == 0, "missing indexed journal files");
            create_segment(dir, 0)?;
            ids.push(0);
        }
        let last = *ids.last().unwrap();
        ensure!(segment <= last, "indexed segment missing");
        loop {
            let file = File::options()
                .read(true)
                .write(true)
                .open(segment_path(dir, segment))?;
            let size = file.metadata()?.len();
            ensure!(size >= end, "segment ends before durable index checkpoint");
            if size > end {
                // A prior process may have died before flushing a complete suffix frame.
                // Make these bytes durable before committing their addresses.
                file.sync_data()?;
            }
            let mut reader = BufReader::new(&file);
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
                        (1..=MAX_FRAME).contains(&len),
                        "invalid frame length at {segment}:{end}"
                    );
                }
                if !complete_header || size - end - 8 < len as u64 {
                    ensure!(segment == last, "truncated sealed segment");
                    file.set_len(end)?;
                    file.sync_all()?;
                    break;
                }
                let mut body = vec![0; len];
                reader.read_exact(&mut body)?;
                ensure!(
                    crc32fast::hash(&body) == u32::from_le_bytes(header[4..].try_into()?),
                    "journal checksum mismatch at {segment}:{end}"
                );
                let frame: Frame = serde_json::from_slice(&body)?;
                index(&tx, &frame, segment, end, len)?;
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
        Ok(Self {
            db,
            dir: dir.to_owned(),
            _lock: lock,
            segment,
            file,
            end,
            recovered_frames,
            failed_write: false,
            payload_reads: Default::default(),
        })
    }

    pub(crate) fn ensure_writable(&self) -> Result<()> {
        ensure!(
            !self.failed_write,
            "restart required to recover an uncertain journal write"
        );
        Ok(())
    }

    pub fn append(&mut self, topic: &str, event: Value) -> Result<i64> {
        self.append_frame(Frame {
            topic: topic.into(),
            event,
            completion: None,
        })
    }

    pub fn complete(&mut self, lease: WorkLease, topic: &str, event: Value) -> Result<i64> {
        if !terminal_status(&event) {
            return Err(Problem::Invalid("expected a completed or failed status".into()).into());
        }
        self.append_frame(Frame {
            topic: topic.into(),
            event,
            completion: Some(lease),
        })
    }

    fn append_frame(&mut self, frame: Frame) -> Result<i64> {
        self.ensure_writable()?;
        let (task, id) = frame.key()?;
        let old: Option<i64> = self
            .db
            .query_row(
                "SELECT offset FROM records WHERE topic=? AND task=? AND event_id=?",
                params![frame.topic, task, id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(offset) = old {
            let saved = self.read_frame(offset)?;
            if saved.event != frame.event {
                return Err(Problem::Conflict(
                    "event_id already used with different content".into(),
                )
                .into());
            }
            if frame.completion.is_some() && frame.completion != saved.completion {
                return Err(Problem::Conflict(
                    "event_id already used outside this completion".into(),
                )
                .into());
            }
            return Ok(offset);
        }
        let body = serde_json::to_vec(&frame)?;
        if body.len() > MAX_FRAME {
            return Err(Problem::Invalid("frame exceeds 1 MiB".into()).into());
        }
        if let Some(lease) = &frame.completion {
            lease.validate_completion(&self.db, task)?;
        }
        // Any uncertain disk operation requires recovery before another append.
        self.failed_write = true;
        if self.end + 8 + body.len() as u64 > SEGMENT_BYTES {
            self.file = create_segment(&self.dir, self.segment + 1)?;
            self.segment += 1;
            self.end = 0;
        }
        self.file.write_all(&(body.len() as u32).to_le_bytes())?;
        self.file.write_all(&crc32fast::hash(&body).to_le_bytes())?;
        self.file.write_all(&body)?;
        self.file.sync_data()?;
        let tx = self.db.transaction()?;
        let offset = index(&tx, &frame, self.segment, self.end, body.len())?;
        tx.commit()?;
        self.end += 8 + body.len() as u64;
        self.failed_write = false;
        Ok(offset)
    }

    pub fn read(&self, offset: i64) -> Result<Record> {
        Ok(Record {
            offset,
            event: self.read_frame(offset)?.event,
        })
    }

    fn read_frame(&self, offset: i64) -> Result<Frame> {
        let (seg, pos, len): (i64, i64, i64) = self.db.query_row(
            "SELECT segment,pos,len FROM records WHERE offset=?",
            [offset],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        let (seg, pos, len) = (
            u64::try_from(seg)?,
            u64::try_from(pos)?,
            usize::try_from(len)?,
        );
        ensure!(len <= MAX_FRAME, "invalid size in address index");
        let mut f = File::open(segment_path(&self.dir, seg))?;
        f.seek(SeekFrom::Start(pos))?;
        let mut head = [0; 8];
        f.read_exact(&mut head)?;
        ensure!(
            u32::from_le_bytes(head[..4].try_into()?) as usize == len,
            "invalid indexed length"
        );
        let mut body = vec![0; len];
        f.read_exact(&mut body)?;
        ensure!(
            crc32fast::hash(&body) == u32::from_le_bytes(head[4..].try_into()?),
            "journal checksum mismatch"
        );
        self.payload_reads.set(self.payload_reads.get() + 1);
        Ok(serde_json::from_slice(&body)?)
    }

    pub fn floor(&self, topic: &str) -> Result<i64> {
        Ok(self
            .db
            .query_row(
                "SELECT first_offset FROM floors WHERE topic=?",
                [topic],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(1))
    }

    pub fn expire_before(&self, topic: &str, offset: i64) -> Result<()> {
        identifier(topic)?;
        let tail: i64 = self.db.query_row(
            "SELECT COALESCE(MAX(offset),0) FROM records WHERE topic=?",
            [topic],
            |r| r.get(0),
        )?;
        if !(1..=tail + 1).contains(&offset) {
            return Err(Problem::Invalid("floor outside topic bounds".into()).into());
        }
        self.db.execute("INSERT INTO floors VALUES(?,?) ON CONFLICT(topic) DO UPDATE SET first_offset=MAX(first_offset,excluded.first_offset)",params![topic,offset])?;
        Ok(())
    }

    fn gap(&self, topic: &str, task: &str, floor: i64) -> Result<Problem> {
        let earliest=self.db.query_row("SELECT event_id FROM records WHERE topic=? AND task=? AND offset>=? ORDER BY offset LIMIT 1",params![topic,task,floor],|r|r.get(0)).optional()?;
        Ok(Problem::Gap { earliest })
    }

    pub fn resolve(&self, topic: &str, task: &str, after: Option<&str>) -> Result<i64> {
        identifier(topic)?;
        identifier(task)?;
        let floor = self.floor(topic)?;
        let Some(id) = after else {
            return Ok(floor - 1);
        };
        let offset: Option<i64> = self
            .db
            .query_row(
                "SELECT offset FROM records WHERE topic=? AND task=? AND event_id=?",
                params![topic, task, id],
                |r| r.get(0),
            )
            .optional()?;
        match offset {
            Some(v) if v >= floor => Ok(v),
            _ => Err(self.gap(topic, task, floor)?.into()),
        }
    }

    pub fn page(&self, topic: &str, task: &str, after: i64) -> Result<Vec<Record>> {
        let floor = self.floor(topic)?;
        if after < floor - 1 {
            return Err(self.gap(topic, task, floor)?.into());
        }
        let mut stmt=self.db.prepare_cached("SELECT offset FROM records WHERE topic=? AND task=? AND offset>? ORDER BY offset LIMIT ?")?;
        let rows = stmt.query_map(params![topic, task, after, PAGE_SIZE], |r| r.get(0))?;
        rows.map(|r| self.read(r?)).collect()
    }

    pub fn latest(&self, topic: &str, task: &str) -> Result<Record> {
        let off=self.db.query_row("SELECT offset FROM records WHERE topic=? AND task=? AND offset>=? ORDER BY offset DESC LIMIT 1",params![topic,task,self.floor(topic)?],|r|r.get(0)).optional()?.ok_or(Problem::Missing)?;
        self.read(off)
    }
}
