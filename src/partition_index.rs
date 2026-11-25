use crate::{journal::Address, store::Frame, subscriptions::WorkLease};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar,
        mpsc::{Receiver, SyncSender, sync_channel},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

pub struct SplitIndex {
    next: i64,
    pub visible_next: i64,
    pub ready: Arc<Condvar>,
}

impl SplitIndex {
    pub fn reserve(&self) -> Result<i64> {
        self.next.checked_add(1).context("offset space exhausted")?;
        Ok(self.next)
    }

    pub fn reserve_next(&mut self) {
        self.next += 1;
    }
}

pub fn enabled(db: &Connection) -> Result<bool> {
    Ok(db.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='partitioned_index')",
        [],
        |row| row.get(0),
    )?)
}

pub fn enable(root: &Path, db: &Connection) -> Result<()> {
    fs::create_dir_all(root.join("partition-indexes"))?;
    File::open(root)?.sync_all()?;
    // An empty table is the durable format marker. New frames always carry their
    // assigned offset, so a missing partition index can replay its journal.
    db.execute_batch("CREATE TABLE IF NOT EXISTS partitioned_index (offset INTEGER PRIMARY KEY);")?;
    Ok(())
}

fn path(root: &Path, partition: u32) -> PathBuf {
    root.join("partition-indexes")
        .join(format!("{partition}.sqlite"))
}

pub fn open(root: &Path, partition: u32) -> Result<Connection> {
    let db = Connection::open(path(root, partition))?;
    let mode: String = db.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
    ensure!(mode == "wal", "partition index requires SQLite WAL mode");
    // The synced journal is authoritative. Recovery can rebuild WAL transactions
    // that were lost on power failure without changing their offsets or receipts.
    db.execute_batch(
        "PRAGMA synchronous=NORMAL;
         CREATE TABLE IF NOT EXISTS records (
             offset INTEGER PRIMARY KEY, topic TEXT NOT NULL, task TEXT NOT NULL, event_id TEXT NOT NULL,
             segment INTEGER NOT NULL, pos INTEGER NOT NULL, len INTEGER NOT NULL,
             partition INTEGER NOT NULL, completion TEXT
         );",
    )?;
    Ok(db)
}

pub fn legacy_checkpoint(db: &Connection, partition: u32) -> Result<(i64, i64)> {
    Ok(db
        .query_row(
            "SELECT segment,pos+8+len FROM main.records WHERE partition=? ORDER BY offset DESC LIMIT 1",
            [partition],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .unwrap_or((0, 0)))
}

struct IndexRow<T> {
    offset: i64,
    topic: T,
    task: T,
    event_id: T,
    partition: u32,
    address: Address,
    completion: Option<String>,
}

impl<'a> IndexRow<&'a str> {
    fn from_frame(frame: &'a Frame, partition: u32, address: Address) -> Result<Self> {
        let offset = frame
            .offset
            .context("partitioned frame is missing its offset")?;
        let (task, event_id) = frame.key()?;
        let completion = frame
            .completion
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        Ok(Self {
            offset,
            topic: &frame.topic,
            task,
            event_id,
            partition,
            address,
            completion,
        })
    }

    fn into_owned(self) -> IndexRow<String> {
        IndexRow {
            offset: self.offset,
            topic: self.topic.into(),
            task: self.task.into(),
            event_id: self.event_id.into(),
            partition: self.partition,
            address: self.address,
            completion: self.completion,
        }
    }
}

impl<T: AsRef<str>> IndexRow<T> {
    fn insert(&self, tx: &Transaction<'_>) -> Result<()> {
        let Address { segment, pos, len } = self.address;
        tx.prepare_cached(
            "INSERT INTO records(offset,topic,task,event_id,segment,pos,len,partition,completion)
             VALUES(?,?,?,?,?,?,?,?,?)",
        )?
        .execute(params![
            self.offset,
            self.topic.as_ref(),
            self.task.as_ref(),
            self.event_id.as_ref(),
            i64::try_from(segment)?,
            i64::try_from(pos)?,
            i64::try_from(len)?,
            self.partition,
            self.completion,
        ])?;
        Ok(())
    }
}

pub fn index_frame(
    tx: &Transaction<'_>,
    frame: &Frame,
    partition: u32,
    address: Address,
) -> Result<i64> {
    let row = IndexRow::from_frame(frame, partition, address)?;
    row.insert(tx)?;
    Ok(row.offset)
}

pub struct BackgroundIndex {
    sender: Option<SyncSender<IndexRow<String>>>,
    worker: Option<JoinHandle<()>>,
}

impl BackgroundIndex {
    pub fn start(db: Connection, size: usize) -> Result<Self> {
        let (sender, receiver) = sync_channel(size * 2);
        let worker = thread::Builder::new()
            .name("spooltail-index".into())
            .spawn(move || {
                if let Err(error) = flush_batches(db, receiver, size) {
                    eprintln!("partition index worker stopped: {error:#}");
                }
            })?;
        Ok(Self {
            sender: Some(sender),
            worker: Some(worker),
        })
    }

    pub fn enqueue(&self, frame: &Frame, partition: u32, address: Address) -> Result<()> {
        let row = IndexRow::from_frame(frame, partition, address)?.into_owned();
        self.sender
            .as_ref()
            .expect("index queue closed")
            .send(row)
            .map_err(|_| {
                anyhow::anyhow!("partition index worker stopped; restart to replay the journal")
            })
    }
}

impl Drop for BackgroundIndex {
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn flush_batches(
    mut db: Connection,
    receiver: Receiver<IndexRow<String>>,
    size: usize,
) -> Result<()> {
    let mut batch = Vec::with_capacity(size);
    while let Ok(row) = receiver.recv() {
        batch.push(row);
        let deadline = Instant::now() + Duration::from_millis(10);
        while batch.len() < size {
            match receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(row) => batch.push(row),
                Err(_) => break,
            }
        }
        // One FIFO producer per journal means every successful batch is a
        // contiguous journal prefix, even with sparse broker-wide offsets.
        let tx = db.transaction()?;
        for row in batch.drain(..) {
            row.insert(&tx)?;
        }
        tx.commit()?;
    }
    Ok(())
}

pub fn apply_completion(tx: &Transaction<'_>, offset: i64, lease: &WorkLease) -> Result<()> {
    // The receipt and settlement commit together in the coordinator. An indexed
    // completion can safely be replayed after a crash at either commit boundary.
    if tx.execute(
        "INSERT OR IGNORE INTO main.partitioned_index VALUES(?)",
        [offset],
    )? != 0
    {
        lease.apply_completion(tx)?;
    }
    Ok(())
}

pub fn restore_projection(
    root: &Path,
    db: &mut Connection,
    partitions: &[u32],
) -> Result<SplitIndex> {
    db.execute_batch(
        "CREATE TEMP TABLE records (
            offset INTEGER PRIMARY KEY, topic TEXT NOT NULL, task TEXT NOT NULL, event_id TEXT NOT NULL,
            segment INTEGER NOT NULL, pos INTEGER NOT NULL, len INTEGER NOT NULL,
            partition INTEGER NOT NULL, work_partition INTEGER NOT NULL
         );
         CREATE TEMP TABLE recover_completions(offset INTEGER PRIMARY KEY, receipt TEXT NOT NULL);
         INSERT INTO temp.records
         SELECT r.offset,r.topic,r.task,r.event_id,r.segment,r.pos,r.len,r.partition,
                COALESCE(t.partition,r.work_partition)
         FROM main.records r LEFT JOIN main.task_routes t ON t.task=r.task;",
    )?;
    for &partition in partitions {
        let path = path(root, partition);
        db.execute(
            "ATTACH DATABASE ? AS shard",
            [path.to_str().context("non-UTF-8 index path")?],
        )?;
        db.execute_batch(
            "INSERT INTO temp.records
             SELECT r.offset,r.topic,r.task,r.event_id,r.segment,r.pos,r.len,r.partition,
                    COALESCE(t.partition,r.partition)
             FROM shard.records r LEFT JOIN main.task_routes t ON t.task=r.task;
             INSERT INTO temp.recover_completions
             SELECT offset,completion FROM shard.records
             WHERE completion IS NOT NULL AND offset NOT IN (SELECT offset FROM main.partitioned_index);
             DETACH DATABASE shard;",
        )?;
    }
    db.execute_batch(
        "INSERT INTO temp.records
         SELECT r.offset,r.topic,r.task,r.event_id,r.segment,r.pos,r.len,r.partition,
                COALESCE(t.partition,r.partition)
         FROM main.retired_records r LEFT JOIN main.task_routes t ON t.task=r.task
         WHERE NOT EXISTS(SELECT 1 FROM temp.records WHERE offset=r.offset);
         CREATE UNIQUE INDEX temp.by_event ON records(topic,task,event_id);
         CREATE INDEX temp.by_task ON records(topic,task,offset);
         CREATE INDEX temp.by_topic ON records(topic,offset);
         CREATE INDEX temp.by_partition_topic ON records(partition,topic,offset);
         CREATE INDEX temp.by_task_route ON records(task);
         CREATE INDEX temp.by_work_partition ON records(work_partition,topic,offset);",
    )?;
    let tx = db.transaction()?;
    {
        let mut stmt =
            tx.prepare("SELECT offset,receipt FROM recover_completions ORDER BY offset")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            apply_completion(
                &tx,
                row.get(0)?,
                &serde_json::from_str(&row.get::<_, String>(1)?)?,
            )?;
        }
    }
    tx.commit()?;
    db.execute_batch("DROP TABLE recover_completions;")?;
    let high: i64 = db.query_row("SELECT COALESCE(MAX(offset),0) FROM records", [], |row| {
        row.get(0)
    })?;
    let next = high.checked_add(1).context("offset space exhausted")?;
    Ok(SplitIndex {
        next,
        visible_next: next,
        ready: Arc::new(Condvar::new()),
    })
}
