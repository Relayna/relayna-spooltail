use crate::{
    journal::{
        self, ACCEPTED_AT_FIELD_BYTES, Address, Journal, MAX_FRAME, OFFSET_FIELD_BYTES,
        partition_dir,
    },
    packing::{self, Manifest},
    partition_index::{self, SplitIndex},
    retention, retire,
    space::Space,
    subscriptions::{WorkLease, now},
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, hash_map::Entry},
    fs::{self, File, OpenOptions},
    io::{Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
};
use tokio::sync::Mutex;

pub const PAGE_SIZE: i64 = 64;
const HOT_WRITER_DEMAND: usize = 4;
const AUTOMATIC_MOVE_COOLDOWN_MS: i64 = 5_000;

#[derive(Debug, thiserror::Error)]
pub enum Problem {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Conflict(String),
    #[error("cursor unavailable")]
    Gap { earliest: Option<String> },
    #[error("insufficient disk headroom, retry after freeing space")]
    Capacity,
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
pub(crate) struct Frame {
    pub(crate) topic: String,
    pub(crate) event: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) completion: Option<WorkLease>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) offset: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) accepted_at_ms: Option<i64>,
}

pub fn terminal_status(event: &Value) -> bool {
    event["status"].as_str().is_some_and(|status| {
        status.eq_ignore_ascii_case("completed") || status.eq_ignore_ascii_case("failed")
    })
}

impl Frame {
    pub fn new(topic: String, event: Value, completion: Option<WorkLease>) -> Result<Self> {
        if completion.is_some() && !terminal_status(&event) {
            return Err(Problem::Invalid("expected a completed or failed status".into()).into());
        }
        let frame = Self {
            topic,
            event,
            completion,
            offset: None,
            accepted_at_ms: None,
        };
        frame.key()?;
        Ok(frame)
    }

    pub fn key(&self) -> Result<(&str, &str)> {
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

    pub fn into_record(self, offset: i64, partition: u32) -> Record {
        Record {
            offset,
            partition,
            event: self.event,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Record {
    pub offset: i64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub partition: u32,
    pub event: Value,
}

pub(crate) struct RecordLocation {
    pub offset: i64,
    pub partition: u32,
    pub address: Address,
}

pub(crate) fn is_zero(partition: &u32) -> bool {
    *partition == 0
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Position {
    pub offset: i64,
    #[serde(skip_serializing_if = "is_zero")]
    pub partition: u32,
}

pub enum Prepared {
    Duplicate(Position),
    Write(Vec<u8>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Placement {
    pub partition: u32,
    epoch: i64,
}

impl Placement {
    fn initial(partition: u32) -> Self {
        Self {
            partition,
            epoch: 0,
        }
    }
}

struct TaskRoute {
    placement: Placement,
    owner: Option<String>,
    moved_at: i64,
}

impl TaskRoute {
    fn initial(partition: u32) -> Self {
        Self {
            placement: Placement::initial(partition),
            owner: None,
            moved_at: 0,
        }
    }
}

struct Partition {
    journal: Arc<Mutex<Journal>>,
    demand: Arc<AtomicUsize>,
}

impl Partition {
    fn new(journal: Journal) -> Self {
        Self {
            journal: Arc::new(Mutex::new(journal)),
            demand: Arc::new(AtomicUsize::new(0)),
        }
    }
}

pub struct WriteTicket {
    pub placement: Placement,
    pub journal: Arc<Mutex<Journal>>,
    pub grouped: bool,
    demand: Option<Arc<AtomicUsize>>,
}

impl Drop for WriteTicket {
    fn drop(&mut self) {
        if let Some(demand) = &self.demand {
            demand.fetch_sub(1, Relaxed);
        }
    }
}

pub struct Store {
    pub(crate) db: Connection,
    dir: PathBuf,
    pub(crate) packed: Manifest,
    pub(crate) space: Space,
    pub(crate) audit_only: BTreeSet<String>,
    journals: BTreeMap<u32, Partition>,
    service_partitions: BTreeMap<String, Vec<u32>>,
    pub dynamic_placement: bool,
    pub automatic_moves: u64,
    index_batch_size: usize,
    reserved_partitions: BTreeSet<u32>,
    pub recovered_frames: usize,
    pub payload_reads: std::cell::Cell<u64>,
    failed_write: bool,
    pub(crate) split: Option<SplitIndex>,
    // Release the volume only after its journals and index workers have closed.
    _lock: File,
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

fn migrate(db: &Connection) -> Result<()> {
    let mut columns = db.prepare("SELECT name FROM pragma_table_info('records')")?;
    let existing = columns
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(columns);
    for (column, statement) in [
        (
            "partition",
            "ALTER TABLE records ADD COLUMN partition INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "work_partition",
            "ALTER TABLE records ADD COLUMN work_partition INTEGER NOT NULL DEFAULT 0",
        ),
    ] {
        if !existing.iter().any(|name| name == column) {
            db.execute(statement, [])?;
        }
    }
    db.execute_batch(
        "CREATE INDEX IF NOT EXISTS by_partition ON records(partition,offset);
         CREATE INDEX IF NOT EXISTS by_partition_topic ON records(partition,topic,offset);
         CREATE INDEX IF NOT EXISTS by_task_route ON records(task);
         CREATE INDEX IF NOT EXISTS by_work_partition ON records(work_partition,topic,offset);
         CREATE TABLE IF NOT EXISTS partitions (id INTEGER PRIMARY KEY);
         INSERT OR IGNORE INTO partitions VALUES(0);
         CREATE TABLE IF NOT EXISTS task_routes (task TEXT PRIMARY KEY, partition INTEGER NOT NULL);
         CREATE TABLE IF NOT EXISTS partitioned_groups (
            topic TEXT NOT NULL, name TEXT NOT NULL, initial_offset INTEGER NOT NULL,
            PRIMARY KEY(topic,name));
         CREATE TABLE IF NOT EXISTS partition_cursors (
            topic TEXT NOT NULL, name TEXT NOT NULL, partition INTEGER NOT NULL, offset INTEGER NOT NULL,
            PRIMARY KEY(topic,name,partition));
         CREATE TEMP TABLE appending (task TEXT PRIMARY KEY, partition INTEGER NOT NULL, work_offset INTEGER UNIQUE, reserved_bytes INTEGER NOT NULL);",
    )?;
    let mut columns = db.prepare("SELECT name FROM pragma_table_info('task_routes')")?;
    let existing = columns
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(columns);
    for (column, statement) in [
        (
            "epoch",
            "ALTER TABLE task_routes ADD COLUMN epoch INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "owner_service",
            "ALTER TABLE task_routes ADD COLUMN owner_service TEXT",
        ),
        (
            "moved_at",
            "ALTER TABLE task_routes ADD COLUMN moved_at INTEGER NOT NULL DEFAULT 0",
        ),
    ] {
        if !existing.iter().any(|name| name == column) {
            db.execute(statement, [])?;
        }
    }
    Ok(())
}

pub(crate) fn index(
    tx: &Transaction<'_>,
    frame: &Frame,
    partition: u32,
    address: Address,
) -> Result<i64> {
    let Address { segment, pos, len } = address;
    let (task, event_id) = frame.key()?;
    tx.prepare_cached(
        "INSERT INTO records(topic,task,event_id,segment,pos,len,partition,work_partition,offset)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?7,?8)",
    )?
    .execute(params![
        frame.topic,
        task,
        event_id,
        i64::try_from(segment)?,
        i64::try_from(pos)?,
        i64::try_from(len)?,
        partition,
        frame.offset,
    ])?;
    let offset = tx.last_insert_rowid();
    if let Some(lease) = &frame.completion {
        if frame.offset.is_some() {
            partition_index::apply_completion(tx, offset, lease)?;
        } else {
            lease.apply_completion(tx)?;
        }
    }
    Ok(offset)
}

impl Store {
    #[cfg(test)]
    pub fn open(dir: &Path) -> Result<Self> {
        Self::open_mode(dir, None)
    }

    pub fn open_mode(dir: &Path, requested: Option<&str>) -> Result<Self> {
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
        let enabled = partition_index::enabled(&db)?;
        let want_split = match requested {
            None => enabled,
            Some("partitioned") => true,
            Some("shared") if !enabled => false,
            Some("shared") => anyhow::bail!("this volume already uses partitioned indexes"),
            Some(_) => anyhow::bail!("SPOOLTAIL_INDEX_MODE must be shared or partitioned"),
        };
        if want_split {
            db.pragma_update(None, "temp_store", "MEMORY")?;
        }
        migrate(&db)?;
        retention::init(&db)?;
        let packed = packing::init(&db)?;
        let audit_only = retire::init(&db)?;
        let partitions = db
            .prepare("SELECT id FROM partitions ORDER BY id")?
            .query_map([], |row| row.get::<_, u32>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut journals = BTreeMap::new();
        let mut recovered_frames = 0;
        if !enabled {
            // Finish any pre-upgrade suffix using the legacy offset allocator.
            for &id in &partitions {
                let (journal, recovered) =
                    Journal::open(&partition_dir(dir, id), &mut db, id, &packed)?;
                journals.insert(id, Partition::new(journal));
                recovered_frames += recovered;
            }
        }
        let split = if want_split {
            partition_index::enable(dir, &db)?;
            journals.clear();
            for &id in &partitions {
                let (journal, recovered) = Journal::open_partitioned(
                    &partition_dir(dir, id),
                    partition_index::open(dir, id)?,
                    id,
                    partition_index::legacy_checkpoint(&db, id)?,
                    &packed,
                )?;
                journals.insert(id, Partition::new(journal));
                recovered_frames += recovered;
            }
            Some(partition_index::restore_projection(
                dir,
                &mut db,
                &partitions,
            )?)
        } else {
            None
        };
        packing::recover(dir, &partitions, &packed)?;
        Ok(Self {
            db,
            dir: dir.to_owned(),
            packed,
            space: Space::default(),
            audit_only,
            _lock: lock,
            journals,
            service_partitions: BTreeMap::new(),
            dynamic_placement: false,
            automatic_moves: 0,
            index_batch_size: 1,
            reserved_partitions: BTreeSet::new(),
            recovered_frames,
            failed_write: false,
            split,
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

    pub fn ensure_readable(&self) -> Result<()> {
        if self.split.is_some() {
            self.ensure_writable()?;
        }
        Ok(())
    }

    pub(crate) fn data_dir(&self) -> &Path {
        &self.dir
    }

    pub fn partition_ids(&self) -> Vec<u32> {
        self.journals.keys().copied().collect()
    }

    pub fn configure_index_batch_size(&mut self, size: usize) -> Result<()> {
        ensure!(
            (1..=1024).contains(&size),
            "SPOOLTAIL_INDEX_BATCH_SIZE must be 1..=1024"
        );
        if size > 1 {
            ensure!(
                self.split.is_some(),
                "background index batching requires partitioned mode"
            );
            for partition in self.journals.values() {
                partition
                    .journal
                    .try_lock()
                    .context("journal busy during index configuration")?
                    .batch_index(size)?;
            }
        }
        self.index_batch_size = size;
        Ok(())
    }

    pub fn groups_journal_sync(&self) -> bool {
        self.index_batch_size > 1
    }

    pub fn ensure_partition(&mut self, partition: u32) -> Result<()> {
        self.ensure_writable()?;
        if self.journals.contains_key(&partition) {
            return Ok(());
        }
        let dir = partition_dir(&self.dir, partition);
        let mut journal = Journal::create(&dir)?;
        if self.split.is_some() {
            journal.enable_index(partition_index::open(&self.dir, partition)?);
            if self.index_batch_size > 1 {
                journal.batch_index(self.index_batch_size)?;
            }
        }
        File::open(dir.parent().context("partition directory has no parent")?)?.sync_all()?;
        File::open(&self.dir)?.sync_all()?;
        self.db
            .execute("INSERT INTO partitions VALUES(?)", [partition])?;
        self.journals.insert(partition, Partition::new(journal));
        Ok(())
    }

    pub fn validate_partition(&self, partition: u32) -> Result<()> {
        if !self.journals.contains_key(&partition) {
            return Err(Problem::Missing.into());
        }
        Ok(())
    }

    fn existing_route(&self, task: &str) -> Result<Option<TaskRoute>> {
        let route = self
            .db
            .prepare_cached(
                "SELECT partition,epoch,owner_service,moved_at FROM task_routes WHERE task=?",
            )?
            .query_row([task], |row| {
                Ok(TaskRoute {
                    placement: Placement {
                        partition: row.get(0)?,
                        epoch: row.get(1)?,
                    },
                    owner: row.get(2)?,
                    moved_at: row.get(3)?,
                })
            })
            .optional()?;
        if route.is_some() {
            return Ok(route);
        }
        let appending = self
            .db
            .query_row(
                "SELECT partition FROM appending WHERE task=?",
                [task],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(partition) = appending {
            return Ok(Some(TaskRoute::initial(partition)));
        }
        let legacy: bool = self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM records WHERE task=?)",
            [task],
            |row| row.get(0),
        )?;
        Ok(legacy.then(|| TaskRoute::initial(0)))
    }

    pub fn task_partition(&self, task: &str) -> Result<u32> {
        identifier(task)?;
        self.existing_route(task)?
            .map(|route| route.placement.partition)
            .ok_or_else(|| Problem::Missing.into())
    }

    pub fn is_appending(&self, task: &str) -> Result<bool> {
        Ok(self
            .db
            .prepare_cached("SELECT EXISTS(SELECT 1 FROM appending WHERE task=?)")?
            .query_row([task], |row| row.get(0))?)
    }

    pub fn move_task(&mut self, task: &str, from: u32, to: u32) -> Result<()> {
        self.ensure_writable()?;
        identifier(task)?;
        self.validate_partition(to)?;
        let current = self.existing_route(task)?.ok_or(Problem::Missing)?;
        if current.placement.partition == to {
            return Ok(());
        }
        if current.placement.partition != from {
            return Err(Problem::Conflict(format!(
                "task currently belongs to partition {}",
                current.placement.partition
            ))
            .into());
        }
        if self.is_appending(task)? {
            return Err(
                Problem::Conflict("task has a publish in progress; retry the move".into()).into(),
            );
        }
        self.update_route(
            task,
            &current,
            to,
            self.inferred_owner(&current),
            current.moved_at,
        )?;
        Ok(())
    }

    fn update_route(
        &mut self,
        task: &str,
        current: &TaskRoute,
        to: u32,
        owner: Option<String>,
        timestamp: i64,
    ) -> Result<Placement> {
        let epoch = current
            .placement
            .epoch
            .checked_add(1)
            .context("task routing epoch exhausted")?;
        // Placement alone changes. The shared index holds the original record addresses,
        // work-group state, and active lease identities across the cutover.
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO task_routes(task,partition,epoch,owner_service,moved_at) VALUES(?,?,?,?,?)
             ON CONFLICT(task) DO UPDATE SET partition=excluded.partition,epoch=excluded.epoch,
                owner_service=excluded.owner_service,moved_at=excluded.moved_at",
            params![task, to, epoch, owner, timestamp],
        )?;
        tx.execute(
            "UPDATE records SET work_partition=? WHERE task=?",
            params![to, task],
        )?;
        let result = tx.commit();
        if result.is_err()
            && let Some(split) = &self.split
        {
            self.failed_write = true;
            split.ready.notify_all();
        }
        result?;
        Ok(Placement {
            partition: to,
            epoch,
        })
    }

    pub fn configure_service_partitions(
        &mut self,
        mut rules: BTreeMap<String, Vec<u32>>,
    ) -> Result<()> {
        let mut reserved = BTreeSet::new();
        for (service, pool) in &mut rules {
            identifier(service).context("invalid service name in SPOOLTAIL_SERVICE_PARTITIONS")?;
            ensure!(
                !pool.is_empty(),
                "service {service} has an empty partition pool"
            );
            pool.sort_unstable();
            for &partition in pool.iter() {
                ensure!(
                    reserved.insert(partition),
                    "partition {partition} appears in more than one service rule or more than once"
                );
            }
        }
        for &partition in &reserved {
            self.ensure_partition(partition)
                .with_context(|| format!("prepare service partition {partition}"))?;
        }
        self.service_partitions = rules;
        self.reserved_partitions = reserved;
        Ok(())
    }

    fn inferred_owner(&self, route: &TaskRoute) -> Option<String> {
        route.owner.clone().or_else(|| {
            if !self
                .reserved_partitions
                .contains(&route.placement.partition)
            {
                return None;
            }
            self.service_partitions
                .iter()
                .find(|(_, pool)| pool.binary_search(&route.placement.partition).is_ok())
                .map(|(owner, _)| owner.clone())
        })
    }

    fn eligible_partitions(&self, owner: Option<&str>) -> Vec<u32> {
        if let Some(pool) = owner.and_then(|owner| self.service_partitions.get(owner)) {
            return pool.clone();
        }
        self.journals
            .keys()
            .copied()
            .filter(|id| !self.reserved_partitions.contains(id))
            .collect()
    }

    fn demand(&self, partition: u32) -> usize {
        self.journals[&partition].demand.load(Relaxed)
    }

    fn choose_partition(&self, task: &str, pool: &[u32]) -> Option<u32> {
        if pool.is_empty() {
            return None;
        }
        let slot = crc32fast::hash(task.as_bytes()) as usize % pool.len();
        if self.dynamic_placement {
            pool[slot..]
                .iter()
                .chain(&pool[..slot])
                .copied()
                .min_by_key(|&id| self.demand(id))
        } else {
            Some(pool[slot])
        }
    }

    fn consider_move(&mut self, task: &str, route: TaskRoute) -> Result<Placement> {
        let source = route.placement.partition;
        let demand = self.demand(source);
        if !self.dynamic_placement || demand < HOT_WRITER_DEMAND {
            return Ok(route.placement);
        }
        let timestamp = now()?;
        if timestamp.saturating_sub(route.moved_at) < AUTOMATIC_MOVE_COOLDOWN_MS {
            return Ok(route.placement);
        }
        let owner = self.inferred_owner(&route);
        let pool = self.eligible_partitions(owner.as_deref());
        // An explicit administrative move outside the pool takes precedence.
        if pool.binary_search(&source).is_err() {
            return Ok(route.placement);
        }
        let Some(destination) = self.choose_partition(task, &pool) else {
            return Ok(route.placement);
        };
        if self.demand(destination) > demand / 2 || self.is_appending(task)? {
            return Ok(route.placement);
        }
        let placement = self.update_route(task, &route, destination, owner, timestamp)?;
        self.automatic_moves += 1;
        Ok(placement)
    }

    pub fn route_for(&mut self, task: &str, owner_service: Option<&str>) -> Result<Placement> {
        self.ensure_writable()?;
        if self.journals.len() == 1 && self.reserved_partitions.is_empty() {
            return Ok(Placement::initial(0));
        }
        if let Some(route) = self.existing_route(task)? {
            return self.consider_move(task, route);
        }
        let owner = owner_service.filter(|owner| self.service_partitions.contains_key(*owner));
        let pool = self.eligible_partitions(owner);
        let partition = self.choose_partition(task, &pool).ok_or_else(|| {
            Problem::Conflict(
                "no general partition is available for new tasks without a service rule".into(),
            )
        })?;
        self.db.execute(
            "INSERT INTO task_routes(task,partition,owner_service) VALUES(?,?,?)",
            params![task, partition, owner],
        )?;
        Ok(Placement::initial(partition))
    }

    #[cfg(test)]
    pub fn partition_for(&mut self, task: &str, owner_service: Option<&str>) -> Result<u32> {
        Ok(self.route_for(task, owner_service)?.partition)
    }

    pub fn journal(&self, partition: u32) -> Result<Arc<Mutex<Journal>>> {
        self.journals
            .get(&partition)
            .map(|entry| entry.journal.clone())
            .ok_or_else(|| Problem::Missing.into())
    }

    pub fn writer_ticket(
        &mut self,
        task: &str,
        owner_service: Option<&str>,
    ) -> Result<WriteTicket> {
        let placement = self.route_for(task, owner_service)?;
        let entry = &self.journals[&placement.partition];
        let demand = self.dynamic_placement.then(|| {
            entry.demand.fetch_add(1, Relaxed);
            entry.demand.clone()
        });
        Ok(WriteTicket {
            placement,
            journal: entry.journal.clone(),
            grouped: self.groups_journal_sync(),
            demand,
        })
    }

    pub fn existing_event(&self, frame: &Frame) -> Result<Option<i64>> {
        let (task, id) = frame.key()?;
        Ok(self
            .db
            .prepare_cached("SELECT offset FROM records WHERE topic=? AND task=? AND event_id=?")?
            .query_row(params![frame.topic, task, id], |row| row.get(0))
            .optional()?)
    }

    pub fn prepare_append(&mut self, frame: &mut Frame, partition: u32) -> Result<Prepared> {
        self.ensure_writable()?;
        let task = frame.key()?.0.to_owned();
        if let Some(offset) = self.existing_event(frame)? {
            if let Some(position) = self.retired_duplicate(offset, frame)? {
                return Ok(Prepared::Duplicate(position));
            }
            let (partition, saved) = self.read_frame(offset)?;
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
            return Ok(Prepared::Duplicate(Position { partition, offset }));
        }
        let next = self.split.as_ref().map(SplitIndex::reserve).transpose()?;
        frame.offset = next;
        let accepted_at = now()?;
        frame.accepted_at_ms = Some(accepted_at);
        let body = serde_json::to_vec(&frame)?;
        let offset_bytes = next.map_or(0, |offset| {
            OFFSET_FIELD_BYTES + offset.ilog10() as usize + 1
        });
        let time_bytes =
            ACCEPTED_AT_FIELD_BYTES + accepted_at.checked_ilog10().unwrap_or(0) as usize + 1;
        if body.len() > MAX_FRAME + offset_bytes + time_bytes {
            return Err(Problem::Invalid("frame exceeds 1 MiB".into()).into());
        }
        let work_offset = frame.completion.as_ref().map(|lease| lease.offset);
        if let Some(lease) = &frame.completion {
            lease.validate_completion(&self.db, &task)?;
        }
        let reserved = self.admit_disk_write(body.len() as u64 + 8, work_offset.is_some())?;
        self.db.execute(
            "INSERT INTO appending VALUES(?,?,?,?)",
            params![task, partition, work_offset, i64::try_from(reserved)?],
        )?;
        if let Some(split) = &mut self.split {
            split.reserve_next();
        }
        Ok(Prepared::Write(body))
    }

    pub fn can_publish(&self, offset: i64) -> bool {
        self.failed_write
            || self
                .split
                .as_ref()
                .is_some_and(|split| split.visible_next == offset)
    }

    pub fn finish_append(
        &mut self,
        frame: &Frame,
        partition: u32,
        write: Result<Address>,
    ) -> Result<Position> {
        let result = write.and_then(|address| {
            if let Some(offset) = frame.offset {
                self.ensure_writable()?;
                ensure!(
                    self.can_publish(offset),
                    "partition record is out of publication order"
                );
            }
            let tx = self.db.transaction()?;
            let offset = index(&tx, frame, partition, address)?;
            tx.commit()?;
            Ok(Position { partition, offset })
        });
        let cleanup = self
            .db
            .execute("DELETE FROM appending WHERE task=?", [frame.key()?.0]);
        if result.is_err() || cleanup.is_err() {
            // Recover every uncertain suffix before allowing another mutation.
            self.failed_write = true;
        }
        let position = result?;
        cleanup?;
        if let Some(split) = &mut self.split {
            split.visible_next += 1;
        }
        Ok(position)
    }

    #[cfg(test)]
    pub fn append(&mut self, topic: &str, event: Value) -> Result<i64> {
        self.write_test_frame(Frame::new(topic.into(), event, None)?)
    }

    #[cfg(test)]
    pub fn complete(&mut self, lease: WorkLease, topic: &str, event: Value) -> Result<i64> {
        self.write_test_frame(Frame::new(topic.into(), event, Some(lease))?)
    }

    #[cfg(test)]
    fn write_test_frame(&mut self, mut frame: Frame) -> Result<i64> {
        let partition = self.partition_for(frame.key()?.0, None)?;
        match self.prepare_append(&mut frame, partition)? {
            Prepared::Duplicate(position) => Ok(position.offset),
            Prepared::Write(body) => {
                let write = self
                    .journal(partition)?
                    .try_lock()
                    .context("journal busy during test setup")?
                    .append(&frame, partition, &body);
                Ok(self.finish_append(&frame, partition, write)?.offset)
            }
        }
    }

    pub fn read(&self, offset: i64) -> Result<Record> {
        let (partition, frame) = self.read_frame(offset)?;
        Ok(frame.into_record(offset, partition))
    }

    pub(crate) fn read_indexed_page(&self, locations: Vec<RecordLocation>) -> Result<Vec<Record>> {
        let mut records = Vec::with_capacity(locations.len());
        self.visit_indexed_page(locations, |location, frame| {
            records.push(frame.into_record(location.offset, location.partition));
            true
        })?;
        Ok(records)
    }

    pub(crate) fn visit_indexed_page(
        &self,
        locations: Vec<RecordLocation>,
        mut visit: impl FnMut(RecordLocation, Frame) -> bool,
    ) -> Result<()> {
        // Reuse readers only within this bounded page. Segment IDs are local to a partition.
        let mut segments = HashMap::new();
        for location in locations {
            let RecordLocation {
                partition, address, ..
            } = location;
            let (reader, position) = match segments.entry((partition, address.segment)) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let file = journal::open_segment(
                        &partition_dir(&self.dir, partition),
                        address.segment,
                        self.packed.get(&(partition, address.segment)).copied(),
                    )?;
                    entry.insert((file, 0))
                }
            };
            if *position != address.pos {
                reader.seek(SeekFrom::Start(address.pos))?;
            }
            let frame = journal::read_frame(reader, address.len)?;
            *position = address.pos + 8 + address.len as u64;
            self.payload_reads.set(self.payload_reads.get() + 1);
            if !visit(location, frame) {
                break;
            }
        }
        Ok(())
    }

    fn read_frame(&self, offset: i64) -> Result<(u32, Frame)> {
        let (partition, segment, pos, len): (u32, i64, i64, i64) = self.db.query_row(
            "SELECT partition,segment,pos,len FROM records WHERE offset=?",
            [offset],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?;
        let address = Address {
            segment: u64::try_from(segment)?,
            pos: u64::try_from(pos)?,
            len: usize::try_from(len)?,
        };
        let frame = journal::read(
            &partition_dir(&self.dir, partition),
            address,
            self.packed.get(&(partition, address.segment)).copied(),
        )?;
        self.payload_reads.set(self.payload_reads.get() + 1);
        Ok((partition, frame))
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
        if self.retention_policy(topic)?.is_some() {
            return Err(Problem::Invalid("topic uses automatic archive retention".into()).into());
        }
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
