//! Payload retirement for explicitly audit-only topics.
use crate::{
    Broker, Frame, Problem, Store, identifier,
    journal::{Address, partition_dir, segment_path},
    packing::{packed_path, stop_at},
    store::{Position, RecordLocation},
    subscriptions::{WorkLease, now},
};
use anyhow::{Result, ensure};
use ring::digest::{SHA256, digest};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    time::Duration,
};

pub fn init(db: &Connection) -> Result<BTreeSet<String>> {
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS audit_only_topics (topic TEXT PRIMARY KEY);
         CREATE TABLE IF NOT EXISTS retired_records (
            offset INTEGER PRIMARY KEY, topic TEXT NOT NULL, task TEXT NOT NULL,
            event_id TEXT NOT NULL, partition INTEGER NOT NULL, segment INTEGER NOT NULL,
            pos INTEGER NOT NULL, len INTEGER NOT NULL, fingerprint BLOB NOT NULL, completion TEXT
         );",
    )?;
    Ok(db
        .prepare("SELECT topic FROM audit_only_topics")?
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?)
}

pub fn configure(store: &mut Store) -> Result<bool> {
    let configured = match std::env::var("SPOOLTAIL_RETIRE_AUDIT_TOPICS") {
        Ok(value) => serde_json::from_str::<BTreeSet<String>>(&value)?,
        Err(std::env::VarError::NotPresent) => BTreeSet::new(),
        Err(error) => return Err(error.into()),
    };
    store.enable_retirement(configured)?;
    Ok(!store.audit_only.is_empty())
}

fn fingerprint(event: &Value) -> Result<ring::digest::Digest> {
    fn normalize(value: &mut Value) {
        match value {
            Value::Number(n) if n.is_f64() && n.as_f64() == Some(0.0) => {
                *n = serde_json::Number::from_f64(0.0).expect("finite zero");
            }
            Value::Array(items) => items.iter_mut().for_each(normalize),
            Value::Object(entries) => entries.values_mut().for_each(normalize),
            _ => {}
        }
    }
    let mut event = event.clone();
    normalize(&mut event);
    Ok(digest(&SHA256, &serde_json::to_vec(&event)?))
}

pub fn remove_retired(dir: &Path, segment: u64) -> Result<()> {
    let mut changed = false;
    for path in [packed_path(dir, segment), segment_path(dir, segment)] {
        match fs::remove_file(path) {
            Ok(()) => changed = true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    if changed {
        stop_at("retired_unlinked");
    }
    // Also sync a retry after an uncertain earlier directory sync.
    fs::File::open(dir)?.sync_all()?;
    Ok(())
}

pub async fn run(broker: Broker) {
    let mut cleanup = None;
    loop {
        let result = async {
            if cleanup.is_none() {
                cleanup = broker.run(|store| store.retire_one(now()?)).await?;
            }
            if let Some((dir, segment)) = cleanup.clone() {
                tokio::task::spawn_blocking(move || {
                    stop_at("retirement_committed");
                    remove_retired(&dir, segment)
                })
                .await??;
                cleanup = None;
                Ok::<_, anyhow::Error>(true)
            } else {
                Ok(false)
            }
        }
        .await;
        let seconds = match result {
            Ok(true) => 1,
            Ok(false) => 5,
            Err(error) => {
                eprintln!("packed journal retirement failed: {error:#}");
                5
            }
        };
        tokio::time::sleep(Duration::from_secs(seconds)).await;
    }
}

impl Store {
    pub fn enable_retirement(&mut self, topics: BTreeSet<String>) -> Result<()> {
        if self.audit_only.is_empty() && topics.is_empty() {
            return Ok(());
        }
        self.ensure_segment_index()?;
        let tx = self.db.transaction()?;
        for topic in &topics {
            identifier(topic)?;
            let has_policy = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM retention_policies WHERE topic=?)",
                [topic],
                |r| r.get::<_, bool>(0),
            )?;
            ensure!(
                has_policy,
                "audit-only topic {topic} requires a retention policy"
            );
            let has_work = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM leases WHERE topic=?)",
                [topic],
                |r| r.get::<_, bool>(0),
            )?;
            ensure!(
                !has_work,
                "audit-only topic {topic} already has work leases"
            );
            tx.execute("INSERT OR IGNORE INTO audit_only_topics VALUES(?)", [topic])?;
        }
        tx.commit()?;
        self.audit_only.extend(topics);
        Ok(())
    }

    pub fn retired_duplicate(&self, offset: i64, frame: &Frame) -> Result<Option<Position>> {
        let receipt: Option<(u32, Vec<u8>, Option<String>)> = self
            .db
            .query_row(
                "SELECT partition,fingerprint,completion FROM retired_records WHERE offset=?",
                [offset],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((partition, saved, receipt)) = receipt else {
            return Ok(None);
        };
        if saved.as_slice() != fingerprint(&frame.event)?.as_ref() {
            return Err(
                Problem::Conflict("event_id already used with different content".into()).into(),
            );
        }
        if frame.completion.is_some() {
            let existing = receipt
                .as_deref()
                .map(serde_json::from_str::<WorkLease>)
                .transpose()?;
            if frame.completion != existing {
                return Err(Problem::Conflict(
                    "event_id already used outside this completion".into(),
                )
                .into());
            }
        }
        Ok(Some(Position { offset, partition }))
    }

    pub fn retire_one(&mut self, timestamp: i64) -> Result<Option<(std::path::PathBuf, u64)>> {
        // A segment must have only expired, nonclaimable rows. A row still present
        // in the audit index pins the whole file, even after its age limit.
        let candidates = self
            .db
            .prepare_cached(
                "SELECT s.partition,s.segment,s.raw_len FROM packed_segments AS s
             WHERE s.raw_len>0 AND NOT EXISTS (
               SELECT 1 FROM records AS r
               LEFT JOIN audit_only_topics AS t ON t.topic=r.topic
               LEFT JOIN retention_policies AS p ON p.topic=r.topic
               LEFT JOIN floors AS f ON f.topic=r.topic
               LEFT JOIN archive_records AS a ON a.offset=r.offset
               WHERE r.partition=s.partition AND r.segment=s.segment
                 AND (t.topic IS NULL OR p.paused IS NOT 0 OR f.first_offset IS NULL
                      OR r.offset>=f.first_offset OR a.offset IS NOT NULL
                      OR EXISTS(SELECT 1 FROM leases AS l WHERE l.topic=r.topic))
             ) ORDER BY s.partition,s.segment",
            )?
            .query_map([], |row| {
                Ok((
                    row.get::<_, u32>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        for (partition, segment, raw_len) in candidates {
            let segment = u64::try_from(segment)?;
            let dir = partition_dir(self.data_dir(), partition);
            if segment_path(&dir, segment).try_exists()? {
                continue;
            }
            if self.retire_segment(partition, segment, u64::try_from(raw_len)?, timestamp)? {
                return Ok(Some((dir, segment)));
            }
        }
        Ok(None)
    }

    fn retire_segment(
        &mut self,
        partition: u32,
        segment: u64,
        raw_len: u64,
        timestamp: i64,
    ) -> Result<bool> {
        let locations = {
            let mut stmt = self.db.prepare_cached(
                "SELECT offset,pos,len FROM records WHERE partition=? AND segment=? ORDER BY pos",
            )?;
            let mut rows = stmt.query(params![partition, i64::try_from(segment)?])?;
            let mut locations = Vec::new();
            let mut end = 0;
            while let Some(r) = rows.next()? {
                let pos = u64::try_from(r.get::<_, i64>(1)?)?;
                let len = usize::try_from(r.get::<_, i64>(2)?)?;
                if pos != end {
                    return Ok(false);
                }
                end += len as u64 + 8;
                locations.push(RecordLocation {
                    offset: r.get(0)?,
                    partition,
                    address: Address { segment, pos, len },
                });
            }
            // Sealing can precede publication of the last frames on grouped writers.
            if end != raw_len {
                return Ok(false);
            }
            locations
        };
        let mut receipts = Vec::with_capacity(locations.len());
        let mut policies = BTreeMap::new();
        let mut result: Result<()> = Ok(());
        self.visit_indexed_page(locations, |location, frame| {
            result = (|| {
                let policy = match policies.entry(frame.topic.clone()) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => entry.insert(
                        self.retention_policy(&frame.topic)?
                            .ok_or_else(|| anyhow::anyhow!("missing retention policy"))?,
                    ),
                };
                if !policy.has_expired(frame.accepted_at_ms, timestamp) {
                    return Ok(());
                }
                let (task, id) = frame.key()?;
                receipts.push((
                    location,
                    frame.topic.clone(),
                    task.to_owned(),
                    id.to_owned(),
                    fingerprint(&frame.event)?,
                    frame
                        .completion
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()?,
                ));
                Ok(())
            })();
            result.is_ok()
        })?;
        result?;
        let receipt_bytes: u64 = receipts
            .iter()
            .map(|(location, ..)| location.address.len as u64 + 8)
            .sum();
        if receipt_bytes != raw_len {
            return Ok(false);
        }
        let tx = self.db.transaction()?;
        {
            let mut insert =
                tx.prepare_cached("INSERT INTO retired_records VALUES (?,?,?,?,?,?,?,?,?,?)")?;
            for (location, topic, task, id, hash, completion) in receipts {
                insert.execute(params![
                    location.offset,
                    topic,
                    task,
                    id,
                    partition,
                    i64::try_from(segment)?,
                    i64::try_from(location.address.pos)?,
                    i64::try_from(location.address.len)?,
                    hash.as_ref(),
                    completion,
                ])?;
            }
        }
        // Zero-length manifest entries are durable tombstones. Segment IDs are never reused.
        tx.execute(
            "UPDATE packed_segments SET raw_len=0 WHERE partition=? AND segment=?",
            params![partition, i64::try_from(segment)?],
        )?;
        tx.commit()?;
        self.packed.insert((partition, segment), 0);
        Ok(true)
    }
}
