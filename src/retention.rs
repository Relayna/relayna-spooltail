use crate::{
    Broker,
    journal::{Address, MAX_FRAME},
    store::{Problem, Record, RecordLocation, Store, identifier},
    subscriptions::{MAX_STREAM_BATCH_BYTES, now},
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Deserialize;
use std::{collections::BTreeMap, time::Duration};

const ARCHIVE_BLOCK_BYTES: usize = 64 * 1024;
const MAX_ARCHIVE_RAW_BYTES: usize = MAX_FRAME + 1024;
const ARCHIVE_BATCH_RECORDS: u32 = 64;
const ARCHIVE_PRUNE_RECORDS: u32 = 256;
const ZSTD_LEVEL: i32 = 1;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS retention_policies (
    topic TEXT PRIMARY KEY, hot_ms INTEGER NOT NULL, retain_ms INTEGER NOT NULL,
    activated_ms INTEGER NOT NULL, paused INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS archive_blocks (
    id INTEGER PRIMARY KEY, raw_len INTEGER NOT NULL, checksum INTEGER NOT NULL,
    compressed BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS archive_records (
    offset INTEGER PRIMARY KEY, topic TEXT NOT NULL, task TEXT NOT NULL, event_id TEXT NOT NULL,
    accepted_at_ms INTEGER NOT NULL, len INTEGER NOT NULL, block_id INTEGER NOT NULL,
    UNIQUE(topic,task,event_id)
);
CREATE INDEX IF NOT EXISTS archive_by_topic ON archive_records(topic,offset);
CREATE INDEX IF NOT EXISTS archive_by_task ON archive_records(topic,task,offset);
CREATE INDEX IF NOT EXISTS archive_by_block ON archive_records(block_id);
";

pub(crate) fn init(db: &Connection) -> Result<()> {
    db.execute_batch(SCHEMA)?;
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetentionRule {
    #[serde(deserialize_with = "duration_ms")]
    hot_for: i64,
    #[serde(deserialize_with = "duration_ms")]
    retain_for: i64,
    #[serde(default)]
    paused: bool,
}

fn duration_ms<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
    let value = String::deserialize(deserializer)?;
    let units = match value.as_bytes().last() {
        Some(b's') => 1_000,
        Some(b'm') => 60_000,
        Some(b'h') => 3_600_000,
        Some(b'd') => 86_400_000,
        _ => {
            return Err(serde::de::Error::custom(
                "expected an integer followed by s, m, h, or d",
            ));
        }
    };
    let digits = &value[..value.len() - 1];
    digits
        .bytes()
        .all(|byte| byte.is_ascii_digit())
        .then(|| digits.parse::<i64>().ok())
        .flatten()
        .and_then(|count| count.checked_mul(units))
        .filter(|&ms| ms > 0)
        .ok_or_else(|| serde::de::Error::custom("expected a positive duration in range"))
}

fn parse_rules(value: &str) -> Result<BTreeMap<String, RetentionRule>> {
    let rules: BTreeMap<String, RetentionRule> = serde_json::from_str(value)?;
    for (topic, rule) in &rules {
        identifier(topic)?;
        ensure!(
            rule.retain_for >= rule.hot_for,
            "retain_for must be at least hot_for for topic {topic}"
        );
    }
    Ok(rules)
}

pub(crate) fn configure(store: &mut Store) -> Result<Vec<String>> {
    match std::env::var("SPOOLTAIL_RETENTION") {
        Ok(value) => store.configure_retention(
            parse_rules(&value).context("invalid SPOOLTAIL_RETENTION")?,
            now()?,
        )?,
        Err(std::env::VarError::NotPresent) => {}
        Err(error) => return Err(error).context("read SPOOLTAIL_RETENTION"),
    }
    store.retention_topics()
}

pub(crate) struct RetentionPolicy {
    hot_ms: i64,
    retain_ms: i64,
    activated_ms: i64,
}

impl RetentionPolicy {
    pub fn has_expired(&self, accepted_at_ms: Option<i64>, timestamp: i64) -> bool {
        accepted_at_ms.unwrap_or(self.activated_ms) <= timestamp.saturating_sub(self.retain_ms)
    }
}

struct ArchiveInput {
    segment: u64,
    already_packed: bool,
    accepted_at_ms: i64,
    len: usize,
    record: Record,
}

struct ArchiveCandidate {
    topic: String,
    floor: i64,
    inputs: Vec<ArchiveInput>,
}

struct ArchiveBlock {
    raw_len: u32,
    checksum: u32,
    compressed: Vec<u8>,
}

impl ArchiveBlock {
    fn encode(inputs: &[ArchiveInput]) -> Result<Self> {
        let records: Vec<_> = inputs
            .iter()
            .filter(|input| !input.already_packed)
            .map(|input| &input.record)
            .collect();
        let raw = serde_json::to_vec(&records)?;
        ensure!(
            raw.len() <= MAX_ARCHIVE_RAW_BYTES,
            "archive block exceeds size limit"
        );
        Ok(Self {
            raw_len: u32::try_from(raw.len())?,
            checksum: crc32fast::hash(&raw),
            compressed: zstd::bulk::compress(&raw, ZSTD_LEVEL)?,
        })
    }

    fn decode(&self) -> Result<Vec<Record>> {
        let raw_len = usize::try_from(self.raw_len)?;
        ensure!(
            (1..=MAX_ARCHIVE_RAW_BYTES).contains(&raw_len),
            "invalid archive block length"
        );
        let raw = zstd::bulk::decompress(&self.compressed, raw_len)
            .context("decode Zstd archive block")?;
        ensure!(
            raw.len() == raw_len && crc32fast::hash(&raw) == self.checksum,
            "archive block checksum mismatch"
        );
        Ok(serde_json::from_slice(&raw)?)
    }
}

pub(crate) async fn run(broker: Broker, topics: Vec<String>) {
    loop {
        let mut progress = false;
        for topic in &topics {
            match sweep(&broker, topic).await {
                Ok(changed) => progress |= changed,
                Err(error) => eprintln!("retention sweep failed for {topic}: {error:#}"),
            }
        }
        let delay = if progress {
            Duration::from_millis(10)
        } else {
            Duration::from_secs(1)
        };
        tokio::time::sleep(delay).await;
    }
}

async fn sweep(broker: &Broker, topic: &str) -> Result<bool> {
    let topic = topic.to_owned();
    let timestamp = now()?;
    let (candidate, pruned) = broker
        .run(move |store| {
            let candidate = store.archive_candidate(&topic, timestamp)?;
            let pruned = store.prune_archive(&topic, timestamp)?;
            Ok((candidate, pruned))
        })
        .await?;
    if let Some(candidate) = candidate {
        let (candidate, block) = tokio::task::spawn_blocking(move || {
            let block = candidate
                .inputs
                .iter()
                .any(|input| !input.already_packed)
                .then(|| ArchiveBlock::encode(&candidate.inputs))
                .transpose()?;
            Ok::<_, anyhow::Error>((candidate, block))
        })
        .await??;
        broker
            .run(move |store| store.publish_archive(candidate, block))
            .await?;
        return Ok(true);
    }
    Ok(pruned)
}

impl Store {
    fn configure_retention(
        &mut self,
        rules: BTreeMap<String, RetentionRule>,
        timestamp: i64,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        for (topic, rule) in rules {
            tx.execute(
                "INSERT INTO retention_policies VALUES (?,?,?,?,?)
                 ON CONFLICT(topic) DO UPDATE SET
                 hot_ms=excluded.hot_ms, retain_ms=excluded.retain_ms, paused=excluded.paused",
                params![topic, rule.hot_for, rule.retain_for, timestamp, rule.paused],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn retention_topics(&self) -> Result<Vec<String>> {
        Ok(self
            .db
            .prepare("SELECT topic FROM retention_policies WHERE paused=0 ORDER BY topic")?
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }

    pub(crate) fn retention_policy(&self, topic: &str) -> Result<Option<RetentionPolicy>> {
        Ok(self
            .db
            .query_row(
                "SELECT hot_ms,retain_ms,activated_ms FROM retention_policies WHERE topic=?",
                [topic],
                |row| {
                    Ok(RetentionPolicy {
                        hot_ms: row.get(0)?,
                        retain_ms: row.get(1)?,
                        activated_ms: row.get(2)?,
                    })
                },
            )
            .optional()?)
    }

    fn archive_candidate(&self, topic: &str, timestamp: i64) -> Result<Option<ArchiveCandidate>> {
        let Some(policy) = self.retention_policy(topic)? else {
            return Ok(None);
        };
        let floor = self.floor(topic)?;
        let mut stmt = self.db.prepare_cached(
            "SELECT offset,partition,segment,pos,len FROM records
             WHERE topic=? AND offset>=? ORDER BY offset LIMIT ?",
        )?;
        let mut rows = stmt.query(params![topic, floor, ARCHIVE_BATCH_RECORDS])?;
        let mut locations = Vec::new();
        let mut bytes = 0_usize;
        while let Some(row) = rows.next()? {
            let len = usize::try_from(row.get::<_, i64>(4)?)?;
            if !locations.is_empty() && bytes.saturating_add(len) > ARCHIVE_BLOCK_BYTES {
                break;
            }
            bytes += len;
            locations.push(RecordLocation {
                offset: row.get(0)?,
                partition: row.get(1)?,
                address: Address {
                    segment: u64::try_from(row.get::<_, i64>(2)?)?,
                    pos: u64::try_from(row.get::<_, i64>(3)?)?,
                    len,
                },
            });
        }

        let mut inputs = Vec::new();
        let cutoff = timestamp.saturating_sub(policy.hot_ms);
        self.visit_indexed_page(locations, |location, frame| {
            let accepted_at_ms = frame.accepted_at_ms.unwrap_or(policy.activated_ms);
            if accepted_at_ms > cutoff {
                return false;
            }
            inputs.push(ArchiveInput {
                segment: location.address.segment,
                already_packed: self
                    .packed
                    .contains_key(&(location.partition, location.address.segment)),
                accepted_at_ms,
                len: location.address.len,
                record: frame.into_record(location.offset, location.partition),
            });
            true
        })?;
        Ok((!inputs.is_empty()).then(|| ArchiveCandidate {
            topic: topic.into(),
            floor,
            inputs,
        }))
    }

    fn publish_archive(
        &mut self,
        candidate: ArchiveCandidate,
        block: Option<ArchiveBlock>,
    ) -> Result<()> {
        ensure!(
            self.floor(&candidate.topic)? == candidate.floor,
            "archive floor changed"
        );
        let next = candidate
            .inputs
            .last()
            .context("empty archive block")?
            .record
            .offset
            .checked_add(1)
            .context("archive offset overflow")?;
        let needs_blob = candidate.inputs.iter().any(|input| {
            !self
                .packed
                .contains_key(&(input.record.partition, input.segment))
        });
        if needs_blob {
            let bytes = block
                .as_ref()
                .context("missing compressed archive block")?
                .compressed
                .len();
            self.admit_disk_write(bytes as u64, false)?;
        }
        let tx = self.db.transaction()?;
        let block_id = if needs_blob {
            let block = block.context("missing compressed archive block")?;
            tx.execute(
                "INSERT INTO archive_blocks(raw_len,checksum,compressed) VALUES (?,?,?)",
                params![block.raw_len, block.checksum, block.compressed],
            )?;
            tx.last_insert_rowid()
        } else {
            0
        };
        {
            let mut stmt =
                tx.prepare_cached("INSERT INTO archive_records VALUES (?,?,?,?,?,?,?)")?;
            for input in candidate.inputs {
                let task = input.record.event["task_id"]
                    .as_str()
                    .context("missing archive task ID")?;
                let event_id = input.record.event["event_id"]
                    .as_str()
                    .context("missing archive event ID")?;
                stmt.execute(params![
                    input.record.offset,
                    candidate.topic,
                    task,
                    event_id,
                    input.accepted_at_ms,
                    i64::try_from(input.len)?,
                    if self
                        .packed
                        .contains_key(&(input.record.partition, input.segment))
                    {
                        0
                    } else {
                        block_id
                    }
                ])?;
            }
        }
        tx.execute(
            "INSERT INTO floors VALUES (?,?) ON CONFLICT(topic) DO UPDATE SET first_offset=excluded.first_offset",
            params![candidate.topic, next],
        )?;
        tx.commit()?;
        self.live.advance_floor(&candidate.topic, next);
        Ok(())
    }

    fn prune_archive(&mut self, topic: &str, timestamp: i64) -> Result<bool> {
        let Some(policy) = self.retention_policy(topic)? else {
            return Ok(false);
        };
        let cutoff = timestamp.saturating_sub(policy.retain_ms);
        let expired = {
            let mut stmt = self.db.prepare_cached(
                "SELECT offset,accepted_at_ms,block_id,len FROM archive_records
                 WHERE topic=? ORDER BY offset LIMIT ?",
            )?;
            let mut rows = stmt.query(params![topic, ARCHIVE_PRUNE_RECORDS])?;
            let mut expired = Vec::<(i64, i64)>::new();
            let mut remaining = MAX_STREAM_BATCH_BYTES;
            while let Some(row) = rows.next()? {
                if row.get::<_, i64>(1)? > cutoff {
                    break;
                }
                let len = usize::try_from(row.get::<_, i64>(3)?)?;
                let Some(left) = remaining.checked_sub(len) else {
                    break;
                };
                remaining = left;
                expired.push((row.get(0)?, row.get(2)?));
            }
            expired
        };
        let Some(&(through, _)) = expired.last() else {
            return Ok(false);
        };
        let tx = self.db.transaction()?;
        tx.execute(
            "DELETE FROM archive_records WHERE topic=? AND offset<=?",
            params![topic, through],
        )?;
        let mut previous = None;
        for (_, block_id) in expired {
            if previous != Some(block_id) {
                tx.execute(
                    "DELETE FROM archive_blocks WHERE id=?1
                     AND NOT EXISTS (SELECT 1 FROM archive_records WHERE block_id=?1)",
                    [block_id],
                )?;
                previous = Some(block_id);
            }
        }
        tx.commit()?;
        Ok(true)
    }

    pub(crate) fn audit_history(
        &self,
        topic: &str,
        task: &str,
        after: Option<&str>,
        through: Option<i64>,
        limit: u32,
    ) -> Result<(Option<i64>, Vec<Record>)> {
        identifier(topic)?;
        identifier(task)?;
        if let Some(id) = after {
            identifier(id)?;
        }
        if through.is_some_and(|offset| offset < 1) {
            return Err(Problem::Invalid("audit through must be positive".into()).into());
        }
        let floor = self.floor(topic)?;
        let after = self.resolve_audit_cursor(topic, task, after, floor)?;
        let through = match through {
            Some(offset) => Some(offset),
            None => self
                .db
                .query_row(
                    "SELECT offset FROM (
                    SELECT offset FROM archive_records WHERE topic=?1 AND task=?2 AND offset<?3
                    UNION ALL
                    SELECT offset FROM records WHERE topic=?1 AND task=?2 AND offset>=?3
                 ) ORDER BY offset DESC LIMIT 1",
                    params![topic, task, floor],
                    |row| row.get(0),
                )
                .optional()?,
        };
        let Some(end) = through else {
            return Ok((None, Vec::new()));
        };
        if after > end {
            return Err(Problem::Invalid("audit cursor is after through offset".into()).into());
        }
        let mut stmt = self.db.prepare_cached(
            "SELECT offset,len,block_id,partition,segment,pos FROM (
                SELECT a.offset,a.len,NULLIF(a.block_id,0) AS block_id,r.partition,r.segment,r.pos
                FROM archive_records AS a LEFT JOIN records AS r ON a.block_id=0 AND r.offset=a.offset
                WHERE a.topic=?1 AND a.task=?2 AND a.offset<?3 AND a.offset>?4 AND a.offset<=?5
                UNION ALL
                SELECT offset,len,NULL,partition,segment,pos
                FROM records WHERE topic=?1 AND task=?2 AND offset>=?3 AND offset>?4 AND offset<=?5
             ) ORDER BY offset LIMIT ?6",
        )?;
        let mut rows = stmt.query(params![topic, task, floor, after, end, limit])?;
        let mut records = Vec::new();
        let mut hot = Vec::new();
        let mut last_block = None;
        let mut archived = BTreeMap::new();
        // Leave room for the protobuf envelope and per-record fields.
        let mut remaining = MAX_STREAM_BATCH_BYTES - 2048;
        while let Some(row) = rows.next()? {
            let offset = row.get::<_, i64>(0)?;
            let len = usize::try_from(row.get::<_, i64>(1)?)?;
            let Some(left) = remaining.checked_sub(len) else {
                break;
            };
            remaining = left;
            if let Some(block_id) = row.get::<_, Option<i64>>(2)? {
                if last_block != Some(block_id) {
                    archived = self.read_archive_block(block_id)?;
                    last_block = Some(block_id);
                }
                records.push(
                    archived
                        .remove(&offset)
                        .context("archive block missing indexed record")?,
                );
            } else {
                hot.push(RecordLocation {
                    offset,
                    partition: row.get(3)?,
                    address: Address {
                        segment: u64::try_from(row.get::<_, i64>(4)?)?,
                        pos: u64::try_from(row.get::<_, i64>(5)?)?,
                        len,
                    },
                });
            }
        }
        records.extend(self.read_indexed_page(hot)?);
        records.sort_unstable_by_key(|record| record.offset);
        Ok((through, records))
    }

    fn resolve_audit_cursor(
        &self,
        topic: &str,
        task: &str,
        after: Option<&str>,
        floor: i64,
    ) -> Result<i64> {
        let Some(id) = after else {
            return Ok(0);
        };
        let offset = self.db.query_row(
            "SELECT offset FROM archive_records WHERE topic=?1 AND task=?2 AND event_id=?3 AND offset<?4
             UNION ALL
             SELECT offset FROM records WHERE topic=?1 AND task=?2 AND event_id=?3 AND offset>=?4",
            params![topic, task, id, floor],
            |row| row.get(0),
        ).optional()?;
        if let Some(offset) = offset {
            return Ok(offset);
        }
        let earliest = self
            .db
            .query_row(
                "SELECT event_id FROM (
                SELECT offset,event_id FROM archive_records WHERE topic=?1 AND task=?2 AND offset<?3
                UNION ALL
                SELECT offset,event_id FROM records WHERE topic=?1 AND task=?2 AND offset>=?3
             ) ORDER BY offset LIMIT 1",
                params![topic, task, floor],
                |row| row.get(0),
            )
            .optional()?;
        Err(Problem::Gap { earliest }.into())
    }

    fn read_archive_block(&self, id: i64) -> Result<BTreeMap<i64, Record>> {
        let block = self
            .db
            .query_row(
                "SELECT raw_len,checksum,compressed FROM archive_blocks WHERE id=?",
                [id],
                |row| {
                    Ok(ArchiveBlock {
                        raw_len: row.get(0)?,
                        checksum: row.get(1)?,
                        compressed: row.get(2)?,
                    })
                },
            )
            .context("load indexed archive block")?;
        Ok(block
            .decode()?
            .into_iter()
            .map(|record| (record.offset, record))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    fn event(id: &str) -> serde_json::Value {
        json!({"task_id":"T1","event_id":id,"payload":{"items":[1,"déjà vu",false]}})
    }

    #[test]
    fn validates_topic_windows_and_persists_pause() -> Result<()> {
        for bad in [
            r#"{"status":{"hot_for":"1d","retain_for":"1h"}}"#,
            r#"{"status":{"hot_for":"0s","retain_for":"1h"}}"#,
            r#"{"status":{"hot_for":"10","retain_for":"1h"}}"#,
            r#"{"status":{"hot_for":"9999999999999999d","retain_for":"1h"}}"#,
            r#"{"":{"hot_for":"1s","retain_for":"1h"}}"#,
        ] {
            assert!(parse_rules(bad).is_err(), "{bad}");
        }
        let dir = tempdir()?;
        let mut store = Store::open(dir.path())?;
        let rules = r#"{"status":{"hot_for":"1s","retain_for":"1h"}}"#;
        store.configure_retention(parse_rules(rules)?, 100)?;
        assert_eq!(store.retention_topics()?, ["status"]);
        store.configure_retention(
            parse_rules(r#"{"status":{"hot_for":"1s","retain_for":"1h","paused":true}}"#)?,
            200,
        )?;
        drop(store);
        let mut store = Store::open(dir.path())?;
        assert!(store.retention_topics()?.is_empty());
        assert!(store.expire_before("status", 1).is_err());
        store.configure_retention(parse_rules(rules)?, 300)?;
        assert_eq!(store.retention_topics()?, ["status"]);
        assert_eq!(store.retention_policy("status")?.unwrap().activated_ms, 100);
        Ok(())
    }

    #[test]
    fn archive_and_floor_commit_together_across_index_modes() -> Result<()> {
        for mode in ["shared", "partitioned"] {
            let dir = tempdir()?;
            let mut store = Store::open_mode(dir.path(), Some(mode))?;
            store.configure_retention(
                parse_rules(r#"{"status":{"hot_for":"1s","retain_for":"1h"}}"#)?,
                now()?,
            )?;
            let first = store.append("status", event("E1"))?;
            let other = store.append("other", event("E2"))?;
            let third = store.append("status", event("E3"))?;
            let timestamp = now()?;
            assert!(
                store
                    .archive_candidate("status", timestamp - 1_000)?
                    .is_none()
            );
            let candidate = store
                .archive_candidate("status", timestamp + 1_000)?
                .unwrap();
            assert_eq!(candidate.inputs.len(), 2);
            let block = ArchiveBlock::encode(&candidate.inputs)?;

            // Interrupt after inserting the block and references but before advancing the floor.
            store.db.execute_batch(
                "CREATE TEMP TRIGGER reject_floor BEFORE INSERT ON floors
                 BEGIN SELECT RAISE(ABORT, 'floor failure'); END;",
            )?;
            assert!(store.publish_archive(candidate, Some(block)).is_err());
            assert_eq!(store.floor("status")?, first);
            assert_eq!(
                store
                    .db
                    .query_row("SELECT COUNT(*) FROM archive_blocks", [], |r| r
                        .get::<_, i64>(0))?,
                0
            );
            drop(store);

            let mut store = Store::open(dir.path())?;
            assert!(store.expire_before("status", third + 1).is_err());
            let candidate = store
                .archive_candidate("status", timestamp + 1_000)?
                .unwrap();
            let block = ArchiveBlock::encode(&candidate.inputs)?;
            store.publish_archive(candidate, Some(block))?;
            assert_eq!(store.floor("status")?, third + 1);
            assert!(store.resolve("status", "T1", Some("E1")).is_err());
            assert_eq!(store.latest("other", "T1")?.offset, other);
            assert_eq!(store.append("status", event("E1"))?, first);
            let hot = store.append("status", event("E4"))?;
            drop(store);

            let mut store = Store::open(dir.path())?;
            let (through, page) = store.audit_history("status", "T1", None, None, 1)?;
            assert_eq!(through, Some(hot));
            assert_eq!(page[0].event, event("E1"));
            let (_, page) = store.audit_history("status", "T1", Some("E1"), through, 64)?;
            assert_eq!(
                page.iter().map(|r| r.offset).collect::<Vec<_>>(),
                [third, hot]
            );
            assert_eq!(page[0].event, event("E3"));
            assert!(!store.prune_archive("status", timestamp)?);
            assert!(store.prune_archive("status", timestamp + 3_600_000)?);
            let gap = store
                .audit_history("status", "T1", Some("E1"), through, 64)
                .unwrap_err();
            assert!(matches!(gap.downcast_ref::<Problem>(),
                Some(Problem::Gap { earliest: Some(id) }) if id == "E4"));
            assert_eq!(
                store
                    .db
                    .query_row("SELECT COUNT(*) FROM archive_blocks", [], |r| r
                        .get::<_, i64>(0))?,
                0
            );
            assert_eq!(store.floor("status")?, third + 1);
        }
        Ok(())
    }

    #[test]
    fn damaged_archive_block_reports_failure() -> Result<()> {
        let dir = tempdir()?;
        let mut store = Store::open(dir.path())?;
        store.configure_retention(
            parse_rules(r#"{"status":{"hot_for":"1s","retain_for":"1h"}}"#)?,
            now()?,
        )?;
        store.append("status", event("E1"))?;
        let candidate = store.archive_candidate("status", now()? + 1_000)?.unwrap();
        let block = ArchiveBlock::encode(&candidate.inputs)?;
        store.publish_archive(candidate, Some(block))?;
        store.db.execute(
            "UPDATE archive_blocks SET checksum=CASE checksum WHEN 0 THEN 1 ELSE 0 END",
            [],
        )?;
        let error = store
            .audit_history("status", "T1", None, None, 1)
            .unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"));
        Ok(())
    }

    #[test]
    fn legacy_inputs_age_from_first_policy_activation() -> Result<()> {
        use crate::store::Frame;

        let dir = tempdir()?;
        let store = Store::open(dir.path())?;
        let frame = Frame::new("status".into(), event("legacy"), None)?;
        let body = serde_json::to_vec(&frame)?;
        store.journal(0)?.try_lock()?.append(&frame, 0, &body)?;
        drop(store);

        let mut store = Store::open(dir.path())?;
        let first_activation = now()?;
        store.configure_retention(
            parse_rules(r#"{"status":{"hot_for":"1s","retain_for":"1h"}}"#)?,
            first_activation,
        )?;
        assert!(
            store
                .archive_candidate("status", first_activation + 999)?
                .is_none()
        );
        let candidate = store
            .archive_candidate("status", first_activation + 1000)?
            .unwrap();
        assert_eq!(candidate.inputs[0].accepted_at_ms, first_activation);
        assert_eq!(candidate.inputs[0].record.event, event("legacy"));
        Ok(())
    }
}
