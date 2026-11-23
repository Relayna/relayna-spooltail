use crate::{
    journal::Address,
    store::{Problem, Record, RecordLocation, Store, identifier},
};
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub const DEFAULT_STREAM_BATCH_RECORDS: u32 = 32;
const MAX_STREAM_BATCH_RECORDS: u32 = 64;
pub const MAX_STREAM_BATCH_BYTES: usize = 2 * 1024 * 1024;

#[derive(Serialize)]
pub struct Claim {
    pub record: Record,
    pub generation: i64,
    pub lease_until_ms: i64,
    #[serde(skip_serializing_if = "crate::store::is_zero")]
    pub partition: u32,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkLease {
    pub topic: String,
    pub group: String,
    pub offset: i64,
    pub generation: i64,
}

impl WorkLease {
    pub fn validate_completion(&self, db: &Connection, task: &str) -> Result<()> {
        valid_group(&self.topic, &self.group)?;
        let active: bool = db.query_row(
            "SELECT EXISTS (
                SELECT 1 FROM leases l JOIN records r ON r.offset=l.offset AND r.topic=l.topic
                WHERE l.topic=? AND l.name=? AND l.offset=? AND l.generation=?
                  AND l.state='pending' AND l.visible_at>? AND r.task=?
            )",
            params![
                self.topic,
                self.group,
                self.offset,
                self.generation,
                now()?,
                task
            ],
            |row| row.get(0),
        )?;
        if !active {
            return Err(Problem::Conflict("no active matching lease for this task".into()).into());
        }
        Ok(())
    }

    pub fn apply_completion(&self, tx: &Transaction<'_>) -> Result<()> {
        // Admission checked the deadline before journaling. Recovery may run long after it.
        // The journal also carries enough information to rebuild this particular settlement.
        let changed = tx.execute(
            "INSERT INTO leases(topic,name,offset,generation,visible_at,state)
             VALUES(?,?,?,?,0,'done') ON CONFLICT(topic,name,offset) DO UPDATE
             SET state='done',visible_at=0
             WHERE leases.generation=excluded.generation AND leases.state='pending'",
            params![self.topic, self.group, self.offset, self.generation],
        )?;
        anyhow::ensure!(
            changed == 1,
            "journal completion conflicts with stored lease"
        );
        Ok(())
    }
}

pub(crate) fn now() -> Result<i64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}

fn valid_group(topic: &str, group: &str) -> Result<()> {
    identifier(topic)?;
    identifier(group)
}

fn validate_batch_count(count: u32) -> Result<()> {
    if !(1..=MAX_STREAM_BATCH_RECORDS).contains(&count) {
        return Err(Problem::Invalid(format!(
            "batch count must be between 1 and {MAX_STREAM_BATCH_RECORDS}"
        ))
        .into());
    }
    Ok(())
}

impl Store {
    fn global_checkpoint(&self, topic: &str, group: &str) -> Result<Option<i64>> {
        Ok(self
            .db
            .query_row(
                "SELECT offset FROM cursors WHERE topic=? AND name=?",
                params![topic, group],
                |r| r.get(0),
            )
            .optional()?)
    }

    fn init_global_checkpoint(&self, topic: &str, group: &str, floor: i64) -> Result<i64> {
        if let Some(offset) = self.global_checkpoint(topic, group)? {
            return Ok(offset);
        }
        self.db.execute(
            "INSERT INTO cursors VALUES(?,?,?)",
            params![topic, group, floor - 1],
        )?;
        Ok(floor - 1)
    }

    fn group_next_offset(&self, topic: &str, group: &str) -> Result<Option<i64>> {
        valid_group(topic, group)?;
        let floor = self.floor(topic)?;
        let next_after = self.init_global_checkpoint(topic, group, floor)?;
        let candidate: Option<i64> = self.db.query_row(
            "SELECT MIN(offset) FROM records WHERE topic=? AND offset>?",
            params![topic, next_after],
            |r| r.get(0),
        )?;
        if candidate.is_some_and(|v| v < floor) {
            return Err(Problem::Conflict(format!(
                "subscription fell behind retention; earliest allowed offset {floor}"
            ))
            .into());
        }
        Ok(candidate)
    }

    fn global_next(&self, topic: &str, group: &str) -> Result<Option<Record>> {
        self.group_next_offset(topic, group)?
            .map(|o| self.read(o))
            .transpose()
    }

    fn global_commit(&self, topic: &str, group: &str, offset: i64) -> Result<()> {
        valid_group(topic, group)?;
        if self.global_checkpoint(topic, group)? == Some(offset) {
            return Ok(());
        }
        if self.group_next_offset(topic, group)? != Some(offset) {
            return Err(
                Problem::Conflict("commit must be the next record in this topic".into()).into(),
            );
        }
        self.save_global_checkpoint(topic, group, offset)
    }

    fn save_global_checkpoint(&self, topic: &str, group: &str, offset: i64) -> Result<()> {
        self.db.execute(
            "INSERT INTO cursors VALUES(?,?,?) ON CONFLICT(topic,name)
             DO UPDATE SET offset=excluded.offset",
            params![topic, group, offset],
        )?;
        Ok(())
    }

    fn partition_checkpoint(
        &self,
        topic: &str,
        group: &str,
        partition: u32,
    ) -> Result<Option<i64>> {
        Ok(self
            .db
            .query_row(
                "SELECT offset FROM partition_cursors WHERE topic=? AND name=? AND partition=?",
                params![topic, group, partition],
                |row| row.get(0),
            )
            .optional()?)
    }

    fn stream_baseline(
        &self,
        topic: &str,
        group: &str,
        partition: Option<u32>,
    ) -> Result<(Option<i64>, bool)> {
        valid_group(topic, group)?;
        let baseline = self
            .db
            .query_row(
                "SELECT initial_offset FROM partitioned_groups WHERE topic=? AND name=?",
                params![topic, group],
                |row| row.get(0),
            )
            .optional()?;
        let Some(partition) = partition else {
            if baseline.is_some() {
                return Err(Problem::Conflict(
                    "stream group uses partition-scoped checkpoints".into(),
                )
                .into());
            }
            return Ok((None, false));
        };
        self.validate_partition(partition)?;
        if let Some(baseline) = baseline {
            return Ok((Some(baseline), true));
        }
        let global = self.global_checkpoint(topic, group)?;
        Ok((Some(global.unwrap_or(self.floor(topic)? - 1)), false))
    }

    fn partition_next_offset(
        &self,
        topic: &str,
        group: &str,
        partition: u32,
        baseline: i64,
    ) -> Result<Option<i64>> {
        let after = self
            .partition_checkpoint(topic, group, partition)?
            .unwrap_or(baseline);
        let candidate: Option<i64> = self.db.query_row(
            "SELECT MIN(offset) FROM records WHERE partition=? AND topic=? AND offset>?",
            params![partition, topic, after],
            |row| row.get(0),
        )?;
        let Some(offset) = candidate else {
            return Ok(None);
        };
        let floor = self.floor(topic)?;
        if offset < floor {
            return Err(Problem::Conflict(format!(
                "subscription fell behind retention; earliest allowed offset {floor}"
            ))
            .into());
        }

        Ok(self
            .partition_record_ready(topic, group, partition, baseline, offset)?
            .then_some(offset))
    }

    fn partition_record_ready(
        &self,
        topic: &str,
        group: &str,
        partition: u32,
        baseline: i64,
        offset: i64,
    ) -> Result<bool> {
        // A moved task's old stream records stay on the source partition. Hold the
        // destination read until this group has processed the previous record.
        let predecessor: Option<(u32, i64)> = self
            .db
            .query_row(
                "SELECT previous.partition,previous.offset
             FROM records AS current JOIN records AS previous ON previous.topic=current.topic
                 AND previous.task=current.task AND previous.offset<current.offset
             WHERE current.offset=? ORDER BY previous.offset DESC LIMIT 1",
                [offset],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((source, previous)) = predecessor
            && source != partition
            && self
                .partition_checkpoint(topic, group, source)?
                .unwrap_or(baseline)
                < previous
        {
            return Ok(false);
        }
        Ok(true)
    }

    pub fn group_next(
        &self,
        topic: &str,
        group: &str,
        partition: Option<u32>,
    ) -> Result<Option<Record>> {
        let (baseline, initialized) = self.stream_baseline(topic, group, partition)?;
        let (Some(partition), Some(baseline)) = (partition, baseline) else {
            return self.global_next(topic, group);
        };
        if !initialized {
            self.db.execute(
                "INSERT INTO partitioned_groups VALUES(?,?,?)",
                params![topic, group, baseline],
            )?;
        }
        self.partition_next_offset(topic, group, partition, baseline)?
            .map(|offset| self.read(offset))
            .transpose()
    }

    pub fn commit(
        &mut self,
        topic: &str,
        group: &str,
        offset: i64,
        partition: Option<u32>,
    ) -> Result<()> {
        let (baseline, initialized) = self.stream_baseline(topic, group, partition)?;
        let (Some(partition), Some(baseline)) = (partition, baseline) else {
            return self.global_commit(topic, group, offset);
        };
        if self.partition_checkpoint(topic, group, partition)? == Some(offset) {
            return Ok(());
        }
        if self.partition_next_offset(topic, group, partition, baseline)? != Some(offset) {
            return Err(Problem::Conflict(
                "commit must be the next record in this partition".into(),
            )
            .into());
        }
        self.save_partition_checkpoint(
            topic,
            group,
            partition,
            offset,
            (!initialized).then_some(baseline),
        )
    }

    fn save_partition_checkpoint(
        &mut self,
        topic: &str,
        group: &str,
        partition: u32,
        offset: i64,
        initial_baseline: Option<i64>,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        if let Some(baseline) = initial_baseline {
            tx.execute(
                "INSERT INTO partitioned_groups VALUES(?,?,?)",
                params![topic, group, baseline],
            )?;
        }
        tx.execute(
            "INSERT INTO partition_cursors VALUES(?,?,?,?)
             ON CONFLICT(topic,name,partition) DO UPDATE SET offset=excluded.offset",
            params![topic, group, partition, offset],
        )?;
        tx.commit()?;
        Ok(())
    }

    fn batch_locations(
        &self,
        topic: &str,
        group: &str,
        scope: Option<(u32, i64)>,
        after: i64,
        limit: u32,
    ) -> Result<Vec<RecordLocation>> {
        let floor = self.floor(topic)?;
        let sql = if scope.is_some() {
            "SELECT offset, len, partition, segment, pos
             FROM records WHERE topic=?1 AND offset>?2 AND partition=?4
             ORDER BY offset LIMIT ?3"
        } else {
            "SELECT offset, len, partition, segment, pos
             FROM records WHERE topic=?1 AND offset>?2 ORDER BY offset LIMIT ?3"
        };
        let mut stmt = self.db.prepare(sql)?;
        let mut rows = match scope {
            Some((partition, _)) => stmt.query(params![topic, after, limit, partition])?,
            None => stmt.query(params![topic, after, limit])?,
        };
        let mut locations = Vec::new();
        let mut remaining = MAX_STREAM_BATCH_BYTES;
        while let Some(row) = rows.next()? {
            let offset: i64 = row.get(0)?;
            if offset < floor {
                return Err(Problem::Conflict(format!(
                    "subscription fell behind retention; earliest allowed offset {floor}"
                ))
                .into());
            }
            if let Some((partition, baseline)) = scope
                && !self.partition_record_ready(topic, group, partition, baseline, offset)?
            {
                break;
            }
            let frame_bytes = usize::try_from(row.get::<_, i64>(1)?)?;
            let Some(left) = remaining.checked_sub(frame_bytes) else {
                break;
            };
            remaining = left;
            locations.push(RecordLocation {
                offset,
                partition: row.get(2)?,
                address: Address {
                    segment: u64::try_from(row.get::<_, i64>(3)?)?,
                    pos: u64::try_from(row.get::<_, i64>(4)?)?,
                    len: frame_bytes,
                },
            });
        }
        Ok(locations)
    }

    pub fn next_batch(
        &self,
        topic: &str,
        group: &str,
        partition: Option<u32>,
        limit: u32,
    ) -> Result<(i64, Vec<Record>)> {
        validate_batch_count(limit)?;
        let (baseline, initialized) = self.stream_baseline(topic, group, partition)?;
        let scope = partition.zip(baseline);
        let after = match scope {
            Some((partition, baseline)) => {
                if !initialized {
                    self.db.execute(
                        "INSERT INTO partitioned_groups VALUES(?,?,?)",
                        params![topic, group, baseline],
                    )?;
                }
                self.partition_checkpoint(topic, group, partition)?
                    .unwrap_or(baseline)
            }
            None => self.init_global_checkpoint(topic, group, self.floor(topic)?)?,
        };
        let locations = self.batch_locations(topic, group, scope, after, limit)?;
        let records = self.read_indexed_page(locations)?;
        Ok((after, records))
    }

    pub fn commit_batch(
        &mut self,
        topic: &str,
        group: &str,
        partition: Option<u32>,
        after: i64,
        through: i64,
        count: u32,
    ) -> Result<()> {
        validate_batch_count(count)?;
        if after < 0 || through <= after {
            return Err(Problem::Invalid(
                "through must be greater than a nonnegative checkpoint".into(),
            )
            .into());
        }
        let (baseline, initialized) = self.stream_baseline(topic, group, partition)?;
        let scope = partition.zip(baseline);
        let saved = match scope {
            Some((partition, baseline)) if initialized => Some(
                self.partition_checkpoint(topic, group, partition)?
                    .unwrap_or(baseline),
            ),
            Some(_) => None,
            None => self.global_checkpoint(topic, group)?,
        };
        if saved == Some(through) {
            return Ok(());
        }
        if saved != Some(after) {
            return Err(Problem::Conflict("stream checkpoint changed since fetch".into()).into());
        }
        let locations = self.batch_locations(topic, group, scope, after, count)?;
        if locations.len() != count as usize
            || locations.last().map(|record| record.offset) != Some(through)
        {
            return Err(
                Problem::Conflict("commit must cover the next consecutive records".into()).into(),
            );
        }
        match partition {
            Some(partition) => {
                self.save_partition_checkpoint(topic, group, partition, through, None)
            }
            None => self.save_global_checkpoint(topic, group, through),
        }
    }

    pub fn claim(
        &mut self,
        topic: &str,
        group: &str,
        lease_ms: i64,
        partition: Option<u32>,
    ) -> Result<Option<Claim>> {
        self.ensure_writable()?;
        valid_group(topic, group)?;
        if !(1..=3_600_000).contains(&lease_ms) {
            return Err(Problem::Invalid("lease_ms must be 1..3600000".into()).into());
        }
        if let Some(partition) = partition {
            self.validate_partition(partition)?;
        }
        let now = now()?;
        let tx = self.db.transaction()?;
        let filter = if partition.is_some() {
            "AND r.work_partition=?4"
        } else {
            "AND ?4 IS NULL"
        };
        let query = format!(
            "SELECT r.offset,COALESCE(l.generation,0)+1,r.work_partition
             FROM records r LEFT JOIN leases l ON l.topic=r.topic AND l.name=?1 AND l.offset=r.offset
             WHERE r.topic=?2 {filter}
               AND (l.offset IS NULL OR (l.state IN ('pending','retry') AND l.visible_at<=?3))
               AND NOT EXISTS(SELECT 1 FROM appending WHERE work_offset=r.offset)
             ORDER BY r.offset LIMIT 1"
        );
        let choice: Option<(i64, i64, u32)> = tx
            .prepare_cached(&query)?
            .query_row(params![group, topic, now, partition], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .optional()?;
        let Some((offset, generation, partition)) = choice else {
            return Ok(None);
        };
        let until = now + lease_ms;
        tx.execute("INSERT INTO leases VALUES(?,?,?, ?,?, 'pending') ON CONFLICT(topic,name,offset) DO UPDATE SET generation=excluded.generation,visible_at=excluded.visible_at,state='pending'",
                   params![topic,group,offset,generation,until])?;
        tx.commit()?;
        Ok(Some(Claim {
            record: self.read(offset)?,
            generation,
            lease_until_ms: until,
            partition,
        }))
    }

    pub fn settle(
        &self,
        topic: &str,
        group: &str,
        offset: i64,
        generation: i64,
        action: &str,
        delay_ms: i64,
    ) -> Result<()> {
        self.ensure_writable()?;
        valid_group(topic, group)?;
        if !(0..=3_600_000).contains(&delay_ms) {
            return Err(Problem::Invalid("delay_ms must be 0..3600000".into()).into());
        }
        let target = match action {
            "ack" => "done",
            "dead-letter" => "dead",
            "retry" => "retry",
            "renew" if delay_ms > 0 => "pending",
            _ => {
                return Err(Problem::Invalid(
                    "expected ack, dead-letter, retry, or renew with a positive delay_ms".into(),
                )
                .into());
            }
        };
        let now = now()?;
        // Retry relinquishes this generation immediately, even when the retry is delayed.
        let changed=self.db.execute(
            "UPDATE leases SET state=?,visible_at=?,generation=generation+? WHERE topic=? AND name=? AND offset=? AND generation=? AND visible_at>? AND state='pending' AND NOT EXISTS(SELECT 1 FROM appending WHERE work_offset=?)",
            params![target,now+delay_ms,i64::from(action=="retry"),topic,group,offset,generation,now,offset])?;
        if changed == 0 {
            return Err(Problem::Conflict("lease expired, settled or superseded".into()).into());
        }
        Ok(())
    }
}
