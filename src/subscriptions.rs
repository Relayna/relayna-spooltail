use crate::store::{Problem, Record, Store, identifier};
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Serialize)]
pub struct Claim {
    pub record: Record,
    pub generation: i64,
    pub lease_until_ms: i64,
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

fn now() -> Result<i64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}

fn valid_group(topic: &str, group: &str) -> Result<()> {
    identifier(topic)?;
    identifier(group)
}

impl Store {
    fn group_next_offset(&self, topic: &str, group: &str) -> Result<Option<i64>> {
        valid_group(topic, group)?;
        let floor = self.floor(topic)?;
        let saved: Option<i64> = self
            .db
            .query_row(
                "SELECT offset FROM cursors WHERE topic=? AND name=?",
                params![topic, group],
                |r| r.get(0),
            )
            .optional()?;
        let next_after = match saved {
            Some(offset) => offset,
            None => {
                self.db.execute(
                    "INSERT INTO cursors VALUES(?,?,?)",
                    params![topic, group, floor - 1],
                )?;
                floor - 1
            }
        };
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

    pub fn group_next(&self, topic: &str, group: &str) -> Result<Option<Record>> {
        self.group_next_offset(topic, group)?
            .map(|o| self.read(o))
            .transpose()
    }

    pub fn commit(&self, topic: &str, group: &str, offset: i64) -> Result<()> {
        valid_group(topic, group)?;
        let saved: Option<i64> = self
            .db
            .query_row(
                "SELECT offset FROM cursors WHERE topic=? AND name=?",
                params![topic, group],
                |r| r.get(0),
            )
            .optional()?;
        if saved == Some(offset) {
            return Ok(());
        }
        if self.group_next_offset(topic, group)? != Some(offset) {
            return Err(
                Problem::Conflict("commit must be the next record in this topic".into()).into(),
            );
        }
        self.db.execute("INSERT INTO cursors VALUES(?,?,?) ON CONFLICT(topic,name) DO UPDATE SET offset=excluded.offset",params![topic,group,offset])?;
        Ok(())
    }

    pub fn claim(&mut self, topic: &str, group: &str, lease_ms: i64) -> Result<Option<Claim>> {
        self.ensure_writable()?;
        valid_group(topic, group)?;
        if !(1..=3_600_000).contains(&lease_ms) {
            return Err(Problem::Invalid("lease_ms must be 1..3600000".into()).into());
        }
        let now = now()?;
        let tx = self.db.transaction()?;
        let choice: Option<(i64, i64)> = tx
            .query_row(
                "SELECT r.offset,COALESCE(l.generation,0)+1 FROM records r LEFT JOIN leases l
             ON l.topic=r.topic AND l.name=? AND l.offset=r.offset
             WHERE r.topic=? AND (l.offset IS NULL OR (l.state IN ('pending','retry') AND l.visible_at<=?))
             ORDER BY r.offset LIMIT 1",
                params![group, topic, now],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let Some((offset, generation)) = choice else {
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
            "UPDATE leases SET state=?,visible_at=?,generation=generation+? WHERE topic=? AND name=? AND offset=? AND generation=? AND visible_at>? AND state='pending'",
            params![target,now+delay_ms,i64::from(action=="retry"),topic,group,offset,generation,now])?;
        if changed == 0 {
            return Err(Problem::Conflict("lease expired, settled or superseded".into()).into());
        }
        Ok(())
    }
}
