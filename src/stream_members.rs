use crate::{Problem, Store, identifier, subscriptions::now};
use anyhow::{Context, Result};
use rusqlite::{OptionalExtension, Transaction, params};
use serde::Serialize;
use std::{cmp::Reverse, collections::BTreeMap};

#[derive(Clone, Copy)]
pub(crate) struct StreamOwnership {
    pub member_epoch: i64,
    pub assignment_epoch: i64,
}

#[derive(Clone, Copy)]
pub(crate) struct StreamAccess {
    pub partition: Option<u32>,
    pub ownership: Option<StreamOwnership>,
}

impl From<Option<u32>> for StreamAccess {
    fn from(partition: Option<u32>) -> Self {
        Self {
            partition,
            ownership: None,
        }
    }
}

#[derive(Serialize)]
pub(crate) struct StreamAssignment {
    pub partition: u32,
    pub assignment_epoch: i64,
}

#[derive(Serialize)]
pub(crate) struct StreamMembership {
    pub member_epoch: i64,
    pub lease_until_ms: i64,
    pub assignments: Vec<StreamAssignment>,
}

impl Store {
    pub(crate) fn check_stream_access(
        &self,
        topic: &str,
        group: &str,
        access: StreamAccess,
    ) -> Result<()> {
        let managed: bool = self
            .db
            .prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM managed_stream_groups WHERE topic=? AND name=?)",
            )?
            .query_row(params![topic, group], |r| r.get(0))?;
        if !managed && access.ownership.is_none() {
            return Ok(());
        }
        let (Some(partition), Some(owner)) = (access.partition, access.ownership) else {
            return Err(Problem::Conflict(
                "managed stream groups require partition ownership".into(),
            )
            .into());
        };
        let current: bool = self
            .db
            .prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM stream_assignments a JOIN stream_members m
              ON m.topic=a.topic AND m.name=a.name AND m.member_epoch=a.member_epoch
              WHERE a.topic=? AND a.name=? AND a.partition=? AND a.member_epoch=?
                AND a.assignment_epoch=? AND m.lease_until_ms>?)",
            )?
            .query_row(
                params![
                    topic,
                    group,
                    partition,
                    owner.member_epoch,
                    owner.assignment_epoch,
                    now()?
                ],
                |r| r.get(0),
            )?;
        if !current {
            return Err(Problem::Conflict("stream partition ownership is stale".into()).into());
        }
        Ok(())
    }

    pub(crate) fn join_stream_group(
        &mut self,
        topic: &str,
        group: &str,
        member_id: &str,
        lease_ms: i64,
    ) -> Result<StreamMembership> {
        self.ensure_writable()?;
        identifier(topic)?;
        identifier(group)?;
        identifier(member_id)?;
        if !(1..=3_600_000).contains(&lease_ms) {
            return Err(Problem::Invalid("lease_ms must be 1..3600000".into()).into());
        }
        let baseline = self
            .global_checkpoint(topic, group)?
            .unwrap_or(self.floor(topic)? - 1);
        let timestamp = now()?;
        let partitions = self.partition_ids();
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO managed_stream_groups VALUES(?,?)",
            params![topic, group],
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO partitioned_groups VALUES(?,?,?)",
            params![topic, group, baseline],
        )?;
        let expired = expire_members(&tx, topic, group, timestamp)?;
        let existing = tx
            .query_row(
                "SELECT member_epoch,lease_until_ms,lease_ms FROM stream_members
             WHERE topic=? AND name=? AND member_id=?",
                params![topic, group, member_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get::<_, i64>(2)?)),
            )
            .optional()?;
        let (member_epoch, lease_until_ms) = match existing {
            Some((epoch, deadline, lease)) if lease == lease_ms => (epoch, deadline),
            Some(_) => {
                return Err(Problem::Conflict(
                    "stream member already uses a different lease duration".into(),
                )
                .into());
            }
            None => {
                let epoch = next_epoch(&tx)?;
                let deadline = timestamp + lease_ms;
                tx.execute(
                    "INSERT INTO stream_members VALUES(?,?,?,?,?,?)",
                    params![topic, group, member_id, epoch, lease_ms, deadline],
                )?;
                (epoch, deadline)
            }
        };
        refresh_assignments(
            &tx,
            topic,
            group,
            &partitions,
            expired || existing.is_none(),
        )?;
        let result = membership(&tx, topic, group, member_epoch, lease_until_ms)?;
        tx.commit()?;
        Ok(result)
    }

    pub(crate) fn heartbeat_stream_group(
        &mut self,
        topic: &str,
        group: &str,
        member_epoch: i64,
    ) -> Result<StreamMembership> {
        self.ensure_writable()?;
        identifier(topic)?;
        identifier(group)?;
        let timestamp = now()?;
        let partitions = self.partition_ids();
        let tx = self.db.transaction()?;
        let deadline = tx
            .query_row(
                "UPDATE stream_members SET lease_until_ms=MAX(lease_until_ms,?4+lease_ms)
             WHERE topic=?1 AND name=?2 AND member_epoch=?3 AND lease_until_ms>?4
             RETURNING lease_until_ms",
                params![topic, group, member_epoch, timestamp],
                |r| r.get(0),
            )
            .optional()?
            .ok_or_else(|| Problem::Conflict("stream membership is stale".into()))?;
        let expired = expire_members(&tx, topic, group, timestamp)?;
        refresh_assignments(&tx, topic, group, &partitions, expired)?;
        let result = membership(&tx, topic, group, member_epoch, deadline)?;
        tx.commit()?;
        Ok(result)
    }

    pub(crate) fn leave_stream_group(
        &mut self,
        topic: &str,
        group: &str,
        member_epoch: i64,
    ) -> Result<()> {
        self.ensure_writable()?;
        identifier(topic)?;
        identifier(group)?;
        let timestamp = now()?;
        let partitions = self.partition_ids();
        let tx = self.db.transaction()?;
        if tx.execute(
            "DELETE FROM stream_members WHERE topic=? AND name=? AND member_epoch=?",
            params![topic, group, member_epoch],
        )? != 0
        {
            expire_members(&tx, topic, group, timestamp)?;
            rebalance(&tx, topic, group, &partitions)?;
        }
        tx.commit()?;
        Ok(())
    }
}

fn expire_members(tx: &Transaction<'_>, topic: &str, group: &str, timestamp: i64) -> Result<bool> {
    Ok(tx.execute(
        "DELETE FROM stream_members WHERE topic=? AND name=? AND lease_until_ms<=?",
        params![topic, group, timestamp],
    )? != 0)
}

fn next_epoch(tx: &Transaction<'_>) -> Result<i64> {
    tx.query_row(
        "UPDATE stream_epoch SET epoch=epoch+1
         WHERE id=1 AND epoch<9223372036854775807 RETURNING epoch",
        [],
        |r| r.get(0),
    )
    .context("allocate a stream fencing epoch")
}

fn membership(
    tx: &Transaction<'_>,
    topic: &str,
    group: &str,
    member_epoch: i64,
    lease_until_ms: i64,
) -> Result<StreamMembership> {
    let mut query = tx.prepare_cached(
        "SELECT partition,assignment_epoch FROM stream_assignments
         WHERE topic=? AND name=? AND member_epoch=? ORDER BY partition",
    )?;
    let assignments = query
        .query_map(params![topic, group, member_epoch], |r| {
            Ok(StreamAssignment {
                partition: r.get(0)?,
                assignment_epoch: r.get(1)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(StreamMembership {
        member_epoch,
        lease_until_ms,
        assignments,
    })
}

fn refresh_assignments(
    tx: &Transaction<'_>,
    topic: &str,
    group: &str,
    partitions: &[u32],
    changed: bool,
) -> Result<()> {
    let assigned: i64 = tx.query_row(
        "SELECT COUNT(*) FROM stream_assignments WHERE topic=? AND name=?",
        params![topic, group],
        |r| r.get(0),
    )?;
    if changed || usize::try_from(assigned)? != partitions.len() {
        rebalance(tx, topic, group, partitions)?;
    }
    Ok(())
}

fn rebalance(tx: &Transaction<'_>, topic: &str, group: &str, partitions: &[u32]) -> Result<()> {
    let mut members = tx
        .prepare_cached("SELECT member_epoch FROM stream_members WHERE topic=? AND name=?")?
        .query_map(params![topic, group], |r| r.get::<_, i64>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if members.is_empty() {
        tx.execute(
            "DELETE FROM stream_assignments WHERE topic=? AND name=?",
            params![topic, group],
        )?;
        return Ok(());
    }
    let previous = tx
        .prepare_cached(
            "SELECT partition,member_epoch FROM stream_assignments WHERE topic=? AND name=?",
        )?
        .query_map(params![topic, group], |r| {
            Ok((r.get::<_, u32>(0)?, r.get::<_, i64>(1)?))
        })?
        .collect::<rusqlite::Result<BTreeMap<_, _>>>()?;
    let mut counts = BTreeMap::<i64, usize>::new();
    for member in previous.values() {
        *counts.entry(*member).or_default() += 1;
    }
    // Give the extra partitions to the members that can retain the most work.
    members.sort_unstable_by_key(|member| {
        (Reverse(counts.get(member).copied().unwrap_or(0)), *member)
    });
    let mut slots: BTreeMap<_, _> = members
        .iter()
        .enumerate()
        .map(|(index, &member)| {
            (
                member,
                partitions.len() / members.len()
                    + usize::from(index < partitions.len() % members.len()),
            )
        })
        .collect();
    let mut unassigned = Vec::new();
    for &partition in partitions {
        match previous
            .get(&partition)
            .and_then(|member| slots.get_mut(member))
        {
            Some(remaining) if *remaining != 0 => *remaining -= 1,
            _ => unassigned.push(partition),
        }
    }
    for (partition, member) in unassigned.into_iter().zip(
        slots
            .into_iter()
            .flat_map(|(member, count)| std::iter::repeat_n(member, count)),
    ) {
        let epoch = next_epoch(tx)?;
        tx.execute(
            "INSERT INTO stream_assignments VALUES(?,?,?,?,?) ON CONFLICT(topic,name,partition)
             DO UPDATE SET member_epoch=excluded.member_epoch,assignment_epoch=excluded.assignment_epoch",
            params![topic, group, partition, member, epoch],
        )?;
    }
    Ok(())
}
