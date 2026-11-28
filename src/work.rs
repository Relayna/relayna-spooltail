use crate::{Problem, Store, identifier, store::Frame, subscriptions::now};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::Arc,
    time::Instant,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const RETRY_WINDOW_SECONDS: u64 = 60;
const ACTIVE_LEASES: &str = "active AS MATERIALIZED (
    SELECT route.owner_service AS service, COUNT(*) AS count
    FROM leases a JOIN records current ON current.offset=a.offset AND current.topic=a.topic
    JOIN task_routes route ON route.task=current.task
    JOIN service_lease_limits quota ON quota.service=route.owner_service
    WHERE a.state='pending' AND a.visible_at>?3 GROUP BY route.owner_service
)";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Quota {
    max_in_flight: Option<u32>,
    max_pending_publishes: Option<u32>,
}

pub(crate) struct WorkPolicy {
    fair: bool,
    lease_limits: bool,
    publish_limits: BTreeMap<String, Arc<Semaphore>>,
    started: Instant,
    counters: HashMap<(String, String), WorkCounters>,
}

impl Default for WorkPolicy {
    fn default() -> Self {
        Self {
            fair: false,
            lease_limits: false,
            publish_limits: BTreeMap::new(),
            started: Instant::now(),
            counters: HashMap::new(),
        }
    }
}

type WorkCounters = HashMap<(String, u32), Counters>;

#[derive(Default)]
struct Counters {
    expired_lease_redeliveries: u64,
    retries: u64,
    retry_buckets: VecDeque<(u64, u64)>,
}

pub(crate) fn configure(store: &mut Store) -> Result<()> {
    let quotas: BTreeMap<String, Quota> = match std::env::var("SPOOLTAIL_SERVICE_QUOTAS") {
        Ok(value) => serde_json::from_str(&value)
            .context("SPOOLTAIL_SERVICE_QUOTAS must map services to quota objects")?,
        Err(std::env::VarError::NotPresent) => BTreeMap::new(),
        Err(error) => return Err(error).context("read SPOOLTAIL_SERVICE_QUOTAS"),
    };
    store.work.fair = match std::env::var("SPOOLTAIL_SERVICE_FAIRNESS") {
        Ok(value) if value == "1" => true,
        Ok(value) if value == "0" => false,
        Err(std::env::VarError::NotPresent) => false,
        _ => anyhow::bail!("SPOOLTAIL_SERVICE_FAIRNESS must be 0 or 1"),
    };
    for (service, quota) in quotas {
        identifier(&service).context("invalid service in SPOOLTAIL_SERVICE_QUOTAS")?;
        ensure!(
            quota.max_in_flight.is_some() || quota.max_pending_publishes.is_some(),
            "service {service} must have at least one quota"
        );
        if let Some(limit) = quota.max_in_flight {
            ensure!(
                limit > 0,
                "service {service} max_in_flight must be positive"
            );
            store.db.execute(
                "INSERT INTO service_lease_limits VALUES (?,?)",
                params![service, limit],
            )?;
            store.work.lease_limits = true;
        }
        if let Some(limit) = quota.max_pending_publishes {
            let limit = usize::try_from(limit)?;
            ensure!(
                (1..=Semaphore::MAX_PERMITS).contains(&limit),
                "service {service} max_pending_publishes is outside the supported range"
            );
            store
                .work
                .publish_limits
                .insert(service, Arc::new(Semaphore::new(limit)));
        }
    }
    if store.work.lease_limits {
        store.db.execute_batch(
            "CREATE INDEX IF NOT EXISTS active_lease_deadlines ON leases(visible_at) WHERE state='pending';",
        )?;
    }
    Ok(())
}

pub(crate) struct ClaimChoice {
    pub offset: i64,
    pub generation: i64,
    pub partition: u32,
    pub expired: bool,
    service: String,
}

impl WorkPolicy {
    pub fn choose(
        &self,
        tx: &Transaction<'_>,
        topic: &str,
        group: &str,
        timestamp: i64,
        partition: Option<u32>,
    ) -> Result<Option<ClaimChoice>> {
        let filter = if partition.is_some() {
            "r.work_partition=?4"
        } else {
            "?4 IS NULL"
        };
        let ready = "(l.offset IS NULL OR (l.state IN ('pending','retry') AND l.visible_at<=?3))
                     AND NOT EXISTS(SELECT 1 FROM appending WHERE work_offset=r.offset)";
        let scope = partition.map_or(-1, i64::from);
        let choice = if self.fair {
            let active = if self.lease_limits {
                ACTIVE_LEASES
            } else {
                "active(service,count) AS (SELECT NULL,0 WHERE 0)"
            };
            let query = format!(
                "WITH {active}, heads AS (
                     SELECT COALESCE(route.owner_service,'') AS service, MIN(r.offset) AS offset
                     FROM records r LEFT JOIN task_routes route ON route.task=r.task
                     LEFT JOIN leases l ON l.topic=r.topic AND l.name=?1 AND l.offset=r.offset
                     WHERE r.topic=?2 AND {filter} AND {ready} GROUP BY COALESCE(route.owner_service,'')
                 )
                 SELECT h.offset,COALESCE(l.generation,0)+1,r.work_partition,
                        COALESCE(l.state='pending',0),h.service
                 FROM heads h JOIN records r ON r.offset=h.offset
                 LEFT JOIN leases l ON l.topic=?2 AND l.name=?1 AND l.offset=h.offset
                 LEFT JOIN service_lease_limits quota ON quota.service=h.service
                 LEFT JOIN active a ON a.service=h.service
                 LEFT JOIN work_service_turns turn
                   ON turn.topic=?2 AND turn.name=?1 AND turn.scope=?5 AND turn.service=h.service
                 WHERE quota.service IS NULL OR COALESCE(a.count,0)<quota.max_in_flight
                 ORDER BY COALESCE(turn.served,0),h.offset LIMIT 1"
            );
            tx.prepare_cached(&query)?
                .query_row(
                    params![group, topic, timestamp, partition, scope],
                    read_choice,
                )
                .optional()?
        } else {
            let (active, limits, eligible) = if self.lease_limits {
                (
                    format!("WITH {ACTIVE_LEASES}"),
                    "LEFT JOIN task_routes route ON route.task=r.task
                     LEFT JOIN service_lease_limits quota ON quota.service=route.owner_service
                     LEFT JOIN active a ON a.service=route.owner_service",
                    "AND (quota.service IS NULL OR COALESCE(a.count,0)<quota.max_in_flight)",
                )
            } else {
                (String::new(), "", "")
            };
            let query = format!(
                "{active} SELECT r.offset,COALESCE(l.generation,0)+1,r.work_partition,
                        COALESCE(l.state='pending',0),''
                 FROM records r LEFT JOIN leases l ON l.topic=r.topic AND l.name=?1 AND l.offset=r.offset
                 {limits} WHERE r.topic=?2 AND {filter} AND {ready} {eligible} ORDER BY r.offset LIMIT 1"
            );
            tx.prepare_cached(&query)?
                .query_row(params![group, topic, timestamp, partition], read_choice)
                .optional()?
        };
        if self.fair
            && let Some(choice) = &choice
        {
            tx.execute(
                "INSERT INTO work_service_turns(topic,name,scope,service,served)
                 VALUES(?1,?2,?3,?4,1+COALESCE((SELECT MAX(served) FROM work_service_turns
                                              WHERE topic=?1 AND name=?2 AND scope=?3),0))
                 ON CONFLICT(topic,name,scope,service) DO UPDATE SET served=excluded.served",
                params![topic, group, scope, choice.service],
            )?;
        }
        Ok(choice)
    }

    fn counters(
        &mut self,
        topic: &str,
        group: &str,
        service: String,
        partition: u32,
    ) -> &mut Counters {
        self.counters
            .entry((topic.into(), group.into()))
            .or_default()
            .entry((service, partition))
            .or_default()
    }

    pub fn note_expiration(&mut self, topic: &str, group: &str, service: String, partition: u32) {
        self.counters(topic, group, service, partition)
            .expired_lease_redeliveries += 1;
    }

    pub fn note_retry(&mut self, topic: &str, group: &str, service: String, partition: u32) {
        let second = self.started.elapsed().as_secs();
        let counters = self.counters(topic, group, service, partition);
        counters.retries += 1;
        while counters
            .retry_buckets
            .front()
            .is_some_and(|&(at, _)| second - at >= RETRY_WINDOW_SECONDS)
        {
            counters.retry_buckets.pop_front();
        }
        match counters.retry_buckets.back_mut() {
            Some((at, count)) if *at == second => *count += 1,
            _ => counters.retry_buckets.push_back((second, 1)),
        }
    }
}

fn read_choice(row: &rusqlite::Row<'_>) -> rusqlite::Result<ClaimChoice> {
    Ok(ClaimChoice {
        offset: row.get(0)?,
        generation: row.get(1)?,
        partition: row.get(2)?,
        expired: row.get(3)?,
        service: row.get(4)?,
    })
}

pub(crate) fn record_owner(db: &Connection, offset: i64) -> Result<(String, u32)> {
    Ok(db
        .prepare_cached(
            "SELECT COALESCE(route.owner_service,''),r.work_partition FROM records r
         LEFT JOIN task_routes route ON route.task=r.task WHERE r.offset=?",
        )?
        .query_row([offset], |row| Ok((row.get(0)?, row.get(1)?)))?)
}

#[derive(Serialize)]
pub(crate) struct WorkStats {
    pub queue_depth: u64,
    pub ready: u64,
    pub leased: u64,
    pub delayed: u64,
    pub dead_lettered: u64,
    pub expired_leases: u64,
    pub oldest_ready_age_ms: Option<u64>,
    pub expired_lease_redeliveries_total: u64,
    pub retries_total: u64,
    pub retries_per_minute: u64,
}

impl Store {
    pub(crate) fn admit_service_publish(
        &self,
        task: &str,
        frame: &Frame,
    ) -> Result<Option<OwnedSemaphorePermit>> {
        if self.work.publish_limits.is_empty() {
            return Ok(None);
        }
        let owner: Option<String> = self
            .db
            .prepare_cached(
                "SELECT owner_service FROM task_routes WHERE task=? AND owner_service IS NOT NULL",
            )?
            .query_row([task], |row| row.get(0))
            .optional()?;
        let Some(limit) = owner
            .as_ref()
            .and_then(|owner| self.work.publish_limits.get(owner))
        else {
            return Ok(None);
        };
        if self.existing_event(frame)?.is_some() {
            return Ok(None);
        }
        Ok(Some(
            limit
                .clone()
                .try_acquire_owned()
                .map_err(|_| Problem::ServiceQuota)?,
        ))
    }

    pub fn work_stats(
        &self,
        topic: &str,
        group: &str,
        partition: Option<u32>,
        owner: Option<&str>,
    ) -> Result<WorkStats> {
        identifier(topic)?;
        identifier(group)?;
        if let Some(partition) = partition {
            self.validate_partition(partition)?;
        }
        if let Some(owner) = owner {
            identifier(owner)?;
        }
        if self.audit_only.contains(topic) {
            return Err(Problem::Invalid("audit-only topics have no work queues".into()).into());
        }
        let timestamp = now()?;
        let partition_filter = if partition.is_some() {
            "r.work_partition=?4"
        } else {
            "?4 IS NULL"
        };
        let query = format!(
            "SELECT COUNT(*) FILTER (WHERE l.state IS NULL OR (l.state IN ('pending','retry') AND l.visible_at<=?3)),
                    COUNT(*) FILTER (WHERE l.state='pending' AND l.visible_at>?3),
                    COUNT(*) FILTER (WHERE l.state='retry' AND l.visible_at>?3),
                    COUNT(*) FILTER (WHERE l.state='dead'),
                    COUNT(*) FILTER (WHERE l.state='pending' AND l.visible_at<=?3),
                    MIN(r.offset) FILTER (WHERE l.state IS NULL OR (l.state IN ('pending','retry') AND l.visible_at<=?3))
             FROM records r LEFT JOIN leases l ON l.topic=r.topic AND l.name=?2 AND l.offset=r.offset
             LEFT JOIN task_routes route ON route.task=r.task
             WHERE r.topic=?1 AND {partition_filter} AND (?5 IS NULL OR route.owner_service=?5)"
        );
        let (ready, leased, delayed, dead_lettered, expired_leases, oldest): (
            _,
            _,
            _,
            _,
            _,
            Option<i64>,
        ) = self.db.prepare_cached(&query)?.query_row(
            params![topic, group, timestamp, partition, owner],
            |r| {
                Ok((
                    r.get::<_, i64>(0)? as u64,
                    r.get::<_, i64>(1)? as u64,
                    r.get::<_, i64>(2)? as u64,
                    r.get::<_, i64>(3)? as u64,
                    r.get::<_, i64>(4)? as u64,
                    r.get(5)?,
                ))
            },
        )?;
        let oldest_ready_age_ms = oldest
            .map(|offset| self.accepted_at(offset))
            .transpose()?
            .flatten()
            .map(|created| timestamp.saturating_sub(created).max(0) as u64);
        let mut stats = WorkStats {
            queue_depth: ready + leased + delayed,
            ready,
            leased,
            delayed,
            dead_lettered,
            expired_leases,
            oldest_ready_age_ms,
            expired_lease_redeliveries_total: 0,
            retries_total: 0,
            retries_per_minute: 0,
        };
        let second = self.work.started.elapsed().as_secs();
        if let Some(counters) = self.work.counters.get(&(topic.into(), group.into())) {
            for ((service, placed), counter) in counters {
                if partition.is_none_or(|p| p == *placed)
                    && owner.is_none_or(|name| name == service)
                {
                    stats.expired_lease_redeliveries_total += counter.expired_lease_redeliveries;
                    stats.retries_total += counter.retries;
                    stats.retries_per_minute += counter
                        .retry_buckets
                        .iter()
                        .filter(|&&(at, _)| second - at < RETRY_WINDOW_SECONDS)
                        .map(|&(_, count)| count)
                        .sum::<u64>();
                }
            }
        }
        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Broker, publish};
    use serde_json::json;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn service_publish_quota_bounds_waiters_and_allows_committed_retries() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut store = Store::open(dir.path())?;
        let limit = Arc::new(Semaphore::new(1));
        store
            .work
            .publish_limits
            .insert("limited".into(), limit.clone());
        let journal = store.journal(0)?;
        let broker = Broker::new(store);
        let event = json!({"task_id":"first","event_id":"queued"});

        let writer = journal.lock().await;
        let first = tokio::spawn(publish(
            broker.clone(),
            "work".into(),
            event.clone(),
            None,
            Some("limited".into()),
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while limit.available_permits() != 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            publish(
                broker.clone(),
                "another.work".into(),
                json!({"task_id":"second","event_id":"queued"}),
                None,
                Some("limited".into()),
            ),
        )
        .await?
        .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<Problem>(),
            Some(Problem::ServiceQuota)
        ));
        drop(writer);
        let first = first.await??;
        assert_eq!(limit.available_permits(), 1);

        let _full = limit.try_acquire_owned()?;
        let retried = publish(broker, "work".into(), event, None, None).await?;
        assert_eq!(retried.offset, first.offset);
        Ok(())
    }
}
