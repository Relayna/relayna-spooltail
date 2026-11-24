use super::*;
use std::{fs::OpenOptions, io::Write};
use tempfile::tempdir;

fn record(id: &str) -> Value {
    json!({"task_id":"T7","event_id":id,"status":"running","meta":{"level":1}})
}

#[tokio::test]
async fn append_during_replay_is_delivered_after_the_page() -> Result<()> {
    let dir = tempdir()?;
    let mut store = Store::open(dir.path())?;
    for id in ["E41", "E42"] {
        store.append("status", record(id))?;
    }
    let b = Broker {
        store: Arc::new(Mutex::new(store)),
        watchers: Default::default(),
    };
    let mut sub = feed(b.clone(), "status".into(), "T7".into(), Some("E41".into()));
    assert_eq!(sub.next().await.unwrap().kind, "ready");
    assert_eq!(sub.next().await.unwrap().id.as_deref(), Some("E42"));
    // The generator is suspended in its replay page, before its first live wait.
    let reply = append(
        State(b),
        Path("status".into()),
        Query(AppendQuery {
            owner_service: None,
        }),
        Json(record("E43")),
    )
    .await
    .map_err(|e| e.0)?;
    assert_eq!(reply.0["offset"], 3);
    let next = tokio::time::timeout(std::time::Duration::from_secs(1), sub.next()).await?;
    assert_eq!(next.unwrap().id.as_deref(), Some("E43"));
    Ok(())
}

#[test]
fn truncates_incomplete_tail_and_rejects_bad_checksum() -> Result<()> {
    let dir = tempdir()?;
    let mut store = Store::open(dir.path())?;
    let first = store.append("status", record("E1"))?;
    assert!(
        Store::open(dir.path()).is_err(),
        "requires exclusive volume ownership"
    );
    drop(store);
    let path = dir.path().join("00000000000000000000.seg");
    let end = path.metadata()?.len();
    let mut file = OpenOptions::new().append(true).open(&path)?;
    file.write_all(&20u32.to_le_bytes())?;
    file.write_all(&0u32.to_le_bytes())?;
    file.write_all(b"torn")?;
    file.sync_all()?;
    let mut store = Store::open(dir.path())?;
    assert_eq!(path.metadata()?.len(), end);
    assert_eq!(store.read(first)?.event, record("E1"));
    assert_eq!(store.append("status", record("E2"))?, first + 1);
    drop(store);
    file.write_all(&2u32.to_le_bytes())?;
    file.write_all(&0u32.to_le_bytes())?;
    file.write_all(b"{}")?;
    file.sync_all()?;
    assert!(
        Store::open(dir.path())
            .err()
            .unwrap()
            .to_string()
            .contains("checksum")
    );
    Ok(())
}

#[tokio::test]
async fn partition_can_write_ahead_of_global_visibility() -> Result<()> {
    use std::time::Duration;

    let dir = tempdir()?;
    let mut store = Store::open_mode(dir.path(), Some("partitioned"))?;
    store.configure_index_batch_size(8)?;
    store.configure_service_partitions([("orders".into(), vec![1])].into())?;
    let slow_journal = store.journal(0)?;
    let mut earlier = Frame::new("status".into(), record("earlier"), None)?;
    let Prepared::Write(body) = store.prepare_append(&mut earlier, 0)? else {
        panic!("expected a new record");
    };
    let broker = Broker {
        store: Arc::new(Mutex::new(store)),
        watchers: Default::default(),
    };
    let publish_order = |task: &str| {
        tokio::spawn(publish(
            broker.clone(),
            "status".into(),
            json!({"task_id":task,"event_id":"placed"}),
            None,
            Some("orders".into()),
        ))
    };
    let first = publish_order("order-a");
    let second = publish_order("order-b");
    let staged = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if broker
                .run(|s| Ok(s.is_appending("order-a")? && s.is_appending("order-b")?))
                .await?
            {
                return Ok::<_, anyhow::Error>(
                    !first.is_finished()
                        && !second.is_finished()
                        && broker
                            .run(|s| s.group_next("status", "audit", None))
                            .await?
                            .is_none(),
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let duplicate = publish_order("order-a");
    // Release the earlier reservation even if staging timed out, so the
    // blocking publishers can finish and the test runtime can shut down.
    let write = slow_journal.lock().await.append(&earlier, 0, &body);
    let earliest = finish_publish(&broker, &earlier, 0, write)?;
    let first = first.await??;
    let second = second.await??;
    let repeated = duplicate.await??;
    assert!(staged??, "a later record became visible across the gap");
    assert_eq!(first.partition, 1);
    assert_eq!(second.partition, 1);
    assert_eq!(repeated.offset, first.offset);
    assert_eq!(
        [
            first.offset.min(second.offset),
            first.offset.max(second.offset)
        ],
        [earliest.offset + 1, earliest.offset + 2]
    );
    Ok(())
}

#[test]
fn grouped_sync_seals_segments_before_rotation() -> Result<()> {
    let dir = tempdir()?;
    let mut store = Store::open_mode(dir.path(), Some("partitioned"))?;
    store.configure_index_batch_size(8)?;
    let payload = "x".repeat(1_000_000);
    let mut staged = Vec::new();
    {
        let journal = store.journal(0)?;
        let mut journal = journal.blocking_lock();
        for n in 0..9 {
            let mut frame = Frame::new(
                "status".into(),
                json!({"task_id":format!("order-{n}"),"event_id":"placed","payload":payload}),
                None,
            )?;
            let Prepared::Write(body) = store.prepare_append(&mut frame, 0)? else {
                panic!("expected a new record");
            };
            let append = journal.stage_append(Arc::new(frame), 0, &body)?;
            assert_eq!(append.leader, n == 0 || n == 8);
            staged.push(append);
        }
        // The first eight receipts must be ready with the next segment open,
        // even though its own sync has not been called.
        for append in staged.drain(..8) {
            assert_eq!(append.wait()?.segment, 0);
        }
        journal.flush_staged()?;
        assert_eq!(staged.pop().unwrap().wait()?.segment, 1);
    }
    assert!(store.group_next("status", "audit", None)?.is_none());
    drop(store);
    let store = Store::open(dir.path())?;
    assert_eq!(store.read(1)?.event["task_id"], "order-0");
    assert_eq!(store.read(9)?.event["task_id"], "order-8");
    Ok(())
}

#[test]
fn group_created_before_retention_cannot_skip_unprocessed_record() -> Result<()> {
    let dir = tempdir()?;
    let mut store = Store::open(dir.path())?;
    let first = store.append("status", record("one"))?;
    let next = store.append("status", record("two"))?;
    assert_eq!(
        store.group_next("status", "audit", None)?.unwrap().offset,
        first
    );
    store.expire_before("status", next)?;
    assert!(store.group_next("status", "audit", None).is_err());
    assert_eq!(
        store
            .group_next("status", "new-group", None)?
            .unwrap()
            .offset,
        next
    );
    assert_eq!(
        store
            .claim("status", "worker", 1000, None)?
            .unwrap()
            .record
            .offset,
        first
    );
    Ok(())
}

#[test]
fn delayed_retry_has_no_active_completion_lease() -> Result<()> {
    let dir = tempdir()?;
    let mut store = Store::open(dir.path())?;
    let offset = store.append("work", record("queued"))?;
    let claim = store.claim("work", "workers", 60_000, None)?.unwrap();
    store.settle("work", "workers", offset, claim.generation, "retry", 60_000)?;
    for generation in [claim.generation, claim.generation + 1] {
        let lease = WorkLease {
            topic: "work".into(),
            group: "workers".into(),
            offset,
            generation,
        };
        let event = json!({"task_id":"T7","event_id":"final","status":"completed"});
        let err = store.complete(lease, "status", event).unwrap_err();
        assert!(matches!(
            err.downcast_ref::<Problem>(),
            Some(Problem::Conflict(_))
        ));
    }
    assert!(store.page("status", "T7", 0)?.is_empty());
    assert!(store.claim("work", "workers", 1000, None)?.is_none());
    Ok(())
}

#[test]
fn torn_completion_preserves_the_lease_and_publishes_nothing() -> Result<()> {
    let dir = tempdir()?;
    let mut store = Store::open(dir.path())?;
    let offset = store.append("work", record("queued"))?;
    let claim = store.claim("work", "workers", 60_000, None)?.unwrap();
    let lease = || WorkLease {
        topic: "work".into(),
        group: "workers".into(),
        offset,
        generation: claim.generation,
    };
    let event = json!({"task_id":"T7","event_id":"final","status":"completed"});
    let segment = dir.path().join("00000000000000000000.seg");
    let before = segment.metadata()?.len();
    // Fail the lease update after the index insert in the same transaction.
    store.db.execute_batch(
        "CREATE TEMP TRIGGER fail_settlement BEFORE UPDATE ON leases
         BEGIN SELECT RAISE(ABORT,'injected failure'); END;",
    )?;
    let error = store
        .complete(lease(), "status", event.clone())
        .unwrap_err();
    assert!(error.to_string().contains("injected failure"));
    assert!(store.page("status", "T7", 0)?.is_empty());
    store.db.execute_batch("DROP TRIGGER fail_settlement")?;
    assert!(store.claim("work", "workers", 1000, None).is_err());
    assert!(
        store
            .settle("work", "workers", offset, claim.generation, "ack", 0)
            .is_err()
    );
    drop(store);

    // Simulate a torn final frame. Recovery must discard the entire operation.
    let file = OpenOptions::new().write(true).open(&segment)?;
    file.set_len(file.metadata()?.len() - 1)?;
    file.sync_all()?;
    let mut store = Store::open(dir.path())?;
    assert_eq!(store.recovered_frames, 0);
    assert_eq!(segment.metadata()?.len(), before);
    assert!(store.page("status", "T7", 0)?.is_empty());
    let status_offset = store.complete(lease(), "status", event.clone())?;
    assert_eq!(store.complete(lease(), "status", event)?, status_offset);
    assert!(store.claim("work", "workers", 1000, None)?.is_none());
    Ok(())
}

#[test]
fn rolling_segments_preserves_indexed_reads_and_topic_isolation() -> Result<()> {
    let dir = tempdir()?;
    let mut store = Store::open(dir.path())?;
    for i in 0..10 {
        let mut event = record(&format!("E{i}"));
        event["payload"] = Value::String("x".repeat(950_000));
        store.append("bulk", event)?;
    }
    let offset = store.append("status", record("E0"))?;
    drop(store);
    let store = Store::open(dir.path())?;
    assert!(dir.path().join("00000000000000000001.seg").exists());
    assert_eq!(store.recovered_frames, 0);
    assert_eq!(store.latest("status", "T7")?.offset, offset);
    assert_eq!(store.latest("bulk", "T7")?.event["event_id"], "E9");
    Ok(())
}

#[test]
fn task_history_pages_sparse_records_across_a_partition_move() -> Result<()> {
    for mode in ["shared", "partitioned"] {
        let dir = tempdir()?;
        let mut store = Store::open_mode(dir.path(), Some(mode))?;
        let mut expected = Vec::new();
        for n in 0..67 {
            if n == 32 {
                store.ensure_partition(1)?;
                store.move_task("T7", 0, 1)?;
            }
            let event = record(&format!("E{n}"));
            let offset = store.append("status", event.clone())?;
            expected.push((offset, u32::from(n >= 32), event.clone()));
            store.append("other-topic", event.clone())?;
            let mut other_task = event;
            other_task["task_id"] = json!("other-task");
            store.append("status", other_task)?;
        }
        drop(store);
        let store = Store::open(dir.path())?;
        for (cursor, expected_page) in [(None, &expected[..64]), (Some("E63"), &expected[64..])] {
            let after = store.resolve("status", "T7", cursor)?;
            let before = store.payload_reads.get();
            let page = store.page("status", "T7", after)?;
            let actual: Vec<_> = page
                .into_iter()
                .map(|r| (r.offset, r.partition, r.event))
                .collect();
            assert_eq!(actual, expected_page);
            assert_eq!(store.payload_reads.get() - before, actual.len() as u64);
        }
        let end = store.resolve("status", "T7", Some("E66"))?;
        assert!(store.page("status", "T7", end)?.is_empty());
    }
    Ok(())
}

#[test]
fn task_history_reports_a_gap_if_retention_advances_between_pages() -> Result<()> {
    let dir = tempdir()?;
    let mut store = Store::open(dir.path())?;
    store.append("status", record("expired"))?;
    let after = store.resolve("status", "T7", Some("expired"))?;
    store.append("status", record("also-expired"))?;
    let retained = store.append("status", record("retained"))?;
    store.expire_before("status", retained)?;
    let err = store.page("status", "T7", after).unwrap_err();
    assert!(
        matches!(err.downcast_ref::<Problem>(), Some(Problem::Gap { earliest }) if earliest.as_deref() == Some("retained"))
    );
    let start = store.resolve("status", "T7", None)?;
    let page = store.page("status", "T7", start)?;
    assert_eq!(
        page.iter().map(|r| r.offset).collect::<Vec<_>>(),
        [retained]
    );

    store.expire_before("status", retained + 1)?;
    let err = store.page("status", "T7", after).unwrap_err();
    assert!(matches!(
        err.downcast_ref::<Problem>(),
        Some(Problem::Gap { earliest: None })
    ));
    Ok(())
}

#[test]
fn stream_batch_reads_sparse_records_across_partitions_and_segment_rollover() -> Result<()> {
    let dir = tempdir()?;
    let mut store = Store::open(dir.path())?;
    let large = |id: &str| {
        let mut event = record(id);
        event["payload"] = Value::String("x".repeat(950_000));
        event
    };
    for i in 0..6 {
        store.append("other", large(&format!("fill-{i}")))?;
    }
    let first = large("first");
    let first_offset = store.append("status", first.clone())?;
    store.append("other", large("skipped"))?;

    store.configure_service_partitions([("other-partition".into(), vec![2])].into())?;
    let mut other = record("from-partition-two");
    other["task_id"] = json!("another-task");
    assert_eq!(
        store.partition_for("another-task", Some("other-partition"))?,
        2
    );
    let other_offset = store.append("status", other.clone())?;

    let second = large("on-next-segment");
    let second_offset = store.append("status", second.clone())?;
    let third = record("same-segment");
    let third_offset = store.append("status", third.clone())?;
    let tail_offset = store.append("status", large("over-byte-limit"))?;
    assert!(dir.path().join("00000000000000000001.seg").exists());

    let before = store.payload_reads.get();
    let (after, page) = store.next_batch("status", "audit", None, 64)?;
    let expected = [
        (first_offset, 0, first),
        (other_offset, 2, other),
        (second_offset, 0, second),
        (third_offset, 0, third),
    ];
    assert_eq!(page.len(), expected.len());
    for (record, (offset, partition, event)) in page.into_iter().zip(expected) {
        assert_eq!(
            (record.offset, record.partition, record.event),
            (offset, partition, event)
        );
    }
    let fetched = store.payload_reads.get();
    assert_eq!(fetched - before, 4);
    store.commit_batch("status", "audit", None, after, third_offset, 4)?;
    assert_eq!(store.payload_reads.get(), fetched);
    let (after, tail) = store.next_batch("status", "audit", None, 64)?;
    assert_eq!(after, third_offset);
    assert_eq!(
        tail.iter().map(|record| record.offset).collect::<Vec<_>>(),
        [tail_offset]
    );
    Ok(())
}

#[test]
fn stateless_replay_obeys_retention_without_creating_checkpoints() -> Result<()> {
    let dir = tempdir()?;
    let mut store = Store::open(dir.path())?;
    let expired = store.append("status", record("expired"))?;
    store.append("other", record("interleaved"))?;
    let retained = store.append("status", record("retained"))?;

    let (after, records) = store.read_after_batch("status", Some(expired), 1)?;
    assert_eq!(after, expired);
    assert_eq!(records[0].offset, retained);
    store.expire_before("status", retained)?;
    assert!(store.read_after_batch("status", Some(expired), 1).is_err());
    let (after, records) = store.read_after_batch("status", None, 1)?;
    assert_eq!(after, retained - 1);
    assert_eq!(records[0].offset, retained);
    assert!(
        store
            .read_after_batch("status", Some(retained), 1)?
            .1
            .is_empty()
    );
    assert!(store.read_after_batch("status", Some(-1), 1).is_err());
    assert!(store.read_after_batch("status", None, 65).is_err());
    let count: i64 = store
        .db
        .query_row("SELECT count(*) FROM cursors", [], |row| row.get(0))?;
    assert_eq!(count, 0);
    Ok(())
}

#[test]
fn stream_batch_commits_a_consecutive_prefix_and_recovers_checkpoint() -> Result<()> {
    let dir = tempdir()?;
    let mut store = Store::open(dir.path())?;
    let first = store.append("status", record("first"))?;
    let unrelated = store.append("other", record("unrelated"))?;
    let second = store.append("status", record("second"))?;
    let third = store.append("status", record("third"))?;
    assert!(store.next_batch("status", "audit", None, 0).is_err());
    assert!(store.next_batch("status", "audit", None, 65).is_err());
    let (after, records) = store.next_batch("status", "audit", None, 2)?;
    assert_eq!(
        (after, records.iter().map(|r| r.offset).collect::<Vec<_>>()),
        (0, vec![first, second])
    );
    assert!(
        store
            .commit_batch("status", "audit", None, after, unrelated, 2)
            .is_err()
    );
    assert!(
        store
            .commit_batch("status", "audit", None, after, third, 2)
            .is_err()
    );
    assert!(
        store
            .commit_batch("status", "audit", None, after, first, 65)
            .is_err()
    );
    store.commit_batch("status", "audit", None, after, first, 1)?;
    store.commit_batch("status", "audit", None, after, first, 1)?;
    assert!(
        store
            .commit_batch("status", "audit", None, after, second, 2)
            .is_err()
    );

    drop(store);
    let mut store = Store::open(dir.path())?;
    assert_eq!(
        store.group_next("status", "audit", None)?.unwrap().offset,
        second
    );
    let (after, records) = store.next_batch("status", "audit", None, 64)?;
    assert_eq!(after, first);
    assert_eq!(
        records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        [second, third]
    );
    store.expire_before("status", third)?;
    assert!(store.next_batch("status", "audit", None, 64).is_err());
    assert!(
        store
            .commit_batch("status", "audit", None, after, third, 2)
            .is_err()
    );
    let (after, records) = store.next_batch("status", "new-audit", None, 64)?;
    assert_eq!(after, third - 1);
    assert_eq!(records[0].offset, third);
    Ok(())
}

#[test]
fn partition_batch_stops_before_a_moved_tasks_uncommitted_predecessor() -> Result<()> {
    let dir = tempdir()?;
    let mut store = Store::open(dir.path())?;
    let source = store.append("status", record("source"))?;
    store.configure_service_partitions([("target".into(), vec![2])].into())?;
    let mut local = record("local");
    local["task_id"] = json!("local-task");
    assert_eq!(store.partition_for("local-task", Some("target"))?, 2);
    let ready = store.append("status", local.clone())?;
    store.move_task("T7", 0, 2)?;
    let blocked = store.append("status", record("destination"))?;
    local["event_id"] = json!("later");
    let later = store.append("status", local)?;
    let (after, batch) = store.next_batch("status", "audit", Some(2), 32)?;
    assert_eq!(batch.iter().map(|r| r.offset).collect::<Vec<_>>(), [ready]);
    assert!(
        store
            .commit_batch(
                "status",
                "audit",
                Some(2),
                after,
                blocked,
                batch.len() as u32 + 1
            )
            .is_err()
    );
    store.commit_batch("status", "audit", Some(2), after, ready, batch.len() as u32)?;
    assert!(
        store
            .next_batch("status", "audit", Some(2), 32)?
            .1
            .is_empty()
    );
    let (after, _) = store.next_batch("status", "audit", Some(0), 32)?;
    store.commit_batch("status", "audit", Some(0), after, source, 1)?;
    let (after, batch) = store.next_batch("status", "audit", Some(2), 32)?;
    assert_eq!(after, ready);
    assert_eq!(
        batch.iter().map(|r| r.offset).collect::<Vec<_>>(),
        [blocked, later]
    );
    assert!(store.next_batch("status", "audit", None, 32).is_err());
    Ok(())
}

#[test]
fn expansion_and_move_preserve_offsets_leases_and_stream_order() -> Result<()> {
    let dir = tempdir()?;
    let mut store = Store::open(dir.path())?;
    let first = store.append("status", record("started"))?;
    let work = store.append("work", record("queued"))?;
    let claim = store.claim("work", "workers", 60_000, None)?.unwrap();
    assert_eq!(claim.record.offset, work);
    assert_eq!(
        store.group_next("status", "audit", None)?.unwrap().offset,
        first
    );
    store.commit("status", "audit", first, None)?;

    store.ensure_partition(1)?;
    store.ensure_partition(2)?;
    store.ensure_partition(2)?;
    assert_eq!(store.partition_ids(), [0, 1, 2]);
    assert_eq!(store.task_partition("T7")?, 0);
    let second = store.append("status", record("running"))?;
    assert_eq!(store.read(second)?.partition, 0);

    store.move_task("T7", 0, 2)?;
    store.move_task("T7", 0, 2)?;
    assert!(store.move_task("T7", 0, 1).is_err());
    assert!(store.claim("work", "workers", 1000, Some(2))?.is_none());
    assert!(store.claim("work", "secondary", 1000, Some(0))?.is_none());
    let independent = store.claim("work", "secondary", 1000, Some(2))?.unwrap();
    assert_eq!(
        (independent.partition, independent.record.partition),
        (2, 0)
    );
    assert_eq!(independent.record.offset, work);
    let completion = || WorkLease {
        topic: "work".into(),
        group: "workers".into(),
        offset: work,
        generation: claim.generation,
    };
    let mut terminal = record("done");
    terminal["status"] = json!("completed");
    let third = store.complete(completion(), "status", terminal.clone())?;
    assert_eq!(store.read(third)?.partition, 2);
    assert_eq!(store.append("status", record("started"))?, first);

    // An old global reader retains its view. A reader opting into partitions
    // starts at the old checkpoint and consumes the source before the target.
    assert_eq!(
        store.group_next("status", "legacy", None)?.unwrap().offset,
        first
    );
    assert!(store.group_next("status", "audit", Some(2))?.is_none());
    assert_eq!(
        store
            .group_next("status", "audit", Some(0))?
            .unwrap()
            .offset,
        second
    );
    assert!(store.commit("status", "audit", third, Some(2)).is_err());
    store.commit("status", "audit", second, Some(0))?;
    assert_eq!(
        store
            .group_next("status", "audit", Some(2))?
            .unwrap()
            .offset,
        third
    );
    store.commit("status", "audit", third, Some(2))?;

    drop(store);
    let mut store = Store::open(dir.path())?;
    assert_eq!(store.task_partition("T7")?, 2);
    assert_eq!(store.complete(completion(), "status", terminal)?, third);
    assert!(store.group_next("status", "audit", Some(2))?.is_none());
    let after = store.resolve("status", "T7", Some("started"))?;
    let history = store.page("status", "T7", after)?;
    assert_eq!(
        history
            .iter()
            .map(|r| (r.offset, r.partition))
            .collect::<Vec<_>>(),
        [(second, 0), (third, 2)]
    );
    Ok(())
}

#[test]
fn recovers_cross_partition_completion_from_destination_journal() -> Result<()> {
    let dir = tempdir()?;
    let mut store = Store::open(dir.path())?;
    let work = store.append("work", record("queued"))?;
    let claim = store.claim("work", "workers", 30_000, None)?.unwrap();
    store.ensure_partition(1)?;
    store.move_task("T7", 0, 1)?;
    let lease = || WorkLease {
        topic: "work".into(),
        group: "workers".into(),
        offset: work,
        generation: claim.generation,
    };
    let terminal = json!({"task_id":"T7","event_id":"final","status":"completed"});
    let mut frame = Frame::new("status".into(), terminal.clone(), Some(lease()))?;
    let Prepared::Write(body) = store.prepare_append(&mut frame, 1)? else {
        panic!("expected to write the terminal event");
    };
    store.journal(1)?.blocking_lock().write(&body)?;
    // Another broker operation cannot settle this lease while its completion is on disk.
    assert!(
        store
            .settle("work", "workers", work, claim.generation, "ack", 0)
            .is_err()
    );
    drop(store);

    let mut store = Store::open(dir.path())?;
    assert_eq!(store.recovered_frames, 1);
    let recovered = store.latest("status", "T7")?;
    assert_eq!(recovered.partition, 1);
    assert_eq!(
        store.complete(lease(), "status", terminal)?,
        recovered.offset
    );
    assert!(store.claim("work", "workers", 1000, Some(1))?.is_none());
    Ok(())
}

#[tokio::test]
async fn expansion_during_publish_keeps_placement_and_other_journals_can_write() -> Result<()> {
    let dir = tempdir()?;
    let mut store = Store::open(dir.path())?;
    let new_task = (0..100)
        .map(|id| format!("new-{id}"))
        .find(|id| crc32fast::hash(id.as_bytes()) % 2 == 1)
        .unwrap();
    let event = json!({"task_id":new_task,"event_id":"first"});
    let mut frame = Frame::new("status".into(), event.clone(), None)?;
    let Prepared::Write(body) = store.prepare_append(&mut frame, 0)? else {
        panic!("expected to write the first event");
    };
    store.ensure_partition(1)?;
    assert_eq!(store.partition_for(&new_task, None)?, 0);
    let journal = store.journal(0)?;
    let write = journal.lock().await.write(&body);
    let initial = store.finish_append(&frame, 0, write)?;
    store.move_task(&new_task, 0, 1)?;
    let fresh = (0..100)
        .map(|id| format!("fresh-{id}"))
        .find(|id| {
            let hash = crc32fast::hash(id.as_bytes());
            hash % 2 == 1 && hash % 3 == 2
        })
        .unwrap();
    let fresh_offset = store.append("status", json!({"task_id":fresh,"event_id":"first"}))?;
    assert_eq!(store.read(fresh_offset)?.partition, 1);
    store.ensure_partition(2)?;
    assert_eq!(store.task_partition(&fresh)?, 1);
    let next_offset = store.append("status", json!({"task_id":fresh,"event_id":"second"}))?;
    assert_eq!(store.read(next_offset)?.partition, 1);
    let broker = Broker {
        store: Arc::new(Mutex::new(store)),
        watchers: Default::default(),
    };
    let (locked, ready) = tokio::sync::oneshot::channel();
    let (unlock, released) = tokio::sync::oneshot::channel();
    let holder = tokio::task::spawn_blocking(move || {
        let _guard = journal.blocking_lock();
        locked.send(()).unwrap();
        let _ = released.blocking_recv();
    });
    ready.await?;
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        publish(
            broker.clone(),
            "status".into(),
            json!({"task_id":new_task,"event_id":"second"}),
            None,
            None,
        ),
    )
    .await;
    unlock.send(()).unwrap();
    holder.await?;
    let position = outcome??;
    assert_eq!(position.partition, 1);
    assert!(position.offset > initial.offset);
    assert_eq!(
        publish(broker, "status".into(), event, None, None)
            .await?
            .offset,
        initial.offset
    );
    Ok(())
}

#[test]
fn opens_legacy_index_without_rewriting_offsets_or_progress() -> Result<()> {
    let dir = tempdir()?;
    let event = record("queued");
    let frame = Frame::new("work".into(), event.clone(), None)?;
    let address = journal::Journal::create(dir.path())?.write(&serde_json::to_vec(&frame)?)?;
    let db = rusqlite::Connection::open(dir.path().join("index.sqlite"))?;
    db.execute_batch(
        "CREATE TABLE records (
            offset INTEGER PRIMARY KEY, topic TEXT NOT NULL, task TEXT NOT NULL,
            event_id TEXT NOT NULL, segment INTEGER NOT NULL, pos INTEGER NOT NULL,
            len INTEGER NOT NULL, UNIQUE(topic,task,event_id));
         CREATE TABLE cursors (topic TEXT, name TEXT, offset INTEGER NOT NULL, PRIMARY KEY(topic,name));
         CREATE TABLE leases (topic TEXT, name TEXT, offset INTEGER, generation INTEGER NOT NULL,
            visible_at INTEGER NOT NULL, state TEXT NOT NULL, PRIMARY KEY(topic,name,offset));
         INSERT INTO cursors VALUES('work','audit',41);
         INSERT INTO leases VALUES('work','workers',41,3,9223372036854775807,'pending');",
    )?;
    db.execute(
        "INSERT INTO records VALUES(41,'work','T7','queued',0,0,?)",
        [i64::try_from(address.len)?],
    )?;
    drop(db);

    let mut store = Store::open(dir.path())?;
    assert_eq!(store.recovered_frames, 0);
    assert_eq!(store.read(41)?.event, event);
    assert!(store.group_next("work", "audit", None)?.is_none());
    store.ensure_partition(1)?;
    store.move_task("T7", 0, 1)?;
    let lease = WorkLease {
        topic: "work".into(),
        group: "workers".into(),
        offset: 41,
        generation: 3,
    };
    let terminal = json!({"task_id":"T7","event_id":"done","status":"completed"});
    assert_eq!(store.complete(lease, "status", terminal)?, 42);
    assert_eq!(store.read(42)?.partition, 1);
    assert_eq!(store.read(41)?.partition, 0);
    Ok(())
}

#[test]
fn split_indexes_publish_in_offset_order_and_recover_a_committed_suffix() -> Result<()> {
    let dir = tempdir()?;
    let mut store = Store::open(dir.path())?;
    let previous = store.append("status", record("legacy"))?;
    let cursor = store.group_next("status", "global", None)?.unwrap();
    assert_eq!(cursor.offset, previous);
    store.commit("status", "global", previous, None)?;
    store.ensure_partition(1)?;
    let other = json!({"task_id":"other","event_id":"legacy"});
    store.append("other-topic", other)?;
    store.move_task("other", 0, 1)?;
    drop(store);

    let mut store = Store::open_mode(dir.path(), Some("partitioned"))?;
    assert_eq!(store.latest("status", "T7")?.offset, previous);
    assert_eq!(store.task_partition("other")?, 1);
    assert!(store.group_next("status", "global", None)?.is_none());
    let mut earlier = Frame::new("status".into(), record("earlier"), None)?;
    let mut later = Frame::new(
        "status".into(),
        json!({"task_id":"other","event_id":"later"}),
        None,
    )?;
    let Prepared::Write(earlier_body) = store.prepare_append(&mut earlier, 0)? else {
        panic!("expected a new record");
    };
    let Prepared::Write(later_body) = store.prepare_append(&mut later, 1)? else {
        panic!("expected a new record");
    };
    let later_offset = later.offset.unwrap();
    let later_write = store
        .journal(1)?
        .blocking_lock()
        .append(&later, 1, &later_body);
    assert!(later_write.is_ok());
    assert!(!store.can_publish(later_offset));
    assert!(store.group_next("status", "global", None)?.is_none());
    assert!(store.latest("status", "other").is_err());
    let earlier_offset = earlier.offset.unwrap();
    let earlier_write = store
        .journal(0)?
        .blocking_lock()
        .append(&earlier, 0, &earlier_body);
    let earlier_receipt = store.finish_append(&earlier, 0, earlier_write)?;
    assert_eq!(earlier_receipt.offset, earlier_offset);
    assert!(store.can_publish(later_offset));
    assert_eq!(
        store.group_next("status", "global", None)?.unwrap().offset,
        earlier_offset
    );
    store.commit("status", "global", earlier_offset, None)?;
    // The second shard committed before the first one, but its result was never
    // published before restart. Both receipt identities are already durable.
    drop(store);
    assert!(Store::open_mode(dir.path(), Some("shared")).is_err());
    let mut store = Store::open(dir.path())?;
    assert_eq!(store.recovered_frames, 0);
    assert_eq!(
        store.group_next("status", "global", None)?.unwrap().offset,
        later_offset
    );
    store.commit("status", "global", later_offset, None)?;
    assert!(store.group_next("status", "global", None)?.is_none());
    assert_eq!(
        store.append("status", json!({"task_id":"other","event_id":"later"}))?,
        later_offset
    );
    // An incomplete earlier reservation must not strand a later durable record.
    let mut missing = Frame::new("status".into(), record("lost-reservation"), None)?;
    let mut survivor = Frame::new(
        "status".into(),
        json!({"task_id":"other","event_id":"survivor"}),
        None,
    )?;
    assert!(matches!(
        store.prepare_append(&mut missing, 0)?,
        Prepared::Write(_)
    ));
    let Prepared::Write(survivor_body) = store.prepare_append(&mut survivor, 1)? else {
        panic!("expected a new record");
    };
    store
        .journal(1)?
        .blocking_lock()
        .append(&survivor, 1, &survivor_body)?;
    drop(store);
    let mut store = Store::open(dir.path())?;
    assert_eq!(
        store.group_next("status", "global", None)?.unwrap().offset,
        survivor.offset.unwrap()
    );
    assert_eq!(
        store.append("status", record("after-gap"))?,
        survivor.offset.unwrap() + 1
    );
    Ok(())
}

#[test]
fn partition_index_rollback_replays_acknowledged_completion() -> Result<()> {
    let dir = tempdir()?;
    let mut store = Store::open_mode(dir.path(), Some("partitioned"))?;
    let work = store.append("work", record("queued"))?;
    let claim = store.claim("work", "workers", 30_000, None)?.unwrap();
    store.append("other-topic", json!({"task_id":"other","event_id":"seed"}))?;
    store.ensure_partition(1)?;
    store.move_task("T7", 0, 1)?;
    let started = store.append("status", record("started"))?;
    store.commit("status", "audit", started, None)?;

    let index = dir.path().join("partition-indexes/1.sqlite");
    let snapshot = dir.path().join("before-completion.sqlite");
    {
        let db = rusqlite::Connection::open(&index)?;
        db.execute("VACUUM INTO ?", [snapshot.to_str().unwrap()])?;
    }

    let lease = || WorkLease {
        topic: "work".into(),
        group: "workers".into(),
        offset: work,
        generation: claim.generation,
    };
    let terminal = json!({"task_id":"T7","event_id":"done","status":"completed"});
    let finished = store.complete(lease(), "status", terminal.clone())?;
    let later = store.append("status", json!({"task_id":"other","event_id":"later"}))?;
    drop(store);

    // Model a power loss that rolls back the partition WAL while the synced
    // journal and the coordinator's applied completion remain durable.
    std::fs::rename(snapshot, index)?;
    let mut store = Store::open(dir.path())?;
    assert_eq!(store.recovered_frames, 1);
    assert_eq!(store.task_partition("T7")?, 1);
    assert!(store.claim("work", "workers", 1000, Some(1))?.is_none());
    assert_eq!(store.complete(lease(), "status", terminal)?, finished);
    let next = store.group_next("status", "audit", None)?.unwrap();
    assert_eq!((next.partition, next.offset), (1, finished));
    store.commit("status", "audit", finished, None)?;
    let next = store.group_next("status", "audit", None)?.unwrap();
    assert_eq!((next.partition, next.offset), (0, later));
    store.commit("status", "audit", later, None)?;
    assert!(store.group_next("status", "audit", None)?.is_none());
    Ok(())
}

#[test]
fn service_rules_protect_existing_routes_and_reserve_partitions_for_new_tasks() -> Result<()> {
    let dir = tempdir()?;
    let mut store = Store::open(dir.path())?;
    store.append("status", record("legacy"))?;

    assert!(
        store
            .configure_service_partitions([("one".into(), vec![8]), ("two".into(), vec![8])].into())
            .is_err()
    );
    assert_eq!(store.partition_ids(), [0]);

    store.configure_service_partitions([("urgent".into(), vec![0])].into())?;
    assert_eq!(store.partition_for("T7", Some("unmapped"))?, 0);
    assert!(store.partition_for("new-general", None).is_err());
    assert_eq!(store.partition_for("new-urgent", Some("urgent"))?, 0);

    store.ensure_partition(3)?;
    assert_eq!(store.partition_for("new-general", Some("unmapped"))?, 3);
    assert_eq!(store.partition_for("new-general", Some("urgent"))?, 3);
    Ok(())
}

#[test]
fn dynamic_moves_wait_for_task_writes_and_fence_old_tickets() -> Result<()> {
    let dir = tempdir()?;
    let mut store = Store::open(dir.path())?;
    store.configure_service_partitions(
        [("urgent".into(), vec![1]), ("general".into(), vec![4])].into(),
    )?;
    let original = store.route_for("T7", Some("urgent"))?;
    for task in ["A", "B", "C", "D"] {
        assert_eq!(store.partition_for(task, Some("urgent"))?, 1);
    }
    store.configure_service_partitions(
        [("urgent".into(), vec![1, 2]), ("general".into(), vec![4])].into(),
    )?;
    store.dynamic_placement = true;
    let queued = ["A", "B", "C", "D"]
        .into_iter()
        .map(|task| store.writer_ticket(task, None))
        .collect::<Result<Vec<_>>>()?;
    assert!(queued.iter().all(|ticket| ticket.placement.partition == 1));
    assert_eq!(store.route_for("fresh", Some("urgent"))?.partition, 2);

    let mut frame = Frame::new("work".into(), record("queued"), None)?;
    let Prepared::Write(body) = store.prepare_append(&mut frame, 1)? else {
        panic!("expected a new event");
    };
    assert_eq!(store.route_for("T7", Some("general"))?, original);
    let write = store.journal(1)?.blocking_lock().append(&frame, 1, &body);
    let work = store.finish_append(&frame, 1, write)?.offset;
    let claim = store.claim("work", "worker", 30_000, Some(1))?.unwrap();
    let moved = store.route_for("T7", Some("general"))?;
    assert_eq!(moved.partition, 2);
    assert_eq!(store.automatic_moves, 1);
    assert_eq!(store.read(work)?.partition, 1);
    let terminal = json!({"task_id":"T7","event_id":"done","status":"completed"});
    let completion = store.complete(
        WorkLease {
            topic: "work".into(),
            group: "worker".into(),
            offset: work,
            generation: claim.generation,
        },
        "status",
        terminal,
    )?;
    assert_eq!(store.read(completion)?.partition, 2);
    drop(queued);
    store.move_task("T7", 2, 1)?;
    let returned = store.route_for("T7", None)?;
    assert_eq!(returned.partition, original.partition);
    assert_ne!(returned, original);
    drop(store);

    let mut store = Store::open(dir.path())?;
    assert_eq!(store.route_for("T7", None)?, returned);
    Ok(())
}
