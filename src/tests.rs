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
    let reply = append(State(b), Path("status".into()), Json(record("E43")))
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

#[test]
fn group_created_before_retention_cannot_skip_unprocessed_record() -> Result<()> {
    let dir = tempdir()?;
    let mut store = Store::open(dir.path())?;
    let first = store.append("status", record("one"))?;
    let next = store.append("status", record("two"))?;
    assert_eq!(store.group_next("status", "audit")?.unwrap().offset, first);
    store.expire_before("status", next)?;
    assert!(store.group_next("status", "audit").is_err());
    assert_eq!(
        store.group_next("status", "new-group")?.unwrap().offset,
        next
    );
    assert_eq!(
        store
            .claim("status", "worker", 1000)?
            .unwrap()
            .record
            .offset,
        first
    );
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
