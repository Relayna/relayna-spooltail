use crate::store::Store;
use axum::Json;
use serde_json::{Value, json};
use std::{
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Instant,
};

static CALLS: AtomicU64 = AtomicU64::new(0);
static WAIT_NS: AtomicU64 = AtomicU64::new(0);
static HOLD_NS: AtomicU64 = AtomicU64::new(0);

pub fn run<T>(store: &Mutex<Store>, operation: impl FnOnce(&mut Store) -> T) -> T {
    let start = Instant::now();
    let mut guard = store.lock().expect("store mutex poisoned");
    let acquired = Instant::now();
    let result = operation(&mut guard);
    let finished = Instant::now();
    drop(guard);
    WAIT_NS.fetch_add((acquired - start).as_nanos() as u64, Relaxed);
    HOLD_NS.fetch_add((finished - acquired).as_nanos() as u64, Relaxed);
    CALLS.fetch_add(1, Relaxed);
    result
}

// The driver reads this only when all request generators are idle.
pub async fn snapshot() -> Json<Value> {
    Json(json!({
        "calls": CALLS.load(Relaxed),
        "wait_ns": WAIT_NS.load(Relaxed),
        "hold_ns": HOLD_NS.load(Relaxed),
    }))
}
