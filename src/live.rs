//! A short live window for each watched task. The journal supplies replay and overflow.
use crate::store::terminal_status;
use serde_json::{Value, value::RawValue};
use std::{
    collections::HashMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicI64, Ordering},
    },
};
use tokio::sync::broadcast;

// Each event fits in the journal's 1 MiB frame limit. Only watched tasks own a window.
pub(crate) const LIVE_SLOTS: usize = 8;

pub(crate) struct Delivery {
    pub kind: &'static str,
    pub data: Box<RawValue>,
    pub id: Option<String>,
    pub terminal: bool,
}

impl Delivery {
    pub fn new(kind: &'static str, data: &Value) -> Arc<Self> {
        Arc::new(Self {
            kind,
            data: serde_json::value::to_raw_value(data).expect("JSON value is serializable"),
            id: data["event_id"].as_str().map(str::to_owned),
            terminal: terminal_status(data),
        })
    }
}

type Notice = (i64, Arc<Delivery>);

pub(crate) struct LiveTask {
    sender: broadcast::Sender<Notice>,
    floor: AtomicI64,
}

impl LiveTask {
    pub fn can_deliver_after(&self, position: i64) -> bool {
        position >= self.floor.load(Ordering::Acquire) - 1
    }
}

#[derive(Default)]
pub(crate) struct LiveTasks(HashMap<String, HashMap<String, Weak<LiveTask>>>);

impl LiveTasks {
    pub fn subscribe(
        &mut self,
        topic: &str,
        task: &str,
    ) -> (Arc<LiveTask>, broadcast::Receiver<Notice>) {
        let existing = self
            .0
            .get(topic)
            .and_then(|tasks| tasks.get(task))
            .and_then(Weak::upgrade);
        let channel = existing.unwrap_or_else(|| {
            self.0.retain(|_, tasks| {
                tasks.retain(|_, entry| entry.strong_count() > 0);
                !tasks.is_empty()
            });
            let channel = Arc::new(LiveTask {
                sender: broadcast::channel(LIVE_SLOTS).0,
                // Every new reader resolves its durable cursor before subscribing, under the same lock.
                floor: AtomicI64::new(1),
            });
            self.0
                .entry(topic.to_owned())
                .or_default()
                .insert(task.to_owned(), Arc::downgrade(&channel));
            channel
        });
        let receiver = channel.sender.subscribe();
        (channel, receiver)
    }

    pub fn publish(&self, topic: &str, task: &str, offset: i64, event: &Value) {
        if let Some(channel) = self
            .0
            .get(topic)
            .and_then(|tasks| tasks.get(task))
            .and_then(Weak::upgrade)
        {
            let _ = channel
                .sender
                .send((offset, Delivery::new("status", event)));
        }
    }

    pub fn advance_floor(&self, topic: &str, floor: i64) {
        if let Some(tasks) = self.0.get(topic) {
            for channel in tasks.values().filter_map(Weak::upgrade) {
                channel.floor.fetch_max(floor, Ordering::Release);
            }
        }
    }
}
