mod store;
mod subscriptions;

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, Path, Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    convert::Infallible,
    path::Path as DiskPath,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
};
use store::{Problem, Store, identifier};
use tokio::sync::watch;
use tokio_stream::{Stream, StreamExt, StreamMap};

type Notices = HashMap<(String, String), Weak<watch::Sender<()>>>;
#[derive(Clone)]
struct Broker {
    store: Arc<Mutex<Store>>,
    watchers: Arc<Mutex<Notices>>,
}

impl Broker {
    async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Store) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || f(&mut store.lock().expect("store mutex poisoned")))
            .await?
    }
    fn subscribe(&self, topic: &str, task: &str) -> Arc<watch::Sender<()>> {
        let mut map = self.watchers.lock().unwrap();
        let key = (topic.to_owned(), task.to_owned());
        if let Some(s) = map.get(&key).and_then(Weak::upgrade) {
            return s;
        }
        // Prune dead subscriptions, including tasks that have stopped publishing.
        map.retain(|_, s| s.strong_count() > 0);
        let s = Arc::new(watch::channel(()).0);
        map.insert(key, Arc::downgrade(&s));
        s
    }
    fn notify(&self, topic: String, task: String) {
        if let Some(s) = self
            .watchers
            .lock()
            .unwrap()
            .get(&(topic, task))
            .and_then(Weak::upgrade)
        {
            s.send_replace(());
        }
    }
}

struct ApiError(anyhow::Error);
impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        Self(e)
    }
}

fn error_body(e: &anyhow::Error) -> (StatusCode, &'static str, Value) {
    match e.downcast_ref::<Problem>() {
        Some(Problem::Gap { earliest }) => (
            StatusCode::GONE,
            "gap",
            json!({"earliest_event_id":earliest}),
        ),
        Some(Problem::Invalid(_)) => (
            StatusCode::BAD_REQUEST,
            "error",
            json!({"error":e.to_string()}),
        ),
        Some(Problem::Conflict(_)) => (
            StatusCode::CONFLICT,
            "error",
            json!({"error":e.to_string()}),
        ),
        Some(Problem::Missing) => (StatusCode::NOT_FOUND, "error", json!({"error":"not found"})),
        None => {
            eprintln!("broker error: {e:#}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "error",
                json!({"error":"broker storage failure"}),
            )
        }
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (code, _, data) = error_body(&self.0);
        (code, Json(data)).into_response()
    }
}
type Reply = Result<Json<Value>, ApiError>;
type Parts = Path<(String, String)>;

#[derive(Default, Deserialize)]
struct Resume {
    after: Option<String>,
}
async fn append(
    State(b): State<Broker>,
    Path(topic): Path<String>,
    Json(event): Json<Value>,
) -> Reply {
    let task = event["task_id"].as_str().unwrap_or_default().to_owned();
    let dest = topic.clone();
    let wake = b.clone();
    let off = b
        .run(move |s| {
            let off = s.append(&dest, event)?;
            // The request can disconnect while blocking I/O completes. Wake readers on commit anyway.
            wake.notify(dest, task);
            Ok(off)
        })
        .await?;
    Ok(Json(json!({"offset":off})))
}
async fn latest(State(b): State<Broker>, Path((topic, task)): Parts) -> Reply {
    Ok(Json(json!(b.run(move |s| s.latest(&topic, &task)).await?)))
}
async fn history(
    State(b): State<Broker>,
    Path((topic, task)): Parts,
    Query(q): Query<Resume>,
) -> Reply {
    Ok(Json(json!(
        b.run(move |s| s.page(&topic, &task, s.resolve(&topic, &task, q.after.as_deref())?))
            .await?
    )))
}

struct Delivery {
    kind: &'static str,
    data: Value,
    id: Option<String>,
}
impl Delivery {
    fn failure(error: anyhow::Error) -> Self {
        let (_, kind, data) = error_body(&error);
        Self {
            kind,
            data,
            id: None,
        }
    }
}
type Feed = Pin<Box<dyn Stream<Item = Delivery> + Send>>;
fn feed(b: Broker, topic: String, task: String, after: Option<String>) -> Feed {
    Box::pin(async_stream::stream! {
        let sender = b.subscribe(&topic, &task);
        let mut wake = sender.subscribe();
        let scope = (topic.clone(), task.clone());
        let start = b.run(move |s| s.resolve(&scope.0, &scope.1, after.as_deref())).await;
        let mut position = match start {
            Ok(offset) => offset,
            Err(error) => { yield Delivery::failure(error); return; }
        };
        yield Delivery { kind: "ready", data: json!({}), id: None };
        loop {
            let scope = (topic.clone(), task.clone());
            let result = b.run(move |s| s.page(&scope.0, &scope.1, position)).await;
            let page = match result {
                Ok(page) => page,
                Err(error) => { yield Delivery::failure(error); return; }
            };
            if page.is_empty() {
                if wake.changed().await.is_err() { return; }
                continue;
            }
            for record in page {
                position = record.offset;
                let terminal = record.event["status"].as_str().is_some_and(|s|
                    s.eq_ignore_ascii_case("completed") || s.eq_ignore_ascii_case("failed"));
                let id = record.event["event_id"].as_str().map(str::to_owned);
                yield Delivery { kind: "status", data: record.event, id };
                if terminal { return; }
            }
        }
    })
}

async fn sse(
    State(b): State<Broker>,
    Path((topic, task)): Parts,
    headers: HeaderMap,
    Query(q): Query<Resume>,
) -> Result<Response, ApiError> {
    let after = match headers.get("last-event-id") {
        Some(v) => Some(
            std::str::from_utf8(v.as_bytes())
                .map_err(|_| anyhow::Error::from(Problem::Invalid("invalid Last-Event-ID".into())))?
                .to_owned(),
        ),
        None => q.after,
    };
    let events = feed(b, topic, task, after).map(|d| {
        let mut event = Event::default().event(d.kind).data(d.data.to_string());
        if let Some(id) = d.id {
            event = event.id(id)
        }
        Ok::<_, Infallible>(event)
    });
    Ok(Sse::new(events)
        .keep_alive(KeepAlive::default())
        .into_response())
}

#[derive(Deserialize)]
struct Command {
    op: String,
    topic: String,
    task_id: String,
    after: Option<String>,
}
async fn websocket(State(b): State<Broker>, upgrade: WebSocketUpgrade) -> Response {
    upgrade
        .max_message_size(8192)
        .on_upgrade(move |socket| session(b, socket))
}
fn change_subscription(
    b: &Broker,
    feeds: &mut StreamMap<(String, String), Feed>,
    text: &str,
) -> Result<()> {
    let command: Command = serde_json::from_str(text)?;
    identifier(&command.topic)?;
    identifier(&command.task_id)?;
    let key = (command.topic.clone(), command.task_id.clone());
    match command.op.as_str() {
        "subscribe" if feeds.len() < 32 || feeds.contains_key(&key) => {
            feeds.insert(
                key,
                feed(b.clone(), command.topic, command.task_id, command.after),
            );
        }
        "unsubscribe" => {
            feeds.remove(&key);
        }
        _ => anyhow::bail!("unknown command or 32-subscription limit reached"),
    }
    Ok(())
}

async fn session(b: Broker, mut socket: WebSocket) {
    let mut feeds = StreamMap::new();
    loop {
        let reply = tokio::select! {
            message = socket.recv() => match message {
                Some(Ok(Message::Text(text))) => match change_subscription(&b, &mut feeds, &text) {
                    Ok(()) => continue,
                    Err(error) => json!({"type":"error","error":error.to_string()}),
                },
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                Some(Ok(Message::Binary(_))) => json!({"type":"error","error":"expected JSON text"}),
                _ => break,
            },
            item = feeds.next(), if !feeds.is_empty() => {
                let Some(((topic,task), delivery)) = item else { continue; };
                json!({"type":delivery.kind, "topic":topic, "task_id":task, "data":delivery.data})
            },
        };
        if socket
            .send(Message::Text(reply.to_string().into()))
            .await
            .is_err()
        {
            break;
        }
    }
}

async fn stats(State(b): State<Broker>) -> Reply {
    Ok(Json(
        b.run(|s| {
            Ok(json!({"payload_reads":s.payload_reads.get(),
                                 "recovered_frames":s.recovered_frames}))
        })
        .await?,
    ))
}

#[derive(Deserialize)]
struct ClaimArgs {
    lease_ms: i64,
}
#[derive(Deserialize)]
struct Settlement {
    offset: i64,
    generation: i64,
    action: String,
    #[serde(default)]
    delay_ms: i64,
}
#[derive(Deserialize)]
struct Commit {
    offset: i64,
}
async fn claim(
    State(b): State<Broker>,
    Path((topic, group)): Parts,
    Json(q): Json<ClaimArgs>,
) -> Reply {
    Ok(Json(json!(
        b.run(move |s| s.claim(&topic, &group, q.lease_ms)).await?
    )))
}
async fn settle(
    State(b): State<Broker>,
    Path((topic, group)): Parts,
    Json(q): Json<Settlement>,
) -> Reply {
    b.run(move |s| {
        s.settle(
            &topic,
            &group,
            q.offset,
            q.generation,
            &q.action,
            q.delay_ms,
        )
    })
    .await?;
    Ok(Json(json!({"ok":true})))
}
async fn next(State(b): State<Broker>, Path((topic, group)): Parts) -> Reply {
    Ok(Json(json!(
        b.run(move |s| s.group_next(&topic, &group)).await?
    )))
}
async fn commit(
    State(b): State<Broker>,
    Path((topic, group)): Parts,
    Json(q): Json<Commit>,
) -> Reply {
    b.run(move |s| s.commit(&topic, &group, q.offset)).await?;
    Ok(Json(json!({"ok":true})))
}

#[tokio::main]
async fn main() -> Result<()> {
    let dir = std::env::var("SPOOLTAIL_DATA").unwrap_or_else(|_| "data".into());
    let args: Vec<_> = std::env::args().skip(1).collect();
    let store = Store::open(DiskPath::new(&dir))?;
    match args.as_slice() {
        [cmd, topic, first] if cmd == "expire-before" => {
            return store.expire_before(topic, first.parse()?);
        }
        [] => {}
        _ => anyhow::bail!("usage: spooltail [expire-before TOPIC FIRST_OFFSET]"),
    }
    let recovered = store.recovered_frames;
    let broker = Broker {
        store: Arc::new(Mutex::new(store)),
        watchers: Default::default(),
    };
    let app = Router::new()
        .route("/topics/{topic}/records", post(append))
        .route("/topics/{topic}/tasks/{task}/latest", get(latest))
        .route("/topics/{topic}/tasks/{task}/history", get(history))
        .route("/topics/{topic}/tasks/{task}/events", get(sse))
        .route("/topics/{topic}/work/{group}/claim", post(claim))
        .route("/topics/{topic}/work/{group}/settle", post(settle))
        .route("/topics/{topic}/streams/{group}/next", get(next))
        .route("/topics/{topic}/streams/{group}/commit", post(commit))
        .route("/stats", get(stats))
        .route("/ws", get(websocket))
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(broker);
    let addr = std::env::var("SPOOLTAIL_LISTEN").unwrap_or_else(|_| "127.0.0.1:7474".into());
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("bind broker listener")?;
    println!(
        "Listening {} recovered_frames={recovered}",
        listener.local_addr()?
    );
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests;
