#[cfg(feature = "bench-lock-timing")]
mod bench_lock;
#[cfg(feature = "bench-tls")]
mod bench_tls;
mod grpc;
mod journal;
mod live;
mod packing;
mod partition_index;
mod retention;
mod retire;
mod space;
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
    routing::{get, post, put},
};
use live::Delivery;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    convert::Infallible,
    path::Path as DiskPath,
    pin::Pin,
    sync::{Arc, Mutex},
};
use store::{Frame, Position, Prepared, Problem, Store, identifier};
use subscriptions::WorkLease;
use tokio::sync::broadcast::error::RecvError;
use tokio_stream::{Stream, StreamExt, StreamMap};

#[derive(Clone)]
struct Broker {
    store: Arc<Mutex<Store>>,
    history_reader: Arc<tokio::sync::Semaphore>,
}

impl Broker {
    fn new(store: Store) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
            history_reader: Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }

    // Store::page already serializes on the store mutex. Queue competing pages
    // asynchronously so they do not occupy blocking threads needed by appends.
    async fn history_page(
        &self,
        fetch: impl FnOnce(&mut Store) -> Result<Vec<store::Record>> + Send + 'static,
    ) -> Result<Vec<store::Record>> {
        let permit = self.history_reader.clone().acquire_owned().await?;
        self.run(move |s| {
            // A disconnected caller can leave an already running read behind.
            let _permit = permit;
            fetch(s)
        })
        .await
    }

    async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Store) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            with_store(&store, |store| {
                store.ensure_readable()?;
                f(store)
            })
        })
        .await?
    }
    async fn move_task(&self, task: String, from: u32, to: u32) -> Result<()> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let (journal, grouped) = with_store(&store, |s| {
                Ok::<_, anyhow::Error>((s.journal(from)?, s.groups_journal_sync()))
            })?;
            // Grouped appends may need the journal again to sync. The metadata
            // lock and in-flight marker fence their ownership changes directly.
            let _writer = (!grouped).then(|| journal.blocking_lock());
            with_idle_task(&store, &task, |s| s.move_task(&task, from, to))
        })
        .await?
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
        Some(Problem::Capacity) => (
            StatusCode::SERVICE_UNAVAILABLE,
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
#[derive(Deserialize)]
struct AppendQuery {
    owner_service: Option<String>,
}

async fn append(
    State(b): State<Broker>,
    Path(topic): Path<String>,
    Query(q): Query<AppendQuery>,
    Json(event): Json<Value>,
) -> Reply {
    Ok(Json(json!(
        publish(b, topic, event, None, q.owner_service).await?
    )))
}

fn with_store<T>(store: &Mutex<Store>, f: impl FnOnce(&mut Store) -> T) -> T {
    #[cfg(feature = "bench-lock-timing")]
    return bench_lock::run(store, f);
    #[cfg(not(feature = "bench-lock-timing"))]
    f(&mut store.lock().expect("store mutex poisoned"))
}

fn with_idle_task<T>(
    store: &Mutex<Store>,
    task: &str,
    operation: impl FnOnce(&mut Store) -> Result<T>,
) -> Result<T> {
    let mut store = store.lock().expect("store mutex poisoned");
    if let Some(split) = &store.split {
        let ready = split.ready.clone();
        while store.is_appending(task)? {
            store.ensure_writable()?;
            store = ready.wait(store).expect("store mutex poisoned");
        }
    }
    operation(&mut store)
}

async fn publish(
    b: Broker,
    topic: String,
    event: Value,
    lease: Option<WorkLease>,
    owner_service: Option<String>,
) -> Result<Position> {
    if let Some(service) = owner_service.as_deref() {
        identifier(service)?;
    }
    tokio::task::spawn_blocking(move || {
        let mut frame = Frame::new(topic, event, lease)?;
        let task = frame.key()?.0.to_owned();
        let position = loop {
            let ticket = with_store(&b.store, |store| {
                if store.space.reserve > 0
                    && frame.completion.is_none()
                    && store.existing_event(&frame)?.is_none()
                {
                    store.admit_disk_write(0, false)?;
                }
                store.writer_ticket(&task, owner_service.as_deref())
            })?;
            let partition = ticket.placement.partition;
            // Ownership can change while queued. Check both partition and epoch
            // under the metadata lock before marking this task in flight.
            // FIFO admission keeps fresh requests from passing older waiters.
            let mut journal = ticket.journal.blocking_lock();
            let prepared = with_store(&b.store, |store| {
                if store.route_for(&task, owner_service.as_deref())? != ticket.placement {
                    return Ok(None);
                }
                if ticket.grouped && store.is_appending(&task)? {
                    return Ok(None);
                }
                store.prepare_append(&mut frame, partition).map(Some)
            })?;
            match prepared {
                None => {
                    drop(journal);
                    let grouped = ticket.grouped;
                    drop(ticket);
                    if grouped {
                        with_idle_task(&b.store, &task, |_| Ok(()))?;
                    }
                }
                Some(Prepared::Duplicate(position)) => break position,
                Some(Prepared::Write(body)) if ticket.grouped => {
                    let frame = Arc::new(frame);
                    let staged = journal.stage_append(frame.clone(), partition, &body);
                    drop(body);
                    drop(journal);
                    let write = staged.and_then(|staged| {
                        // A leader queues behind any publishers already waiting.
                        // One sync covers their staged writes as well as its own.
                        if staged.leader {
                            ticket.journal.blocking_lock().flush_staged()?;
                        }
                        staged.wait()
                    });
                    break finish_publish(&b, &frame, partition, write)?;
                }
                Some(Prepared::Write(body)) => {
                    let write = journal.append(&frame, partition, &body);
                    break finish_publish(&b, &frame, partition, write)?;
                }
            }
        };
        Ok(position)
    })
    .await?
}
fn finish_publish(
    b: &Broker,
    frame: &Frame,
    partition: u32,
    write: Result<journal::Address>,
) -> Result<Position> {
    let Some(offset) = frame.offset else {
        return with_store(&b.store, |store| {
            store.finish_append(frame, partition, write)
        });
    };
    let mut store = b.store.lock().expect("store mutex poisoned");
    let ready = store
        .split
        .as_ref()
        .expect("partitioned writer without index")
        .ready
        .clone();
    if write.is_ok() {
        store = ready
            .wait_while(store, |s| !s.can_publish(offset))
            .expect("store mutex poisoned");
    }
    let result = store.finish_append(frame, partition, write);
    ready.notify_all();
    result
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
        b.history_page(move |s| s.page(
            &topic,
            &task,
            s.resolve(&topic, &task, q.after.as_deref())?
        ))
        .await?
    )))
}

fn failure(error: anyhow::Error) -> Arc<Delivery> {
    let (_, kind, data) = error_body(&error);
    Delivery::new(kind, &data)
}

type Feed = Pin<Box<dyn Stream<Item = Arc<Delivery>> + Send>>;
fn feed(b: Broker, topic: String, task: String, after: Option<String>) -> Feed {
    Box::pin(async_stream::stream! {
        let scope = (topic.clone(), task.clone());
        let start = b.run(move |s| {
            let position = s.resolve(&scope.0, &scope.1, after.as_deref())?;
            let replay = s.has_task_records_after(&scope.0, &scope.1, position)?;
            // Atomic completion ends a worker's task. A status sent by a
            // standalone publisher can still be followed by another event.
            let finished = after.is_some() && !replay && s.is_completion(position)?;
            let live = (!finished).then(|| s.live.subscribe(&scope.0, &scope.1));
            Ok((position, replay, live))
        }).await;
        let (mut position, mut replay, live) = match start {
            Ok(start) => start,
            Err(error) => { yield failure(error); return; }
        };
        yield Delivery::new("ready", &json!({}));
        let Some((channel, mut receiver)) = live else { return; };
        loop {
            if replay {
                let scope = (topic.clone(), task.clone());
                let page = match b.history_page(move |s| s.page(&scope.0, &scope.1, position)).await {
                    Ok(page) => page,
                    Err(error) => { yield failure(error); return; }
                };
                // A short page covers all commits through the query snapshot.
                replay = page.len() == store::PAGE_SIZE as usize;
                for record in page {
                    position = record.offset;
                    let delivery = Delivery::new("status", &record.event);
                    let terminal = delivery.terminal;
                    yield delivery;
                    if terminal { return; }
                }
                if replay { continue; }
            }
            match receiver.recv().await {
                Ok((offset, _)) if offset <= position => continue,
                Ok((offset, delivery)) if channel.can_deliver_after(position) => {
                    position = offset;
                    let terminal = delivery.terminal;
                    yield delivery;
                    if terminal { return; }
                }
                Ok(_) | Err(RecvError::Lagged(_)) => {
                    // Register for new commits before reading the missing interval.
                    receiver = receiver.resubscribe();
                    replay = true;
                }
                Err(RecvError::Closed) => return,
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
        let mut event = Event::default().event(d.kind).data(d.data.get());
        if let Some(id) = &d.id {
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

#[derive(Serialize)]
struct WebSocketDelivery<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    topic: &'a str,
    task_id: &'a str,
    data: &'a serde_json::value::RawValue,
}

async fn session(b: Broker, mut socket: WebSocket) {
    let mut feeds = StreamMap::new();
    loop {
        let reply = tokio::select! {
            message = socket.recv() => match message {
                Some(Ok(Message::Text(text))) => match change_subscription(&b, &mut feeds, &text) {
                    Ok(()) => continue,
                    Err(error) => json!({"type":"error","error":error.to_string()}).to_string(),
                },
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                Some(Ok(Message::Binary(_))) => json!({"type":"error","error":"expected JSON text"}).to_string(),
                _ => break,
            },
            item = feeds.next(), if !feeds.is_empty() => {
                let Some(((topic,task), delivery)) = item else { continue; };
                serde_json::to_string(&WebSocketDelivery {
                    kind: delivery.kind, topic: &topic, task_id: &task, data: &delivery.data,
                }).expect("delivery contains valid JSON")
            },
        };
        if socket.send(Message::Text(reply.into())).await.is_err() {
            break;
        }
    }
}

async fn stats(State(b): State<Broker>) -> Reply {
    Ok(Json(
        serde_json::to_value(b.stats().await?).map_err(anyhow::Error::from)?,
    ))
}

#[derive(Deserialize)]
struct ClaimArgs {
    lease_ms: i64,
    partition: Option<u32>,
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
struct Completion {
    offset: i64,
    generation: i64,
    status_topic: String,
    event: Value,
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
        b.run(move |s| s.claim(&topic, &group, q.lease_ms, q.partition))
            .await?
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
async fn complete(
    State(b): State<Broker>,
    Path((topic, group)): Parts,
    Json(q): Json<Completion>,
) -> Reply {
    let lease = WorkLease {
        topic,
        group,
        offset: q.offset,
        generation: q.generation,
    };
    Ok(Json(json!(
        publish(b, q.status_topic, q.event, Some(lease), None).await?
    )))
}

#[derive(Deserialize)]
struct PartitionQuery {
    partition: Option<u32>,
}

async fn next(
    State(b): State<Broker>,
    Path((topic, group)): Parts,
    Query(q): Query<PartitionQuery>,
) -> Reply {
    Ok(Json(json!(
        b.run(move |s| s.group_next(&topic, &group, q.partition))
            .await?
    )))
}
async fn commit(
    State(b): State<Broker>,
    Path((topic, group)): Parts,
    Query(scope): Query<PartitionQuery>,
    Json(q): Json<Commit>,
) -> Reply {
    b.run(move |s| s.commit(&topic, &group, q.offset, scope.partition))
        .await?;
    Ok(Json(json!({"ok":true})))
}

async fn partitions(State(b): State<Broker>) -> Reply {
    let partitions = b.run(|s| Ok(s.partition_ids())).await?;
    Ok(Json(json!({"partitions": partitions})))
}

async fn ensure_partition(State(b): State<Broker>, Path(id): Path<u32>) -> Reply {
    b.run(move |s| s.ensure_partition(id)).await?;
    Ok(Json(json!({"partition": id})))
}

#[derive(Deserialize)]
struct TaskMove {
    from_partition: u32,
    to_partition: u32,
}

async fn task_partition(State(b): State<Broker>, Path(task): Path<String>) -> Reply {
    let partition = b.run(move |s| s.task_partition(&task)).await?;
    Ok(Json(json!({"partition": partition})))
}

async fn move_task(
    State(b): State<Broker>,
    Path(task): Path<String>,
    Json(q): Json<TaskMove>,
) -> Reply {
    b.move_task(task, q.from_partition, q.to_partition).await?;
    Ok(Json(json!({"partition": q.to_partition})))
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ServicePartitions {
    Partition(u32),
    Pool(Vec<u32>),
}

fn read_service_rules(store: &mut Store) -> Result<()> {
    let rules = match std::env::var("SPOOLTAIL_SERVICE_PARTITIONS") {
        Ok(rules) => rules,
        Err(std::env::VarError::NotPresent) => return Ok(()),
        Err(error) => return Err(error).context("read SPOOLTAIL_SERVICE_PARTITIONS"),
    };
    let rules: std::collections::BTreeMap<String, ServicePartitions> = serde_json::from_str(&rules)
        .context("SPOOLTAIL_SERVICE_PARTITIONS must map each service to a partition ID or array of partition IDs")?;
    store.configure_service_partitions(
        rules
            .into_iter()
            .map(|(name, rule)| {
                let pool = match rule {
                    ServicePartitions::Partition(id) => vec![id],
                    ServicePartitions::Pool(pool) => pool,
                };
                (name, pool)
            })
            .collect(),
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    let dir = std::env::var("SPOOLTAIL_DATA").unwrap_or_else(|_| "data".into());
    let args: Vec<_> = std::env::args().skip(1).collect();
    let mode = std::env::var("SPOOLTAIL_INDEX_MODE").ok();
    let mut store = Store::open_mode(DiskPath::new(&dir), mode.as_deref())?;
    match args.as_slice() {
        [cmd, topic, first] if cmd == "expire-before" => {
            return store.expire_before(topic, first.parse()?);
        }
        [] => {}
        _ => anyhow::bail!("usage: spooltail [expire-before TOPIC FIRST_OFFSET]"),
    }
    read_service_rules(&mut store)?;
    store.dynamic_placement = match std::env::var("SPOOLTAIL_DYNAMIC_PLACEMENT") {
        Ok(value) if value == "1" => true,
        Ok(value) if value == "0" => false,
        Err(std::env::VarError::NotPresent) => false,
        _ => anyhow::bail!("SPOOLTAIL_DYNAMIC_PLACEMENT must be 0 or 1"),
    };
    match std::env::var("SPOOLTAIL_INDEX_BATCH_SIZE") {
        Ok(size) => store.configure_index_batch_size(
            size.parse()
                .context("SPOOLTAIL_INDEX_BATCH_SIZE must be 1..=1024")?,
        )?,
        Err(std::env::VarError::NotPresent) => {}
        Err(error) => return Err(error).context("read SPOOLTAIL_INDEX_BATCH_SIZE"),
    }
    store.space = space::Space::configure()?;
    let retention_topics = retention::configure(&mut store)?;
    let pack_min_age = packing::configure(&mut store)?;
    let retire = retire::configure(&mut store)?;
    let recovered = store.recovered_frames;
    let broker = Broker::new(store);
    if !retention_topics.is_empty() {
        tokio::spawn(retention::run(broker.clone(), retention_topics));
    }
    #[cfg(feature = "bench-transports")]
    if grpc::SETTINGS
        .iter()
        .any(|name| std::env::var_os(name).is_some())
    {
        anyhow::ensure!(
            [
                "SPOOLTAIL_BENCH_GRPC",
                "SPOOLTAIL_BENCH_UNIX",
                "SPOOLTAIL_BENCH_CERT",
                "SPOOLTAIL_BENCH_KEY"
            ]
            .iter()
            .all(|name| std::env::var_os(name).is_none()),
            "gRPC listener settings cannot be combined with benchmark listener settings",
        );
    }
    #[cfg(feature = "bench-transports")]
    if let Ok(addr) = std::env::var("SPOOLTAIL_BENCH_GRPC") {
        return grpc::serve_benchmark(broker, &addr).await;
    }
    let grpc_listener = grpc::Listener::bind_from_env(broker.clone()).await?;
    if retire {
        tokio::spawn(retire::run(broker.clone()));
    }
    if let Some(min_age) = pack_min_age {
        tokio::spawn(packing::run(broker.clone(), min_age));
    }
    let app = Router::new()
        .route("/topics/{topic}/records", post(append))
        .route("/topics/{topic}/tasks/{task}/latest", get(latest))
        .route("/topics/{topic}/tasks/{task}/history", get(history))
        .route("/topics/{topic}/tasks/{task}/events", get(sse))
        .route("/topics/{topic}/work/{group}/claim", post(claim))
        .route("/topics/{topic}/work/{group}/settle", post(settle))
        .route("/topics/{topic}/work/{group}/complete", post(complete))
        .route("/topics/{topic}/streams/{group}/next", get(next))
        .route("/topics/{topic}/streams/{group}/commit", post(commit))
        .route("/partitions", get(partitions))
        .route("/partitions/{id}", put(ensure_partition))
        .route("/tasks/{task}/partition", get(task_partition))
        .route("/tasks/{task}/move", post(move_task))
        .route("/stats", get(stats))
        .route("/ws", get(websocket));
    #[cfg(feature = "bench-lock-timing")]
    let app = app.route("/bench/store-lock", get(bench_lock::snapshot));
    #[cfg(feature = "bench-transports")]
    let app = app.route("/bench/ping", get(|| async { StatusCode::NO_CONTENT }));
    let app = app
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(broker);
    #[cfg(all(feature = "bench-transports", unix))]
    if let Ok(path) = std::env::var("SPOOLTAIL_BENCH_UNIX") {
        let listener =
            tokio::net::UnixListener::bind(&path).context("bind benchmark Unix socket")?;
        println!("Listening {path} recovered_frames={recovered}");
        axum::serve(listener, app).await?;
        return Ok(());
    }
    let addr = std::env::var("SPOOLTAIL_LISTEN").unwrap_or_else(|_| "127.0.0.1:7474".into());
    #[cfg(feature = "bench-tls")]
    if let Some(files) = bench_tls::identity_files()? {
        return bench_tls::serve_http(app, &addr, files).await;
    }
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("bind broker listener")?;
    println!(
        "Listening {} recovered_frames={recovered}",
        listener.local_addr()?
    );
    let http = async { axum::serve(listener, app).await.context("serve HTTP") };
    if let Some(grpc) = grpc_listener {
        tokio::try_join!(http, grpc.serve())?;
    } else {
        http.await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
