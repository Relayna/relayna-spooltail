# Relayna Spooltail

Spooltail is a standalone Rust broker for durable task queues and replayable topics. A worker can lease the next task, an audit service can replay the topic from its own cursor, and a browser can resume one task’s history over SSE or WebSocket.

This first prototype runs one process with exclusive access to one persistent directory. It stores event bytes in native segment files and uses bundled SQLite as an embedded address index and store for subscription state. The deployment requires zero external broker or database services.

## Run the prototype

Use a Unix host with a Rust toolchain (tested with 1.95). Build and start the broker from this checkout.

```sh
cargo build --release --locked
SPOOLTAIL_DATA=./data SPOOLTAIL_LISTEN=127.0.0.1:7474 ./target/release/spooltail
```

In another terminal, publish two records and resume after the first. The terminal status closes this subscription.

```sh
curl -sS -H 'Content-Type: application/json' http://127.0.0.1:7474/topics/status/records \
  -d '{"task_id":"T7","event_id":"E41","status":"running"}'
curl -sS -H 'Content-Type: application/json' http://127.0.0.1:7474/topics/status/records \
  -d '{"task_id":"T7","event_id":"E42","status":"completed"}'
curl -N -H 'Last-Event-ID: E41' http://127.0.0.1:7474/topics/status/tasks/T7/events
```

For the same replay over WebSocket, connect to `ws://127.0.0.1:7474/ws` and send this JSON text frame.

```json
{"op":"subscribe","topic":"status","task_id":"T7","after":"E41"}
```

Both transports send `ready` and then the original `E42` payload. A WebSocket wraps it as `{"type":"status","topic":"status","task_id":"T7","data":{...}}`. A terminal event ends that task’s subscription. Other subscriptions on the socket continue. A browser SSE client should close its EventSource after a terminal event or an explicit gap.

## Broker operations

| Method and path | Behavior |
| --- | --- |
| `POST /topics/{topic}/records` | Append a JSON object with string `task_id` and `event_id`. Returns its offset after durable append and index commit. |
| `GET /topics/{topic}/tasks/{task}/latest` | Return `{offset,event}` or HTTP 404. |
| `GET /topics/{topic}/tasks/{task}/history?after=E41` | Return up to 64 retained records in order. For the next page, pass the last record’s event ID. An empty array means caught up. |
| `GET /topics/{topic}/tasks/{task}/events` | SSE, with an exclusive `Last-Event-ID` cursor or an initial `?after=` value. The header takes precedence. |
| `GET /ws` | WebSocket upgrade. Send `subscribe` or `unsubscribe` with a topic and task. Up to 32 task subscriptions per socket. |
| `GET /topics/{topic}/streams/{group}/next` | Read one record after this group’s durable checkpoint. Returns JSON `null` when caught up. |
| `POST /topics/{topic}/streams/{group}/commit` | Send `{"offset":42}` after processing. Advances through consecutive records of this topic. |
| `POST /topics/{topic}/work/{group}/claim` | Send `{"lease_ms":30000}`. Returns `{record,generation,lease_until_ms}` or JSON `null`. |
| `POST /topics/{topic}/work/{group}/settle` | Send the claimed `offset`, `generation`, and an `action` of `ack`, `renew`, `retry`, or `dead-letter`. Set `delay_ms` for renewal or delayed retry. Stale settlements return HTTP 409. |
| `GET /stats` | Process-local counts of recovered frames and indexed record payload reads. |

Without a cursor, history starts at the first retained event for the task. Streaming clients receive a `gap` with `earliest_event_id` if an ID is unknown or outside retention. An empty retained history gives a null earliest ID. For the offline retention experiment, stop the broker and run `SPOOLTAIL_DATA=./data ./target/release/spooltail expire-before status 42`. This sets a logical floor for readers. Lease state and physical bytes are retained.

Identifiers are scoped by topic and task. Retrying an unchanged JSON event with the same ID returns its original offset. Reusing that ID with a different payload returns HTTP 409. This prototype’s HTTP routes are experimental. Use loopback and synthetic inputs while it lacks authentication and a released SDK adapter.

## Verify

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo build --release --locked
uv run --script tests/acceptance.py --events 800000
```

The acceptance driver creates a temporary data directory, starts real broker processes on loopback, kills and restarts them, and cleans up afterward. Use `--events 1000` for a quick run.

Read the [architecture draft](docs/design.md) for the durability model and the [prototype expectations](docs/expectations.md) for acceptance criteria, benchmarks, and open decisions.
