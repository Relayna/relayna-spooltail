# Relayna Spooltail

Spooltail is a standalone Rust broker for durable task queues and replayable topics. A worker can lease the next task, an audit service can replay the topic from its own cursor, and a browser can resume one task’s history over SSE or WebSocket.

## Prototype status

The single-node prototype runs one broker process with one persistent directory. It stores events in journal segments and keeps the index, worker leases, and consumer checkpoints in embedded SQLite.

- **Task replay.** Clients resumed from SSE to WebSocket and from WebSocket to SSE across broker restarts. With 800,000 events indexed, the broker replayed zero journal frames at startup and read one event payload to return the selected task’s first missed status.
- **Worker recovery.** A second worker reclaimed an expired lease. The broker rejected the first worker’s late acknowledgement, and an independent audit subscriber resumed from its own checkpoint.
- **Publish recovery.** When the broker stopped after the journal write and before the index commit, it recovered the event on restart. Retrying the publish with the same event ID left one history entry.
- **Atomic task completion.** A worker can acknowledge its claim and publish a terminal status in one request. After a crash, retrying the same completion returns the original status offset.
- **Service and browser listeners.** A broker can serve gRPC over TLS and HTTP/SSE at the same time, using the same journal, leases, cursors, and live notifications.

The [native Python client](#python-client) is available from this checkout. Replication and failover, physical storage reclamation, and an adapter for the existing Relayna SDK remain open. The [local RabbitMQ and Redis benchmark](benchmarks/README.md) compares status recovery and task completion with the Relayna SDK. A [random-read benchmark](benchmarks/random-reads.md) measures latest status and recent history by task ID. AKS measurements remain open. See the [acceptance cases and benchmark plan](docs/expectations.md).

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

### Enable gRPC

Set `SPOOLTAIL_GRPC_LISTEN` to enable the TCP listener. TLS is on by default and can be selected explicitly with `SPOOLTAIL_GRPC_TLS=on`. Set `SPOOLTAIL_GRPC_TLS_CERT` to a PEM certificate chain whose leaf covers the broker hostname and `SPOOLTAIL_GRPC_TLS_KEY` to its PEM private key. The browser HTTP listener uses `SPOOLTAIL_LISTEN` independently.

```sh
SPOOLTAIL_DATA=./data \
  SPOOLTAIL_LISTEN=127.0.0.1:7474 \
  SPOOLTAIL_GRPC_LISTEN=127.0.0.1:7443 \
  SPOOLTAIL_GRPC_TLS_CERT=./server.pem \
  SPOOLTAIL_GRPC_TLS_KEY=./server-key.pem \
  ./target/release/spooltail

grpcurl -cacert ./ca.pem -import-path proto/spooltail/v1 -proto spooltail.proto \
  -d '{}' localhost:7443 spooltail.v1.Spooltail/Ping
```

For plaintext gRPC, set `SPOOLTAIL_GRPC_TLS=off` and leave both certificate settings unset. Use a plaintext client channel, such as `grpc.aio.insecure_channel` or `grpcurl -plaintext`.

```sh
SPOOLTAIL_GRPC_LISTEN=127.0.0.1:7443 SPOOLTAIL_GRPC_TLS=off \
  ./target/release/spooltail
```

Generate client stubs from the [Spooltail v1 Protobuf schema](proto/spooltail/v1/spooltail.proto). Service clients use `Append`, `Latest`, `History`, `Subscribe`, `Claim`, `Settle`, `Complete`, `Next`, and `Commit`. `Ping` checks the connection and `Stats` reports broker counters. Event fields retain their JSON encoding inside the Protobuf messages. `Claim` and `Next` return an absent record when there is nothing to deliver.

History and subscriptions take the same exclusive event ID cursor as HTTP/SSE. A unary history gap returns `FAILED_PRECONDITION` with `spooltail-earliest-event-id-bin` trailing metadata when a retained event exists. The metadata value is the UTF-8 event ID. A streaming gap carries `earliest_event_id` in its JSON body. The TLS listener authenticates the broker to its clients.

## Python client

The SDK is a uv library project under `python/`. Set it up from the repository root with

```sh
uv sync --project python --locked
```

See the [Python SDK quickstart](python/README.md) for a publish-and-complete example.

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
| `POST /topics/{topic}/work/{group}/complete` | Acknowledge the claim and append its terminal event to `status_topic` atomically. Returns the status record’s `offset`. |
| `GET /stats` | Process-local counts of recovered frames and indexed record payload reads. |

Without a cursor, history starts at the first retained event for the task. Streaming clients receive a `gap` with `earliest_event_id` if an ID is unknown or outside retention. An empty retained history gives a null earliest ID. For the offline retention experiment, stop the broker and run `SPOOLTAIL_DATA=./data ./target/release/spooltail expire-before status 42`. This sets a logical floor for readers. Lease state and physical bytes are retained.

Identifiers are scoped by topic and task. Retrying an unchanged JSON event with the same ID returns its original offset. Reusing that ID with a different payload returns HTTP 409. This prototype’s HTTP routes are experimental. Use loopback and synthetic inputs while it lacks authentication and a released SDK adapter.

### Complete a claimed task

Suppose a worker claims task `T7` from `inference.tasks` in group `workers` and receives work offset `42` and generation `3`. To finish it, send one request to `POST /topics/inference.tasks/work/workers/complete`.

```json
{
  "offset": 42,
  "generation": 3,
  "status_topic": "inference.status",
  "event": {"task_id": "T7", "event_id": "T7-final", "status": "completed"}
}
```

Keep the producer’s `task_id` for the life of the task. Choose the terminal `event_id` once and reuse the entire request if the response is lost. The claim’s `generation` identifies the worker’s lease attempt. An exact retry returns the same status offset, even after restart or lease expiry. A stale claim, a different task, or a conflicting use of the same event ID returns HTTP 409. The terminal status can be `completed` or `failed`. Either settles this work subscription and wakes readers of the status topic. External effects performed by a worker still need their own idempotency guarantees.

## Verify

The [GitHub Actions workflow](.github/workflows/ci.yml) runs these checks on Ubuntu for every pull request and push to `main`. It uses Rust 1.95.0 and uv 0.13.9.

```sh
cargo fmt --all --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
cargo build --release --locked
uv run --project python --locked python python/generate_wire.py
git status --short --untracked-files=all -- python/src/relayna_spooltail/_wire
uv build --project python
uv run --python 3.11 --script tests/acceptance.py --events 800000
uv run --python 3.13 --script tests/grpc_acceptance.py
uv run --project python --locked --no-editable python tests/python_sdk_acceptance.py
```

Regenerating the Python gRPC bindings should leave `git status` empty for the `_wire` directory. The acceptance drivers create temporary data directories and start real broker processes on loopback. The gRPC and Python SDK drivers use OpenSSL to create a test identity. The SDK driver checks task completion, replay gaps, and SSE resume after restart. Use `--events 1000` for a quick HTTP acceptance run.

Read the [architecture draft](docs/design.md) for the durability model and the [prototype expectations](docs/expectations.md) for acceptance criteria, benchmarks, and open decisions.
