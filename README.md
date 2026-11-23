# Relayna Spooltail

Spooltail is a standalone Rust broker for durable task queues and replayable topics. A worker can lease the next task, an audit service can replay the topic from its own cursor, and a browser can resume one task’s history over SSE or WebSocket.

## Prototype status

The single-node prototype runs one broker process with one persistent directory. It stores events in per-partition journal segments and keeps the shared index, worker leases, and consumer checkpoints in embedded SQLite.

- **Task replay.** Clients resumed from SSE to WebSocket and from WebSocket to SSE across broker restarts. With 800,000 events indexed, the broker replayed zero journal frames at startup and read one event payload to return the selected task’s first missed status.
- **Worker recovery.** A second worker reclaimed an expired lease. The broker rejected the first worker’s late acknowledgement, and an independent audit subscriber resumed from its own checkpoint.
- **Publish recovery.** When the broker stopped after the journal write and before the index commit, it recovered the event on restart. Retrying the publish with the same event ID left one history entry.
- **Atomic task completion.** A worker can acknowledge its claim and publish a terminal status in one request. After a crash, retrying the same completion returns the original status offset.
- **Service and browser listeners.** A broker can serve gRPC over TLS and HTTP/SSE at the same time, using the same journal, leases, cursors, and live notifications.

The [native Python client](#python-client) is distributed through GitHub Releases. Replication and failover, physical storage reclamation, and an adapter for the existing Relayna SDK remain open. The [local RabbitMQ and Redis benchmark](benchmarks/README.md) compares status recovery and task completion with the Relayna SDK. A [random-read benchmark](benchmarks/random-reads.md) measures latest status and recent history by task ID. AKS measurements remain open. See the [acceptance cases and benchmark plan](docs/expectations.md).

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

Generate client stubs from the [Spooltail v1 Protobuf schema](proto/spooltail/v1/spooltail.proto). Service clients use `Append`, `Latest`, `History`, `Subscribe`, `Claim`, `Settle`, and `Complete`. Stream groups can use `Next` and `Commit` for one record at a time, or `NextBatch` and `CommitBatch` for a page. `Ping` checks the connection and `Stats` reports broker counters. Event fields retain their JSON encoding inside the Protobuf messages. `Claim` and `Next` return an absent record when there is nothing to deliver.

`NextBatch` returns the group's saved checkpoint and up to 32 records by default. Request 1 to 64 records. Each encoded response is capped at 2 MiB, so a page can contain fewer records than requested. After processing a consecutive prefix, pass the returned checkpoint as `after`, the last processed record's offset as `through`, and the number of processed records as `count` to `CommitBatch`. The broker advances the checkpoint durably in its embedded SQLite database. A retry succeeds while the checkpoint is still at `through`. Fetch again after a checkpoint conflict. An empty page needs no commit.

Batch operations share the same replay checkpoints and partition scope as single-record operations. The application assigns one logical reader per checkpoint and applies downstream effects durably before committing. A crash between applying effects and checkpointing can redeliver records. Use stable event IDs for idempotent effects.

`ReadAfterBatch` reads a bounded page in topic offset order for consumers that persist their own cursors. Omit `after` to start at the first retained record. An explicit offset behind the retention floor returns a failed precondition. The RPC uses the same page bounds as `NextBatch`, with replay progress owned by the caller. The [Python SDK audit timeline](python/README.md#audit-a-task) uses this operation to save consumer outcomes and replay progress together in local SQLite.

History and subscriptions take the same exclusive event ID cursor as HTTP/SSE. A unary history gap returns `FAILED_PRECONDITION` with `spooltail-earliest-event-id-bin` trailing metadata when a retained event exists. The metadata value is the UTF-8 event ID. A streaming gap carries `earliest_event_id` in its JSON body. The TLS listener authenticates the broker to its clients.

## Python client

Install the `relayna-spooltail` wheel from [GitHub Releases](https://github.com/sarattha/relayna-spooltail/releases). The [Python SDK quickstart](python/README.md) shows how to install it into a Relayna environment and use the native client to publish and complete work.

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

### Add partitions and move tasks

The broker starts with partition `0`. Existing volumes open as partition `0` with their original offsets. New partitions can be added while the broker is running. Each new task gets a durable assignment based on its task ID and the available partitions at first publish. The assignment applies across all topics in this broker's namespace.

```sh
curl -fsS -X PUT http://127.0.0.1:7474/partitions/1
curl -fsS http://127.0.0.1:7474/partitions
curl -fsS http://127.0.0.1:7474/tasks/T7/partition
curl -fsS -X POST -H 'Content-Type: application/json' \
  http://127.0.0.1:7474/tasks/T7/move \
  -d '{"from_partition":0,"to_partition":1}'
```

Creating the same partition ID again is safe. A move changes where future events are appended and which partition can claim the task's work. It waits for the current source append to finish. Active leases can still settle or complete using their original offsets and generations. Historical records stay on their original partition and can still be replayed by event ID. Append, completion, and record responses include the original partition as `partition`; HTTP omits partition zero for compatibility. A claim's outer `partition` identifies its current placement.

| Operation | Select a partition |
| --- | --- |
| Claim work | Include `"partition":1` in the claim body to claim tasks currently assigned there. Omit it to claim across all partitions. |
| Read or commit stream progress | Add `?partition=1` to both the `next` and `commit` paths. The group keeps a separate checkpoint for each partition. |
| Call from gRPC | Use `ListPartitions`, `EnsurePartition`, `TaskPartition`, and `MoveTask`. `Claim`, `Next`, `Commit`, and both batch replay operations accept an optional `partition`. |

Unscoped stream groups continue to read in broker-wide offset order across all partitions. Stop the unscoped reader before switching its group to partition-scoped reads. The first scoped read uses the saved unscoped checkpoint as its starting point, then that group uses scoped operations. Assign one logical reader per partition in each group and poll all partitions. After a task move, a destination reader waits until its group commits the task's preceding record on the source partition.

Each partition has its own journal writer. All writers share the broker process and SQLite index. Moving tasks redistributes future journal writes and partition-scoped claims. Historical reads still use the original journal. The offline `expire-before` command continues to set one logical floor per topic. Use the partition-aware broker binary for subsequent restarts of a partitioned volume.

### Assign partitions by owner service

Set `SPOOLTAIL_SERVICE_PARTITIONS` to a JSON object mapping each owner service to a partition ID or an array of partition IDs. Each partition can belong to one service pool. On startup the broker creates any missing partitions named by the rules.

```sh
SPOOLTAIL_SERVICE_PARTITIONS='{"urgent":2,"billing":4}' ./target/release/spooltail
curl -fsS -H 'Content-Type: application/json' \
  'http://127.0.0.1:7474/topics/work/records?owner_service=urgent' \
  -d '{"task_id":"T42","event_id":"queued"}'
```

Send `owner_service` on the first publish. HTTP uses the query parameter above, gRPC uses the optional `Publish.owner_service` field, and the Python SDK accepts `publish(..., owner_service="urgent")`. The broker saves the selected partition for that task ID across all topics. Another service can publish a status update using the same task ID, with or without a different service hint. A service publishing the first event on behalf of another service should send the task owner's name.

New tasks without a matching rule hash across unreserved partitions. If every partition is reserved, a new task needs a matching rule until you add a general partition. Restarting with changed rules keeps saved placements. An operator move or opt-in dynamic placement can change an existing assignment.

The owner-service field is a publisher-supplied routing hint. Dedicated placement provides queue separation when workers claim by partition. Access control and resource scheduling are separate broker concerns.

### Enable dynamic task ownership

```sh
SPOOLTAIL_SERVICE_PARTITIONS='{"urgent":[2,3],"billing":4}' SPOOLTAIL_DYNAMIC_PLACEMENT=1 ./target/release/spooltail
```

With dynamic placement enabled, a new task goes to the eligible partition with the fewest outstanding publish requests. Hashing breaks ties. On a later publish, the broker can move an idle task within its pool when the current partition has at least four outstanding publishes and another has at most half as many. A task has a five-second cooldown after an automatic move. General tasks use the unreserved pool. A service with a single partition keeps its dedicated placement.

The route update persists a new ownership version. Queued publishes recheck their version before writing. The broker lets an in-flight append finish before it can move that task. Existing events stay in their original journals, and existing leases can still complete. The HTTP `/stats` response reports `automatic_task_moves`.

### Use a SQLite index per partition

Set `SPOOLTAIL_INDEX_MODE=partitioned` on startup to give each partition its own SQLite index. The setting is saved on the volume. An existing volume retains its offsets, tasks, leases, checkpoints, and journal files. Keep using a partition-aware binary with that volume. Fresh volumes use the shared index by default, or you can select it explicitly with `SPOOLTAIL_INDEX_MODE=shared`.

Partitioned mode keeps the registry, task placements, leases, and checkpoints in `index.sqlite`. Each new record is synced to its partition journal before it is indexed in `partition-indexes/<id>.sqlite`. The journal contains the assigned offset and any completion receipt. Partition indexes use WAL with `synchronous=NORMAL` to avoid a second sync on every append. Startup replays any index entries lost after a power failure from the synced journal. The coordinator still uses full synchronous commits. The broker publishes records in global offset order, so an unscoped replay reader can still checkpoint a contiguous prefix. A slow earlier write can delay later acknowledgements.

Startup rebuilds the in-memory replay index from the SQLite files and recovers only journal suffixes. The original index provides the history that predates the upgrade. Rebuilding the replay index adds startup work and uses memory proportional to retained index rows. A backup needs a consistent snapshot of the entire data directory.

#### Batch partition index updates

Set `SPOOLTAIL_INDEX_BATCH_SIZE=32` with partitioned indexes to enable one background index writer per partition. The default size of `1` uses inline index updates. A worker commits up to the configured number of rows at once and flushes a partial batch after about 10 ms. Each queue is bounded, so sustained index stalls apply backpressure.

In this mode, up to 16 queued publishes on a partition can share a journal sync. A writer gathers requests that are already waiting, with no batching timer on the journal path. After sync, the partition can start its next write while durable records wait for their turn in global replay order.

The broker syncs the event journal before acknowledging the append. Global replay uses the in-memory index, and completion settlement still commits to the coordinator before acknowledgement. After a crash, startup can replay a longer journal suffix while rebuilding index rows that were still queued.

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
