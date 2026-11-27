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

Both transports send `ready` and then the original `E42` payload. A WebSocket wraps it as `{"type":"status","topic":"status","task_id":"T7","data":{...}}`. A terminal event ends that task’s subscription. Other subscriptions on the socket continue. Resuming from the latest retained event written by atomic task completion returns `ready` and ends the subscription without repeating the result. Clients can read latest status to recover the terminal result. A browser SSE client should close its EventSource after a terminal event or an explicit gap.

### Run the broker in Docker

Build the broker image from this checkout and use a named volume for its journal and SQLite index.

```sh
docker build -t relayna-spooltail:0.0.6 .
docker run --rm --name spooltail \
  --mount type=volume,source=spooltail-data,target=/data \
  -p 127.0.0.1:7474:7474 -p 127.0.0.1:7443:7443 \
  -e SPOOLTAIL_GRPC_LISTEN=0.0.0.0:7443 -e SPOOLTAIL_GRPC_TLS=off \
  relayna-spooltail:0.0.6
```

The image runs as UID/GID 10001. If you bind-mount a directory at `/data`, give that user write access. Use `docker run --rm relayna-spooltail:0.0.6 --version` to check the broker version.

The [Helm chart](deploy/helm/spooltail) uses one broker, one PVC, and an internal service with plaintext gRPC on port 7443 for a trusted local cluster. Make the tagged image available to the cluster, then run `helm upgrade --install spooltail deploy/helm/spooltail`. SDK services in the same namespace can connect to `spooltail:7443`. Configure `persistence.storageClassName` if the cluster has no default. Helm uninstall removes the chart-managed PVC.

The [broker build workflow](.github/workflows/broker-build.yml) packages a Linux amd64 image archive and the Helm chart as Actions artifacts. Use `docker load -i` to load the downloaded image archive. Broker versions come from `Cargo.toml` and use `broker-v<version>` build tags. Python SDK releases retain their separate package versions and `v<version>` release tags. The broker workflow can also package an untagged commit through a manual run.

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

`AuditHistory` reads a task's original inputs across the journal and the Zstd archive. Omit `through` on the first page to capture the latest retained offset. Pass that offset and the last event ID as `through` and `after` for each subsequent page. Responses contain up to 64 records and at most 2 MiB. An expired event ID returns the same gap metadata as `History`. The Python SDK uses this RPC to join inputs with local outcomes.

History and subscriptions take the same exclusive event ID cursor as HTTP/SSE. A unary history gap returns `FAILED_PRECONDITION` with `spooltail-earliest-event-id-bin` trailing metadata when a retained event exists. The metadata value is the UTF-8 event ID. A streaming gap carries `earliest_event_id` in its JSON body. The TLS listener authenticates the broker to its clients.

## Python client

Install the `relayna-spooltail` wheel from [GitHub Releases](https://github.com/sarattha/relayna-spooltail/releases). The [Python SDK quickstart](python/README.md) shows how to install it into a Relayna environment and use the native client to publish and complete work. For saved-offset consumers, see [when to use the native decoder or a numeric projection](python/README.md#choosing-a-replay-reader).

### Relayna task services

Configure a work topic, a status topic, and one shared worker group per task service. The SDK task service owns the task envelope and exposes client-facing submit, status, history, and events routes. Gateway and Studio can use those routes while Spooltail durably stores the work and status events.

The SDK publishes a single work event for each task ID. Concurrent publishes of identical content return the same offset. If a retry uses a new server-generated timestamp, the broker reports a content conflict. A service using a single work event per task can use `Latest` on the work topic to compare the original request with the stored event. When a worker loses its lease, another worker in the same group can reclaim the work. The broker fences stale completion and writes the active claim's terminal result atomically.

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
| `GET /stats` | Process counters and journal storage statistics. |

Without a cursor, history starts at the first retained event for the task. Streaming clients receive a `gap` with `earliest_event_id` if an ID is unknown or outside retention. An empty retained history gives a null earliest ID. For the offline retention experiment, stop the broker and run `SPOOLTAIL_DATA=./data ./target/release/spooltail expire-before status 42`. This sets a logical floor for readers. Lease state and physical bytes are retained.

Identifiers are scoped by topic and task. Retrying an unchanged JSON event with the same ID returns its original offset. Reusing that ID with a different payload returns HTTP 409. This prototype’s HTTP routes are experimental. Use loopback and synthetic inputs while it lacks authentication and a released SDK adapter.

### Configure audit input retention

The broker can archive a topic's inputs before advancing its hot history floor. Configure the policy at broker startup with `SPOOLTAIL_RETENTION`.

```sh
SPOOLTAIL_RETENTION='{"status":{"hot_for":"7d","retain_for":"90d"}}' \
  ./target/release/spooltail
```

| Setting | Meaning |
| --- | --- |
| `hot_for` | Age at which an input becomes eligible to move out of ordinary history and replay. Set this above the expected consumer lag. |
| `retain_for` | Total age at which an archived input becomes eligible to expire from audit reads. It must be at least `hot_for`. |
| `paused` | Optional boolean. Set it to `true` to pause archiving and expiry at the current floors. Resume with `false`. |

Durations are positive integers followed by `s`, `m`, `h`, or `d`. Both windows use the broker's acceptance time. Older journal records that lack an acceptance timestamp use the time the topic policy was first configured. Automatic retention is opt-in per topic. The broker persists each supplied policy, and omitted topics keep their saved settings. On configured topics, the automatic policy owns the hot floor. Topics without a policy can still use the offline `expire-before` command. Existing floors define the starting point for archiving.

Background sweeps process consecutive prefixes in topic order. They compress inputs with Zstd level 1 in blocks targeting 64 KiB, with larger inputs stored individually. The broker commits the block, its index, and the updated hot floor together in `index.sqlite`. A failed archive write leaves the hot floor at its previous offset. Expiry is asynchronous and may lag during backlog or downtime. Reducing a window can make existing records eligible on the next sweep.

`AuditHistory` spans both tiers. `History`, `Latest`, subscriptions, and consumer replay use the hot window. A consumer that falls behind hot retention receives a gap. Archive expiry applies to original inputs. The consumer manages its outcome database separately.

Audit expiry removes archive rows and makes their SQLite pages reusable. The packed journal retirement policy below can also reclaim payload files. Back up the broker volume and the consumer's outcome SQLite separately to preserve a full audit timeline.

### Reclaim space from sealed journals

Enable background packing with `SPOOLTAIL_PACK_SEALED=1`. By default a sealed segment becomes eligible one hour after its last write. Set `SPOOLTAIL_PACK_MIN_AGE_SECS` to adjust that delay. The broker works through older segments first and starts at most one pack per second. The active journal in each partition stays raw.

Spooltail packs complete journal frames in seekable Zstd blocks at level 1. It keeps a raw segment when packing would increase its size. For each replacement it syncs the packed file and directory, commits the manifest in SQLite, then removes the raw file. A restart can clean up an interrupted switch. The packer needs free disk space for one temporary replacement.

Packed frames retain their original offsets and completion receipts. Replay, work claims, publish retries, index recovery, and audit reads can all use them. Auditing a packed input shares its journal payload. When all references to an old archive BLOB move to packed frames, SQLite can reuse the freed pages.

Disabling packing stops new replacements. Existing packed volumes require a broker that supports this format. Archive expiry removes inputs from audit history. Packed frames can still serve work claims and publish retries.

### Limit disk pressure

Set `SPOOLTAIL_DISK_RESERVE_BYTES` to protect free space for work already in flight. The default of `0` disables admission checks. Below the configured headroom, Spooltail rejects new appends and work claims with HTTP 503 or gRPC `RESOURCE_EXHAUSTED` before writing a journal frame. Exact publish retries can still return their saved receipt. Settlement and completion can use the reserve. Completion still requires enough room for its frame and index updates.

The packer checks and reserves room for a replacement before starting. Packing and archive BLOB writes retry when enough space is available. Admission accounts for broker writes in flight and uses the volume's current free space. Other writers and filesystem allocation overhead can still cause I/O failures.

The HTTP and gRPC stats report free bytes, the configured reserve, raw and packed journal file bytes, and the number of packing checks paused by low space. `archive_pending_records` counts unarchived records on configured topics, including records still inside their hot window. `retired_segments` counts durable segment retirement markers.

### Retire expired audit-only payloads

Set `SPOOLTAIL_RETIRE_AUDIT_TOPICS='["status"]'` to declare that a topic is used for replay and audit. The broker persists this declaration and rejects claims on the topic. Each named topic needs a retention policy and must have no existing work leases. Enable journal packing to make sealed raw segments eligible for retirement.

A packed file can be removed once every frame in it belongs to an audit-only topic, is past total retention, and has left both hot replay and the audit index. A paused policy or a record from a work topic pins the file. Spooltail commits segment markers, original offsets, completion receipts, and SHA-256 event fingerprints before unlinking the file. These receipts persist for the life of the volume so exact retries retain their original offsets and conflicting retries fail. Payload bytes can be reclaimed while deduplication and index metadata continue to grow.

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

Keep the producer’s `task_id` for the life of the task. Choose the terminal `event_id` once and reuse the entire request if the response is lost. The claim’s `generation` identifies the worker’s lease attempt. An exact retry returns the same status offset, even after restart or lease expiry. A stale claim, a different task, or a conflicting use of the same event ID returns HTTP 409. The terminal status can be `completed`, `failed`, or `cancelled`. Each settles the claim and wakes readers of the status topic. Broker 0.0.6 adds `cancelled` using the existing `Complete` RPC and JSON event format. The SDK service owns cancellation requests and stores any cancellation intent separately. The broker commits only the terminal event submitted by the active lease. External effects performed by a worker still need their own idempotency guarantees.

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

### Packed journal recovery

These suites use a dedicated broker build with fault hooks.

```sh
cargo build --release --locked --features pack-fault-injection
uv run --project python --locked python tests/packed_journal_acceptance.py
uv run --project python --locked python tests/packed_journal_disk_full.py
uv run --project python --locked python tests/packed_journal_retirement.py
```

CI runs these suites. The disk-full suite creates and fills its own filesystem image. Linux requires `mkfs.ext4`, loop mounting, and root or passwordless sudo for the mount. On macOS the suite uses an APFS disk image. It checks admission, failed pack writes, SQLite manifest commits, and foreground publishes. The ext4 cases also verify truncation of a partially written journal frame. The retirement suite checks interrupted deletion and receipt recovery after removing a partition index.

For the full-VM power-cut suite, build the same fault-injection binary for Linux ARM64. Run the driver on an Apple Silicon Mac with Lima and uv installed.

```sh
uv run --project python --locked python tests/packed_journal_power_cut.py --broker /path/to/linux-arm64/spooltail
```

The driver creates a disposable Ubuntu VM with no host directories mounted. It force-stops the VM at each of the five pack switch points in both index modes, checks the boot ID after restart, and verifies which journal copy the broker recovers. This exercises abrupt guest power loss on ext4.

Read the [architecture draft](docs/design.md) for the durability model and the [prototype expectations](docs/expectations.md) for acceptance criteria, benchmarks, and open decisions.
