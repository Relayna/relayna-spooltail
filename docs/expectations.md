# First prototype expectations

The prototype answers two questions. Can a worker recover a task after another worker dies? Can a client find the next status for one task after a restart, even when hundreds of thousands of unrelated records intervene? The tests use real segment files, an embedded SQLite index and two network transports.

## Executable acceptance cases

| Case | Pass condition | Coverage |
| --- | --- | --- |
| SSE → WebSocket across broker restart | Save `E41` from SSE. Write `E42` while the client is offline. Kill the broker, reattach the same volume, and reconnect over WebSocket after `E41`. Deliver `E42` and live `E43` with stable IDs and no unrelated task records. | Process test |
| WebSocket → SSE | Save `E43` over WebSocket, append `E44`, kill and restart the broker, and reconnect with SSE `Last-Event-ID: E43`. Deliver `E44` then the terminal `E45`. | Process test |
| Append at replay/live boundary | Suspend the shared reader while it yields the final item of a replay page. Append a new event before the first live wait. The next read must return the new event. | Deterministic Rust test |
| Rare-task seek | After a fully indexed restart, the first missed event requires one payload read. The broker reports zero replayed prefix frames. | Process test, `/stats` |
| Crash after writing and before acknowledgement | Hold SQLite’s writer lock, start a real HTTP append, verify and flush its complete journal bytes, then kill the blocked broker. On restart exactly one suffix frame is applied. Retrying the same ID yields one history entry. Different content with the same ID is rejected. | Process test |
| Worker loss with independent subscribers | Two workers claim distinct tasks. Audit commits its first record. Kill the broker, allow worker A’s lease to expire, then reclaim that record. A’s stale acknowledgement fails. Audit resumes on the second record. Another stream group begins on the first. | Process test |
| Retry and settlement | Retrying releases the generation, renewal extends the new lease, and acknowledgement or dead-letter settlement removes work from that group’s ready set. | Process test |
| Atomic task completion | A valid claim settles together with one terminal status. Concurrent identical requests return the same status offset. Both streaming transports deliver the event, and other work and stream subscriptions keep their own progress. Stale leases, task mismatches, and event ID collisions leave the claim and history intact. | Process test |
| Crash during completion | Kill the broker after the completion frame reaches the journal and before SQLite commits. Restart after lease expiry. The status appears once, the work stays settled, and repeating the request returns its original result. A torn frame leaves the task available to its lease holder or a later worker. | Process and Rust tests |
| Retention passes a cursor | Move the status read floor past `E41`. Both protocols report a gap and the earliest retained event ID. An existing stream group cannot silently skip an unprocessed expired record. Work leases retain independent semantics. | Process and Rust tests |
| Segment integrity | An incomplete tail is truncated, a complete corrupt suffix frame fails recovery, a second writer cannot open the volume, and lookups survive segment rotation. | Rust tests |

Run the commands in the [README](../README.md#verify). The Python driver writes the initial journal fixture offline into a fresh temporary directory. Its first startup intentionally builds the index. In the full case this seeds 799,000 events. A disconnected task reader then misses 1,000 ordinary HTTP appends, including the sparse task’s `E42`. Exactly 800,000 events are retained at the measured broker restart. The driver times connection through first missed WebSocket status on loopback and verifies the number of decoded payloads. That sample includes connection setup and excludes broker startup. OS page caches can be warm. This is a correctness workload rather than an AKS latency target.

## Initial verification

The November 20, 2026 local run passed Rust formatting, strict Clippy, all four Rust tests, Python harness linting, a locked release build and the 800,000-event process suite. After the fully indexed restart, recovery read zero suffix frames and the rare-task replay decoded one payload. The first missed status arrived in 3.82 ms in one loopback WebSocket sample on the development machine. Treat this as a functional run record. Production latency and comparative CPU measurements require the workload below.

The atomic completion update passed all six Rust tests and the extended 800,000-event process suite. When the process died after writing a completion frame, recovery settled the claim and published one status even though the lease had expired during downtime. Repeated requests returned that status’s offset. The torn-frame test verified that an incomplete write preserved the original lease and left status history empty.

## Comparative benchmarks

The [local comparison](../benchmarks/README.md) runs the released SDK with RabbitMQ and Redis alongside Spooltail. It covers consumer restart, per-task SSE resume, and task completion. The broader AKS workload below adds deployment-level measurements.

Use the same ordered inputs, stable IDs, payload distribution, topic/task skew, reader population and post-restart publish rate in every arm. Preserve each arm’s pre-crash durable state. Record deployed versions, pod counts, CPU limits, prefetch, deduplication lifetime, persistence and replication settings. Run single-volume and equivalent replicated configurations separately. Keep the reader restart separate from a storage-node failure. Report warm and cold-cache trials and repeated-run distributions.

| Arm | Recovery behavior to test |
| --- | --- |
| Current Python SDK, RabbitMQ stream and Redis, `first` | Revisit the retained prefix, measure per-component CPU and duplicate checks, verify the downtime IDs. |
| Current stack, `next` | Record time to attach and the exact IDs missed during downtime. This is a loss control. |
| **Checkpointed current stack** | Persist the last contiguous successfully applied RabbitMQ stream offset under a stable consumer identity after Redis effects are durable. Read the actual broker offset from deliveries. Resume at its successor, handle deduplicated rewrites, fence concurrent owners and detect retention passing the cursor. Keep the same Redis history and fanout work. |
| **Spooltail** | Seek in the durable task index for a reconnecting client. Separately resume an equivalent all-record consumer from its persisted group checkpoint. Count suffix work, complete effects, missing IDs and duplicate deliveries. |

For the all-record consumer, process and checkpoint through the first 799,000 events, disconnect, and publish the known 1,000-event suffix during downtime. Measure time until all suffix IDs appear in its durable sink. For the task reader, measure time to the first missed status and to live catch-up. Record CPU-seconds and throttling per component, payloads decoded, disk reads/writes, memory, p99 publish latency, p99 task-claim latency and lag per group. A Kafka comparison can use the same retained topic/fanout workload with equivalent acknowledged durability. Specify whether per-task reads use a separate index in that arm and include its cost.

## Decisions before production

1. Specify peak publish rate, number of groups, concurrent task streams, target p99 resume latency and maximum acceptable recovery time.
2. Choose acknowledgement guarantees for pod, volume, node and zone failure. Define replica placement, fencing, quorum and snapshot recovery for the chosen fault model.
3. Choose history retention, deduplication period and archival policy. Define which readers may accept a state snapshot after missing intermediate events. Decide how long unfinished work survives and when dead-lettered work expires.
4. Decide which released Relayna SDK/Studio features the first adopter needs, whether any existing Kafka/AMQP clients must connect directly, and how to migrate existing public event IDs and durable progress.
5. Establish limits for payload and page bytes, subscriber fanout, ingestion/backpressure and fairness between service topics. Measure mutex contention, small synchronous writes, segment-file opens per record and ready-queue scans with many settled tasks.

This initial commit supplies the single-broker acceptance workload. The comparative AKS/RabbitMQ/Redis and Kafka runs, cold-cache distributions, storage reclamation and replicated failure tests are future verification work.
