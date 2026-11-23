# Relayna SDK comparison

This workload compares Spooltail with the Python Relayna SDK running against a real RabbitMQ status stream and a Redis status store. It exercises reader recovery and worker task completion through the public interfaces of both stacks.

For random lookups by task ID, see the [latest-status and recent-history benchmark](random-reads.md).
The [multiple-generator benchmark](load-generators.md) measures latest-status throughput as client processes and concurrent readers increase.
The [resource benchmark](read-resources.md) measures server and caller CPU and memory at idle, 100 reads/s and 1,000 reads/s.
The [transport benchmark](protocols/README.md) compares HTTP/1.1, HTTP/2, gRPC, and Unix sockets against the same Spooltail broker logic.

## Measured results

Three local trials each used 5,000 prior statuses, 250 downtime statuses, and 200 worker tasks per stack. Times below are medians across the three trials. Services ran on Linux ARM64 inside Docker Desktop with 10 CPUs. The run used RabbitMQ 4.3.6, Redis 7.4.12, and the local Spooltail build with atomic completion. The JSON records the Rust source hashes alongside the base commit. [Raw trial data](local-results.json)

| Recovery arm | Downtime IDs recovered | First missed task status over SSE | All-record catch-up | Recovery CPU¹ |
| --- | ---: | ---: | ---: | ---: |
| SDK `first`, prefetch 1 | 250 / 250 | 2,757.92 ms | 2,758.34 ms | 2.44 s |
| SDK `first`, prefetch 200 | 250 / 250 | 1,045.35 ms | 1,046.04 ms | 1.42 s |
| SDK `last`, prefetch 1 | 1 / 250 | 8.40 ms | — | — |
| SDK `next`, prefetch 1 | 0 / 250 | — | — | — |
| SDK with saved offset | 250 / 250 | 441.89 ms | 442.76 ms | 0.26 s |
| Spooltail | 250 / 250 | 2.77 ms | 694.98 ms | 0.51 s |

¹ CPU is the median total across the driver and participating services. The JSON breaks it down by component. The table reports catch-up time and CPU for complete recovery only.

The rare task’s last status during downtime was `E5249`. Spooltail already exposed it before the independent consumer reconnected. The SDK’s Redis latest-status lookup still returned the prior `E2500` at that point.

| Worker stack | Submit to claim or handler entry, p50 | Submit to settlement and latest read, p50 | Submit to settlement and latest read, p99 |
| --- | ---: | ---: | ---: |
| Relayna SDK + RabbitMQ + Redis | 0.45 ms | 2.39 ms | 3.01 ms |
| Spooltail | 2.01 ms | 3.73 ms | 6.22 ms |

## Run locally

Use Python 3.11 or newer on the host, Docker Engine with Linux cgroup v2, and Docker Compose with support for additional build contexts and health check start intervals. The driver builds the SDK from the checkout in `../relayna` relative to the Spooltail repository root. Set `RELAYNA_SDK_PATH` to use another checkout. The recorded run used the SDK’s `v1.14.0` release and its locked dependencies.

From the Spooltail repository root, run the recorded workload with the following command.

```sh
python3 benchmarks/run.py --prefix 5000 --offline 250 --tasks 200 --trials 3 --output /tmp/spooltail-results.json
```

For a smoke test, use `--prefix 20 --offline 6 --tasks 4 --trials 1`. For the original 800,000-status layout, use `--prefix 799000 --offline 1000` and increase `--timeout` (seconds per trial) to suit the host. The harness publishes every status through the live API and applies the prefix before disconnecting.

Each invocation creates a unique Compose project with no published host ports. The SDK publisher, hub, worker and benchmark client run in a Linux driver container. The SDK’s HTTP status router runs in a separate container. RabbitMQ, Redis and the release build of Spooltail run on the same Docker network. The runner cleans up its containers, volumes and locally built images on exit. The JSON file contains the versions, broker source hashes, settings and individual trials.

## What the measurements mean

**Status recovery.** Each arm gets its own topic and history namespace. One producer publishes in order, awaiting each confirmation. Both arms receive the same event IDs and JSON fields, including 128 bytes of metadata payload. The task distribution keeps every task inside the SDK’s 50-event history limit. One consumer applies every prefix event to the SDK’s `RedisStatusStore`. The harness stops that consumer, publishes the downtime suffix, then reconnects a new consumer and an HTTP SSE reader. The driver recreates the SDK hub and AMQP consumer channel, or resumes the named Spooltail group. The publisher connections and storage processes stay online with warm caches. Driver process startup and storage recovery are outside the timer. The reader sends the ID of the rare task’s last pre-disconnect event. Its missed event is the final record in the suffix. A further live event verifies the transition back to live delivery.

| Recovery arm | Consumer behavior |
| --- | --- |
| SDK `first`, prefetch 1 or 200 | The released `StatusHub` reads from the beginning. Redis handles deduplication of statuses it already applied. Prefetch 200 is the SDK hub default. |
| SDK `last`, prefetch 1 | The released topology’s default offset begins at the last RabbitMQ chunk. The number of downtime records recovered depends on chunk boundaries. |
| SDK `next`, prefetch 1 | The harness waits for RabbitMQ to register the new consumer before publishing the live event. Earlier downtime IDs are recorded as missing. |
| SDK with a saved offset, prefetch 1 | A benchmark-only subclass uses the delivery’s actual `x-stream-offset`. It saves that offset in Redis after the SDK has completed its normal history and fanout writes. The new consumer resumes at the saved offset plus one. This control assumes one owner and an untrimmed stream. |
| Spooltail group | The group reads the next record and commits its offset after applying the event to the same SDK Redis store. This gives the all-record consumers equivalent downstream work. Spooltail serves the simultaneous SSE request from its own durable per-task index. |

RabbitMQ documents its offset and chunk semantics in the [streams guide](https://www.rabbitmq.com/docs/streams#consuming).

- **All-record catch-up** runs from consumer reconnect through applying and acknowledging the entire suffix. An incomplete recovery has a null catch-up time. The harness checks exact event IDs, ordering, payloads, and multiplicity in the Redis histories after the timer stops.
- **Time to first missed task status** runs from the same reconnect through the first matching SSE payload received over HTTP. Both clients send `Last-Event-ID`. The SDK serves the event when its hub has populated Redis. Spooltail can serve it as soon as the publisher receives confirmation, even while the independent group is offline.
- **Publish latency** measures one sequential confirmed status publish during downtime. An SDK publish confirms the persistent RabbitMQ message. A Spooltail publish also commits the task index. The SDK hub populates Redis separately.
- **Resource counters** cover recovery through the group and SSE observations. The runner samples service cgroup CPU seconds, throttled seconds, block reads and writes, and ending cgroup memory. The driver reports its process CPU seconds. Cgroup memory includes file cache and earlier work on that service. Very short trials include sampling and idle-process overhead. `next` has no recovery resource sample.
- **Worker lifecycle** runs one task at a time. The SDK uses `TaskConsumer` to publish a deterministic completed status, acknowledge the task and materialize the result via `StatusHub`. Spooltail uses claim and atomic completion, with no Redis access in this lane. Both timers end after a successful latest-status read and settlement. The raw data also records time from submission to handler entry or claim response. Per-trial percentiles use the nearest-rank method.

The local workload uses one node per service, no per-container CPU quota, Redis AOF with `appendfsync always`, persistent RabbitMQ messages with publisher confirms, and Spooltail's synchronous single-volume journal and index. No storage service restarts during the comparison. Task execution uses one healthy worker and a synthetic handler. These measurements cover this configuration’s consumer restart and normal task latency. Sustained saturation, storage-node failure, replication, cold caches and AKS performance need separate runs.

## Saved-offset replay under live publishing

The durable replay driver creates disposable Linux Docker services and volumes. Build a Linux Spooltail release binary with `--features pack-fault-injection` and pass it to the driver. The test hook lets the driver release a sealed-segment pack when the catch-up timer starts.

```sh
python3 benchmarks/durable_replay.py --broker /path/to/linux/spooltail --output /tmp/durable-replay.json
```

Each trial seeds an acknowledged prefix and an offline backlog, restarts the broker, and opens a fresh consumer. Spooltail applies outcomes and its saved offset in one local SQLite transaction. RabbitMQ uses an AMQP stream consumer with prefetch 128 and an atomic Redis script for task IDs, payload digests, and the saved offset. Both consumers apply 32 records per batch. Redis uses AOF with `appendfsync always`. The driver verifies record order for the backlog, every final outcome, and the saved checkpoint. It measures catch-up latency alongside p95 confirmed publish latency from two paced publishers. The default event payload combines repeated text and base64-encoded opaque bytes.

The standard RabbitMQ stream arm uses normal publisher confirms. The synchronous-storage arm uses Linux `chattr +S` on its fresh named volume and verifies that stream segment files inherit the synchronous-write flag. It requires a Docker volume filesystem that supports that attribute. The sync arm tests a stricter local-write configuration. The arms exercise process restarts on one machine with warm caches.

The recorded mixed-payload run includes three trials per arm, with 768 missed records and about 240 live publishes per second. [Raw trial results](durable-replay-results.json)
