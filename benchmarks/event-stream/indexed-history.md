# Many subscribers watching one task

Spooltail and RabbitMQ + Redis completed all **258,336 expected subscriber deliveries** in 54 measured runs. Both stacks preserved ordering, complete payloads and terminal stream closure. At 256 subscribers on one live task, RabbitMQ + Redis delivered updates faster. When 256 readers reconnected together with 24 missed updates and publishing continued, Spooltail recovered faster.

## Connected subscribers

Each row compares the same 48 updates at 40 publishes/s. Times are milliseconds. The last-reader metric measures elapsed time from publish dispatch until every subscriber has received that event.

| Subscribers | Delivery p95, Spooltail | Delivery p95, RabbitMQ + Redis | Last reader p95, Spooltail | Last reader p95, RabbitMQ + Redis | Publish ACK p95, Spooltail | Publish ACK p95, RabbitMQ |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 8.8 | 7.1 | 8.8 | 7.1 | 8.9 | 2.2 |
| 16 | 6.8 | 8.2 | 8.1 | 9.1 | 5.8 | 1.8 |
| 64 | 8.2 | 8.2 | 10.0 | 9.2 | 4.4 | 1.1 |
| 256 | 47.4 | 18.8 | 82.4 | 20.8 | 33.9 | 1.0 |

At 256 subscribers the trial p95 delivery ranges were 44.9–49.6 ms for Spooltail and 18.2–19.1 ms for RabbitMQ + Redis. The producer dispatched 39.7–40.0 events/s across those trials. Median server CPU per trial was 2.36 CPU-seconds on Spooltail and 1.03 across the RabbitMQ, Redis, hub and SSE API containers.

## Slow readers

These two cases use the same 32 subscribers, 32 KiB payloads and 50 publishes/s. In the second case, four readers consume at most 20 events/s.

| Readers | Fast delivery p95, Spooltail | Fast delivery p95, RabbitMQ + Redis | Slow delivery p95, Spooltail | Slow delivery p95, RabbitMQ + Redis |
| --- | ---: | ---: | ---: | ---: |
| 32 fast | 7.2 | 14.7 | — | — |
| 28 fast, 4 slow | 6.9 | 13.8 | 3,101.4 | 3,070.9 |

All four delayed subscribers drained their backlog on both stacks. Fast subscribers continued receiving events while the delayed readers fell seconds behind.

## Simultaneous reconnect

Times are in milliseconds from releasing all readers to reconnect until the subscriber receives its first or 24th missed event. The live columns time the 12 new publishes from dispatch to receipt. In the final case those publishes overlap the reconnect burst.

| Readers and publisher | First missed p95, Spooltail | First missed p95, RabbitMQ + Redis | Caught up p95, Spooltail | Caught up p95, RabbitMQ + Redis | Live delivery p95, Spooltail | Live delivery p95, RabbitMQ + Redis |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 64, publisher waits | 20.8 | 91.2 | 21.0 | 109.7 | 8.0 | 9.9 |
| 256, publisher waits | 70.7 | 261.3 | 71.0 | 374.3 | 35.7 | 21.3 |
| 256, publishing continues | 80.6 | 283.5 | 80.9 | 427.5 | 59.6 | 424.4 |

## What to prototype next

Spooltail's live notification wakes every watcher of the task. Each watcher asks for its own journal page. At 256 connected subscribers, the broker counted **12,288 journal payload reads for 48 publishes** in every trial. Even with a warm cache and indexed pages, each subscriber causes a separate frame decode. A comparison with shared live delivery would quantify how much of the latency comes from repeated reads. [Broker feed](../../src/main.rs) · [Journal page reader](../../src/store.rs)

Prototype sharing a committed event among a task’s connected subscribers. Bound the shared buffer by bytes and let a reader that falls behind resume from its last delivered event through the existing indexed history. Have live delivery resume strictly after the last replayed offset. Compare journal reads per publish, delivery p95, publish p95 and reconnect correctness on the same workload.

## How to read the measurements

Both arms served plaintext HTTP/1.1 Server-Sent Events to the same Python load generator. One independent publisher sent ordered updates to one task. Up to four reader processes shared the work. Every subscriber checked the SSE ID and full event body against the expected sequence and read through terminal stream closure. The warm-up record is excluded from delivery counts. Timing uses one Linux monotonic clock across processes.

- **Spooltail** ran the broker from the indexed per-task history branch with the default shared SQLite index. Each HTTP publish returned after the broker's normal durable append. Subscribers used the public per-task events endpoint.
- **RabbitMQ + Redis** used RabbitMQ 4.3.6 with a durable status stream, confirmed persistent AMQP publishes, one Relayna SDK v1.14.0 `StatusHub` with prefetch 200, Redis 7.4.12 with AOF and `appendfsync always`, and one Uvicorn worker serving the SDK's Redis history and Pub/Sub SSE route. The API Redis connection pool was set to 650 for the 256-reader workload. Hub ingestion and fan-out happen after the RabbitMQ confirmation. The SDK retained its default 50-event task history. All reconnect cursors and missed events fit inside that window.
- **Live** trials released the publisher after every subscriber had received the seed event. Small events carried a 256-byte application payload, with 48 publishes paced at 40/s. The large-event control and slow-reader test each sent 96 events with 32 KiB application payloads at 50/s. Four of the 32 slow-case readers requested a 16 KiB socket receive buffer and waited 50 ms before consuming the next event. The slow-reader numbers include that deliberate application delay. The test exercises finite reader lag and measures the effect on the other 28 subscribers.
- **Reconnect** trials disconnected every reader after the seed event, published 24 updates, and waited for the last update to be available in each store before releasing all readers with `Last-Event-ID: E0`. In the paused-publisher cases, 12 further live updates followed after every reader caught up. In the overlapping case, those 12 publishes started as soon as the reconnect gate opened. The recovery timer includes connection setup and stops separately for each reader when it receives the 24th missed update.
- **Delivery latency** runs from publish dispatch to receipt of the complete SSE event at the subscriber. **Publish latency** runs from dispatch to the producer's acknowledgement. **Last-reader latency** uses the last subscriber to receive each live event. CPU is the sum of service-container CPU seconds between samples, including the broker, Redis, hub and API for the comparison stack. Reader processes and the publisher are excluded. The Spooltail payload-read counter counts journal frames decoded, including warm filesystem-cache hits.
- Each case ran three times in a private Docker network on one 10-CPU Linux ARM64 Docker Desktop VM. The two stacks ran one at a time with alternating order. Reported p95s are the median of the three trial-level nearest-rank p95s. Samples are short and local, with warm services and one task receiving all updates. Production subscriber limits, long-lived backpressure and multi-node deployments require separate measurements.

## Reproduce

Use a checkout of the Relayna SDK at tag `v1.14.0`, Docker Compose with Linux cgroup v2, and Python 3.11 or later on the host. Run this command from the Spooltail repository root.

```sh
python3 benchmarks/event-stream/run_fanout.py \
  --sdk /path/to/relayna --trials 3 --output /tmp/spooltail-fanout.json
```

The output path must be new. To run a smaller test, add `--cases live-16 slow-32 resume-64` and set `--trials 1`. The runner uses isolated services and volumes, exposes no host ports, and saves per-trial results. The JSON includes image IDs, commits, driver hashes, counts, publish pacing, latency percentiles, service CPU, and Spooltail journal reads.

[Raw results](results.json) · [Docker runner](run_fanout.py) · [Publisher and subscriber workload](fanout.py)
