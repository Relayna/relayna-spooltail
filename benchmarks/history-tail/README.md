# Publish latency during task history reads

Four readers repeatedly requested full, indexed history pages while a separate process waited for each durable publish acknowledgement. The largest timed component of a slow indexed publish was the journal flush. The store lock had been the main source of contention in the older history implementation. Indexing the page reduced that contention.

## What changed

HTTP and gRPC history requests and the page reads behind SSE, WebSocket and gRPC subscriptions now share one history-read permit per broker. Previously, each request could start a blocking job and wait on the same store mutex. Waiting for a permit asynchronously lets a burst queue before occupying blocking threads that also serve appends. Only the active page read owns the permit. A client can disconnect, but its blocking read holds the permit until it finishes. Serialization of responses and network delivery proceed outside the permit. The publisher still syncs its journal and commits its SQLite index before replying. [Tokio blocking-task and cancellation behavior](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html)

## Before and after

Each number is the median of the trial percentiles, in milliseconds. “Indexed” is the original #14 commit and “bounded” adds history admission to that commit. Compare builds within each workload. Smaller values are faster.

| Readers | Publishes per trial | Rounds | Build | History p50 | History p99 | Publish p50 | Publish p99 |
| ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 4 | 240 | 12 | #13 | 4.50 | 7.25 | 5.63 | 11.55 |
| 4 | 240 | 12 | Indexed | 2.08 | 5.68 | 6.55 | 11.93 |
| 4 | 240 | 12 | Bounded | 2.13 | 6.19 | 6.65 | 11.75 |
| 4 | 1,200 | 6 | Indexed | 1.97 | 5.87 | 6.54 | 11.24 |
| 4 | 1,200 | 6 | Bounded | 1.95 | 5.39 | 6.53 | 11.12 |
| 16 | 240 | 6 | Indexed | 2.77 | 5.02 | 5.24 | 9.36 |
| 16 | 240 | 6 | Bounded | 2.69 | 5.02 | 5.14 | 8.26 |

The historical four-reader measurement reported 9.46 ms for #13 and 13.04 ms for #14. The fresh 12-round comparison reproduced a smaller increase of 0.38 ms. Admission then reduced publish p99 in 7 of 12 paired rounds. The aggregate difference was 0.17 ms, and history p99 increased by 0.51 ms. A longer four-reader trial raised the sample to 1,200 publishes per build per round. Publish p99 was lower in three of six pairs. The data do not establish a repeatable four-reader publish improvement. With 16 readers, publish p99 improved in all six paired rounds, a reduction of 11.7% in the median trial p99, with comparable history p99. The stronger case for bounding reads is burst contention. [Four-reader trials](macos.json) · [Longer four-reader trials](sustained-macos.json) · [16-reader trials](burst-macos.json)

A separate live-delivery check used 64 gRPC subscribers watching the same task. In four trials per build, each subscriber received all 64 updates with the expected event IDs, payloads and order.

## Where publish time went

A separate diagnostic build timed intervals at publish entry, blocking task dispatch, store acquisitions, journal write, journal sync and index completion. Each group ran four rounds. The publisher also timed the whole RPC. “Outside” is that elapsed time less the broker's measured publish span. It can include transport, client scheduling and reply delivery. The acquisition spans include short stretches of adjacent frame preparation and journal acquisition. Each column ranks its own samples, so adding p99s would give a misleading total.

| Build and timed readers | Publish RPC p99 | Journal sync p99 | Store acquisition spans p99 | Index finish p99 | Outside p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| #13, four | 11.72 | 7.21 | 5.38 | 0.30 | 0.81 |
| Indexed, four | 12.32 | 8.35 | 1.82 | 1.15 | 3.49 |
| Indexed, zero | 14.50 | 8.79 | 0.02 | 2.70 | 3.95 |

In the slowest 5% of indexed publishes, the median store acquisition spans were about 0.01 ms in each of the four rounds. A high publish tail also occurred with zero timed readers. The experiment isolates the expensive operation, but it cannot determine the specific device or scheduling event behind the original 9.46-to-13.04 ms change. [Per-round profiles](profile-macos.json)

As a causal check, two disposable binaries omitted the single call to `self.file.sync_data()?` in `Journal::write`. With four readers, indexed publish p99 fell from 10.99 to 4.79 ms. With the same diagnostic omission, #13 took 7.57 ms. This control sacrifices crash durability and was used only on temporary benchmark volumes. The release build still performs the flush. On this Mac the Rust toolchain implements it with `F_FULLFSYNC`, which asks the drive to flush buffered data to permanent storage. [Flush-control trials](flush-control-macos.json) · [Apple file control reference](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fcntl.2.html)

## Other approaches tested

These screens used the same short four-reader workload and a fresh baseline within each comparison. [Per-round publish p99](screening-macos.json)

- Reusing the existing buffered segment reader across relative seeks gave 12.83 ms publish p99 versus 12.35 ms for indexed history in four rounds.
- Bypassing the macOS file cache for the journal writer gave 11.36 ms versus 11.69 ms in eight rounds. This host-specific change had a small effect.
- Reducing Tokio to two workers improved one four-round comparison (12.52 to 11.09 ms) and regressed in another (10.90 to 11.57 ms).
- Moving gRPC history response encoding onto the blocking worker gave 10.20 ms versus 9.67 ms for indexed history. Combining that with admission gave 9.82 ms.
- Preallocating journal space on macOS gave 11.01 ms versus 12.47 ms in a short screen. Shipping that change would also require headroom accounting and disk-full and rollover tests.

For a latency target with a single outstanding durable append, measure the journal flush tail on the intended storage volume. For workloads that can issue several independent appends concurrently, Spooltail already supports sharing a journal sync with partitioned indexes and index batching. A serial publisher has no second append available to share that flush. This study measured the serial case. [Partitioned index and journal batching design](../../docs/design.md)

## Reproduce

Build the parent (`fd4c13f`), original indexed implementation (`1f6054c`) and updated PR with `cargo build --release --locked` in their respective checkouts. Copy the binaries to distinct filenames. Run the following from the repository root with Python 3.11 or later and OpenSSL available.

```sh
uv run --project python --locked python benchmarks/history-tail/mixed.py \
    /tmp/current /tmp/bounded /tmp/parent --rounds 12 --output /tmp/four.json

uv run --project python --locked python benchmarks/history-tail/mixed.py \
    /tmp/current /tmp/bounded --readers 16 --rounds 6 --output /tmp/burst.json

uv run --project python --locked python benchmarks/history-tail/mixed.py \
    /tmp/current /tmp/bounded --publishes 1200 --rounds 6 --output /tmp/sustained.json
```

These runs used one Apple M1 Pro with macOS 27.2.1 and APFS. The release broker served TLS gRPC over localhost with one partition and a shared SQLite index. The benchmark seeds 512 interleaved events across eight tasks and copies that volume for each trial. Readers target synchronized bursts at 50 full pages/s each. A separate Python process targets 75 confirmed appends/s with one outstanding request and a 256-byte payload string. Timing covers the RPC from invocation to response. Time spent waiting to issue a scheduled request is excluded. The test verifies every returned payload, offset and partition in each history page and every acknowledged publish offset. Run order rotates and reverses. With 240 publishes, the nearest-rank p99 is the third-slowest request. These results apply to the measured host and storage.
