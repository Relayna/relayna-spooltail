# Publish latency during task history reads

Four readers repeatedly requested full, indexed history pages while a separate process waited for each durable publish acknowledgement. The largest timed component of a slow indexed publish was the journal flush. The store lock had been the main source of contention in the older history implementation. Indexing the page reduced that contention.

## What changed

HTTP and gRPC history requests and the page reads behind SSE, WebSocket and gRPC subscriptions now share one history-read permit per broker. Previously, each request could start a blocking job and wait on the same store mutex. Waiting for a permit asynchronously lets a burst queue before occupying blocking threads that also serve appends. Only the active page read owns the permit. A client can disconnect, but its blocking read holds the permit until it finishes. Serialization of responses and network delivery proceed outside the permit. The publisher still syncs its journal and commits its SQLite index before replying. [Tokio blocking-task and cancellation behavior](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html)

## Before and after

Each number is the median of the trial percentiles, in milliseconds. “Indexed” is the original #14 commit and “bounded” adds history admission at `ea062e8`. The macOS allocation experiment is measured separately below. Compare builds within each workload. Smaller values are faster.

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

## Four-reader storage controls

In a control on macOS, the four readers still issued History RPCs, but the publisher process never connected to Spooltail. At 75 writes/s it wrote a separate temporary file on the same APFS volume and called `F_FULLFSYNC` after each write. The process verified the final contents. Its write-plus-sync p99 rose from 7.33 ms with #13 serving history to 8.80 ms with the original indexed #14 serving history, increasing in seven of eight paired rounds of 360 writes. On the Linux VM, the same independent writer used `fdatasync` and its p99 rose from 3.83 to 7.93 ms in six of six paired rounds. The reader workload affected elapsed write-and-sync time outside the broker on both systems. The timed calls include time waiting for the filesystem, device and CPU scheduler. These separate trials cannot apportion the exact historical 3.58 ms increase.

The same four-reader workload also ran on a Docker Desktop Linux VM on this Mac. The SQLite database and journals were on a Docker-managed ext4 volume. It ran Ubuntu 24.04 user space, LinuxKit 7.0.14 and Python 3.12.3. Each of six rounds included 600 confirmed publishes per build. Latencies are in milliseconds.

| Linux build | History p50 | History p99 | Publish p50 | Publish p99 |
| --- | ---: | ---: | ---: | ---: |
| #13 | 2.54 | 4.61 | 2.60 | 6.78 |
| Original indexed #14 | 2.05 | 4.47 | 2.97 | 6.60 |
| Bounded #14 | 2.07 | 5.02 | 2.86 | 6.49 |

Bounded #14 beat the original indexed version in two of six pairs. The historical increase did not reproduce in this environment. Storage, kernel, runtime scheduling and the Python interpreter all differed between environments.

Five short macOS rounds compared the pushed admission change with three scratch builds. Reserving 1 MiB at the physical end of the open journal gave 12.61 ms publish p99 against 12.98 ms for the same code with ordinary file growth. Increasing SQLite's WAL auto-checkpoint threshold from 1,000 to 8,192 pages gave 13.09 ms. Setting the publish thread's disk I/O policy to important for the duration of the append gave 13.74 ms. All builds kept the journal flush and SQLite durability settings. Larger automatic checkpoints trade fewer checkpoints for a larger WAL. [SQLite WAL checkpoint behavior](https://sqlite.org/wal.html#ckpt)

The longer preallocation screen used six rounds of 900 publishes. Ordinary growth measured 13.78 ms publish p99 and the startup allocation prototype measured 12.96 ms, winning five pairs. Repeating that startup call on a one-byte APFS file allocated another 1 MiB each time. Three reservations still occupied more than 3 MiB after close and reopen.

Six further rounds used the same release binary, four readers and 480 confirmed appends per trial. Shared indexing gave 12.45 ms publish p99, inline partition indexing gave 13.06 ms, and partition indexing with batch size 32 gave 12.92 ms. The corresponding publish medians were 6.83, 6.55 and 6.40 ms. Each trial started on a private copy of the shared seed. The two partitioned cases opened that copy with `SPOOLTAIL_INDEX_MODE=partitioned` and `SPOOLTAIL_INDEX_BATCH_SIZE` set to 1 or 32. The runner cleared inherited broker settings, so these two configurations used executable wrappers that set those variables before starting the identical release binary.

### Bounded preallocation and the shipping decision

The follow-up prototype requested space only on writes to one shared-index journal on macOS. It counted existing physical blocks, capped the requested extent, stopped short of the region where a segment can roll over, and skipped the hint under disk pressure or a configured reserve. Its release candidate capped every request at 1 MiB and checked that at least 16 MiB would remain available before calling the filesystem. The journal write, flush, and SQLite commit ran in their original order.

| Four-reader experiment | Original #14 publish p99 | Bounded #14 publish p99 | Allocation prototype publish p99 | Paired wins over bounded #14 |
| --- | ---: | ---: | ---: | ---: |
| First bounded prototype, 600 publishes per round | 13.41 | 13.45 | 12.49 | 6/8 |
| Exact release candidate, 900 publishes per round | 13.12 | 13.23 | 13.23 | 3/6 |

The release candidate beat the original indexed build in two of six pairs. Its history p50 was 3.51 ms, compared with 3.58 ms for the original indexed build and 3.60 ms for bounded admission. Earlier gains did not repeat in the longer comparison. PR #14 keeps bounded history admission. The allocator remains a benchmark experiment. [Ordered trials, binary hashes and environment details](storage-followup.json) · [Reproducible allocator patch](macos-preallocation.patch)

The release candidate retained exactly 1 MiB of physical allocation across eight forced restarts while the acknowledged record history advanced. Reads resumed across a segment rollover. On an isolated APFS disk image, a small append used ordinary growth under low headroom, a full disk produced a write error, and restart after freeing space preserved the prior acknowledgement. Retrying the failed event twice returned the same offset. These checks exercised process exits and disk exhaustion. Power loss was outside this experiment.

## Reproduce

Build the parent (`fd4c13f`), original indexed implementation (`1f6054c`) and bounded build (`ea062e8`) with `cargo build --release --locked` in their respective checkouts. Copy the binaries to distinct filenames. For the optional `final-extent` comparison, apply `benchmarks/history-tail/macos-preallocation.patch` in a disposable checkout of the bounded build and compile again. Pass the patch from this report’s checkout if the bounded checkout predates it. Run the following from the repository root with Python 3.11 or later and OpenSSL available.

```sh
uv run --project python --locked python benchmarks/history-tail/mixed.py \
    /tmp/current /tmp/bounded /tmp/parent --rounds 12 --output /tmp/four.json

uv run --project python --locked python benchmarks/history-tail/mixed.py \
    /tmp/current /tmp/bounded --readers 16 --rounds 6 --output /tmp/burst.json

uv run --project python --locked python benchmarks/history-tail/mixed.py \
    /tmp/current /tmp/bounded --publishes 1200 --rounds 6 --output /tmp/sustained.json

uv run --project python --locked python benchmarks/history-tail/mixed.py \
    /tmp/final-extent /tmp/current /tmp/bounded --publishes 900 --rounds 6 \
    --output /tmp/allocation.json

uv run --project python --locked python benchmarks/history-tail/mixed.py \
    /tmp/parent /tmp/current --file-sync-control --publishes 360 --rounds 8 \
    --output /tmp/independent-file.json
```

The native runs used an Apple M1 Pro with macOS 27.2.1 and APFS. The Linux runs used the VM and ext4 volume described above. The default comparison serves TLS gRPC over localhost with one partition and a shared SQLite index. Set `TMPDIR` to a directory on the storage volume you want to measure. The benchmark seeds 512 interleaved events across eight tasks and copies that volume for each trial. Readers target synchronized bursts at 50 full pages/s each. A separate Python process targets 75 confirmed appends/s with one outstanding request and a 256-byte payload string. Timing covers the RPC from invocation to response. Time spent waiting to issue a scheduled request is excluded. The test verifies every returned payload, offset and partition in each history page and every acknowledged publish offset. Run order rotates and reverses. With 240 publishes, the nearest-rank p99 is the third-slowest request. These results apply to the measured host and storage.
