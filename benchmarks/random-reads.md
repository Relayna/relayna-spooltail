# Random task-status reads

This workload asks what an HTTP client sees when it already knows a task ID. The client supplies that ID on each request. Both latest-status lookups and recent-history reads run without a stream offset, consumer checkpoint, or SSE `Last-Event-ID`.

The [multiple-generator benchmark](load-generators.md) repeats immediate latest-status reads with independent client processes and measures store lock contention.

## Measured results

Three trials ran on the same Linux ARM64 Docker host with Relayna SDK v1.14.0, RabbitMQ 4.3.6 and Redis 7.4.12. All 40,800 timed requests returned their expected payloads or 404s. Values below are medians of per-trial statistics. [Raw trial data](random-read-results.json)

| Operation | Readers | HTTP read path | p50 | p95 | p99 | Reads/s |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Latest status | 1 | SDK default | 217.43 ms | 238.46 ms | 245.20 ms | 8 |
| Latest status | 1 | SDK single attempt | 0.74 ms | 0.89 ms | 1.37 ms | 1,294 |
| Latest status | 1 | Spooltail | 0.52 ms | 0.67 ms | 1.03 ms | 1,832 |
| Latest status | 16 | SDK default | 213.85 ms | 245.22 ms | 257.28 ms | 123 |
| Latest status | 16 | SDK single attempt | 7.55 ms | 44.27 ms | 73.26 ms | 1,128 |
| Latest status | 16 | Spooltail | 8.15 ms | 29.66 ms | 54.54 ms | 1,129 |
| Six-event SSE history | 1 | SDK default | 2.29 ms | 2.82 ms | 3.58 ms | 388 |
| Six-event SSE history | 1 | Spooltail | 1.12 ms | 1.32 ms | 1.87 ms | 768 |
| Six-event SSE history | 16 | SDK default | 23.56 ms | 34.82 ms | 39.77 ms | 665 |
| Six-event SSE history | 16 | Spooltail | 16.50 ms | 18.41 ms | 19.54 ms | 871 |

Latest-status p50 broken down by outcome shows where the SDK route waits. Its default makes up to five reads with 50 ms between attempts when a task is running or missing. The single-attempt control uses the same Redis keys, SDK handler, and data.

| Readers | HTTP read path | Completed | Running | Unknown ID (404) |
| ---: | --- | ---: | ---: | ---: |
| 1 | SDK default | 2.51 ms | 227.90 ms | 225.36 ms |
| 1 | SDK single attempt | 0.74 ms | 0.75 ms | 0.74 ms |
| 1 | Spooltail | 0.52 ms | 0.52 ms | 0.50 ms |
| 16 | SDK default | 6.36 ms | 227.64 ms | 228.26 ms |
| 16 | SDK single attempt | 7.22 ms | 7.49 ms | 6.42 ms |
| 16 | Spooltail | 8.23 ms | 8.17 ms | 7.98 ms |

## Workload

- **Corpus.** Publish six events for each of 10,000 tasks into both Spooltail and the real Relayna SDK RabbitMQ status stream. The SDK hub populates Redis. Half the tasks finish and half remain running. Wait for all 60,000 distinct SDK event IDs and all Spooltail publish confirmations before measuring. Both stores keep the same corpus across trials.
- **Latest status.** Choose random task IDs with replacement, with 45% completed, 45% running, and 10% absent. Shuffle the resulting list. Compare Spooltail’s `/latest` route, the released SDK’s `/status/{task_id}` route with its defaults, and the same SDK route configured with `latest_retry_attempts=1`. Check the entire event payload or the expected HTTP 404 on every request.
- **Recent history.** Choose uniformly from all 10,000 existing tasks. Open each stack’s HTTP SSE route with only the task ID and read through the sixth status. Check every event ID and payload in order. The client then closes the stream, including for running tasks. This measures the Redis-backed SDK history used by SSE. The SDK’s separate RabbitMQ full-stream history route has a different read path.
- **Measurement.** Use independent seeded request lists for three trials. Each stack receives exactly the same list within a scenario, and stack order rotates between trials. Run 160 timed reads with one client worker and 2,560 with 16 concurrent workers per arm and trial. Reuse a connection pool and warm each arm with a separate request list. A worker submits its next request after consuming and verifying the previous response. Latency ends at receipt of the HTTP body or sixth SSE event. Achieved reads per second includes client verification and stream teardown.
- **Scope.** These are warm, stationary, single-node reads. The SDK’s status data is already present in Redis and all six events fit in its 50-event recent-history window. Read timings exclude publishing, consumer catch-up, cold starts, and archival stream scans. The stacks include their real HTTP handlers, serialization and client overhead.

## Reproduce

Use the [benchmark prerequisites](README.md#run-locally) and run from the repository root.

```sh
python3 benchmarks/run.py --workload random-reads \
  --read-tasks 10000 --statuses 6 --reads-per-reader 160 --readers 16 \
  --trials 3 --seed 1729 --output /tmp/spooltail-random-reads.json
```

For a smoke run, use `--read-tasks 40 --statuses 4 --reads-per-reader 20 --readers 4 --trials 1`. The runner prints median trial results and writes JSON with per-trial timings, percentiles by outcome, request counts and trace hashes, throughput, and service resource counters.
