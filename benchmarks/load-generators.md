# Random task-status reads with multiple load generators

This benchmark compares Spooltail’s latest-status endpoint with Relayna SDK’s Redis-backed latest-status endpoint set to `latest_retry_attempts=1`. Both return the status available at request time. The [earlier benchmark](random-reads.md) also measured the SDK's default retries and six-event SSE history.

The [client and server resource benchmark](read-resources.md) measures CPU and memory at idle and at matched request rates.

## Measured results

At 16 total readers, moving from one generator to eight raised Spooltail throughput from 1,269 to 12,302 verified reads/s. The SDK went from 1,260 to 4,258. At eight generators Spooltail delivered 2.89× the throughput with a p95 of 2.06 ms versus 4.21 ms. Both stacks returned the expected result for every request at this setting.

Three trials ran on one Linux ARM64 Docker host with 10 CPUs, Relayna SDK v1.14.0, RabbitMQ 4.3.6 and Redis 7.4.12. The table shows medians of per-trial measurements for the SDK's single-attempt route with its default Redis pool. Failures are totals across trials. Across all configurations, 719,946 of 720,000 attempts returned the expected event or 404. [Raw per-trial and per-generator data](load-generator-results.json)

| Generators | Total readers | Spooltail reads/s | SDK reads/s | Spooltail p95 | SDK p95 | SDK failures / attempts |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 16 | 1,269 | 1,260 | 39.70 ms | 39.39 ms | 0 / 48,000 |
| 2 | 16 | 3,183 | 3,483 | 4.01 ms | 11.97 ms | 0 / 48,000 |
| 4 | 16 | 8,876 | 4,361 | 2.15 ms | 4.20 ms | 0 / 48,000 |
| 8 | 16 | 12,302 | 4,258 | 2.06 ms | 4.21 ms | 0 / 48,000 |
| 4 | 64 | 4,124 | 4,334 | 46.80 ms | 20.44 ms | 0 / 48,000 |
| 8 | 64 | 11,464 | 4,325 | 16.82 ms | 15.84 ms | 0 / 48,000 |
| 8 | 128 | 7,793 | 4,174 | 48.09 ms | 32.38 ms | 54 / 48,000 |

### CPU and mutex measurements

CPU columns show average cores used during each run. The mutex columns show the average wait per Spooltail request and the fraction of the timed interval spent executing store operations under the mutex. All entries are medians of three trials.

| Generators | Readers | Spooltail client cores | SDK client cores | Spooltail server cores | SDK API cores | Redis cores | Mean mutex wait | Mutex held |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 16 | 1.00 | 1.00 | 0.12 | 0.34 | 0.03 | <0.001 ms | 3.9% |
| 2 | 16 | 1.91 | 1.98 | 0.31 | 0.83 | 0.07 | 0.007 ms | 9.9% |
| 4 | 16 | 3.96 | 2.25 | 0.85 | 1.00 | 0.10 | 0.021 ms | 25.7% |
| 8 | 16 | 6.59 | 2.38 | 1.33 | 1.00 | 0.10 | 0.134 ms | 51.7% |
| 4 | 64 | 3.97 | 2.75 | 0.38 | 0.99 | 0.10 | 0.007 ms | 12.1% |
| 8 | 64 | 7.41 | 2.53 | 1.39 | 1.00 | 0.10 | 0.298 ms | 60.5% |
| 8 | 128 | 7.56 | 2.48 | 1.07 | 1.00 | 0.10 | 0.151 ms | 45.9% |

With one generator, the load process used one full CPU core on either stack. At eight generators and 16 readers, Spooltail's clients used 6.59 cores. The SDK API worker used 1.00 core and Redis used 0.10, consistent with the single API worker limiting SDK throughput. The benchmark used a single API worker throughout.

Spooltail's highest rate in this sweep was at eight generators and 16 readers. Its mean mutex wait there was 0.134 ms, with 51.7% of elapsed time spent executing operations under the lock. Increasing to 128 readers reduced its throughput to 7,793 reads/s and raised p95 to 48.09 ms. Its clients consumed 7.56 cores at that point and the mutex was held 45.9% of the interval. These measurements leave several possible sources of delay, including client scheduling, and do not establish a backend capacity limit.

### Redis pool at 128 readers

The SDK's default 100-connection pool had 54 failures in 48,000 timed attempts (27 HTTP 500s and 27 transport read errors). The SDK API container logged 47 Redis pool-exhaustion exceptions across warmup and timing. The 128-connection control returned all 48,000 expected results and sustained 4,158 verified reads/s with a p95 of 32.57 ms. Spooltail also returned all 48,000 expected results. At this load Spooltail had higher throughput and the SDK had lower p95 latency.

## Workload

The runner publishes six statuses per task for 10,000 tasks to each stack. It waits until Spooltail has acknowledged every write and the SDK hub has applied all 60,000 events from RabbitMQ to Redis. The timed phase uses warm data with no concurrent writes. Each trial uses the same shuffled list of 16,000 task IDs for every stack and load setting. The list contains 45% completed tasks, 45% running tasks and 10% unknown IDs. The generators verify the complete payload of each returned event and check each expected 404. They count HTTP 5xx responses and transport failures separately. Each timed request gets one attempt.

Each generator is an independent Python process with its own event loop and persistent HTTP connection pool. The coordinator divides the request list across generators, waits for all pools to warm up, then releases the generators together. In `4x16`, for example, four processes each run four concurrent requests, for 16 in total. Each reader submits its next request after verifying the previous response. The coordinator shuffles the order of load settings across trials and alternates which stack runs first.

Throughput is the total verified requests divided by wall time from the first generator starting until the last generator finishes. Latency percentiles pool verified HTTP responses from all generators within each trial. The JSON also includes latency by error class. Generator CPU is summed process CPU time divided by the common elapsed time. Server CPU comes from Linux cgroup counters around the run. All services and generators share one Docker host. The SDK API runs one Uvicorn worker. RabbitMQ handles ingestion before the timed phase.

The SDK's Redis client uses its default 100-connection pool. For settings above 100 readers the runner also tests the same handler and Redis data with a separate pool sized to the highest requested concurrency. It collects Redis client exceptions from the API container logs, including warmup.

A benchmark-only Spooltail build measures time spent waiting for the shared store mutex and time executing the store operation while holding it. The driver samples counters after warmup and again after all timed requests finish. It checks for exactly one store operation per lookup, including 404s. The wait measurement starts when the blocking thread reaches the mutex. HTTP handling and time waiting for a blocking thread fall outside this measurement.

## Reproduce

Use the [benchmark prerequisites](README.md#run-locally) and run from the repository root.

```sh
python3 benchmarks/run.py --workload load-generators \
  --load-shapes 1x16,2x16,4x16,8x16,4x64,8x64,8x128 \
  --read-tasks 10000 --statuses 6 --requests 16000 --trials 3 --seed 1729 \
  --output /tmp/spooltail-load-generators.json
```

For a smoke run, use `--load-shapes 1x4,2x4,3x8 --read-tasks 40 --statuses 4 --requests 200 --trials 1`.
