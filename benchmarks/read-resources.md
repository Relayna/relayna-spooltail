# CPU and memory for random task-status reads

This experiment measures the resources used by Spooltail, by the Relayna SDK deployment, and by a small Python service that looks up task status over HTTP. The offered read rate is the same for each stack.

## Measured CPU and memory

At 1,000 random reads/s, Spooltail used 0.102 CPU cores and 49.4 MiB. The Python caller used 0.599 cores and 35.9 MiB. Of the server's 49.4 MiB cgroup footprint, 31.5 MiB was file cache and 13.8 MiB was anonymous memory. The caller's process RSS was 40.6 MiB and its proportional set size was 35.3 MiB.

All 79,200 timed lookups across three trials returned the expected event or 404. Each stack achieved between 999.9 and 1,000.0 reads/s in the 1,000 reads/s trials. The following numbers are medians of per-trial measurements on a single 10-CPU Linux ARM64 Docker host. The SDK read path uses one API worker and Redis. Its server totals also include the online RabbitMQ broker. [Raw trials](read-resource-results.json)

| Offered reads/s | Spooltail server CPU | Spooltail server memory | Caller CPU with Spooltail | Caller memory with Spooltail | SDK servers CPU | SDK servers memory | Caller CPU with SDK | Caller memory with SDK |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Idle | 0.0013 cores | 49.5 MiB | <0.001 cores | 35.4 MiB | 0.0254 cores | 345.9 MiB | <0.001 cores | 35.5 MiB |
| 100 | 0.056 cores | 51.8 MiB | 0.286 cores | 35.4 MiB | 0.163 cores | 350.6 MiB | 0.240 cores | 35.7 MiB |
| 1,000 | 0.102 cores | 49.4 MiB | 0.599 cores | 35.9 MiB | 0.309 cores | 350.6 MiB | 0.559 cores | 35.5 MiB |

At 1,000 reads/s the SDK API and Redis together used 0.307 cores and 166.6 MiB. RabbitMQ stayed online and used 0.002 cores and 182.8 MiB while reads went to Redis. The Spooltail caller used about 7% more CPU per lookup than the same caller against the SDK. Across the caller and servers combined, Spooltail used about 19% less CPU and 78% less memory. The SDK hub had exited after ingestion.

| At 1,000 reads/s | CPU-seconds per 1,000 verified reads | End memory |
| --- | ---: | ---: |
| Spooltail server and caller | 0.701 | 85.2 MiB |
| SDK API, Redis, RabbitMQ and caller | 0.861 | 385.9 MiB |

Totals are summed within each trial before taking the median. The CPU measurements depend on the offered rate. At 100 reads/s, Spooltail and its caller used 3.431 CPU-seconds per 1,000 reads, compared with 0.701 at 1,000 reads/s. The Python process includes request pacing, HTTP, JSON parsing and full response validation. The benchmark measures their combined cost.

## Workload and measurements

- The seeder writes six events for each of 10,000 tasks, waits for all 60,000 SDK events to reach Redis, then exits. The timed windows use warm data with no writes. The SDK HTTP API runs one Uvicorn worker and uses its single-attempt route. RabbitMQ stays online. The hub and publisher are absent during reads.
- One fresh Linux container per stack and trial runs a Python process with an HTTPX connection pool and up to 16 in-flight requests. It imports the HTTP client and benchmark helpers. The seeder's RabbitMQ, Redis, and Relayna libraries are loaded in a separate process. The client verifies each returned JSON event or expected 404. It runs no application-specific task handler.
- Both arms use the same seeded task IDs in each trial, with 45% completed tasks, 45% running tasks, and 10% unknown IDs. A separate trace warms the connections before each window. Requests are scheduled evenly across 12 seconds. The idle window holds the warmed client open and issues zero requests. Client order alternates and rate order rotates across the three trials.
- CPU cores are CPU seconds divided by measured wall seconds. One core means 100% of one logical CPU. CPU seconds per 1,000 verified reads include parsing and verification on the calling side. Server CPU comes from Linux cgroups. Sampling those counters has a small cost, most visible at idle. Client CPU comes from the process clock while it serves the scheduled workload.
- Memory is the end-of-window cgroup charge in MiB. It includes anonymous memory, charged file cache and kernel memory. The raw trials include starting and ending values, anonymous memory, and file cache. Client process RSS, proportional set size (PSS), and lifetime peak RSS are also recorded. These measurements cover the benchmark corpus and exclude the business logic of a deployed client service.

## Reproduce

Use the [benchmark prerequisites](README.md#run-locally) and run from the repository root.

```sh
python3 benchmarks/run.py --workload read-resources \
  --rates 0,100,1000 --seconds 12 --readers 16 --read-tasks 10000 \
  --statuses 6 --trials 3 --seed 1729 --output /tmp/read-resources.json
python3 benchmarks/read_resources_report.py /tmp/read-resources.json
```
