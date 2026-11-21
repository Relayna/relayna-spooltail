# Spooltail transport benchmark

The [TLS follow-up](TLS.md) compares HTTP and gRPC within one Linux VM and across two Linux VMs.

## Measured results

Three trials covered 225 measurement windows and verified 614,460 API operations or individual live deliveries. All five arms passed the pre-run semantic checks and the post-restart completion check. The host had 10 logical CPUs. The broker used Axum 0.8.9 and Tonic 0.14.9. The Python clients used HTTPX 0.28.1 and grpcio 1.80.0. [Raw trials and source hashes](results.json)

### At an offered 1,000 status reads/s

| Transport | Verified reads/s | p95 | p99 | Broker CPU cores | Caller CPU cores | Combined CPU cores | Broker cgroup memory | Caller PSS | TCP bytes/read |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| HTTP/1.1 TCP | 1,000 | 1.47 ms | 2.00 ms | 0.109 | 0.640 | 0.749 | 62.0 MiB | 30.5 MiB | 684 |
| HTTP/1.1 Unix | 1,000 | 1.52 ms | 1.91 ms | 0.107 | 0.611 | 0.718 | 62.0 MiB | 30.2 MiB | — |
| HTTP/2 TCP | 993 | 3.52 ms | 4.13 ms | 0.118 | 0.610 | 0.728 | 59.5 MiB | 31.2 MiB | 451 |
| gRPC TCP | 1,000 | 1.09 ms | 1.39 ms | 0.262 | 0.339 | 0.601 | 63.4 MiB | 37.0 MiB | 614 |
| gRPC Unix | 1,000 | 1.04 ms | 1.37 ms | 0.277 | 0.365 | 0.642 | 61.1 MiB | 38.0 MiB | — |

gRPC over TCP cut combined CPU by about 20% against the current HTTP/1.1 TCP path. Python caller CPU fell 47%, while broker CPU rose from 0.109 to 0.262 cores. The Python gRPC client used around 6.5 MiB more PSS. HTTP/1.1 over Unix lowered combined CPU by about 4% at this rate and had similar latency. Moving gRPC from TCP to a Unix socket raised combined CPU in this workload.

### Throughput and latency across workloads

Each non-idle cell shows verified operations per second and p95 latency, except live delivery, which reports p95 latency and timed-out rounds per trial. A history operation reads six events. A replay operation opens a stream and verifies all six events through terminal closure. A worker lifecycle includes four client calls.

| Workload | HTTP/1.1 TCP | HTTP/1.1 Unix | HTTP/2 TCP | gRPC TCP | gRPC Unix |
| --- | ---: | ---: | ---: | ---: | ---: |
| empty-rpc | 2,325/s · 0.51 ms | 2,421/s · 0.49 ms | 1,936/s · 0.62 ms | 5,033/s · 0.24 ms | 5,097/s · 0.24 ms |
| cold-latest | 60/s · 17.66 ms | 62/s · 17.69 ms | 58/s · 22.80 ms | 994/s · 1.19 ms | 1,497/s · 0.79 ms |
| latest-serial | 1,710/s · 0.71 ms | 1,712/s · 0.72 ms | 1,442/s · 0.90 ms | 3,221/s · 0.39 ms | 3,473/s · 0.34 ms |
| latest-1000 | 1,000/s · 1.47 ms | 1,000/s · 1.52 ms | 993/s · 3.52 ms | 1,000/s · 1.09 ms | 1,000/s · 1.04 ms |
| latest-16 | 6,487/s · 2.29 ms | 7,235/s · 4.14 ms | 7,198/s · 2.65 ms | 20,542/s · 1.37 ms | 20,825/s · 1.40 ms |
| latest-64 | 10,341/s · 13.55 ms | 9,952/s · 16.02 ms | 11,006/s · 7.18 ms | 21,050/s · 8.19 ms | 21,108/s · 8.33 ms |
| latest-16k | 5,613/s · 2.50 ms | 6,466/s · 4.43 ms | 5,850/s · 3.40 ms | 16,036/s · 1.73 ms | 16,022/s · 1.78 ms |
| history-6 | 7,083/s · 2.57 ms | 6,720/s · 3.74 ms | 6,780/s · 3.00 ms | 9,637/s · 3.61 ms | 9,839/s · 3.69 ms |
| replay-6 | 5,277/s · 3.40 ms | 5,810/s · 4.63 ms | 4,730/s · 3.94 ms | 7,625/s · 3.19 ms | 7,465/s · 3.27 ms |
| mixed-10pct-writes | 5,682/s · 4.64 ms | 5,330/s · 5.38 ms | 3,359/s · 4.48 ms | 8,105/s · 3.67 ms | 8,262/s · 3.50 ms |
| durable-append | 1,603/s · 12.96 ms | 1,583/s · 13.97 ms | 1,385/s · 44.55 ms | 1,840/s · 10.02 ms | 1,807/s · 10.44 ms |
| worker-lifecycle | 544/s · 37.34 ms | 551/s · 38.03 ms | 463/s · 78.86 ms | 668/s · 27.38 ms | 642/s · 31.23 ms |
| live-16 | 5.89 ms p95 · 0 timeouts | 4.79 ms p95 · 0 timeouts | — · 20 timeouts | 2.27 ms p95 · 0 timeouts | 2.13 ms p95 · 0 timeouts |
| idle | 0.002 cores | 0.002 cores | 0.002 cores | 0.005 cores | 0.005 cores |
| idle-64-streams | 0.003 cores | 0.002 cores | 0.002 cores | 0.005 cores | 0.005 cores |

- **Read saturation.** At 64 concurrent lookups, gRPC TCP sustained 21,050 reads/s against 10,341 for HTTP/1.1 TCP. Combined CPU cost was 0.300 versus 0.810 CPU-seconds per 1,000 verified reads. At 16 concurrent lookups with 16 KiB payloads it sustained 16,036 reads/s against 5,613. The gRPC paths still decode JSON event bodies.
- **History, writes and worker work.** gRPC TCP improved history throughput but its history p95 was 3.61 ms versus 2.57 ms for HTTP/1.1 TCP at each protocol's achieved rate. The durable append gap was smaller at 1,840 versus 1,603 writes/s, with the same flush and commit requirements. gRPC TCP completed 668 full worker cycles/s versus 544 for HTTP/1.1 TCP.
- **Streams.** All 60 live rounds completed on both gRPC and both HTTP/1.1 variants. HTTP/2 with HTTPX missed subscriber readiness in all 60 rounds before the publisher sent the event. The harness reconnected after each deadline. An earlier diagnostic without reconnection reached the HTTPX transport's 200-open-stream error. The failure observation applies to this HTTP/2, HTTPX, and SSE setup. HTTP/2 unary requests and finite SSE replay completed.
- **Idle subscriptions.** With 64 open streams, median end-of-window broker memory was 59.3 MiB for HTTP/1.1 TCP and 57.4 MiB for gRPC TCP. Caller PSS was 30.1 and 37.9 MiB respectively. Combined idle CPU was below 0.006 cores for every arm. Each idle window lasted six seconds.
- **Connection setup and network traffic.** The first status read with a newly constructed client took 17.66 ms p95 on HTTP/1.1 TCP and 1.19 ms on gRPC TCP. Those measurements include library client construction. The warmed empty request took 0.51 and 0.24 ms p95 respectively. HTTP/2 with JSON used about 34% fewer TCP interface bytes per completed read than HTTP/1.1 at the offered 1,000 reads/s.

### Protocol choice

For Python service-to-broker traffic on the tested workload, gRPC over TCP is the strongest candidate when total CPU, read capacity, and live delivery matter. HTTP/1.1 remains the lower-CPU server choice at 1,000 reads/s and serves existing browser SSE clients. HTTP over a Unix socket is a deployment option for colocated processes that can share a socket path. gRPC over Unix had nearly identical 64-reader throughput to gRPC over TCP.

This comparison used HTTPX for HTTP and grpcio for gRPC. The results describe those client stacks together with their protocols. These measurements predate the versioned gRPC service and apply to the benchmark adapter. The [TLS follow-up](TLS.md) measured cross-VM traffic separately.

## Compared variants

- **HTTP/1.1 over TCP.** The current Axum API and HTTPX client.
- **HTTP/1.1 over Unix socket.** The same Axum routes, JSON and HTTPX client over a shared Unix socket.
- **HTTP/2 over TCP.** The same Axum routes and JSON. HTTPX uses cleartext HTTP/2 prior knowledge, and the benchmark checks the negotiated version.
- **gRPC over TCP and over Unix socket.** Tonic, generated Python gRPC asyncio stubs, and a Protobuf message envelope. Arbitrary event fields remain JSON bytes to preserve the existing event contract. Both transports call the same broker, SQLite, journal, lease persistence, and subscription implementation.

The comparison uses a single Linux ARM64 Docker host and separate persistent data directories for the five server processes. Callers and servers run in separate containers. TCP crosses the Compose bridge and the Unix transports share a mounted socket directory. The same release build serves all arms with HTTP/2 and the benchmark adapters compiled in. The default broker build retains its ordinary HTTP routes. Traffic is local and unencrypted, with no compression or external load balancer. Each store starts with 1,500 small-payload tasks and 250 large-payload tasks, with six events apiece.

## Workloads and measurements

- **Empty request.** A warmed connection calls a no-store HTTP 204 or an empty gRPC response.
- **First read.** Each operation creates a fresh client and connection, reads a task, and closes the client. This includes the Python client's initialization and the connection handshake. gRPC uses a per-channel subchannel pool in this workload.
- **Latest by task ID.** The same random trace runs against all variants, with 10% missing IDs. Each arm runs serially, at 1,000 reads/s with up to 16 in flight, with four generators and 16 in flight, and with eight generators and 64 in flight. A separate window uses 16 KiB event bodies at the four-process load. The standard body is 128 bytes plus identifiers and metadata.
- **History and replay.** A page reads six retained records per task. The replay test requests those six events through SSE or gRPC server streaming and verifies the stream closes after a terminal event.
- **Mixed traffic and worker work.** One window mixes 90% reads with 10% distinct durable writes. A second window measures durable appends. A worker cycle includes append, claim, atomic completion, and a verified latest status read. Worker cycles run on independent work topics so each worker can verify its own claim. Durability uses the standard journal flush and SQLite commit in every transport.
- **Live delivery and idle.** The live test attaches 16 watchers to one task, waits for all to report ready, and sends a terminal status from an independent publisher connection. Latency runs from publish dispatch until each watcher receives the event. Each round has a two-second deadline for readiness, publication, delivery, and stream closure. The report counts timed-out rounds separately. Separate idle windows measure an established client with one reusable channel and one with 64 open subscriptions.
- **Parity checks.** Before timing, each arm verifies duplicate append, conflicting event ID, missing task, ordered resume, empty claim, rejection of a stale lease, idempotent completion, live delivery, terminal closure, and a retention-gap notification for an unknown cursor. After timing, the runner restarts every broker and rechecks the completion receipt and settled lease. All timed responses are checked for the expected event, order, offset, or missing status. Unexpected failures fail the run.

Reported percentiles are medians of each trial's percentiles. Throughput divides verified workload operations by their common wall-clock interval. Client CPU is summed process CPU time across all generators, including JSON decoding, verification, scheduling, and the gRPC client's native threads. Server CPU and memory come from Linux cgroups. Idle CPU includes a small sampling cost. Container memory includes filesystem cache, and the samples also record anonymous memory separately. Client PSS apportions shared process pages. TCP interface bytes include protocol framing and transport traffic on the container interface. Unix sockets bypass this interface, so those counters do not describe the number of bytes copied through a Unix socket.

## Reproduce

Run from the repository root with Docker Compose and a Linux cgroup v2 Docker engine. The `bench-transports` feature selects the benchmark listeners. The benchmark image builds Python stubs from the current [Spooltail v1 schema](../../proto/spooltail/v1/spooltail.proto).

```sh
python3 benchmarks/run_protocols.py --trials 3 --tasks 1500 --large-tasks 250 \
  --requests 10000 --seconds 6 --seed 1729 --output /tmp/spooltail-protocols.json
python3 benchmarks/protocols/summary.py /tmp/spooltail-protocols.json
```

For a smoke run, use `--trials 1 --tasks 12 --large-tasks 4 --requests 100 --seconds 1`. Use `--wires http1 grpc` for a subset or `--live-only` to focus on fan-out. The runner uses throwaway containers and volumes with no published host ports.
