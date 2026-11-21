# Spooltail TLS across Linux VMs

This benchmark compares HTTP/1.1 with JSON, HTTP/2 with JSON, and gRPC with Tonic. Each uses the same Spooltail storage and workload. The benchmark gRPC adapter carries arbitrary event JSON in its Protobuf messages.

## Results

Three trials covered 234 measurement windows and verified 386,082 operations or live deliveries. All 108 timed fan-out rounds completed. The TLS probes verified the same leaf certificate, TLS 1.3, and the expected ALPN on all six endpoints and topology combinations. [Raw results](tls-results.json)

### At an offered 1,000 status reads/s

| Topology | Transport | Verified reads/s | p95 request | p95 dispatch delay | Broker CPU | Client CPU | CPU-s per 1,000 reads | Broker memory | Client PSS |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| same-vm | HTTP/1.1 TCP | 1,000 | 1.69 ms | 1.23 ms | 0.108 cores | 0.664 cores | 0.771 | 47.3 MiB | 29.6 MiB |
| same-vm | HTTP/2 TCP | 1,000 | 1.59 ms | 1.18 ms | 0.123 cores | 0.624 cores | 0.747 | 42.5 MiB | 29.7 MiB |
| same-vm | gRPC TCP | 1,000 | 1.02 ms | 1.15 ms | 0.251 cores | 0.314 cores | 0.565 | 48.7 MiB | 38.0 MiB |
| cross-vm | HTTP/1.1 TCP | 812 | 60.46 ms | 1117.22 ms | 0.118 cores | 0.999 cores | 1.376 | 45.9 MiB | 30.7 MiB |
| cross-vm | HTTP/2 TCP | 1,000 | 2.61 ms | 0.66 ms | 0.165 cores | 0.766 cores | 0.931 | 40.8 MiB | 30.4 MiB |
| cross-vm | gRPC TCP | 1,000 | 1.80 ms | 1.26 ms | 0.177 cores | 0.272 cores | 0.447 | 45.8 MiB | 37.6 MiB |

Across VMs, gRPC used 0.447 combined CPU-seconds per 1,000 verified reads versus 0.931 for HTTP/2. The broker used 0.177 versus 0.165 cores, while the Python caller used 0.272 versus 0.766. HTTP/1.1 fell behind the offered rate with its Python caller at one full core. Its p95 request latency excludes the dispatch queue, whose p95 delay reached 1.12 seconds. CPU cost is summed within each trial before taking the median.

### Throughput and p95 by workload

Table cells show verified operations per second and p95 request latency. Live delivery measures publish dispatch to subscriber receipt and reports timed-out rounds separately. Fresh-client results include pool or channel setup and certificate verification.

#### same-vm

| Workload | HTTP/1.1 TCP | HTTP/2 TCP | gRPC TCP |
| --- | ---: | ---: | ---: |
| empty-rpc | 2,336/s · 0.50 ms | 1,939/s · 0.61 ms | 5,170/s · 0.23 ms |
| cold-latest | 430/s · 2.79 ms | 316/s · 4.00 ms | 571/s · 2.01 ms |
| latest-serial | 1,525/s · 0.86 ms | 1,413/s · 0.88 ms | 3,336/s · 0.36 ms |
| latest-1000 | 1,000/s · 1.69 ms | 1,000/s · 1.59 ms | 1,000/s · 1.02 ms |
| latest-16 | 3,026/s · 8.30 ms | 3,276/s · 8.71 ms | 16,477/s · 1.62 ms |
| latest-64 | 2,441/s · 23.28 ms | 3,090/s · 29.54 ms | 18,250/s · 5.57 ms |
| latest-16k | 2,351/s · 8.71 ms | 2,526/s · 9.44 ms | 8,789/s · 2.85 ms |
| history-6 | 2,603/s · 8.48 ms | 2,938/s · 8.96 ms | 8,588/s · 2.98 ms |
| replay-6 | 2,284/s · 9.24 ms | 1,869/s · 16.37 ms | 5,040/s · 5.96 ms |
| mixed-10pct-writes | 2,614/s · 7.82 ms | 2,845/s · 9.30 ms | 7,180/s · 3.52 ms |
| durable-append | 976/s · 19.45 ms | 910/s · 21.55 ms | 1,615/s · 11.16 ms |
| worker-lifecycle | 530/s · 40.61 ms | 477/s · 47.52 ms | 608/s · 28.63 ms |
| live-16 | 5.31 ms p95 · 0 timeouts | 6.11 ms p95 · 0 timeouts | 2.93 ms p95 · 0 timeouts |

#### cross-vm

| Workload | HTTP/1.1 TCP | HTTP/2 TCP | gRPC TCP |
| --- | ---: | ---: | ---: |
| empty-rpc | 1,004/s · 1.28 ms | 937/s · 1.31 ms | 1,446/s · 0.85 ms |
| cold-latest | 243/s · 4.81 ms | 195/s · 5.95 ms | 253/s · 4.63 ms |
| latest-serial | 776/s · 1.67 ms | 684/s · 1.76 ms | 1,174/s · 1.07 ms |
| latest-1000 | 812/s · 60.46 ms | 1,000/s · 2.61 ms | 1,000/s · 1.80 ms |
| latest-16 | 2,397/s · 8.92 ms | 2,350/s · 8.66 ms | 8,678/s · 2.67 ms |
| latest-64 | 1,940/s · 31.91 ms | 2,283/s · 32.45 ms | 11,746/s · 10.13 ms |
| latest-16k | 1,958/s · 14.25 ms | 1,815/s · 13.11 ms | 5,198/s · 5.89 ms |
| history-6 | 2,116/s · 10.60 ms | 2,139/s · 12.02 ms | 6,282/s · 3.82 ms |
| replay-6 | 1,727/s · 12.36 ms | 1,324/s · 14.67 ms | 3,611/s · 5.93 ms |
| mixed-10pct-writes | 2,334/s · 10.07 ms | 2,385/s · 9.12 ms | 4,990/s · 5.33 ms |
| durable-append | 1,167/s · 16.03 ms | 1,129/s · 19.38 ms | 1,350/s · 20.73 ms |
| worker-lifecycle | 416/s · 47.04 ms | 392/s · 47.21 ms | 456/s · 43.75 ms |
| live-16 | 6.13 ms p95 · 0 timeouts | 6.42 ms p95 · 0 timeouts | 3.85 ms p95 · 0 timeouts |

### Decision

- **Internal service API.** Choose gRPC over TCP with TLS for the tested Python workload. Across VMs, gRPC sustained 11,746 reads/s at 64 concurrent reads versus 2,283 for HTTP/2 and 1,940 for HTTP/1.1. At 1,000 reads/s it used about 52% less combined CPU than HTTP/2. The client used about 7 MiB more PSS.
- **Writes and startup.** For cross-VM durable append, gRPC delivered 1,350/s versus 1,167/s for HTTP/1.1, with p95 latency of 20.73 ms versus 16.03 ms at each arm's achieved load. Worker-cycle throughput improved from 416 to 456/s. Fresh-client p95 was close for HTTP/1.1 and gRPC at 4.81 and 4.63 ms.
- **Browser and HTTP compatibility.** Keep the existing SSE interface for browsers. In this TLS run, HTTP/2 with HTTPX completed all live fan-out rounds. This run changed both TLS and the TCP socket setting from the earlier plaintext comparison, leaving the cause of those earlier readiness failures open.

The recommendation applies to the tested Python libraries and a local two-VM network over one physical machine. The cross-VM path includes Docker Desktop's macOS port forwarding. The large read-throughput ratios reflect this workload and two-vCPU caller setup.

## Setup

The Docker Desktop Linux VM runs the broker for both topologies. In the same-VM control, the client connects over a private Docker bridge. In the cross-VM run, the same client filesystem runs in a separate Lima Linux VM and reaches the broker through a Docker Desktop port forwarded on macOS loopback. The runner checks the image's filesystem-layer hashes and both guest kernel boot IDs. Both client configurations are restricted to two vCPUs. The VMs share one physical Mac.

Axum and Tonic terminate TLS in the broker with `TCP_NODELAY` enabled. Each receives the same ECDSA server identity. The test CA is generated per run and is the only trust root supplied to the clients. A protocol probe checks the negotiated TLS version, ALPN, and leaf fingerprint from each topology and rejects a mismatched hostname. The HTTP driver uses HTTPX and the gRPC driver uses grpcio.

The run uses the existing deterministic workload generator. All matched read traces and payloads are identical within a trial. The two sets of writes use distinct topics so each hits the durable write path. The timed phases cover fresh connections, warmed reads, paced load, saturation, large payloads, history, replay, mixed reads and writes, durable append, worker completion, and live fan-out. A phase with `1000` in its name offers 1,000 reads/s and separately reports completed throughput and dispatch delay.

CPU is measured in the broker container and the Python load-generator processes. TCP forwarding, the host runtime, and other system overhead are outside those CPU counters. Latency includes the full client-to-server path. Fresh-client latency includes constructing a new pool or channel and connecting, with trust materials already loaded. The benchmark reuses connections in the other phases.

## Reproduce

Run from the repository root on macOS with Docker Desktop, Lima, Python, and OpenSSL. Supply a running two-vCPU Linux VM with Docker, a reachable `host.lima.internal`, and passwordless Docker access through `sudo -n`. For example, provision a benchmark VM from Lima's rootful Docker template.

```sh
limactl start --name=spooltail-tls-client --cpus=2 --memory=3 --disk=12 \
  --vm-type=vz --mount-none --tty=false template:docker-rootful
python3 benchmarks/run_tls_protocols.py --lima spooltail-tls-client \
  --output /tmp/spooltail-tls-results.json
python3 benchmarks/protocols/tls_summary.py /tmp/spooltail-tls-results.json
```

The runner creates isolated containers, volumes, and credentials and removes them when finished. It does not install the benchmark CA in either VM's system trust store. Use `--phases empty-rpc latest-1000` for a focused run.
