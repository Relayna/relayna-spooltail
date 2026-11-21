"""Summarize protocol results with per-trial normalization before taking medians."""

import argparse
import json
from pathlib import Path
from statistics import median

LABEL = {
    "http1": "HTTP/1.1 TCP",
    "http-uds": "HTTP/1.1 Unix",
    "http2": "HTTP/2 TCP",
    "grpc": "gRPC TCP",
    "grpc-uds": "gRPC Unix",
}
ORDER = (
    "empty-rpc",
    "cold-latest",
    "latest-serial",
    "latest-1000",
    "latest-16",
    "latest-64",
    "latest-16k",
    "history-6",
    "replay-6",
    "mixed-10pct-writes",
    "durable-append",
    "worker-lifecycle",
    "live-16",
    "idle",
    "idle-64-streams",
)


def metrics(row):
    after = row["server_after"]
    before = row["server_before"]
    client_cpu = row["client_cpu_s"]
    server_cpu = after["cpu_s"] - before["cpu_s"]
    elapsed = row["elapsed_s"]
    result = {
        "goodput": row["operations_per_s"],
        "client_cores": client_cpu / elapsed,
        "server_cores": server_cpu / elapsed,
        "total_cores": (client_cpu + server_cpu) / elapsed,
        "server_mib": after["memory_bytes"] / 2**20,
        "server_anon_mib": after["anon_bytes"] / 2**20,
        "client_pss_mib": sum(
            proc["pss_bytes"] for proc in row["client_process_memory"]
        )
        / 2**20,
    }
    completed = row["count"] - row.get("timed_out", 0)
    if completed:
        result.update(row["latency"])
        result["cpu_s_1k"] = (client_cpu + server_cpu) * 1000 / completed
        result["tcp_bytes_per_op"] = (
            after["rx_bytes"]
            + after["tx_bytes"]
            - before["rx_bytes"]
            - before["tx_bytes"]
        ) / completed
    if row["operation"] == "live":
        result["live_timeouts"] = row.get("timed_out", 0)
    if row["dispatch_lag"]:
        result["dispatch_p95_ms"] = row["dispatch_lag"]["p95_ms"]
    for outcome, timing in row["outcomes"].items():
        result[f"{outcome}_p95_ms"] = timing["p95_ms"]
    return result


def aggregate(result):
    points = {}
    for wire in result["settings"]["wires"]:
        points[wire] = {}
        available = {s["name"] for s in result["runs"][0]["arms"][wire]["scenarios"]}
        for phase in ORDER:
            if phase not in available:
                continue
            rows = [
                metrics(
                    next(
                        s
                        for s in trial["arms"][wire]["scenarios"]
                        if s["name"] == phase
                    )
                )
                for trial in result["runs"]
            ]
            keys = set.intersection(*(set(row) for row in rows))
            points[wire][phase] = {
                key: median(row[key] for row in rows) for key in sorted(keys)
            }
    return points


def summary(result):
    data = aggregate(result)
    lines = [
        "| Workload | " + " | ".join(LABEL[wire] for wire in data) + " |",
        "| --- | " + " | ".join("---:" for _ in data) + " |",
    ]
    for phase in next(iter(data.values())):
        values = []
        for arm in data.values():
            row = arm[phase]
            if phase.startswith("idle"):
                values.append(f"{row['total_cores']:.3f} cores")
            elif phase == "live-16":
                latency = f"{row['p95_ms']:.2f} ms p95" if "p95_ms" in row else "—"
                values.append(f"{latency} · {row['live_timeouts']:g} timeouts")
            else:
                values.append(f"{row['goodput']:,.0f}/s · {row['p95_ms']:.2f} ms")
        lines.append(f"| {phase} | " + " | ".join(values) + " |")
    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    results = json.loads(args.results.read_text())
    print(json.dumps(aggregate(results), indent=2) if args.json else summary(results))
