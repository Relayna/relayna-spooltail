"""Summarize matched-rate read resource trials using median per-trial quantities."""

import argparse
import json
from pathlib import Path
from statistics import median


def total(components):
    cpu, memory = zip(*components, strict=True)
    return sum(cpu), sum(memory)


def summary(result):
    lines = [
        "| Target reads/s | Component | CPU cores | CPU-s / 1,000 verified reads | End memory (MiB) |",
        "| ---: | --- | ---: | ---: | ---: |",
    ]
    for rate in map(int, result["settings"]["rates"].split(",")):
        for arm in ("spooltail", "sdk-once"):
            samples = {}
            for run in result["runs"]:
                row = next(
                    s for s in run["arms"][arm]["scenarios"] if s["target_rps"] == rate
                )
                components = {}
                for service, after in row["servers_after"].items():
                    cpu = after["cpu_s"] - row["servers_before"][service]["cpu_s"]
                    components[service] = (cpu, after["memory_bytes"])
                server = total(components.values())
                label = "Spooltail" if arm == "spooltail" else "SDK"
                if arm == "sdk-once":
                    components["SDK API + Redis"] = total(
                        [components["sdk-api"], components["redis"]]
                    )
                    components["SDK API + Redis + RabbitMQ"] = server
                caller = (
                    row["client_cpu_s"],
                    row["client_memory_after"]["memory_bytes"],
                )
                components[label + " caller"] = caller
                components[label + " servers and caller"] = total([server, caller])
                for who, (cpu, memory) in components.items():
                    samples.setdefault(who, []).append(
                        (
                            cpu / row["elapsed_s"],
                            cpu * 1000 / row["requests"] if row["requests"] else None,
                            memory / 2**20,
                        )
                    )
            for who, measurements in samples.items():
                cores, cost, memory = zip(*measurements, strict=True)
                per_thousand = f"{median(cost):.3f}" if rate else "—"
                lines.append(
                    f"| {rate:,} | {who} | {median(cores):.4f} | {per_thousand} | {median(memory):.1f} |"
                )
    return "\n".join(lines)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("result", type=Path)
    print(summary(json.loads(p.parse_args().result.read_text())))
