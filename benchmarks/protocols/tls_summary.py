"""Summarize a TLS benchmark, with per-topology throughput and p95 latency."""

import argparse
import json
from pathlib import Path

from summary import aggregate, summary


def topology_results(result):
    for topology in result["tls_probes"]:
        yield (
            topology,
            {
                "settings": result["settings"],
                "runs": [trial["topologies"][topology] for trial in result["runs"]],
            },
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    topologies = dict(topology_results(json.loads(args.results.read_text())))
    if args.json:
        print(
            json.dumps(
                {name: aggregate(result) for name, result in topologies.items()},
                indent=2,
            )
        )
    else:
        for name, result in topologies.items():
            print(f"## {name}\n\n{summary(result)}\n")
