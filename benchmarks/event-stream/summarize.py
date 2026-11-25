"""Print median trial p95s and compare each matched reader across stacks."""

import argparse
import json
from pathlib import Path
from statistics import median


def summarize(path):
    data = json.loads(path.read_text())
    rows = data["runs"]
    assert len(rows) == 2 * len(data["cases"]) * data["trials"]
    assert all(r["verified"] == r["events_per_reader"] * r["readers"] for r in rows)
    print(
        f"{path.name}: {len(rows)} trials, {sum(r['verified'] for r in rows):,} verified deliveries"
    )
    print(
        "| Case | Spooltail delivery p95 | Control delivery p95 | Spooltail last p95 | Control last p95 | Readers faster | Last-reader trial wins |"
    )
    print("|---|---:|---:|---:|---:|---:|---:|")
    for case in data["cases"]:
        arms = [
            sorted(
                (r for r in rows if r["case"] == case and r["arm"] == arm),
                key=lambda r: r["trial"],
            )
            for arm in ("spooltail", "rabbit-redis")
        ]
        for arm in arms:
            assert [r["trial"] for r in arm] == list(range(1, data["trials"] + 1))
        values = [
            median(r[metric]["p95"] for r in arm)
            for metric in ("fast_delivery_ms", "last_reader_per_event_ms")
            for arm in arms
        ]
        faster = "—"
        if "reader_p95_ms" in arms[0][0]:
            reader_medians = [
                [
                    median(times)
                    for times in zip(*(r["reader_p95_ms"] for r in arm), strict=True)
                ]
                for arm in arms
            ]
            pairs = list(zip(*reader_medians, strict=True))
            assert len(pairs) == arms[0][0]["readers"]
            faster = f"{sum(s < r for s, r in pairs)}/{len(pairs)}"
        trial_wins = sum(
            s["last_reader_per_event_ms"]["p95"] < r["last_reader_per_event_ms"]["p95"]
            for s, r in zip(*arms, strict=True)
        )
        print(
            "|",
            case,
            "|",
            " | ".join(f"{value:.2f}" for value in values),
            "|",
            faster,
            "|",
            f"{trial_wins}/{data['trials']}",
            "|",
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path, nargs="+")
    for file in parser.parse_args().results:
        summarize(file)
