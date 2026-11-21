"""Timing and counter helpers shared by the benchmark clients."""

import json
import math
import time


def emit(**message):
    print(json.dumps(message), flush=True)


def validate(condition, message):
    if not condition:
        raise RuntimeError(message)


def milliseconds(start):
    return (time.perf_counter() - start) * 1000


def distribution(samples):
    values = sorted(samples)
    return {
        f"p{p}_ms": values[math.ceil(len(values) * p / 100) - 1] for p in (50, 95, 99)
    }


def sample_resources(services):
    emit(sample=services)
    stats = json.loads(input())
    stats["driver"] = {"cpu_s": time.process_time()}
    return stats


def delta(before, after):
    return {
        component: {
            k: after[component][k] if k == "memory_bytes" else after[component][k] - v
            for k, v in counters.items()
        }
        for component, counters in before.items()
    }
