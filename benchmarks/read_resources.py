"""Resource cost of one persistent HTTP client service at a fixed offered read rate."""

import argparse
import asyncio
import hashlib
import json
import os
import random
import resource
import sys
import time
from collections import Counter
from importlib.metadata import version
from pathlib import Path

import httpx
from metrics import distribution, emit, sample_resources, validate
from random_reads import SPOOL, make_trace, one_read, read_samples, seed


def client_memory():
    # One Python process per container. Inspect its own cgroup without a docker exec.
    cgroup = Path("/sys/fs/cgroup")
    stats = dict(
        line.split() for line in (cgroup / "memory.stat").read_text().splitlines()
    )
    rollup = dict(
        line.split(":", 1)
        for line in Path("/proc/self/smaps_rollup").read_text().splitlines()[1:]
    )
    return {
        "memory_bytes": int((cgroup / "memory.current").read_text()),
        "anon_bytes": int(stats["anon"]),
        "file_cache_bytes": int(stats["file"]),
        "rss_bytes": int(rollup["Rss"].split()[0]) * 1024,
        "pss_bytes": int(rollup["Pss"].split()[0]) * 1024,
        "lifetime_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        * 1024,
    }


def servers(arm):
    names = ["spool"] if arm == "spooltail" else ["sdk-api", "redis", "rabbit"]
    values = sample_resources(names)
    return {name: values[name] for name in names}


async def paced_reads(http, args, rate, trace):
    start = time.perf_counter()
    cpu_start = time.process_time()
    lags = []
    latencies = []
    outcomes = Counter()

    async def lane(index):
        for i in range(index, len(trace), args.readers):
            due = start + i / rate
            delay = due - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            lags.append(max(0, time.perf_counter() - due) * 1000)
            outcome, latency = await one_read(
                http, args.arm, False, trace[i], args.statuses
            )
            outcomes[outcome] += 1
            latencies.append(latency)

    async with asyncio.TaskGroup() as tasks:
        for i in range(min(args.readers, len(trace))):
            tasks.create_task(lane(i))
    # Idle and active windows both include their full scheduled duration. If a
    # client falls behind, its actual measured rate falls as it drains the queue.
    await asyncio.sleep(max(0, start + args.seconds - time.perf_counter()))
    elapsed = time.perf_counter() - start
    cpu_s = time.process_time() - cpu_start
    validate(sum(outcomes.values()) == len(trace), "incomplete read trace")
    return {
        "elapsed_s": elapsed,
        "requests": len(trace),
        "outcomes": dict(outcomes),
        "requests_per_s": len(trace) / elapsed,
        "client_cpu_s": cpu_s,
        "latency": distribution(latencies) if latencies else {},
        "dispatch_delay": distribution(lags) if lags else {},
    }


async def main(args):
    if args.seed_only:
        from redis.asyncio import Redis

        print(
            f"Seeding {args.read_tasks * args.statuses:,} statuses per stack",
            file=sys.stderr,
        )
        async with (
            Redis(host="redis") as redis,
            httpx.AsyncClient(base_url=SPOOL, timeout=30, trust_env=False) as http,
        ):
            await seed(args, redis, http)
        return

    rates = list(map(int, args.rates.split(",")))
    rotation = args.trial % len(rates)
    order = rates[rotation:] + rates[:rotation]
    data = []
    async with httpx.AsyncClient(
        trust_env=False,
        timeout=30,
        limits=httpx.Limits(
            max_connections=args.readers, max_keepalive_connections=args.readers
        ),
    ) as http:
        for rate in order:
            seed_value = f"{args.seed}/{args.trial}/read-resources/{rate}"
            trace = make_trace(
                random.Random(seed_value), args.read_tasks, rate * args.seconds, False
            )
            trace_hash = hashlib.sha256(json.dumps(trace).encode()).hexdigest()
            warm = make_trace(
                random.Random(seed_value + "/warm"),
                args.read_tasks,
                max(160, args.readers * 8),
                False,
            )
            await read_samples(http, args.arm, False, warm, args.readers, args.statuses)
            before = servers(args.arm)
            mem_before = client_memory()
            print(
                f"Trial {args.trial + 1}: {args.arm}, {rate} reads/s for {args.seconds}s",
                file=sys.stderr,
            )
            row = await paced_reads(http, args, rate, trace)
            mem_after = client_memory()
            after = servers(args.arm)
            row.update(
                target_rps=rate,
                trace_sha256=trace_hash,
                client_memory_before=mem_before,
                client_memory_after=mem_after,
                servers_before=before,
                servers_after=after,
            )
            data.append(row)
    emit(
        result={
            "scenarios": data,
            "client_pid": os.getpid(),
            "versions": {key: version(key) for key in ("httpx", "relayna")},
            "sdk_loaded_in_client": "relayna" in sys.modules,
        }
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--seed-only", action="store_true")
    p.add_argument("--arm", choices=("spooltail", "sdk-once"))
    p.add_argument("--rates")
    for key in (
        "trial",
        "read-tasks",
        "statuses",
        "readers",
        "seconds",
        "seed",
        "timeout",
    ):
        p.add_argument("--" + key, type=int)
    args = p.parse_args()
    asyncio.run(asyncio.wait_for(main(args), args.timeout))
