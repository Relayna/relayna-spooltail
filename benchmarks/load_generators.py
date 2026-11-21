"""Measure random latest-status lookups with independent, synchronized HTTP clients."""

import argparse
import asyncio
import hashlib
import json
import os
import platform
import random
import sys
import time
from collections import defaultdict
from contextlib import AsyncExitStack, asynccontextmanager
from importlib.metadata import version

import httpx
from compare import delta, distribution, emit, sample_resources, validate
from random_reads import SPOOL, make_trace, read_samples, seed
from redis.asyncio import Redis


async def generator():
    job = json.loads(sys.stdin.buffer.readline())
    readers = job["readers"]
    async with httpx.AsyncClient(
        trust_env=False,
        timeout=30,
        limits=httpx.Limits(max_connections=readers, max_keepalive_connections=readers),
    ) as http:

        async def read(trace):
            return await read_samples(
                http,
                job["arm"],
                False,
                trace,
                readers,
                job["steps"],
                count_failures=True,
            )

        await read(job["warm"])
        print("ready", flush=True)
        validate(sys.stdin.buffer.readline() == b"go\n", "coordinator disconnected")
        start = time.perf_counter()
        cpu = time.process_time()
        timings = await read(job["trace"])
        end = time.perf_counter()
        cpu = time.process_time() - cpu
    emit(pid=os.getpid(), start=start, end=end, cpu_s=cpu, timings=timings)


@asynccontextmanager
async def start_generator():
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        __file__,
        "--generator",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
    )
    try:
        yield process
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()


async def prepare(process, job):
    process.stdin.write(json.dumps(job).encode() + b"\n")
    await process.stdin.drain()
    validate(
        await process.stdout.readline() == b"ready\n", "generator failed during warmup"
    )


async def collect(process):
    raw = await process.stdout.read()
    validate(await process.wait() == 0, "generator failed during timed reads")
    return json.loads(raw)


async def lock_counters(http):
    response = await http.get("/bench/store-lock")
    response.raise_for_status()
    return response.json()


async def measure(http, arm, generators, readers, steps, trace, warm):
    services = ["spool"] if arm == "spooltail" else ["sdk-api", "redis"]
    async with AsyncExitStack() as stack:
        children = [
            await stack.enter_async_context(start_generator())
            for _ in range(generators)
        ]
        jobs = [
            {
                "arm": arm,
                "steps": steps,
                "readers": readers // generators + (i < readers % generators),
                "trace": trace[i::generators],
                "warm": warm[i::generators],
            }
            for i in range(generators)
        ]
        async with asyncio.TaskGroup() as group:
            for child, job in zip(children, jobs, strict=True):
                group.create_task(prepare(child, job))
        lock_before = await lock_counters(http) if arm == "spooltail" else {}
        before = sample_resources(services)
        for child in children:
            child.stdin.write(b"go\n")
        async with asyncio.TaskGroup() as group:
            collectors = [group.create_task(collect(child)) for child in children]
        results = [collector.result() for collector in collectors]
        resources = delta(before, sample_resources(services))

    # All processes share the Linux monotonic clock. Pool the samples and count
    # each request once across the common wall-clock window.
    start = min(result["start"] for result in results)
    end = max(result["end"] for result in results)
    elapsed = end - start
    timings = defaultdict(list)
    clients = []
    for result, job in zip(results, jobs, strict=True):
        count = sum(map(len, result["timings"].values()))
        validate(count == len(job["trace"]), "incomplete generator trace")
        for outcome, values in result["timings"].items():
            timings[outcome].extend(values)
        clients.append(
            {
                "pid": result["pid"],
                "readers": job["readers"],
                "requests": count,
                "cpu_s": result["cpu_s"],
                "start_lag_s": result["start"] - start,
                "finish_s": result["end"] - start,
            }
        )
    valid_outcomes = {"completed", "running", "missing"}
    samples = [
        v for key, values in timings.items() if key in valid_outcomes for v in values
    ]
    errors = {
        key: len(values) for key, values in timings.items() if key not in valid_outcomes
    }
    validate(len(samples) + sum(errors.values()) == len(trace), "incomplete trace")
    resources.pop("driver")  # This process coordinated the child generators.
    row = {
        "requests": len(trace),
        "verified": len(samples),
        "errors": errors,
        "elapsed_s": elapsed,
        "requests_per_s": len(samples) / elapsed,
        **(
            distribution(samples)
            if samples
            else dict.fromkeys(("p50_ms", "p95_ms", "p99_ms"))
        ),
        "outcomes": {
            key: {"requests": len(values), **distribution(values)}
            for key, values in timings.items()
        },
        "generators": clients,
        "generator_cpu_cores": sum(c["cpu_s"] for c in clients) / elapsed,
        "resources": resources,
    }
    if lock_before:
        after = await lock_counters(http)
        lock = {k: v - lock_before[k] for k, v in after.items()}
        validate(
            lock["calls"] == len(trace),
            "lock counters include unexpected store operations",
        )
        row["store_lock"] = lock
    return row


async def main(args):
    shapes = [
        tuple(map(int, shape.split("x"))) for shape in args.load_shapes.split(",")
    ]
    async with (
        Redis(host="redis") as redis,
        httpx.AsyncClient(base_url=SPOOL, timeout=30, trust_env=False) as http,
    ):
        if args.trial == 0:
            print(
                f"Seeding {args.read_tasks * args.statuses:,} statuses per stack",
                file=sys.stderr,
            )
            await seed(args, redis, http)
        seed_value = f"{args.seed}/{args.trial}/load-generators"
        trace = make_trace(
            random.Random(seed_value), args.read_tasks, args.requests, False
        )
        digest = hashlib.sha256(json.dumps(trace).encode()).hexdigest()
        order = shapes.copy()
        random.Random(seed_value + "/order").shuffle(order)
        scenarios = {}
        for generators, readers in order:
            warm = make_trace(
                random.Random(seed_value + "/warm"),
                args.read_tasks,
                max(160, readers * 8),
                False,
            )
            arms = ["spooltail", "sdk-once"]
            if (args.trial + shapes.index((generators, readers))) % 2:
                arms.reverse()
            if readers > 100:
                arms.insert(args.trial % 3, "sdk-wide")
            rows = {}
            for arm in arms:
                print(
                    f"Trial {args.trial + 1}: {generators} generators, {readers} readers, {arm}",
                    file=sys.stderr,
                )
                rows[arm] = await measure(
                    http, arm, generators, readers, args.statuses, trace, warm
                )
            scenarios[generators, readers] = {
                "operation": "latest",
                "generators": generators,
                "readers": readers,
                "requests": len(trace),
                "trace_sha256": digest,
                "unique_task_ids": len(set(trace)),
                "arms": rows,
            }
        emit(
            result={
                "trial": args.trial + 1,
                "scenarios": [scenarios[shape] for shape in shapes],
                "execution_order": order,
                "versions": {
                    "python": platform.python_version(),
                    "relayna": version("relayna"),
                    "httpx": version("httpx"),
                    "redis_server": (await redis.info("server"))["redis_version"],
                },
            }
        )


if __name__ == "__main__":
    if sys.argv[1:] == ["--generator"]:
        asyncio.run(generator())
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--load-shapes", required=True)
        for key in ("trial", "read-tasks", "statuses", "requests", "seed", "timeout"):
            parser.add_argument("--" + key, type=int, required=True)
        args = parser.parse_args()
        asyncio.run(asyncio.wait_for(main(args), args.timeout))
