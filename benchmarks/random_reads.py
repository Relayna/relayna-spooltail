"""Random task-ID lookups against fully populated, stationary histories."""

import argparse
import asyncio
import hashlib
import json
import platform
import random
import sys
import time
from collections import defaultdict
from importlib.metadata import version

import httpx
from metrics import delta, distribution, emit, milliseconds, sample_resources, validate

NAMESPACE = "random-reads"
TOPIC = NAMESPACE + ".status"
SDK = "http://sdk-api:8080"
SPOOL = "http://spool:7474"


def event(task, step, steps):
    key = f"task-{task}"
    return {
        "task_id": key,
        "event_id": f"{task}-{step}",
        "correlation_id": key,
        "status": "completed" if step == steps - 1 and task % 2 == 0 else "running",
        "meta": {"step": step, "payload": "x" * 128},
    }


async def seed(args, redis, http):
    """Every source record is published and the real SDK hub finishes all Redis writes."""
    from compare import client_for, running, spool_record
    from relayna.observability import StatusHubStoredEvent
    from relayna.status import RedisStatusStore, StatusHub

    rabbit = client_for(NAMESPACE)
    total = args.read_tasks * args.statuses
    seen = set()
    caught_up = asyncio.Event()

    async def stored(obs):
        if isinstance(obs, StatusHubStoredEvent):
            seen.add(obs.event_id)
            if len(seen) == total:
                caught_up.set()

    try:
        await rabbit.initialize()
        store = RedisStatusStore(redis, prefix=NAMESPACE)
        hub = StatusHub(
            rabbitmq=rabbit,
            store=store,
            consume_arguments={"x-stream-offset": "first"},
            observation_sink=stored,
        )
        async with running(hub.run_forever()):
            # Interleave a block of histories per segment region so random task IDs
            # point throughout the journal. Within a task, all writes are ordered.
            for begin in range(0, args.read_tasks, 64):
                batch = range(begin, min(begin + 64, args.read_tasks))
                for step in range(args.statuses):
                    records = [event(task, step, args.statuses) for task in batch]
                    await asyncio.gather(
                        *(rabbit.publish_status(record) for record in records)
                    )
                    await asyncio.gather(
                        *(spool_record(http, TOPIC, record) for record in records)
                    )
            await caught_up.wait()
        validate(
            seen
            == {
                f"{task}-{step}"
                for task in range(args.read_tasks)
                for step in range(args.statuses)
            },
            "incorrect SDK seed event IDs",
        )
    finally:
        await rabbit.close()


def make_trace(rng, task_count, count, history):
    if history:
        return [rng.randrange(task_count) for _ in range(count)]
    # Negative IDs are absent from both stores; even existing IDs have completed.
    missing = count // 10
    completed = (count - missing) // 2
    active = count - missing - completed
    trace = [2 * rng.randrange((task_count + 1) // 2) for _ in range(completed)]
    trace += [2 * rng.randrange(task_count // 2) + 1 for _ in range(active)]
    trace += [-1 - rng.randrange(task_count) for _ in range(missing)]
    rng.shuffle(trace)
    return trace


async def one_read(http, arm, history, task, steps, count_failures=False):
    key = f"task-{task}"
    if arm == "spooltail":
        route = "events" if history else "latest"
        url = f"{SPOOL}/topics/{TOPIC}/tasks/{key}/{route}"
    else:
        route = "events" if history else "status"
        url = f"{SDK}/{arm}/{route}/{key}"

    start = time.perf_counter()
    if not history:
        try:
            response = await http.get(url)
        except httpx.TransportError as error:
            if count_failures:
                return f"transport_{type(error).__name__}", milliseconds(start)
            raise
        latency = milliseconds(start)
        if count_failures and response.is_server_error:
            return f"http_{response.status_code}", latency
        if task < 0:
            validate(response.status_code == 404, f"{arm}: expected 404 for {key}")
            return "missing", latency
        response.raise_for_status()
        expected = event(task, steps - 1, steps)
        validate(
            response.json()["event"] == expected,
            f"{arm}: stale or incorrect latest for {key}",
        )
        return expected["status"], latency

    # Both public SSE routes replay retained history when Last-Event-ID is omitted.
    async with http.stream("GET", url) as response:
        response.raise_for_status()
        received = []
        fields = {}
        async for line in response.aiter_lines():
            if line:
                field, _, value = line.partition(":")
                fields[field] = value.lstrip()
            else:
                if fields.get("event") == "status":
                    received.append((fields.get("id"), fields["data"]))
                    if len(received) == steps:
                        break
                fields.clear()
        latency = milliseconds(start)
    validate(len(received) == steps, f"{arm}: incomplete timeline for {key}")
    for step, (eid, data) in enumerate(received):
        expected = event(task, step, steps)
        validate(
            eid == expected["event_id"] and json.loads(data) == expected,
            f"{arm}: incorrect timeline for {key} at step {step}",
        )
    return "history", latency


async def read_samples(
    http, arm, history, trace, workers, steps, *, count_failures=False
):
    pending = iter(trace)
    timings = defaultdict(list)

    async def worker():
        for task in pending:
            outcome, elapsed = await one_read(
                http, arm, history, task, steps, count_failures
            )
            timings[outcome].append(elapsed)

    async with asyncio.TaskGroup() as group:
        for _ in range(workers):
            group.create_task(worker())
    return timings


async def timed_reads(http, arm, history, trace, workers, steps):
    start = time.perf_counter()
    timings = await read_samples(http, arm, history, trace, workers, steps)
    elapsed = time.perf_counter() - start
    samples = [v for values in timings.values() for v in values]
    validate(len(samples) == len(trace), "incomplete read trace")
    return {
        "requests": len(trace),
        "elapsed_s": elapsed,
        "requests_per_s": len(trace) / elapsed,
        **distribution(samples),
        "outcomes": {
            outcome: {"requests": len(values), **distribution(values)}
            for outcome, values in sorted(timings.items())
        },
    }


async def main(args):
    from redis.asyncio import Redis

    async with (
        Redis(host="redis") as redis,
        httpx.AsyncClient(
            base_url=SPOOL,
            trust_env=False,
            timeout=30,
            limits=httpx.Limits(max_connections=64, max_keepalive_connections=64),
        ) as http,
    ):
        if args.trial == 0:
            print(
                f"Seeding {args.read_tasks * args.statuses:,} statuses on each stack",
                file=sys.stderr,
            )
            await seed(args, redis, http)
        scenarios = []
        for history in (False, True):
            arms = (
                ["sdk-default", "spooltail"]
                if history
                else ["sdk-default", "sdk-once", "spooltail"]
            )
            rotation = args.trial % len(arms)
            arms = arms[rotation:] + arms[:rotation]
            for workers in dict.fromkeys((1, args.readers)):
                seed_value = f"{args.seed}/{args.trial}/{history}/{workers}"
                count = args.reads_per_reader * workers
                trace = make_trace(
                    random.Random(seed_value), args.read_tasks, count, history
                )
                warm = make_trace(
                    random.Random(seed_value + "/warm"),
                    args.read_tasks,
                    max(20, workers * 2),
                    history,
                )
                name = "history" if history else "latest"
                scenario = {
                    "operation": name,
                    "readers": workers,
                    "requests": count,
                    "trace_sha256": hashlib.sha256(
                        json.dumps(trace).encode()
                    ).hexdigest(),
                    "unique_task_ids": len(set(trace)),
                    "arms": {},
                }
                for arm in arms:
                    print(
                        f"trial {args.trial + 1}: {name}, {workers} reader(s), {arm}",
                        file=sys.stderr,
                    )
                    services = ["spool"] if arm == "spooltail" else ["sdk-api", "redis"]
                    async with httpx.AsyncClient(
                        trust_env=False,
                        timeout=30,
                        limits=httpx.Limits(
                            max_connections=workers, max_keepalive_connections=workers
                        ),
                    ) as reader:
                        await timed_reads(
                            reader, arm, history, warm, workers, args.statuses
                        )
                        before = sample_resources(services)
                        row = await timed_reads(
                            reader, arm, history, trace, workers, args.statuses
                        )
                        row["resources"] = delta(before, sample_resources(services))
                        scenario["arms"][arm] = row
                scenarios.append(scenario)
        emit(
            result={
                "trial": args.trial + 1,
                "scenarios": scenarios,
                "versions": {
                    "python": platform.python_version(),
                    "relayna": version("relayna"),
                    "httpx": version("httpx"),
                    "redis_server": (await redis.info("server"))["redis_version"],
                },
            }
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for key in (
        "trial",
        "read-tasks",
        "statuses",
        "readers",
        "reads-per-reader",
        "seed",
        "timeout",
    ):
        parser.add_argument("--" + key, required=True, type=int)
    args = parser.parse_args()
    asyncio.run(asyncio.wait_for(main(args), args.timeout))
