"""Run comparable status, durability, and live workloads through each transport."""

import argparse
import asyncio
import hashlib
import json
import platform
import random
import resource
import sys
import time
from collections import Counter, defaultdict
from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from importlib.metadata import version
from pathlib import Path

from metrics import distribution, emit, validate
from wire_client import WIRES, Rejected, client

SMALL = 128
LARGE = 16 * 1024
STEPS = 6
PAYLOADS = {size: "x" * size for size in (SMALL, LARGE)}


def event(task, step=STEPS - 1, size=SMALL, status=None):
    return {
        "task_id": task,
        "event_id": f"{task}-{step}",
        "status": status or ("completed" if step == STEPS - 1 else "running"),
        "meta": {
            "step": step,
            "body": PAYLOADS[size],
            "labels": ["sample", "✓"],
            "available": True,
        },
    }


def corpus(index, size):
    return f"status-{size}", f"task-{index}"


def check_record(record, expected):
    validate(
        record is not None and record[0] > 0 and record[1] == expected,
        "incorrect status record",
    )


async def expect_error(reason, request):
    try:
        await request
    except Rejected as error:
        validate(error.reason == reason, f"unexpected result {error.reason}")
    else:
        raise RuntimeError(f"expected {reason}")


async def parity(api):
    topic = "parity"
    task = "task-1"
    initial = event(task, 0)
    terminal = event(task)
    offset = await api.append(topic, initial)
    validate(await api.append(topic, initial) == offset, "append retry changed offset")
    await expect_error(
        "precondition", api.append(topic, event(task, 0, status="failed"))
    )
    check_record(await api.latest(topic, task), initial)
    validate(await api.latest(topic, "missing") is None, "missing status returned")
    validate(
        await api.history(topic, task, initial["event_id"]) == [],
        "history resume repeated event",
    )
    validate(await api.claim("parity-work", "workers") is None, "expected empty claim")
    work = await api.append("parity-work", initial)
    lease = await api.claim("parity-work", "workers")
    validate(
        lease is not None and lease[0] == work and lease[2] == initial,
        "incorrect lease",
    )
    await expect_error(
        "precondition",
        api.complete("parity-work", "workers", work, lease[1] + 1, topic, terminal),
    )
    async with aclosing(api.subscribe(topic, task, initial["event_id"])) as feed:
        validate((await anext(feed))[0] == "ready", "feed did not become ready")
        done = await api.complete(
            "parity-work", "workers", work, lease[1], topic, terminal
        )
        validate(
            await anext(feed) == ("status", terminal["event_id"], terminal),
            "terminal delivery mismatch",
        )
        validate(await anext(feed, None) is None, "terminal feed did not close")
    validate(
        await api.complete("parity-work", "workers", work, lease[1], topic, terminal)
        == done,
        "completion retry changed offset",
    )
    validate(
        await api.claim("parity-work", "workers") is None,
        "completed work still claimable",
    )
    validate(
        await api.history(topic, task, initial["event_id"]) == [(done, terminal)],
        "resume did not return terminal event",
    )
    async with aclosing(api.subscribe(topic, task, "unknown")) as feed:
        validate((await anext(feed))[0] == "gap", "unknown cursor did not return gap")


async def seed(wire, args):
    async with client(wire, 16) as api:
        await api.ping()
        await parity(api)

        async def task(index, size):
            topic, key = corpus(index, size)
            for step in range(STEPS):
                validate(
                    await api.append(topic, event(key, step, size)) > 0,
                    "unconfirmed seed",
                )

        for size, count in [(SMALL, args.tasks), (LARGE, args.large_tasks)]:
            for start in range(0, count, 16):
                await asyncio.gather(
                    *(task(i, size) for i in range(start, min(start + 16, count)))
                )


async def verify_restart(wire):
    async with client(wire) as api:
        await api.ping()
        task = "task-1"
        terminal = event(task)
        latest = await api.latest("parity", task)
        check_record(latest, terminal)
        work = await api.latest("parity-work", task)
        check_record(work, event(task, 0))
        retry = await api.complete(
            "parity-work", "workers", work[0], 1, "parity", terminal
        )
        validate(retry == latest[0], "restarted broker lost the completion receipt")
        validate(
            await api.claim("parity-work", "workers") is None,
            "restarted broker lost the settlement",
        )


def trace_for(args, op, count, size=SMALL):
    rng = random.Random(f"{args.seed}/{args.trial}/{op}")
    limit = args.large_tasks if size == LARGE else args.tasks
    if op.startswith("latest") or op == "cold":
        absent = count // 10
        trace = [rng.randrange(limit) for _ in range(count - absent)] + [
            -i - 1 for i in range(absent)
        ]
        rng.shuffle(trace)
        return trace
    if op in ("history", "replay"):
        return [rng.randrange(limit) for _ in range(count)]
    if op == "mixed":
        return [-i - 1 if i % 10 == 0 else rng.randrange(limit) for i in range(count)]
    return list(range(count))


async def operation(api, job, item, sequence, lane):
    op, name, size = job["operation"], job["name"], job["size"]
    topic, task = corpus(item, size)
    if op == "ping":
        await api.ping()
        return "ping"
    if op == "append" or (op == "mixed" and item < 0):
        validate(
            await api.append(f"append-{name}", event(f"entry-{sequence}")) > 0,
            "unconfirmed append",
        )
        return "append"
    if op.startswith("latest") or op in ("cold", "mixed"):
        row = await api.latest(topic, task)
        if item < 0:
            validate(row is None, "missing ID returned an event")
            return "missing"
        check_record(row, event(task, size=size))
        return "hit"
    if op == "history":
        rows = await api.history(topic, task)
        validate(len(rows) == STEPS, "incomplete history")
        last = 0
        for step, row in enumerate(rows):
            check_record(row, event(task, step, size))
            validate(row[0] > last, "history out of order")
            last = row[0]
        return "history"
    if op == "replay":
        async with aclosing(api.subscribe(topic, task)) as feed:
            validate((await anext(feed))[0] == "ready", "replay missing ready")
            for step in range(STEPS):
                expected = event(task, step, size)
                validate(
                    await anext(feed) == ("status", expected["event_id"], expected),
                    "replay mismatch",
                )
            validate(await anext(feed, None) is None, "replay did not close")
        return "replay"
    if op == "lifecycle":
        work = f"work-{name}-{lane}"
        statuses = f"done-{name}"
        task = f"entry-{sequence}"
        initial = event(task, 0)
        offset = await api.append(work, initial)
        claim = await api.claim(work, "workers")
        validate(
            claim is not None and claim[0] == offset and claim[2] == initial,
            "incorrect claim",
        )
        terminal = event(task)
        done = await api.complete(work, "workers", offset, claim[1], statuses, terminal)
        validate(
            await api.latest(statuses, task) == (done, terminal), "incorrect completion"
        )
        return "lifecycle"
    raise ValueError(f"unknown operation {op}")


async def live(publisher, job, rounds):
    """Measure publish dispatch to each watcher receiving a terminal event."""
    timings = []
    timed_out = Counter()

    async def watcher(api, topic, task, ready, terminal):
        async with aclosing(api.subscribe(topic, task)) as feed:
            validate((await anext(feed))[0] == "ready", "missing live subscription")
            ready.put_nowait(None)
            message = await anext(feed)
            arrived = time.perf_counter()
            validate(
                message == ("status", terminal["event_id"], terminal),
                "live delivery mismatch",
            )
            validate(await anext(feed, None) is None, "live feed did not close")
            return arrived

    async with AsyncExitStack() as subscribers:
        api = await subscribers.enter_async_context(
            client(job["wire"], job["watchers"])
        )
        await api.ping()
        for seq in range(rounds):
            topic, task = f"live-{job['name']}", f"event-{seq}"
            ready = asyncio.Queue()
            terminal = event(task)
            stage = "waiting_for_ready"
            try:
                async with asyncio.timeout(2), asyncio.TaskGroup() as group:
                    tasks = [
                        group.create_task(watcher(api, topic, task, ready, terminal))
                        for _ in range(job["watchers"])
                    ]
                    for _ in tasks:
                        await ready.get()
                    stage = "waiting_for_publish"
                    published = time.perf_counter()
                    validate(
                        await publisher.append(topic, terminal) > 0,
                        "unconfirmed live publish",
                    )
                    stage = "waiting_for_delivery_or_close"
            except TimeoutError:
                timed_out[stage] += 1
                # Cancelled stream state can occupy HTTP/2 stream slots on a reused client.
                # Keep the failed round in the results and reconnect before taking more samples.
                await subscribers.aclose()
                api = await subscribers.enter_async_context(
                    client(job["wire"], job["watchers"])
                )
                await api.ping()
                continue
            timings.extend((task.result() - published) * 1000 for task in tasks)
    return timings, timed_out


def memory():
    data = Path("/proc/self/smaps_rollup").read_text().splitlines()[1:]
    fields = dict(line.split(":", 1) for line in data)
    return {
        "rss_bytes": int(fields["Rss"].split()[0]) * 1024,
        "pss_bytes": int(fields["Pss"].split()[0]) * 1024,
        "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
    }


@asynccontextmanager
async def idle_streams(api, name, count):
    async with AsyncExitStack() as stack:

        async def connect(i):
            feed = await stack.enter_async_context(
                aclosing(api.subscribe(f"idle-{name}", f"task-{i}"))
            )
            validate((await anext(feed))[0] == "ready", "idle stream did not open")

        async with asyncio.TaskGroup() as group:
            for i in range(count):
                group.create_task(connect(i))
        yield


async def worker():
    job = json.loads(sys.stdin.readline())
    wire, op, readers = job["wire"], job["operation"], job["readers"]
    async with (
        client(wire, max(readers, job["watchers"] + 1)) as api,
        AsyncExitStack() as stack,
    ):
        await api.ping()
        if op == "idle":
            await stack.enter_async_context(
                idle_streams(api, job["name"], job["watchers"])
            )
        if op not in ("idle", "cold", "live"):
            warm_job = {**job, "name": job["name"] + "-warm"}
            for i, item in enumerate(job["warm"]):
                await operation(api, warm_job, item, i, 0)
        print("ready", flush=True)
        start_at = float(sys.stdin.readline())
        timings = defaultdict(list)
        lag_ms = []
        await asyncio.sleep(max(0, start_at - time.perf_counter()))
        start = time.perf_counter()
        cpu = time.process_time()

        async def lane(entries, lane_id):
            for seq, item in entries:
                if rate := job["rate"]:
                    due = start_at + seq / rate
                    delay = due - time.perf_counter()
                    if delay > 0:
                        await asyncio.sleep(delay)
                    lag_ms.append(max(0, time.perf_counter() - due) * 1000)
                began = time.perf_counter()
                if op == "cold":
                    async with client(wire, cold=True) as fresh:
                        result = await operation(fresh, job, item, seq, lane_id)
                else:
                    result = await operation(api, job, item, seq, lane_id)
                timings[result].append((time.perf_counter() - began) * 1000)

        timed_out = {}
        if op == "idle":
            await asyncio.sleep(job["duration"])
        elif op == "live":
            delivered, timed_out = await live(api, job, len(job["trace"]))
            if delivered:
                timings["delivery"] = delivered
        else:
            async with asyncio.TaskGroup() as group:
                for lane_id in range(readers):
                    group.create_task(
                        lane(
                            job["trace"][lane_id::readers],
                            job["worker"] * readers + lane_id,
                        )
                    )
            if job["rate"]:
                await asyncio.sleep(
                    max(0, start_at + job["duration"] - time.perf_counter())
                )
        end = time.perf_counter()
        cpu = time.process_time() - cpu
        footprint = memory()
    emit(
        start=start,
        end=end,
        cpu_s=cpu,
        memory=footprint,
        timings=timings,
        lag_ms=lag_ms,
        timed_out=timed_out,
    )


@asynccontextmanager
async def process(job):
    p = await asyncio.create_subprocess_exec(
        sys.executable,
        __file__,
        "--worker",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
    )
    try:
        p.stdin.write(json.dumps(job).encode() + b"\n")
        await p.stdin.drain()
        yield p
    finally:
        if p.returncode is None:
            p.kill()
            await p.communicate()


async def collect(child):
    data = await child.stdout.read()
    validate(await child.wait() == 0, "workload process failed")
    return json.loads(data)


def server_sample(wire):
    emit(sample=wire)
    return json.loads(sys.stdin.readline())


async def measure(
    wire,
    args,
    name,
    op,
    processes,
    readers,
    count,
    *,
    size=SMALL,
    rate=0,
    duration=0,
    watchers=0,
):
    trace = trace_for(args, op, count, size)
    warm = trace_for(args, op, 16, size)
    job = {
        "name": f"{name}-{args.trial}{args.label}",
        "wire": wire,
        "operation": op,
        "readers": readers // processes,
        "size": size,
        "rate": rate,
        "duration": duration,
        "watchers": watchers,
        "warm": warm,
    }

    async with AsyncExitStack() as stack:
        children = [
            await stack.enter_async_context(
                process(
                    {
                        **job,
                        "worker": i,
                        "trace": list(enumerate(trace))[i::processes],
                        "name": f"{job['name']}-{i}",
                    }
                )
            )
            for i in range(processes)
        ]
        async with asyncio.TaskGroup() as group:
            ready = [group.create_task(child.stdout.readline()) for child in children]
        validate(all(r.result() == b"ready\n" for r in ready), "warmup failed")
        before = server_sample(wire)
        start = time.perf_counter() + 0.030
        for child in children:
            child.stdin.write(f"{start}\n".encode())
        async with asyncio.TaskGroup() as group:
            pending = [group.create_task(collect(child)) for child in children]
        results = [task.result() for task in pending]
        after = server_sample(wire)
    elapsed = max(r["end"] for r in results) - min(r["start"] for r in results)
    outcomes = defaultdict(list)
    for r in results:
        for outcome, values in r["timings"].items():
            outcomes[outcome].extend(values)
    samples = [v for values in outcomes.values() for v in values]
    timeout_stages = Counter()
    for result in results:
        timeout_stages.update(result["timed_out"])
    timed_out = timeout_stages.total()
    completed = count - timed_out
    if timed_out:
        print(f"{wire}: {timed_out}/{count} live rounds timed out", file=sys.stderr)
    validate(
        len(samples) == completed * max(1, watchers),
        "incorrect number of verified operations",
    )
    lags = [v for r in results for v in r["lag_ms"]]
    return {
        "name": name,
        "operation": op,
        "processes": processes,
        "readers": readers,
        "payload_bytes": size,
        "target_rate": rate,
        "watchers": watchers,
        "count": count,
        "samples": len(samples),
        "trace_sha256": hashlib.sha256(json.dumps(trace).encode()).hexdigest(),
        "elapsed_s": elapsed,
        "operations_per_s": completed / elapsed,
        "timed_out": timed_out,
        "live_timeout_stages": dict(timeout_stages),
        "latency": distribution(samples) if samples else {},
        "dispatch_lag": distribution(lags) if lags else {},
        "outcomes": {
            k: {"count": len(v), **distribution(v)} for k, v in outcomes.items()
        },
        "client_cpu_s": sum(r["cpu_s"] for r in results),
        "client_process_memory": [r["memory"] for r in results],
        "server_before": before,
        "server_after": after,
    }


async def main(args):
    if args.verify_restart:
        for wire in args.wires:
            await verify_restart(wire)
            print(
                f"{wire}: verified completion and lease state after restart",
                file=sys.stderr,
            )
        return
    if args.seed_only:
        for wire in args.wires:
            print(f"Seeding and checking semantics for {wire}", file=sys.stderr)
            await seed(wire, args)
        return

    count = args.requests
    small = max(16, count // 5)
    duration = args.seconds
    workloads = [
        ("idle", "idle", 1, 1, 0, {"duration": duration}),
        ("idle-64-streams", "idle", 1, 64, 0, {"duration": duration, "watchers": 64}),
        ("empty-rpc", "ping", 1, 1, small, {}),
        ("cold-latest", "cold", 1, 1, max(10, count // 120), {}),
        ("latest-serial", "latest", 1, 1, small, {}),
        (
            "latest-1000",
            "latest",
            1,
            16,
            1000 * duration,
            {"rate": 1000, "duration": duration},
        ),
        ("latest-16", "latest", 4, 16, count, {}),
        ("latest-64", "latest", 8, 64, count, {}),
        ("latest-16k", "latest-large", 4, 16, small, {"size": LARGE}),
        ("history-6", "history", 4, 16, small, {}),
        ("replay-6", "replay", 4, 16, small, {}),
        ("mixed-10pct-writes", "mixed", 4, 16, small, {}),
        ("durable-append", "append", 4, 16, small, {}),
        ("worker-lifecycle", "lifecycle", 4, 16, max(16, count // 16), {}),
        (
            "live-16",
            "live",
            1,
            16,
            args.live_rounds or max(2, count // 500),
            {"watchers": 16},
        ),
    ]
    # Rotate the expensive/cheap windows across trials reproducibly.
    random.Random(f"{args.seed}/{args.trial}/order").shuffle(workloads)
    rows = []
    for name, op, workers, readers, n, extra in workloads:
        if args.phases and name not in args.phases:
            continue
        if args.live_only and op != "live":
            continue
        print(f"Trial {args.trial + 1}, {args.wire}: {name}", file=sys.stderr)
        rows.append(
            await measure(args.wire, args, name, op, workers, readers, n, **extra)
        )
    emit(
        result={
            "wire": args.wire,
            "scenarios": rows,
            "versions": {
                "python": platform.python_version(),
                "grpcio": version("grpcio"),
                "httpx": version("httpx"),
                "h2": version("h2"),
                "protobuf": version("protobuf"),
            },
        }
    )


if __name__ == "__main__":
    if sys.argv[1:] == ["--worker"]:
        asyncio.run(worker())
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--wire", choices=WIRES, default="http1")
        parser.add_argument("--wires", nargs="+", choices=WIRES, default=WIRES)
        parser.add_argument("--seed-only", action="store_true")
        parser.add_argument("--live-only", action="store_true")
        parser.add_argument("--verify-restart", action="store_true")
        parser.add_argument("--phases", nargs="+")
        parser.add_argument("--label", default="")
        parser.add_argument("--live-rounds", type=int, default=0)
        for name, default in [
            ("trial", 0),
            ("tasks", 1500),
            ("large-tasks", 250),
            ("requests", 10000),
            ("seconds", 6),
            ("seed", 1729),
            ("timeout", 800),
        ]:
            parser.add_argument("--" + name, type=int, default=default)
        args = parser.parse_args()
        asyncio.run(asyncio.wait_for(main(args), args.timeout))
