"""Runs inside the benchmark network. The parent samples service CPU on request."""

import argparse
import asyncio
import json
import math
import platform
import sys
import time
from collections import Counter, defaultdict
from contextlib import asynccontextmanager
from importlib.metadata import version

import httpx
from metrics import delta, distribution, emit, milliseconds, sample_resources, validate
from redis.asyncio import Redis
from relayna.consumer import TaskConsumer
from relayna.contracts import StatusEventEnvelope
from relayna.observability import StatusHubStoredEvent, TaskMessageAcked
from relayna.rabbitmq import RelaynaRabbitClient
from relayna.status import RedisStatusStore, StatusHub
from relayna.topology import SharedTasksSharedStatusTopology

MODES = ["first", "next", "checkpoint", "spooltail", "last", "first-200"]


def status(i, prefix, offline):
    # Two rare statuses straddle the disconnect. Keep other task histories inside the SDK's 50-entry cap.
    tasks = max(16, math.ceil((prefix + offline) / 40))
    task = (
        "rare"
        if i in (prefix // 2, prefix + offline - 1, prefix + offline)
        else f"task-{i % tasks}"
    )
    return {
        "task_id": task,
        "event_id": f"E{i}",
        "correlation_id": task,
        "status": "completed" if i == prefix + offline else "running",
        "meta": {"payload": "x" * 128},
    }


@asynccontextmanager
async def running(coroutine):
    async with asyncio.TaskGroup() as group:
        task = group.create_task(coroutine)
        try:
            yield
        finally:
            task.cancel()


async def until_consumer(queue):
    # The AMQP passive declaration can report zero consumers for a RabbitMQ stream.
    # Wait out-of-band for broker registration before publishing the live sentinel.
    emit(await_consumer=queue)
    validate(
        json.loads(await asyncio.to_thread(input)), "consumer registration timed out"
    )


def client_for(name):
    return RelaynaRabbitClient(
        SharedTasksSharedStatusTopology(
            rabbitmq_url="amqp://bench:bench@rabbit:5672/",
            tasks_queue=name + ".tasks",
            tasks_exchange=name + ".tasks.ex",
            tasks_routing_key="work",
            status_queue=name + ".status",
            status_exchange=name + ".status.ex",
            prefetch_count=1,
        )
    )


class CheckpointHub(StatusHub):
    """Single-reader experimental control using the released SDK's full ingestion path."""

    def __init__(self, store, **kwargs):
        super().__init__(store=store, **kwargs)
        self.redis = store.redis
        self.cursor_key = store.prefix + ":source-offset"

    async def _handle_message(self, message):
        # All benchmark hubs use prefetch 1, so only one delivery can update this cursor.
        self.source_offset = int(message.headers["x-stream-offset"])
        await super()._handle_message(message)

    async def _store_ordered(self, task_id, data):
        await super()._store_ordered(task_id, data)
        await self.redis.set(self.cursor_key, self.source_offset)


async def request(http, method, path, **kwargs):
    response = await http.request(method, path, **kwargs)
    response.raise_for_status()
    return response.json()


async def spool_record(http, topic, event):
    return await request(http, "POST", f"/topics/{topic}/records", json=event)


async def spool_consumer(http, topic, store, count, observe):
    group = f"/topics/{topic}/streams/audit"
    for _ in range(count):
        item = await request(http, "GET", group + "/next")
        validate(item is not None, "consumer reached the tail early")
        event = item["event"]
        await store.set_history(event["task_id"], event)
        await request(http, "POST", group + "/commit", json={"offset": item["offset"]})
        observe(event["event_id"])


async def verify_history(store, events, missing):
    by_task = defaultdict(list)
    for event in events:
        history = by_task[event["task_id"]]
        if event["event_id"] not in missing:
            history.append(event)
    # Compare complete payloads, order and multiplicity outside the timing window.
    for task, expected in by_task.items():
        stored = await store.get_history(task)
        validate(
            stored == list(reversed(expected)), f"incorrect Redis history for {task}"
        )


async def sparse_resume(http, url, after, start):
    async with http.stream(
        "GET",
        url,
        headers={"Last-Event-ID": after},
        timeout=httpx.Timeout(None, connect=10),
    ) as response:
        response.raise_for_status()
        fields = {}
        async for line in response.aiter_lines():
            if not line:
                if fields.get("event") == "status":
                    event = json.loads(fields["data"])
                    validate(
                        event["event_id"] == fields["id"]
                        and event["task_id"] == "rare",
                        "bad SSE payload",
                    )
                    return fields["id"], milliseconds(start)
                fields.clear()
            elif ":" in line:
                key, value = line.split(":", 1)
                fields[key] = value.lstrip()
    raise RuntimeError("SSE ended before a status")


async def replay(mode, name, args, rabbit, redis, http):
    prefix, offline = args.prefix, args.offline
    topic = name + ".status"
    store = RedisStatusStore(redis, prefix=name)
    counts = Counter()
    reached = asyncio.Event()
    target = None
    target_count = prefix if mode == "first-200" else None

    def observe(eid):
        counts[eid] += 1
        if target_count is not None:
            if len(counts) == target_count:
                reached.set()
        elif eid == target:
            reached.set()

    async def on_event(event):
        if isinstance(event, StatusHubStoredEvent):
            observe(event.event_id)

    def hub(offset):
        cls = CheckpointHub if mode == "checkpoint" else StatusHub
        return cls(
            rabbitmq=rabbit,
            store=store,
            consume_arguments={} if offset == "last" else {"x-stream-offset": offset},
            prefetch=200 if mode == "first-200" else 1,
            observation_sink=on_event,
        )

    async def publish(event):
        if mode == "spooltail":
            await spool_record(http, topic, event)
        else:
            await rabbit.publish_status(event)

    def event(i):
        return status(i, prefix, offline)

    for i in range(prefix):
        await publish(event(i))
    target = event(prefix - 1)["event_id"]
    if mode == "spooltail":
        await spool_consumer(http, topic, store, prefix, observe)
    else:
        async with running(hub("first").run_forever()):
            await reached.wait()
    validate(
        counts == Counter(f"E{i}" for i in range(prefix)), "prefix not fully applied"
    )
    # The old consumer is gone. All publishes below are acknowledged before reconnect.
    counts.clear()
    reached.clear()
    publish_ms = []
    for i in range(prefix, prefix + offline):
        start = time.perf_counter()
        await publish(event(i))
        publish_ms.append(milliseconds(start))

    if mode == "spooltail":
        visible = (await request(http, "GET", f"/topics/{topic}/tasks/rare/latest"))[
            "event"
        ]
        expected_visible = event(prefix + offline - 1)
    else:
        visible = await store.get_latest("rare")
        expected_visible = event(prefix // 2)
    validate(
        visible == expected_visible,
        "unexpected native status while consumer was offline",
    )
    target_count = prefix + offline if mode == "first-200" else None
    target = event(prefix + offline - 1)["event_id"]
    live = event(prefix + offline)
    services = (
        ["spool", "redis"] if mode == "spooltail" else ["rabbit", "redis", "sdk-api"]
    )
    before = sample_resources(services) if mode != "next" else None
    start = time.perf_counter()
    url = (
        f"/topics/{topic}/tasks/rare/events"
        if mode == "spooltail"
        else f"http://sdk-api:8080/{name}/events/rare"
    )
    async with asyncio.TaskGroup() as readers:
        reader = readers.create_task(
            sparse_resume(http, url, event(prefix // 2)["event_id"], start)
        )
        if mode == "spooltail":
            await spool_consumer(http, topic, store, offline, observe)
            elapsed = milliseconds(start)
            await reader
            counters = delta(before, sample_resources(services))
            await publish(live)
            await spool_consumer(http, topic, store, 1, observe)
        else:
            offset = "first" if mode == "first-200" else mode
            if mode == "checkpoint":
                saved = await redis.get(store.prefix + ":source-offset")
                validate(saved is not None, "missing source checkpoint")
                offset = int(saved) + 1
            async with running(hub(offset).run_forever()):
                if mode == "next":
                    await until_consumer(topic)
                    elapsed = None
                else:
                    await reached.wait()
                    elapsed = milliseconds(start)
                    await reader
                counters = delta(before, sample_resources(services)) if before else None
                target_count = None
                target = live["event_id"]
                reached.clear()
                await publish(live)
                await reached.wait()

    missing = [i for i in range(prefix, prefix + offline) if not counts[f"E{i}"]]
    wanted = list(range(prefix, prefix + offline)) if mode == "next" else []
    validate(
        mode == "last" or missing == wanted,
        f"{mode} unexpected missing downtime IDs: {missing[:10]}",
    )
    first = {"first": 0, "first-200": 0, "next": prefix + offline}.get(mode, prefix)
    if mode == "last":
        first = min(int(eid[1:]) for eid in counts)
    validate(
        counts == Counter(f"E{i}" for i in range(first, prefix + offline + 1)),
        "unexpected replay deliveries",
    )
    await verify_history(
        store,
        (event(i) for i in range(prefix + offline + 1)),
        {f"E{i}" for i in missing},
    )
    eid, sparse_ms = reader.result()
    validate(
        eid
        == (
            live["event_id"]
            if mode == "next"
            else event(prefix + offline - 1)["event_id"]
        ),
        "wrong resumed status",
    )
    return {
        "catch_up_ms": elapsed if not missing else None,
        "time_to_retained_tail_ms": elapsed,
        "replayed_prefix": sum(counts[f"E{i}"] for i in range(prefix)),
        "recovered": offline - len(missing),
        "missing_count": len(missing),
        "missing_event_range": [f"E{missing[0]}", f"E{missing[-1]}"] if missing else [],
        "live_event_verified": True,
        "first_sse_id": eid,
        "reconnect_to_missed_status_ms": sparse_ms if mode != "next" else None,
        "native_latest_during_disconnect": visible["event_id"],
        "offline_publish": distribution(publish_ms),
        "recovery_resources": counters,
    }


async def task_lane(mode, name, n, rabbit, redis, http):
    store = RedisStatusStore(redis, prefix=name)
    topic = name + ".tasks"
    statuses = name + ".status"
    group = f"/topics/{topic}/work/workers"
    acked, stored = asyncio.Event(), asyncio.Event()
    expected = None
    claim_time = None
    start = None
    latencies, claims = [], []

    def terminal(task):
        return StatusEventEnvelope(
            task_id=task,
            event_id=task + "-done",
            correlation_id=task,
            status="completed",
            timestamp="2026-01-01T00:00:00Z",
        )

    async def handler(task, context):
        nonlocal claim_time
        claim_time = milliseconds(start)
        await context.publish_status(terminal(task.task_id))

    async def observe(event):
        if isinstance(event, TaskMessageAcked) and event.task_id == expected:
            acked.set()
        if (
            isinstance(event, StatusHubStoredEvent)
            and event.event_id == expected + "-done"
        ):
            stored.set()

    async def process():
        nonlocal expected, start, claim_time
        for i in range(n):
            expected = f"work-{i}"
            task = {
                "task_id": expected,
                "event_id": expected + "-queued",
                "payload": {"input": "x" * 128},
                "created_at": "2026-01-01T00:00:00Z",
                "spec_version": "1.0",
                "correlation_id": expected,
            }
            acked.clear()
            stored.clear()
            start = time.perf_counter()
            if mode == "spooltail":
                await spool_record(http, topic, task)
                claim = await request(
                    http, "POST", group + "/claim", json={"lease_ms": 30_000}
                )
                claim_time = milliseconds(start)
                validate(claim["record"]["event"] == task, "wrong task claimed")
                await request(
                    http,
                    "POST",
                    group + "/complete",
                    json={
                        "offset": claim["record"]["offset"],
                        "generation": claim["generation"],
                        "status_topic": statuses,
                        "event": terminal(expected).as_transport_dict(),
                    },
                )
                latest = (
                    await request(
                        http, "GET", f"/topics/{statuses}/tasks/{expected}/latest"
                    )
                )["event"]
            else:
                await rabbit.publish_task(task)
                await acked.wait()
                await stored.wait()
                latest = await store.get_latest(expected)
            latencies.append(milliseconds(start))
            claims.append(claim_time)
            validate(
                latest == terminal(expected).as_transport_dict(),
                "missing or incorrect terminal effect",
            )

    if mode == "spooltail":
        await process()
    else:
        hub = StatusHub(
            rabbitmq=rabbit, store=store, prefetch=1, observation_sink=observe
        )
        consumer = TaskConsumer(
            rabbitmq=rabbit, handler=handler, prefetch=1, observation_sink=observe
        )
        async with running(hub.run_forever()), running(consumer.run_forever()):
            await until_consumer(statuses)
            await until_consumer(topic)
            await process()
    return {
        "verified_tasks": n,
        "submit_to_claim": distribution(claims),
        "submit_to_settled_and_read": distribution(latencies),
    }


async def main(args):
    modes = MODES
    modes = modes[args.trial % len(modes) :] + modes[: args.trial % len(modes)]
    rows, tasks = {}, {}
    async with (
        Redis(host="redis") as redis,
        httpx.AsyncClient(base_url="http://spool:7474", trust_env=False) as http,
    ):
        for mode in modes:
            name = f"trial{args.trial}-{mode}"
            rabbit = client_for(name)
            try:
                if mode != "spooltail":
                    await rabbit.initialize()
                print(
                    f"trial {args.trial + 1}: {mode} ({args.prefix}+{args.offline})",
                    file=sys.stderr,
                )
                rows[mode] = await replay(mode, name, args, rabbit, redis, http)
            finally:
                await rabbit.close()
        for mode in (
            ["sdk", "spooltail"] if args.trial % 2 == 0 else ["spooltail", "sdk"]
        ):
            name = f"lifecycle{args.trial}-{mode}"
            rabbit = client_for(name)
            try:
                if mode == "sdk":
                    await rabbit.initialize()
                tasks[mode] = await task_lane(
                    mode, name, args.tasks, rabbit, redis, http
                )
            finally:
                await rabbit.close()
        emit(
            result={
                "trial": args.trial + 1,
                "modes": rows,
                "task_lifecycle": tasks,
                "versions": {
                    "python": platform.python_version(),
                    "relayna": version("relayna"),
                    "redis_client": version("redis"),
                    "aio_pika": version("aio-pika"),
                    "httpx": version("httpx"),
                    "redis_server": (await redis.info("server"))["redis_version"],
                },
            }
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for key in ["prefix", "offline", "tasks", "trial", "timeout"]:
        parser.add_argument("--" + key, type=int, required=True)
    args = parser.parse_args()
    asyncio.run(asyncio.wait_for(main(args), timeout=args.timeout))
