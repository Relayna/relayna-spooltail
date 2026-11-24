"""Check same-task fan-out through the public SSE endpoints or Python SDKs."""

import asyncio
import json
import math
import os
import socket
import sys
from contextlib import AsyncExitStack, aclosing
from pathlib import Path
from time import monotonic_ns

import httpx

CASES = {
    # Readers, payload bytes, events after seed, target events/s, slow readers, offline events.
    **{
        f"live-{readers}": (
            readers,
            256,
            int(os.environ.get("BENCH_LIVE_EVENTS", "48")),
            int(os.environ.get("BENCH_LIVE_RATE", "40")),
            0,
            0,
        )
        for readers in range(1, 257)
    },
    "large-32": (32, 32768, 96, 50, 0, 0),
    "slow-32": (32, 32768, 96, 50, 4, 0),
    "resume-64": (64, 256, 36, 40, 0, 24),
    "resume-256": (256, 256, 36, 40, 0, 24),
    "resume-256-publishing": (256, 256, 36, 40, 0, 24),
}


def emit(**msg):
    print(json.dumps(msg, separators=(",", ":")), flush=True)


def quantiles(values):
    ordered = sorted(values)
    return {
        f"p{p}": round(ordered[math.ceil(p * len(ordered) / 100) - 1], 3)
        for p in (50, 95, 99, 100)
    }


def events(task, size, count):
    return [
        {
            "task_id": task,
            "event_id": f"E{n}",
            "correlation_id": task,
            "status": "completed" if n == count else "running",
            "meta": {"payload": chr(97 + n % 26) * size},
        }
        for n in range(count + 1)
    ]


async def statuses(response):
    response.raise_for_status()
    fields = {}
    async for line in response.aiter_lines():
        if not line:
            if fields.get("event") == "status":
                yield fields["id"], fields["data"]
            elif fields.get("event") not in (None, "ready"):
                raise ValueError(f"unexpected SSE message {fields}")
            fields.clear()
        elif not line.startswith(":"):
            key, _, value = line.partition(":")
            fields[key] = value.lstrip()


def client(limit, slow=False):
    transport = httpx.AsyncHTTPTransport(
        limits=httpx.Limits(
            max_connections=limit + 2, max_keepalive_connections=limit + 2
        ),
        socket_options=[(socket.SOL_SOCKET, socket.SO_RCVBUF, 16384)] if slow else None,
    )
    return httpx.AsyncClient(
        transport=transport, trust_env=False, timeout=httpx.Timeout(30, connect=10)
    )


async def watchers(case, task, url, shard, shards):
    count, size, total, _, slow, offline = CASES[case]
    indices = list(range(shard, count, shards))
    expected = events(task, size, total)
    stages = {"ready": 0, "replayed": 0}
    resume = asyncio.Event()

    def reached(stage):
        stages[stage] += 1
        if stages[stage] == len(indices):
            emit(stage=stage)

    async def open_gate():
        if offline:
            assert (await asyncio.to_thread(sys.stdin.readline)).strip() == "resume"
            resume.set()

    sdk = os.environ.get("BENCH_PYTHON_SDK") == "1"

    async def watch(i, http, rpc):
        times = []
        next_seq = 0

        async def messages(last_id):
            if rpc is not None:
                async with aclosing(
                    rpc.subscribe("status", task, after=last_id)
                ) as stream:
                    async for msg in stream:
                        if msg.kind == "status":
                            yield msg.event_id, msg.data
                        elif msg.kind != "ready":
                            raise ValueError(f"unexpected gRPC delivery {msg.kind}")
                return
            headers = {"Last-Event-ID": last_id} if last_id else {}
            async with http.stream("GET", url, headers=headers) as response:
                async for event_id, data in statuses(response):
                    # The SDK comparison times decoded application dictionaries on both arms.
                    yield event_id, json.loads(data) if sdk else data

        async def consume(last_id=None):
            nonlocal next_seq
            async with aclosing(messages(last_id)) as stream:
                async for event_id, data in stream:
                    receipt = monotonic_ns()
                    if not sdk:
                        data = json.loads(data)
                    assert (
                        next_seq <= total and event_id == expected[next_seq]["event_id"]
                    ), (i, event_id, next_seq)
                    assert data == expected[next_seq], (
                        i,
                        event_id,
                        "payload",
                    )
                    if next_seq == 0:
                        next_seq += 1
                        if offline:
                            return
                        reached("ready")
                    else:
                        times.append(receipt)
                        if next_seq == offline:
                            reached("replayed")
                        next_seq += 1
                    if i < slow and next_seq <= total:
                        await asyncio.sleep(0.05)

        await consume()
        if offline:
            assert next_seq == 1
            reached(
                "ready"
            )  # The cursor was seen and this reader's connection is closed.
            await resume.wait()
            await consume("E0")
        assert next_seq == total + 1, (i, next_seq, total)
        return {"reader": i, "slow": i < slow, "received": times}

    async with AsyncExitStack() as scope:
        fast = await scope.enter_async_context(client(len(indices)))
        delayed = await scope.enter_async_context(client(len(indices), slow=not sdk))
        rpc = None
        if sdk and url.startswith("http://spool:"):
            from relayna_spooltail import SpooltailClient

            rpc = await scope.enter_async_context(
                SpooltailClient("spool:7475", tls=False)
            )
        gate = asyncio.create_task(open_gate())
        streams = await asyncio.gather(
            *(watch(i, delayed if i < slow else fast, rpc) for i in indices)
        )
        await gate
    emit(streams=streams)


async def hub():
    from compare import client_for
    from redis.asyncio import Redis
    from relayna.status import RedisStatusStore, StatusHub

    rabbit = client_for("fanout")
    async with Redis(host="redis") as redis:
        await rabbit.initialize()
        try:
            await StatusHub(
                rabbitmq=rabbit,
                store=RedisStatusStore(redis, prefix="random-reads"),
                consume_arguments={"x-stream-offset": "first"},
            ).run_forever()
        finally:
            await rabbit.close()


def samples(services):
    emit(sample=services)
    return json.loads(sys.stdin.readline())


async def native_publisher(scope, arm, expected):
    worker = await asyncio.create_subprocess_exec(
        "/publisher/native-publish",
        arm,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
    )

    async def close():
        worker.stdin.close()
        await worker.wait()

    scope.push_async_callback(close)
    worker.stdin.write(json.dumps(expected).encode() + b"\n")
    await worker.stdin.drain()
    assert await worker.stdout.readline() == b"ready\n"

    async def publish(n):
        worker.stdin.write(f"{n}\n".encode())
        await worker.stdin.drain()
        if await worker.stdout.readline() != b"ack\n":
            raise RuntimeError("producer exited before confirming the event")

    return publish


async def trial(case, arm, task):
    from compare import client_for
    from redis.asyncio import Redis
    from relayna.status import RedisStatusStore

    readers, size, total, rate, slow, offline = CASES[case]
    expected = events(task, size, total)
    spool = arm == "spooltail"
    publish_during_resume = case == "resume-256-publishing"
    services = ["spool"] if spool else ["rabbit", "redis", "sdk-api", "hub"]
    url = (
        f"http://spool:7474/topics/status/tasks/{task}/events"
        if spool
        else f"http://sdk-api:8080/sdk-wide/events/{task}"
    )
    async with AsyncExitStack() as scope:
        http = await scope.enter_async_context(client(4))
        rpc = None
        if spool and os.environ.get("BENCH_PYTHON_SDK") == "1":
            from relayna_spooltail import SpooltailClient

            rpc = await scope.enter_async_context(
                SpooltailClient("spool:7475", tls=False)
            )
        if not spool:
            rabbit = client_for("fanout")
            await rabbit.initialize()
            scope.push_async_callback(rabbit.close)
            redis = await scope.enter_async_context(Redis(host="redis"))
            store = RedisStatusStore(redis, prefix="random-reads")

        async def publish(n):
            if rpc is not None:
                assert await rpc.publish("status", expected[n]) > 0
            elif spool:
                resp = await http.post(
                    "http://spool:7474/topics/status/records", json=expected[n]
                )
                resp.raise_for_status()
                assert resp.json()["offset"] > 0
            else:
                await rabbit.publish_status(expected[n])

        if os.environ.get("BENCH_PUBLISHER") == "go":
            publish = await native_publisher(scope, arm, expected)

        async def wait_visible(n):
            if not spool:
                async with asyncio.timeout(30):
                    while await store.get_latest(task) != expected[n]:
                        await asyncio.sleep(0.01)

        async def reads():
            resp = await http.get("http://spool:7474/stats")
            resp.raise_for_status()
            return resp.json()["payload_reads"]

        await publish(0)
        await wait_visible(0)
        workers = []

        async def stop(worker):
            if worker.returncode is None:
                worker.kill()
            await worker.wait()

        shards = min(4, readers)
        for shard in range(shards):
            worker = await asyncio.create_subprocess_exec(
                sys.executable,
                str(Path(__file__)),
                "watch",
                case,
                task,
                url,
                str(shard),
                str(shards),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                limit=8 * 1024 * 1024,
            )
            workers.append(worker)
            scope.push_async_callback(stop, worker)

        async def message(worker):
            line = await worker.stdout.readline()
            if not line:
                raise RuntimeError(f"watcher exited with {await worker.wait()}")
            return json.loads(line)

        async def barrier(stage):
            replies = await asyncio.gather(*(message(w) for w in workers))
            assert all(r == {"stage": stage} for r in replies), stage

        await barrier("ready")
        sent = {}
        for n in range(1, offline + 1):
            await publish(n)
        await wait_visible(offline)
        initial_reads = await reads() if spool else None
        before = samples(services)
        if offline:
            resume_at = monotonic_ns()
            for worker in workers:
                worker.stdin.write(b"resume\n")
            if not publish_during_resume:
                await barrier("replayed")
        start = monotonic_ns()
        confirms, schedule = [], []
        for i, n in enumerate(range(offline + 1, total + 1)):
            target = start + i * 1e9 / rate
            await asyncio.sleep(max(0, (target - monotonic_ns()) / 1e9))
            sent[n] = monotonic_ns()
            schedule.append((sent[n] - target) / 1e6)
            await publish(n)
            confirms.append((monotonic_ns() - sent[n]) / 1e6)
        if publish_during_resume:
            await barrier("replayed")
        replies = await asyncio.gather(*(message(w) for w in workers))
        assert all(
            code == 0 for code in await asyncio.gather(*(w.wait() for w in workers))
        )
        after = samples(services)
        read_delta = (await reads() - initial_reads) if spool else None

    streams = [stream for reply in replies for stream in reply["streams"]]
    assert len(streams) == readers and all(len(s["received"]) == total for s in streams)

    def delivery(cohort):
        return quantiles(
            [(s["received"][n - 1] - t) / 1e6 for s in cohort for n, t in sent.items()]
        )

    last = [
        (max(s["received"][n - 1] for s in streams) - t) / 1e6 for n, t in sent.items()
    ]
    result = {
        "case": case,
        "arm": arm,
        "readers": readers,
        "events_per_reader": total,
        "verified": readers * total,
        "publish_ms": quantiles(confirms),
        "publisher_start_lateness_ms": quantiles(schedule),
        "dispatch_rate_per_s": round(
            (len(sent) - 1) * 1e9 / (sent[total] - sent[offline + 1]), 2
        ),
        "fast_delivery_ms": delivery([s for s in streams if not s["slow"]]),
        "last_reader_per_event_ms": quantiles(last),
        # Reader IDs are assigned to the same client shards in both arms.
        "reader_p95_ms": [
            delivery([stream])["p95"]
            for stream in sorted(streams, key=lambda s: s["reader"])
        ],
        "journal_reads": read_delta,
        "server_cpu_s": {
            s: round(after[s]["cpu_s"] - before[s]["cpu_s"], 4) for s in services
        },
    }
    if slow:
        result["slow_delivery_ms"] = delivery([s for s in streams if s["slow"]])
    if offline:
        for name, index in [("first_missed_ms", 0), ("caught_up_ms", offline - 1)]:
            result[name] = quantiles(
                [(s["received"][index] - resume_at) / 1e6 for s in streams]
            )
    return result


async def main(args):
    if args[0] == "watch":
        await watchers(*args[1:4], int(args[4]), int(args[5]))
    elif args[0] == "hub":
        await hub()
    elif args[0] == "trial":
        async with asyncio.timeout(90):
            emit(result=await trial(*args[1:]))
    else:
        raise ValueError(args[0])


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
