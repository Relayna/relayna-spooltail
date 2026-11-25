"""Compare durable saved-offset catch-up with a fixed concurrent publish workload."""

import argparse
import asyncio
import base64
import hashlib
import json
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from pathlib import Path
from uuid import uuid4

TOPIC = "saved-offset"
CONSUMER = "audit"
BATCH = 32
PREFETCH = 128
PREFIX = 64
PAYLOAD_BYTES = 16 * 1024
REDIS_SAVE = """
local old = redis.call('GET', KEYS[1])
if (old or '-1') ~= ARGV[1] then return redis.error_reply('checkpoint changed') end
for i=3,#ARGV,2 do redis.call('HSET', KEYS[2], ARGV[i], ARGV[i+1]) end
redis.call('SET', KEYS[1], ARGV[2])
return 1
"""
ROOT = Path(__file__).resolve().parents[1]


def event(i):
    opaque = base64.b64encode(hashlib.shake_256(f"event-{i}".encode()).digest(PAYLOAD_BYTES // 4)).decode()
    payload = str(i % 10) * (PAYLOAD_BYTES - len(opaque)) + opaque
    return {"event_id": f"E{i}", "task_id": f"T{i % 128}", "payload": payload}


def checksum(item):
    return hashlib.sha256(item["payload"].encode()).hexdigest()


def p95(values):
    return sorted(values)[(95 * len(values) + 99) // 100 - 1]


class Spooltail:
    def __init__(self):
        from relayna_spooltail import AuditStore, SpooltailClient
        self.client = SpooltailClient("broker:7475", tls=False, timeout=30)
        self.store = AuditStore("/state/audit.sqlite")

    async def ready(self):
        await self.client.ping()

    async def close(self):
        await self.client.close()
        self.store.close()

    def publisher(self):
        return self.client.publish

    async def consume(self, count, ordered_ids=None):
        from relayna_spooltail import AuditOutcome

        after = self.store.checkpoint(TOPIC, CONSUMER)
        observed = []
        while len(observed) < count:
            batch = await self.client.read_after_batch(
                TOPIC, after=after, limit=min(BATCH, count - len(observed)),
            )
            if not batch.records:
                await asyncio.sleep(0.002)
                continue
            self.store.commit_batch(CONSUMER, batch, [
                AuditOutcome(r.offset, "apply", "applied", "benchmark", checksum(r.event))
                for r in batch.records
            ])
            observed.extend(r.event["event_id"] for r in batch.records)
            after = batch.records[-1].offset
        assert ordered_ids is None or observed == ordered_ids
        return after

    async def verified(self, count, offset):
        with sqlite3.connect("/state/audit.sqlite") as db:
            rows = db.execute(
                "SELECT source_event_id, task_id, reason FROM spooltail_audit_outcome"
            ).fetchall()
        expected = {(item["event_id"], item["task_id"], checksum(item))
                    for item in map(event, range(count))}
        assert set(rows) == expected and len(rows) == count
        assert self.store.checkpoint(TOPIC, CONSUMER) == offset


class RabbitRedis:
    async def ready(self, *, restored=False):
        import aio_pika
        import redis.asyncio as redis

        self.redis = redis.Redis(host="redis", decode_responses=True)
        await self.redis.ping()
        assert (await self.redis.config_get("appendfsync"))["appendfsync"] == "always"
        self.rabbit = await aio_pika.connect("amqp://bench:bench@broker/", timeout=4)
        channel = await self.rabbit.channel()
        await channel.declare_queue(TOPIC, durable=True, arguments={"x-queue-type": "stream"})
        await channel.close()
        if restored:
            channel = await self.rabbit.channel()
            await channel.set_qos(prefetch_count=1)
            queue = await channel.get_queue(TOPIC)
            async with asyncio.timeout(4):
                async with queue.iterator(arguments={"x-stream-offset": "first"}) as iterator:
                    message = await anext(iterator)
                    assert json.loads(message.body) == event(0)
                    await message.ack()
            await channel.close()

    async def close(self):
        if getattr(self, "rabbit", None) is not None:
            await self.rabbit.close()
        if getattr(self, "redis", None) is not None:
            await self.redis.aclose()

    async def publisher(self):
        import aio_pika

        from pamqp.commands import Basic

        channel = await self.rabbit.channel(publisher_confirms=True)

        async def publish(topic, item):
            ack = await channel.default_exchange.publish(
                aio_pika.Message(json.dumps(item, separators=(",", ":")).encode(),
                                 delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                routing_key=topic,
            )
            assert isinstance(ack, Basic.Ack)
        return publish

    async def consume(self, count, ordered_ids=None):
        after = int(await self.redis.get("checkpoint") or -1)
        channel = await self.rabbit.channel(publisher_confirms=False)
        await channel.set_qos(prefetch_count=PREFETCH)
        queue = await channel.get_queue(TOPIC)
        observed = []
        try:
            async with queue.iterator(arguments={"x-stream-offset": "first" if after < 0 else after + 1}) as iterator:
                while len(observed) < count:
                    messages = [await anext(iterator) for _ in range(min(BATCH, count - len(observed)))]
                    last = int(messages[-1].headers["x-stream-offset"])
                    values = []
                    for message in messages:
                        item = json.loads(message.body)
                        observed.append(item["event_id"])
                        values.extend([item["event_id"], json.dumps([item["task_id"], checksum(item)])])
                    assert await self.redis.eval(REDIS_SAVE, 2, "checkpoint", "outcomes", after, last, *values) == 1
                    await messages[-1].ack(multiple=True)
                    after = last
        finally:
            await channel.close()
        assert ordered_ids is None or observed == ordered_ids
        return after

    async def verified(self, count, offset):
        recorded = await self.redis.hgetall("outcomes")
        expected = {item["event_id"]: [item["task_id"], checksum(item)] for item in map(event, range(count))}
        assert {k: json.loads(v) for k, v in recorded.items()} == expected
        assert int(await self.redis.get("checkpoint")) == offset


async def run_client(args):
    for attempt in range(60):
        engine = Spooltail() if args.arm == "spooltail" else RabbitRedis()
        try:
            if args.arm == "spooltail":
                await engine.ready()
            else:
                await engine.ready(restored=args.phase == "measure")
            break
        except Exception:
            await engine.close()
            if attempt == 59:
                raise
            await asyncio.sleep(0.5)

    async def publisher():
        return engine.publisher() if args.arm == "spooltail" else await engine.publisher()

    try:
        if args.phase == "seed":
            publish = await publisher()
            for i in range(PREFIX):
                await publish(TOPIC, event(i))
            checkpoint = await engine.consume(PREFIX, [f"E{i}" for i in range(PREFIX)])
            await engine.verified(PREFIX, checkpoint)
            for i in range(PREFIX, PREFIX + args.backlog):
                await publish(TOPIC, event(i))
            Path("/state/checkpoint").write_text(str(checkpoint))
            return

        async with asyncio.timeout(90):
            await engine.verified(PREFIX, int(Path("/state/checkpoint").read_text()))
            if args.arm == "spooltail":
                while not list(Path("/spool").glob("*.seg.zst.writing")):
                    await asyncio.sleep(0.025)
                assert (await engine.client.stats()).packed_journal_bytes == 0

            base = PREFIX + args.backlog
            samples = []
            start_gate = asyncio.Event()
            publishers = [await publisher(), await publisher()]

            async def produce(lane):
                await start_gate.wait()
                for i in range(lane, args.live, 2):
                    await asyncio.sleep(max(0, epoch + i / args.rate - time.perf_counter()))
                    item = event(base + i)
                    start = time.perf_counter()
                    await publishers[lane](TOPIC, item)
                    samples.append((start, time.perf_counter()))

            tasks = [asyncio.create_task(produce(lane)) for lane in range(2)]
            epoch = time.perf_counter()
            start_gate.set()
            # A fixed 20 ms producer head start places storage maintenance and
            # the catch-up timer inside the same paced publishing window.
            await asyncio.sleep(0.02)
            begin = time.perf_counter()
            if args.arm == "spooltail":
                Path("/spool/resume-packing").touch()
            await engine.consume(args.backlog, [f"E{i}" for i in range(PREFIX, base)])
            caught_up = time.perf_counter()
            await asyncio.gather(*tasks)
            packed_bytes = None
            if args.arm == "spooltail":
                packed_bytes = (await engine.client.stats()).packed_journal_bytes
                assert packed_bytes > 0, "no segment was packed during the live workload"
            offset = await engine.consume(args.live)
            await engine.verified(base + args.live, offset)
            throughput_period = max(stop for _, stop in samples) - epoch
            overlap = sum(start < caught_up and stop >= begin for start, stop in samples)
            assert overlap > 0, "catch-up ran without overlapping a publish"
            result = {
                "arm": args.arm,
                "catchup_ms": (caught_up - begin) * 1000,
                "publish_p95_ms": p95([(stop - start) * 1000 for start, stop in samples]),
                "publish_overlap_count": overlap,
                "publish_rate_observed": args.live / throughput_period,
                "verified_records": base + args.live,
            }
            if packed_bytes is not None:
                result["packed_bytes"] = packed_bytes
            print(json.dumps(result))
    finally:
        await engine.close()


def docker(*args, quiet=False, check=True, timeout=180):
    run = subprocess.run(("docker", *(str(arg) for arg in args)), capture_output=True,
                         text=True, check=False, timeout=timeout)
    if check and run.returncode:
        raise RuntimeError(f"Docker failed ({' '.join(map(str,args[:3]))})\n{run.stdout}{run.stderr}")
    if not quiet and run.stderr:
        print(run.stderr, end="", file=sys.stderr)
    return run.stdout.strip()


def run_trials(args):
    if args.trials < 1 or args.live < 2 or args.rate < 1 or args.backlog * PAYLOAD_BYTES <= 8 * 1024**2:
        raise ValueError("provide positive trials and rate, at least two live events, and a backlog over 8 MiB")
    name = f"spooltail-bench-{uuid4().hex[:10]}"
    binary = args.broker.resolve()
    if not binary.is_file():
        raise ValueError("Provide a Linux release binary built with pack-fault-injection")
    client_image = "python:3.13-slim-bookworm"
    rabbit_image, redis_image = "rabbitmq:4-alpine", "redis:7-alpine"
    results = []
    with tempfile.TemporaryDirectory(prefix="spooltail-bench-deps-") as deps:
        docker("run", "--rm", "-v", f"{deps}:/deps", client_image,
               "pip", "install", "--disable-pip-version-check", "--root-user-action=ignore", "-q", "--target", "/deps",
               "aio-pika==9.6.1", "redis==7.4.0", "grpcio==1.80.0", "protobuf==6.33.5")
        docker("network", "create", name)
        images = {image: docker("image", "inspect", "--format", "{{.Id}}", image)
                  for image in (client_image, rabbit_image, redis_image)}
        try:
            for trial in range(args.trials):
                arms = ("spooltail", "rabbit-redis", "rabbit-redis-sync")
                pivot = trial % len(arms)
                order = arms[pivot:] + arms[:pivot]
                for arm in order:
                    prefix = f"{name}-{trial}-{arm}"
                    broker, redis, state, data = (f"{prefix}-{suffix}" for suffix in
                                                  ("broker", "redis", "state", "data"))
                    common = ["--network", name]
                    started = []
                    try:
                        if arm == "spooltail":
                            docker("run", "-d", "--name", broker, *common, "--network-alias", "broker",
                                   "-v", f"{data}:/data", "-v", f"{binary}:/spooltail:ro",
                                   "-e", "SPOOLTAIL_DATA=/data", "-e", "SPOOLTAIL_INDEX_MODE=partitioned",
                                   "-e", "SPOOLTAIL_GRPC_LISTEN=0.0.0.0:7475", "-e", "SPOOLTAIL_GRPC_TLS=off",
                                   "-e", "SPOOLTAIL_PACK_SEALED=1", "-e", "SPOOLTAIL_PACK_MIN_AGE_SECS=0",
                                   "-e", "SPOOLTAIL_PACK_STOP_AT=pack_opened",
                                   "-e", "SPOOLTAIL_PACK_TEST_RESUME=/data/resume-packing",
                                   "-e", "SPOOLTAIL_DISK_RESERVE_BYTES=16777216",
                                   client_image, "/spooltail")
                            started.append(broker)
                        else:
                            if arm == "rabbit-redis-sync":
                                docker("run", "--rm", "-v", f"{data}:/data", client_image,
                                       "chattr", "+S", "/data")
                            docker("run", "-d", "--name", broker, *common, "--network-alias", "broker",
                                   "-v", f"{data}:/var/lib/rabbitmq", "--hostname", "benchmark-rabbit",
                                   "-e", "RABBITMQ_DEFAULT_USER=bench", "-e", "RABBITMQ_DEFAULT_PASS=bench",
                                   rabbit_image)
                            started.append(broker)
                            docker("run", "-d", "--name", redis, *common, "--network-alias", "redis",
                                   "-v", f"{state}:/data", redis_image, "redis-server",
                                   "--appendonly", "yes", "--appendfsync", "always", "--save", "")
                            started.append(redis)

                        def driver(phase):
                            return docker(
                                "run", "--rm", *common, "-v", f"{state}:/state",
                                "-v", f"{deps}:/deps:ro", "-v", f"{ROOT}:/repo:ro",
                                *(["-v", f"{data}:/spool"] if arm == "spooltail" else []),
                                "-e", "PYTHONPATH=/deps:/repo/python/src", client_image,
                                "python", "/repo/benchmarks/durable_replay.py", "--arm", arm,
                                "--phase", phase, "--backlog", args.backlog,
                                "--live", args.live, "--rate", args.rate,
                            )

                        driver("seed")
                        if arm == "rabbit-redis-sync":
                            flags = docker("run", "--rm", "-v", f"{data}:/data:ro", client_image,
                                           "find", "/data", "-name", "*.segment", "-exec", "lsattr", "{}", "+")
                            assert flags and all("S" in line.split()[0] for line in flags.splitlines()), flags
                        docker("restart", *started)
                        result = json.loads(driver("measure").splitlines()[-1])
                        results.append({**result, "trial": trial + 1})
                        print(json.dumps(results[-1]), flush=True)
                    except Exception:
                        for container in started:
                            print(docker("logs", "--tail", 100, container, check=False), file=sys.stderr)
                        raise
                    finally:
                        if started:
                            docker("rm", "-f", *started, check=False, quiet=True)
                        docker("volume", "rm", state, data, check=False, quiet=True)
        finally:
            docker("network", "rm", name, check=False, quiet=True)
    groups = defaultdict(list)
    for row in results:
        groups[row["arm"]].append(row)
    output = {
        "settings": {"durability": "single node; confirmed publishes; synchronous sink writes; sync Rabbit arm uses chattr +S",
                     "restart": "broker restarted after seeding; Redis restarted and SQLite reopened; fresh consumer process",
                     "backlog": args.backlog, "live_count": args.live, "offered_rate": args.rate,
                     "batch": BATCH, "rabbit_prefetch": PREFETCH, "event_payload_bytes": PAYLOAD_BYTES,
                     "event_shape": "repeated text with 4 KiB of deterministic opaque bytes encoded as base64",
                     "image_ids": images,
                     "broker_sha256": hashlib.sha256(binary.read_bytes()).hexdigest()},
        "medians": {arm: {metric: statistics.median(row[metric] for row in rows)
                          for metric in ("catchup_ms", "publish_p95_ms", "publish_rate_observed")}
                    for arm, rows in groups.items()},
        "trials": results,
    }
    args.output.write_text(json.dumps(output, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker", type=Path)
    parser.add_argument("--output", type=Path, default=Path("durable-replay-results.json"))
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--backlog", type=int, default=768)
    parser.add_argument("--live", type=int, default=384)
    parser.add_argument("--rate", type=int, default=240)
    parser.add_argument("--arm", choices=("spooltail", "rabbit-redis", "rabbit-redis-sync"), help=argparse.SUPPRESS)
    parser.add_argument("--phase", choices=("seed", "measure"), help=argparse.SUPPRESS)
    arguments = parser.parse_args()
    if arguments.arm:
        asyncio.run(run_client(arguments))
    else:
        if arguments.broker is None:
            parser.error("--broker is required")
        run_trials(arguments)
