"""Measure task history and durable publish latency during synchronized reader bursts."""

import argparse
import asyncio
import hashlib
import json
import math
import os
import platform
import shutil
import sys
import tempfile
from contextlib import AsyncExitStack
from pathlib import Path
from statistics import median
from time import perf_counter

import grpc
from relayna_spooltail._wire import spooltail_pb2 as pb
from relayna_spooltail._wire.spooltail_pb2_grpc import SpooltailStub

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tests"))
import grpc_acceptance as fixture  # noqa: E402

TASKS = 8
PAGE = 64
READERS = 4
PUBLISHES = 240


def payload(task, n, size=256):
    return {"task_id": str(task), "event_id": f"e{n}", "payload": "x" * size}


def p99(values):
    return sorted(values)[math.ceil(0.99 * len(values)) - 1]


def channel(port, cert):
    credentials = grpc.ssl_channel_credentials(cert.read_bytes())
    return grpc.aio.secure_channel(f"localhost:{port}", credentials)


async def publisher(port, cert, size, publishes):
    async with channel(port, cert) as connection:
        stub = SpooltailStub(connection)
        await stub.Ping(pb.Empty(), timeout=5)
        print("ready", flush=True)
        epoch = float(sys.stdin.readline())
        times = []
        for n in range(publishes):
            await asyncio.sleep(max(0, epoch + n / 75 - perf_counter()))
            request = pb.Publish(
                topic="live", event_json=json.dumps(payload("live", n, size)).encode()
            )
            before = perf_counter()
            result = await stub.Append(request, timeout=5)
            times.append((perf_counter() - before) * 1000)
            assert result.offset == TASKS * PAGE + n + 1
        print(json.dumps(times))


async def seed(config, cert):
    expected = [[] for _ in range(TASKS)]
    async with fixture.start(config) as (_, port), channel(port, cert) as conn:
        stub = SpooltailStub(conn)
        for n in range(PAGE):
            for task in range(TASKS):
                event = payload(task, n)
                result = await stub.Append(
                    pb.Publish(topic="status", event_json=json.dumps(event).encode()),
                    timeout=5,
                )
                expected[task].append((result.offset, event))
    return expected


async def read_pages(stub, worker, epoch, expected, count):
    times = []
    for n in range(count):
        await asyncio.sleep(max(0, epoch + n / 50 - perf_counter()))
        task = (worker + n) % TASKS
        request = pb.Task(topic="status", task_id=str(task))
        before = perf_counter()
        page = await stub.History(request, timeout=5)
        times.append((perf_counter() - before) * 1000)
        assert [
            (r.offset, r.partition, r.event_json) for r in page.records
        ] == expected[task]
    return times


async def measure(config, cert, expected, args):
    async with fixture.start(config) as (_, port), AsyncExitStack() as stack:
        stubs = []
        # The control with zero timed readers still verifies the seeded history.
        for _ in range(max(1, args.readers)):
            stub = SpooltailStub(await stack.enter_async_context(channel(port, cert)))
            await stub.Ping(pb.Empty(), timeout=5)
            stubs.append(stub)
        wire = {}
        for task in range(TASKS):
            page = await stubs[0].History(
                pb.Task(topic="status", task_id=str(task)), timeout=5
            )
            assert [
                (r.offset, json.loads(r.event_json)) for r in page.records
            ] == expected[task]
            assert all(r.partition == 0 for r in page.records)
            wire[task] = [(r.offset, r.partition, r.event_json) for r in page.records]

        writer = await asyncio.create_subprocess_exec(
            sys.executable,
            __file__,
            "publisher",
            str(port),
            str(cert),
            str(args.writer_bytes),
            str(args.publishes),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
        )
        try:
            async with asyncio.timeout(args.publishes / 75 + 45):
                assert await writer.stdout.readline() == b"ready\n"
                epoch = perf_counter() + 0.03
                writer.stdin.write(f"{epoch}\n".encode())
                await writer.stdin.drain()
                pages_per_reader = math.ceil(args.publishes * 50 / 75)
                *series, output = await asyncio.gather(
                    *(
                        read_pages(stub, i, epoch, wire, pages_per_reader)
                        for i, stub in enumerate(stubs[: args.readers])
                    ),
                    writer.communicate(),
                )
            elapsed = perf_counter() - epoch
            assert writer.returncode == 0
            acks = json.loads(output[0])
            assert len(acks) == args.publishes
        finally:
            if writer.returncode is None:
                writer.kill()
                await writer.wait()
        reads = [t for s in series for t in s]
        return {
            "elapsed_s": elapsed,
            "history_p50": median(reads) if reads else None,
            "history_p99": p99(reads) if reads else None,
            "publish_p50": median(acks),
            "publish_p99": p99(acks),
        }


async def compare(args, root):
    binaries = [Path(p).resolve(strict=True) for p in args.brokers]
    cert, key = fixture.identity(root)
    fixture.BROKER = binaries[0]
    template = fixture.env(root / "seed", cert, key)
    expected = await seed(template, cert)
    rows = []
    for trial in range(args.rounds):
        pivot = trial // 2 % len(binaries)
        order = binaries[pivot:] + binaries[:pivot]
        for binary in order if trial % 2 == 0 else reversed(order):
            with tempfile.TemporaryDirectory(dir=root) as testdir:
                config = fixture.env(Path(testdir), cert, key)
                shutil.copytree(template["SPOOLTAIL_DATA"], config["SPOOLTAIL_DATA"])
                fixture.BROKER = binary
                stats = await measure(config, cert, expected, args)
                rows.append({"round": trial + 1, "binary": binary.name, **stats})
                print(rows[-1], flush=True)
    return {
        "platform": platform.platform(),
        "readers": args.readers,
        "publishes_per_trial": args.publishes,
        "writer_payload_bytes": args.writer_bytes,
        "binary_sha256": {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in binaries
        },
        "latency_units": "ms",
        "trials": rows,
    }


if __name__ == "__main__":
    if sys.argv[1:2] == ["publisher"]:
        asyncio.run(
            publisher(
                sys.argv[2],
                Path(sys.argv[3]),
                int(sys.argv[4]),
                int(sys.argv[5]),
            )
        )
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument(
            "brokers", nargs="+", help="Release binaries with distinct filenames"
        )
        parser.add_argument("--rounds", type=int, default=8)
        parser.add_argument("--publishes", type=int, default=PUBLISHES)
        parser.add_argument("--readers", type=int, default=READERS)
        parser.add_argument("--writer-bytes", type=int, default=256)
        parser.add_argument("--output", type=Path, required=True)
        args = parser.parse_args()
        if (
            len({Path(p).name for p in args.brokers}) != len(args.brokers)
            or args.rounds < 1
            or args.publishes < 1
            or args.writer_bytes < 0
            or args.readers < 0
            or args.output.exists()
        ):
            parser.error(
                "use distinct binary names, positive rounds and publishes, "
                "nonnegative readers and payload size, and a new output path"
            )
        # Keep inherited service settings out of comparisons on a developer machine.
        for key in list(os.environ):
            if key.startswith("SPOOLTAIL_") or key == "TOKIO_WORKER_THREADS":
                del os.environ[key]
        with tempfile.TemporaryDirectory() as tmp:
            result = asyncio.run(compare(args, Path(tmp)))
        args.output.write_text(json.dumps(result, indent=2) + "\n")
