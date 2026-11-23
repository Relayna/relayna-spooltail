"""Force process exits at the packed journal switch using a fault-injection build."""

import asyncio
import json
import os
import shutil
import sqlite3
import tempfile
from contextlib import asynccontextmanager, closing
from pathlib import Path

from relayna_spooltail import ConflictError, SettlementAction, SpooltailClient

BROKER = Path(os.environ.get("SPOOLTAIL_PACK_TEST_BROKER", "target/release/spooltail")).resolve()
FIRST_RAW = "00000000000000000000.seg"
FIRST_PACK = FIRST_RAW + ".zst"
CUTS = {
    "pack_synced": (False, True, False, True),
    "pack_renamed": (False, True, True, False),
    "pack_named": (False, True, True, False),
    "manifest_committed": (True, True, True, False),
    "raw_unlinked": (True, False, True, False),
}


def query(volume, sql):
    with closing(sqlite3.connect(volume / "index.sqlite")) as db:
        return db.execute(sql).fetchall()


async def launch(
    volume, mode, *, packing=False, paused=True, cut=None, resume=None,
    retain_for="1h", audit_only=(), disk_reserve=0,
):
    env = {key: value for key, value in os.environ.items() if not key.startswith("SPOOLTAIL_")}
    env.update(
        SPOOLTAIL_DATA=str(volume),
        SPOOLTAIL_INDEX_MODE=mode,
        SPOOLTAIL_LISTEN="127.0.0.1:0",
        SPOOLTAIL_GRPC_LISTEN="127.0.0.1:0",
        SPOOLTAIL_GRPC_TLS="off",
        SPOOLTAIL_PACK_SEALED=str(int(packing)),
        SPOOLTAIL_PACK_MIN_AGE_SECS="0",
        SPOOLTAIL_DISK_RESERVE_BYTES=str(disk_reserve),
        SPOOLTAIL_RETIRE_AUDIT_TOPICS=json.dumps(audit_only),
        SPOOLTAIL_RETENTION=json.dumps(
            {"status": {"hot_for": "1s", "retain_for": retain_for, "paused": paused}}
        ),
    )
    if cut:
        env["SPOOLTAIL_PACK_STOP_AT"] = cut
    if resume is not None:
        env["SPOOLTAIL_PACK_TEST_RESUME"] = str(resume)
    return await asyncio.create_subprocess_exec(
        BROKER, env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )


async def kill(process):
    if process.returncode is None:
        process.kill()
    return await process.communicate()


@asynccontextmanager
async def running_broker(volume, mode, **options):
    process = await launch(volume, mode, **options)
    try:
        async with asyncio.timeout(10):
            http = (await process.stdout.readline()).decode()
            grpc = (await process.stdout.readline()).decode()
        if not (http.startswith("Listening ") and grpc.startswith("gRPC listening on ")):
            _, error = await kill(process)
            raise AssertionError(error.decode())
        recovered = int(http.split()[-1].split("=")[1])
        async with SpooltailClient(grpc.split()[-1].removeprefix("http://"), tls=False) as client:
            yield client, recovered, process
    finally:
        await kill(process)


@asynccontextmanager
async def broker(volume, mode, **options):
    async with running_broker(volume, mode, **options) as (client, recovered, _):
        yield client, recovered


async def wait_for_log(process, predicate):
    async with asyncio.timeout(15):
        while line := await process.stderr.readline():
            line = line.decode().strip()
            if predicate(line):
                return line
    raise AssertionError("broker stopped before emitting the expected log entry")


async def wait_for_cut(process, stage):
    assert await wait_for_log(process, lambda line: line.startswith("PACK_STOP ")) == f"PACK_STOP {stage}"


async def wait_for(predicate):
    async with asyncio.timeout(10):
        while not predicate():
            await asyncio.sleep(0.03)


def event(i):
    return {"task_id": "T1", "event_id": f"E{i}", "payload": str(i % 10) * (128 * 1024)}


async def seed(volume, mode, *, archive_first=True):
    volume.mkdir()
    status, replay = [], []
    async with broker(volume, mode) as (client, _):
        work = event(-1)
        work_offset = await client.publish("work", work)
        lease = await client.claim("work", "original", lease_ms=600_000)
        assert lease.record.offset == work_offset
        terminal = {**work, "status": "completed"}
        status.append((await client.complete(lease, "status", terminal), terminal))
        for i in range(72):
            topic, sequence = ("status", status) if i % 2 else ("replay", replay)
            item = event(i)
            sequence.append((await client.publish(topic, item), item))
    assert len(list(volume.glob("*.seg"))) == 2
    if archive_first:
        async with broker(volume, mode, paused=False):
            await wait_for(lambda: query(volume, "SELECT COUNT(*) FROM archive_records") == [(len(status),)])
    return work, lease, status, replay


async def verify_history(client, status, replay):
    inputs, through = [], None
    while len(inputs) < len(status):
        after = inputs[-1][1]["event_id"] if inputs else None
        page, through = await client.audit_history("status", "T1", after=after, through=through)
        assert page
        inputs.extend((record.offset, record.event) for record in page)
    assert inputs == status and through == status[-1][0]
    assert await client.history("status", "T1") == []

    after = replay[0][0]
    replayed = []
    while len(replayed) < len(replay) - 1:
        page = await client.read_after_batch("replay", after=after)
        assert page.records
        replayed.extend((record.offset, record.event) for record in page.records)
        after = replayed[-1][0]
    assert replayed == replay[1:]


async def verify_reads(client, work, lease, status, replay):
    await verify_history(client, status, replay)
    offset, terminal = status[0]
    assert await client.publish("status", terminal) == offset
    assert await client.complete(lease, "status", terminal) == offset
    try:
        await client.publish("status", {**terminal, "payload": "different"})
    except ConflictError:
        pass
    else:
        raise AssertionError("changed duplicate was accepted")
    claim = await client.claim("work", "restarted")
    assert claim.record.event == work and claim.record.offset == lease.record.offset
    await client.settle(claim, SettlementAction.ACK)
    assert await client.claim("work", "original") is None


async def crash(volume, template, mode, stage, expected):
    shutil.copytree(template, volume)
    process = await launch(volume, mode, packing=True, cut=stage)
    raw = volume / FIRST_RAW
    pack = volume / FIRST_PACK
    writing = volume / (FIRST_PACK + ".writing")
    try:
        await wait_for_cut(process, stage)
        committed = bool(query(volume, "SELECT 1 FROM packed_segments WHERE segment=0"))
        assert (committed, raw.exists(), pack.exists(), writing.exists()) == CUTS[stage]
    finally:
        await kill(process)
    assert process.returncode == -9
    await verify_restart(volume, mode, committed, expected)
    print(f"PASS {mode} {stage}", flush=True)


async def verify_restart(volume, mode, committed, expected):
    async with broker(volume, mode) as (client, _):
        # Opening with packing disabled isolates startup cleanup from a fresh packing run.
        assert not (volume / (FIRST_PACK + ".writing")).exists()
        assert (volume / FIRST_RAW).exists() != committed
        assert (volume / FIRST_PACK).exists() == committed
        assert (query(volume, "SELECT COUNT(*) FROM archive_records WHERE block_id=0")[0][0] > 0) == committed
        await verify_reads(client, *expected)
        item = {"task_id": "after-restart", "event_id": "after-restart"}
        offset = await client.publish("restart", item)
        assert offset > max(expected[2][-1][0], expected[3][-1][0])
        assert await client.publish("restart", item) == offset
        records = await client.history("restart", "after-restart")
        assert [(record.offset, record.event) for record in records] == [(offset, item)]
    assert query(volume, "PRAGMA integrity_check") == [("ok",)]


async def main(root):
    for mode in ("shared", "partitioned"):
        template = root / mode
        expected = await seed(template, mode)
        for stage in CUTS:
            await crash(root / (mode + "-" + stage), template, mode, stage, expected)
        async with broker(template, mode, packing=True) as (client, _):
            await wait_for(lambda: not (template / FIRST_RAW).exists())
            await verify_reads(client, *expected)
        if mode == "partitioned":
            shard = template / "partition-indexes/0.sqlite"
            for path in shard.parent.glob(shard.name + "*"):
                path.unlink()
            async with broker(template, mode) as (client, recovered):
                assert recovered == 74
                (offset, terminal), *_ = expected[2]
                assert await client.complete(expected[1], "status", terminal) == offset
                assert await client.claim("work", "original") is None
                print(f"PASS rebuilt partition index from {recovered} packed and raw frames", flush=True)

    volume = root / "pack-before-archive"
    expected = await seed(volume, "partitioned", archive_first=False)
    async with broker(volume, "partitioned", packing=True):
        await wait_for(lambda: not (volume / FIRST_RAW).exists())
        assert query(volume, "SELECT COUNT(*) FROM archive_records") == [(0,)]
    async with broker(volume, "partitioned", paused=False) as (client, _):
        await wait_for(lambda: query(volume, "SELECT COUNT(*) FROM archive_records") == [(len(expected[2]),)])
        assert query(volume, "SELECT COUNT(*) FROM archive_records WHERE block_id=0")[0][0] > 0
        await verify_reads(client, *expected)
    print("PASS archived inputs directly from a packed segment", flush=True)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="spooltail-packed-journal-") as directory:
        asyncio.run(main(Path(directory)))
