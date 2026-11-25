"""Exercise retirement commits, interrupted unlinks, and missing partition indexes."""

import asyncio
import shutil
import tempfile
from dataclasses import replace
from pathlib import Path

import grpc

from packed_journal_acceptance import (
    FIRST_PACK, FIRST_RAW, broker, event, kill, launch, query,
    wait_for, wait_for_cut,
)
from relayna_spooltail import ConflictError, CursorGapError

# Include signed floating zero so receipts preserve the original JSON equality rule.
ITEMS = [{**event(i), "zero": -0.0} for i in range(72)]


async def seed(volume, mode, *, mixed=False):
    volume.mkdir()
    async with broker(volume, mode) as (client, _):
        if mixed:
            await client.publish("work", event(-1))
        offsets = [await client.publish("status", item) for item in ITEMS]
    async with broker(volume, mode, paused=False, packing=True, audit_only=["status"]) as (client, _):
        await wait_for(lambda: not (volume / FIRST_RAW).exists())
        await wait_for(lambda: query(volume, "SELECT COUNT(*) FROM archive_records") == [(len(ITEMS),)])
        page, through = await client.audit_history("status", "T1")
        assert page[0].event == ITEMS[0] and through == offsets[-1]
        assert query(volume, "SELECT COUNT(*) FROM retired_records") == [(0,)]
    return offsets


async def verify(volume, mode, offsets):
    async with broker(volume, mode) as (client, _):
        assert not (volume / FIRST_PACK).exists()
        assert (await client.stats()).retired_segments == 1
        retired_tail = query(volume, "SELECT MAX(offset) FROM retired_records")[0][0]
        page, _ = await client.audit_history("status", "T1")
        assert retired_tail >= offsets[0] and all(r.offset > retired_tail for r in page)
        try:
            await client.audit_history("status", "T1", after=ITEMS[0]["event_id"])
        except CursorGapError:
            pass
        else:
            raise AssertionError("expired audit cursor did not report a gap")
        try:
            await client.claim("status", "work")
        except grpc.aio.AioRpcError as error:
            assert error.code() == grpc.StatusCode.INVALID_ARGUMENT
        else:
            raise AssertionError("audit-only topic still accepts work claims")
        for item in ({**ITEMS[0], "zero": 0.0}, ITEMS[0]):
            assert await client.publish("status", item) == offsets[0]
        try:
            await client.publish("status", {**ITEMS[0], "zero": 1.0})
        except ConflictError:
            pass
        else:
            raise AssertionError("retired receipt accepted a conflicting retry")
        assert await client.publish("status", event(1000)) > offsets[-1]
    assert query(volume, "PRAGMA integrity_check") == [("ok",)]


async def completion_receipt(volume):
    volume.mkdir()
    async with broker(volume, "partitioned") as (client, _):
        await client.publish("work", event(-1))
        lease = await client.claim("work", "completion", lease_ms=600_000)
        await client.ensure_partition(1)
        await client.move_task("T1", 0, 1)
        terminal = {**event(-1), "status": "completed"}
        offset = await client.complete(lease, "status", terminal)
        for item in ITEMS:
            await client.publish("status", item)
    async with broker(volume, "partitioned", paused=False, retain_for="1s",
                      packing=True, audit_only=["status"]) as (client, _):
        packed_file = volume / "partitions/1" / FIRST_PACK
        await wait_for(lambda: query(
            volume, "SELECT raw_len FROM packed_segments WHERE partition=1 AND segment=0"
        ) == [(0,)] and not packed_file.exists())
        assert await client.complete(lease, "status", terminal) == offset
        try:
            await client.complete(replace(lease, generation=lease.generation + 1), "status", terminal)
        except ConflictError:
            pass
        else:
            raise AssertionError("retired completion receipt accepted a different lease generation")
        assert await client.claim("work", "completion") is None
    print("PASS retired completion receipt retains its lease and offset after a partition move", flush=True)


async def main(root):
    for mode in ("shared", "partitioned"):
        volume = root / mode
        offsets = await seed(volume, mode)
        for cut in ("retirement_committed", "retired_unlinked"):
            attempt = root / f"{mode}-{cut}"
            shutil.copytree(volume, attempt)
            process = await launch(attempt, mode, paused=False, retain_for="1s", cut=cut)
            try:
                await wait_for_cut(process, cut)
                assert query(attempt, "SELECT raw_len FROM packed_segments") == [(0,)]
                assert (attempt / FIRST_PACK).exists() == (cut == "retirement_committed")
            finally:
                await kill(process)
            await verify(attempt, mode, offsets)
            if mode == "partitioned":
                for path in (attempt / "partition-indexes").glob("0.sqlite*"):
                    path.unlink()
                await verify(attempt, mode, offsets)
            print(f"PASS {mode} {cut} and retry after restart", flush=True)
    mixed = root / "mixed"
    await seed(mixed, "shared", mixed=True)
    async with broker(mixed, "shared", paused=False, retain_for="1s") as (client, _):
        await wait_for(lambda: query(mixed, "SELECT COUNT(*) FROM archive_records") == [(0,)])
        assert await client.claim("work", "workers") is not None
        assert (mixed / FIRST_PACK).exists()
        assert (await client.stats()).retired_segments == 0
    print("PASS packed segment containing work remains readable after audit expiry", flush=True)
    await completion_receipt(root / "completion")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="spooltail-retirement-") as directory:
        asyncio.run(main(Path(directory)))
