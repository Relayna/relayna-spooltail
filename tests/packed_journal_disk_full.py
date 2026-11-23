"""Exercise real disk-full errors on a disposable ext4 or APFS image."""

import asyncio
import errno
import os
import platform
import shutil
import sqlite3
import subprocess
import tempfile
from contextlib import closing, contextmanager
from pathlib import Path

import grpc

from packed_journal_acceptance import (
    FIRST_PACK,
    FIRST_RAW,
    broker,
    kill,
    query,
    running_broker,
    seed,
    verify_history,
    verify_reads,
    wait_for,
    wait_for_cut,
    wait_for_log,
)


def run(*args):
    subprocess.run(args, check=True, timeout=30)


@contextmanager
def disposable_filesystem(root):
    # Never accept a caller-supplied mount to fill. Create and own this image.
    volume = root / "mount"
    volume.mkdir()
    system = platform.system()
    if system == "Linux":
        image = root / "pressure.ext4"
        with image.open("xb") as file:
            file.truncate(96 * 1024**2)
        run("mkfs.ext4", "-F", "-q", "-m", "0", image)
        elevate = () if os.geteuid() == 0 else ("sudo", "-n")
        run(*elevate, "mount", "-o", "loop", image, volume)
        unmount = (*elevate, "umount", volume)
    elif system == "Darwin":
        image = root / "pressure.dmg"
        run("hdiutil", "create", "-size", "128m", "-fs", "APFS", "-volname", "SpooltailTest", image)
        run("hdiutil", "attach", "-nobrowse", "-mountpoint", volume, image)
        unmount = ("hdiutil", "detach", volume)
    else:
        raise RuntimeError("disk-full tests require Linux or macOS")
    try:
        assert os.path.ismount(volume) and root.stat().st_dev != volume.stat().st_dev
        if system == "Linux":
            run(*elevate, "chown", f"{os.getuid()}:{os.getgid()}", volume)
        yield volume
    finally:
        run(*unmount)


def fill(path, mount):
    assert path.parent.stat().st_dev == mount.stat().st_dev
    chunk = os.urandom(1024**2)
    with path.open("xb", buffering=0) as file:
        for size in (len(chunk), 65536, 4096):
            while True:
                try:
                    file.write(chunk[:size])
                except OSError as error:
                    if error.errno != errno.ENOSPC:
                        raise
                    break
        try:
            os.fsync(file.fileno())
        except OSError as error:
            if error.errno != errno.ENOSPC:
                raise
    if platform.system() == "Linux":
        assert os.statvfs(mount).f_bavail == 0


def published(volume):
    return query(volume, "SELECT partition,segment FROM packed_segments") == [(0, 0)]


async def pack_fails(mount, control, template, mode, expected, cut):
    volume = mount / cut
    shutil.copytree(template, volume)
    reserve = volume / "reserve"
    resume = control / "resume"
    raw = volume / FIRST_RAW
    packed = volume / FIRST_PACK
    async with running_broker(volume, mode, packing=True, cut=cut, resume=resume) as (client, _, process):
        await wait_for_cut(process, cut)
        if cut == "pack_named":
            # With no spare WAL pages, publishing the manifest must allocate on disk.
            with closing(sqlite3.connect(volume / "index.sqlite")) as db:
                assert db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone() == (0, 0, 0)
            assert (volume / "index.sqlite-wal").stat().st_size == 0
        fill(reserve, mount)
        try:
            resume.touch()
            error = await wait_for_log(process, lambda line: line.startswith("packing failed:"))
            if cut == "pack_named":
                assert "database or disk is full" in error, error
            else:
                assert "(os error 28)" in error, error
            assert raw.exists() and not published(volume)
            assert packed.exists() == (cut == "pack_named")
            assert not (volume / (FIRST_PACK + ".writing")).exists()
            assert query(volume, "SELECT COUNT(*) FROM archive_records WHERE block_id=0") == [(0,)]
            await verify_history(client, expected[2], expected[3])
        finally:
            reserve.unlink()
        await wait_for(lambda: published(volume) and not raw.exists())
        await verify_reads(client, *expected)
    async with broker(volume, mode) as (client, _):
        await verify_history(client, expected[2], expected[3])
    assert query(volume, "PRAGMA integrity_check") == [("ok",)]
    resume.unlink()
    shutil.rmtree(volume)
    print(f"PASS disk full {mode} {cut} and same-process packing retry", flush=True)


async def append_fails(mount, template, mode, expected, *, torn=False):
    volume = mount / "append"
    shutil.copytree(template, volume)
    reserve = volume / "reserve"
    active = sorted(volume.glob("*.seg"))[-1]
    size = active.stat().st_size
    item = {"task_id": "full", "event_id": "full", "payload": "x" * (256 * 1024)}
    async with running_broker(volume, mode) as (client, _, process):
        fill(reserve, mount)
        try:
            if torn:
                # Leave enough ext4 blocks for a frame prefix, but nowhere near the full append.
                with reserve.open("r+b", buffering=0) as file:
                    file.truncate(reserve.stat().st_size - 96 * 1024)
                    os.fsync(file.fileno())
            try:
                await client.publish("foreground", item)
            except grpc.aio.AioRpcError as error:
                assert error.code() == grpc.StatusCode.INTERNAL
            else:
                raise AssertionError("the broker acknowledged an append on a full disk")
            await wait_for_log(process, lambda line: "os error 28" in line or "disk is full" in line)
            written = active.stat().st_size - size
            if torn:
                assert 8 < written < len(item["payload"]), written
            await kill(process)
        finally:
            reserve.unlink()
    async with broker(volume, mode) as (client, _):
        await verify_reads(client, *expected)
        recovered = await client.latest("foreground", "full")
        if written < len(item["payload"]):
            assert active.stat().st_size == size and recovered is None
        offset = await client.publish("foreground", item)
        if recovered is not None:
            assert (recovered.offset, recovered.event) == (offset, item)
        assert await client.publish("foreground", item) == offset
        records = await client.history("foreground", "full")
        assert [(record.offset, record.event) for record in records] == [(offset, item)]
    assert query(volume, "PRAGMA integrity_check") == [("ok",)]
    shutil.rmtree(volume)
    print(f"PASS full disk {mode} foreground append and retry (discarded {written} bytes)", flush=True)


async def main(mount, control):
    for mode in ("shared", "partitioned"):
        template = mount / "template"
        expected = await seed(template, mode)
        for cut in ("pack_opened", "pack_named"):
            await pack_fails(mount, control, template, mode, expected, cut)
        await append_fails(mount, template, mode, expected)
        if platform.system() == "Linux":
            await append_fails(mount, template, mode, expected, torn=True)
        shutil.rmtree(template)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="spooltail-disk-full-") as directory:
        control = Path(directory).resolve()
        with disposable_filesystem(control) as mount:
            asyncio.run(main(mount, control))
