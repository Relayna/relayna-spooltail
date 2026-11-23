"""Power off a disposable Lima VZ guest at each packed-journal cut point."""

import argparse
import asyncio
import json
import os
import platform
import selectors
import shutil
import subprocess
import sys
import tarfile
import tempfile
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

from packed_journal_acceptance import (
    CUTS,
    FIRST_PACK,
    FIRST_RAW,
    kill,
    launch,
    query,
    seed,
    verify_restart,
    wait_for_cut,
)

ROOT = Path(__file__).resolve().parents[1]
GUEST_ROOT = Path(__file__).resolve().parent
BOOT_ID = Path("/proc/sys/kernel/random/boot_id")


async def guest_seed():
    filesystem = command("findmnt", "-n", "-o", "FSTYPE", "-T", GUEST_ROOT)
    assert filesystem.stdout.strip() == "ext4"
    for mode in ("shared", "partitioned"):
        work, lease, status, replay = await seed(GUEST_ROOT / f"{mode}-template", mode)
        (GUEST_ROOT / f"{mode}.json").write_text(json.dumps([work, asdict(lease), status, replay]))
    os.sync()


def fixture(mode):
    from relayna_spooltail import Lease, Record

    work, lease, status, replay = json.loads((GUEST_ROOT / f"{mode}.json").read_text())
    lease["record"] = Record(**lease["record"])
    return work, Lease(**lease), list(map(tuple, status)), list(map(tuple, replay))


def case_volume(mode, cut):
    assert mode in ("shared", "partitioned") and cut in CUTS
    return GUEST_ROOT / f"{mode}-{cut}"


async def guest_arm(mode, cut):
    volume = case_volume(mode, cut)
    shutil.copytree(GUEST_ROOT / f"{mode}-template", volume)
    os.sync()  # Establish the baseline before exposing the switch to power loss.
    process = await launch(volume, mode, packing=True, cut=cut)
    try:
        await wait_for_cut(process, cut)
        print("ARMED " + BOOT_ID.read_text().strip(), flush=True)
        await process.wait()
        raise AssertionError("broker exited before the VM power cut")
    finally:
        await kill(process)


async def guest_verify(mode, cut, previous_boot):
    assert BOOT_ID.read_text().strip() != previous_boot
    volume = case_volume(mode, cut)
    committed = CUTS[cut][0]
    # Before cleanup, either unsynced directory operation can have rolled back.
    before = {
        "raw": (volume / FIRST_RAW).exists(),
        "pack": (volume / FIRST_PACK).exists(),
        "temporary": (volume / (FIRST_PACK + ".writing")).exists(),
    }
    assert before["pack" if committed else "raw"], before
    assert query(volume, "SELECT partition,segment FROM packed_segments") == ([(0, 0)] if committed else [])
    await verify_restart(volume, mode, committed, fixture(mode))
    print(f"PASS VM power cut {mode} {cut} (files before recovery {before})", flush=True)


def command(*args, check=True, timeout=90):
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"{args[0]} failed with {result.returncode}\n{result.stdout}{result.stderr}")
    return result


def guest(vm, *args):
    script = (
        'cd "$HOME/spooltail-power-test" && '
        'PYTHONPATH=deps:sdk SPOOLTAIL_PACK_TEST_BROKER="$PWD/spooltail" '
        'exec python3 packed_journal_power_cut.py --guest "$@"'
    )
    return ("limactl", "shell", vm, "--", "bash", "-c", script, "guest", *args)


def bundle(lab, binary):
    files = lab / "files"
    files.mkdir()
    req = lab / "requirements.txt"
    req.write_text(command(
        "uv", "export", "--locked", "--project", ROOT / "python",
        "--no-dev", "--no-emit-project", "--format", "requirements-txt",
    ).stdout)
    command(
        "uv", "pip", "install", "--target", files / "deps", "--python-version", "3.12",
        "--python-platform", "aarch64-manylinux_2_28", "--only-binary", ":all:",
        "--require-hashes", "-r", req,
    )
    shutil.copytree(ROOT / "python/src/relayna_spooltail", files / "sdk/relayna_spooltail")
    shutil.copyfile(binary, files / "spooltail")
    (files / "spooltail").chmod(0o755)
    for name in ("packed_journal_power_cut.py", "packed_journal_acceptance.py"):
        shutil.copyfile(ROOT / "tests" / name, files / name)
    archive = lab / "bundle.tar"
    with tarfile.open(archive, "w") as output:
        for path in files.iterdir():
            output.add(path, arcname=path.name)
    return archive


def power_cut(vm, mode, cut):
    with tempfile.TemporaryFile() as log:
        process = subprocess.Popen(guest(vm, "arm", mode, cut), stdout=subprocess.PIPE, stderr=log)
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                if not selector.select(timeout=30):
                    raise RuntimeError("timed out waiting for the guest to arm the power cut")
                armed = process.stdout.readline().decode().split()
            if not armed or armed[0] != "ARMED":
                log.seek(0)
                raise RuntimeError(f"guest did not arm the power cut: {armed}\n{log.read().decode()}")
            stopped = command("limactl", "stop", "--force", vm, timeout=30)
            assert "SIGKILL to the vz driver process" in stopped.stderr, stopped.stderr
            assert process.wait(timeout=15) != 0
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            process.stdout.close()
    command("limactl", "start", "-y", "--timeout=60s", vm)
    verified = command(*guest(vm, "verify", mode, cut, armed[1]), timeout=30)
    assert verified.stdout.startswith("PASS VM power cut "), verified.stdout
    print(verified.stdout.strip(), flush=True)


def run_lab(binary):
    if platform.system() != "Darwin" or platform.machine() != "arm64":
        raise RuntimeError("the VM power-cut lab requires macOS on Apple Silicon")
    if not binary.is_file():
        raise ValueError("provide a Linux ARM64 broker built with pack-fault-injection")
    with tempfile.TemporaryDirectory(prefix="spooltail-power-") as directory:
        archive = bundle(Path(directory), binary)
        # Never use a caller's VM. No host directories are mounted in this guest.
        vm = "spooltail-power-" + uuid4().hex[:12]
        try:
            command(
                "limactl", "start", "-y", f"--name={vm}", "--vm-type=vz", "--cpus=3",
                "--memory=3", "--disk=8", "--mount-none", "--containerd=none",
                "--timeout=3m", "template:ubuntu-24.04", timeout=190,
            )
            command("limactl", "copy", archive, f"{vm}:/tmp/spooltail-power.tar")
            command("limactl", "shell", vm, "--", "bash", "-c",
                    'mkdir "$HOME/spooltail-power-test" && tar -xf /tmp/spooltail-power.tar -C "$HOME/spooltail-power-test"')
            command(*guest(vm, "seed"))
            for mode in ("shared", "partitioned"):
                for cut in CUTS:
                    power_cut(vm, mode, cut)
        finally:
            deleted = command("limactl", "delete", "--force", vm, check=False, timeout=45)
            if deleted.returncode:
                print(f"Could not remove disposable VM {vm}\n{deleted.stderr}", file=sys.stderr)


if __name__ == "__main__":
    if sys.argv[1:2] == ["--guest"]:
        operation, *args = sys.argv[2:]
        asyncio.run({"seed": guest_seed, "arm": guest_arm, "verify": guest_verify}[operation](*args))
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--broker", type=Path, required=True, help="Linux ARM64 broker with fault injection")
        run_lab(parser.parse_args().broker.resolve())
