"""Power-cut a disposable Linux ARM64 VM immediately after an acknowledged journal suffix."""

import argparse
import json
import os
import selectors
import subprocess
import sys
import time
import uuid
from contextlib import closing, contextmanager
from pathlib import Path
from urllib.request import Request, urlopen


def guest(case, previous_boot=None):
    boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    assert previous_boot != boot
    mount = Path("/mnt/spooltail")
    filesystem = subprocess.check_output(
        ["findmnt", "-n", "-o", "TARGET,FSTYPE", "--mountpoint", mount], text=True
    ).split()
    assert filesystem == [str(mount), "ext4"], filesystem
    if previous_boot is None:
        # Establish the empty-volume boundary before creating any broker records.
        os.sync()
    process = subprocess.Popen(
        ["./spooltail"],
        env=dict(
            os.environ,
            SPOOLTAIL_DATA=str(mount / "data" / case),
            SPOOLTAIL_LISTEN="127.0.0.1:0",
        ),
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        banner = process.stdout.readline().split()
        assert banner[0] == "Listening", banner

        def request(path, body=None, method=None):
            data = None if body is None else json.dumps(body).encode()
            req = Request(
                f"http://{banner[1]}{path}",
                data=data,
                method=method,
                headers={"Content-Type": "application/json"},
            )
            with urlopen(req, timeout=10) as response:
                return json.load(response)

        stream = "/topics/status/streams/audit"
        work = "/topics/work/work/workers"
        queued = {"task_id": "work", "event_id": "queued"}
        completed = {"task_id": "work", "event_id": "done", "status": "completed"}
        terminal = {
            "offset": 1,
            "generation": 1,
            "status_topic": "status",
            "event": completed,
        }
        updates = [
            {"task_id": "watched", "event_id": str(i), "payload": [i, "東京"]}
            for i in range(96)
        ]
        if previous_boot is None:
            assert request("/topics/work/records", queued)["offset"] == 1
            claim = request(work + "/claim", {"lease_ms": 30000})
            assert claim["generation"] == terminal["generation"]
            assert request(work + "/complete", terminal)["offset"] == 2
            request(stream + "/commit", {"offset": 2})
            for i, event in enumerate(updates):
                if case == "expanded" and i == 48:
                    request("/partitions/1", method="PUT")
                    request(
                        "/tasks/watched/move", {"from_partition": 0, "to_partition": 1}
                    )
                assert request("/topics/status/records", event)["offset"] == i + 3
            print(f"ARMED {boot}", flush=True)
            time.sleep(120)
            raise RuntimeError("VM power cut did not occur")

        route = "/topics/status/tasks/watched/history"
        history = request(route) + request(route + "?after=63")
        assert [r["event"] for r in history] == updates
        assert [r["offset"] for r in history] == list(range(3, 99))
        assert [r.get("partition", 0) for r in history] == [
            int(case == "expanded" and i >= 48) for i in range(96)
        ]
        assert request(stream + "/next")["offset"] == 3
        assert request(work + "/claim", {"lease_ms": 30000}) is None
        assert request(work + "/complete", terminal)["offset"] == 2
        for i, event in enumerate(updates):
            assert request("/topics/status/records", event)["offset"] == i + 3
        print(
            f"PASS {case}: 96 receipts, payloads, offset order, cursor, completion, boot change; {banner[2]}"
        )
    finally:
        process.kill()
        process.wait()
        process.stdout.close()


def lab(binary):
    prefix = "spooltail-live-" + uuid.uuid4().hex[:12]
    disk = prefix + "-data"

    def lima(*args, **kwargs):
        return subprocess.run(["limactl", *args], check=True, timeout=180, **kwargs)

    def home_command(vm, *args):
        return [
            "shell",
            vm,
            "--",
            "sh",
            "-c",
            'cd "$HOME" && exec "$@"',
            "probe",
            *args,
        ]

    def probe(vm, *args):
        return ["limactl", *home_command(vm, "python3", "probe.py", "--guest", *args)]

    @contextmanager
    def guest_with_volume(suffix, *, format_disk=False):
        vm = f"{prefix}-{suffix}"
        attachment = {
            "name": disk,
            "format": format_disk,
            "fsType": "ext4",
            "mountPoint": "/mnt/spooltail",
        }
        try:
            lima(
                "start",
                "-y",
                f"--name={vm}",
                "--vm-type=vz",
                "--cpus=2",
                "--memory=2",
                "--disk=8",
                "--mount-none",
                "--containerd=none",
                "--set",
                ".additionalDisks = " + json.dumps([attachment]),
                "template:ubuntu-24.04",
            )
            lima("copy", str(binary), vm + ":spooltail")
            lima("copy", __file__, vm + ":probe.py")
            lima(*home_command(vm, "chmod", "+x", "spooltail"))
            if format_disk:
                lima(
                    "shell",
                    vm,
                    "--",
                    "sh",
                    "-c",
                    'sudo install -d -o "$(id -u)" -g "$(id -g)" /mnt/spooltail/data',
                )
            yield vm
        finally:
            lima("delete", "--force", vm)

    def cut(vm, case):
        with subprocess.Popen(
            probe(vm, case), stdout=subprocess.PIPE, text=True
        ) as arm:
            try:
                with closing(selectors.DefaultSelector()) as poll:
                    poll.register(arm.stdout, selectors.EVENT_READ)
                    if not poll.select(45):
                        raise RuntimeError("timed out waiting for receipts")
                armed = arm.stdout.readline().split()
                assert len(armed) == 2 and armed[0] == "ARMED", armed
                stopped = lima("stop", "--force", vm, capture_output=True, text=True)
                assert "SIGKILL to the vz driver process" in stopped.stderr, (
                    stopped.stderr
                )
                assert arm.wait(timeout=20) != 0
                return armed[1]
            finally:
                if arm.poll() is None:
                    arm.kill()

    lima("disk", "create", disk, "--size", "1GiB", "--format", "raw")
    try:
        with guest_with_volume("a", format_disk=True) as vm:
            first_boot = cut(vm, "single")
        with guest_with_volume("b") as vm:
            subprocess.run(probe(vm, "single", first_boot), check=True, timeout=30)
            second_boot = cut(vm, "expanded")
        with guest_with_volume("c") as vm:
            subprocess.run(probe(vm, "expanded", second_boot), check=True, timeout=30)
    finally:
        lima("disk", "delete", disk)


if __name__ == "__main__":
    if sys.argv[1:2] == ["--guest"]:
        guest(*sys.argv[2:])
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument(
            "--broker", required=True, type=Path, help="Linux ARM64 release binary"
        )
        lab(parser.parse_args().broker.resolve(strict=True))
