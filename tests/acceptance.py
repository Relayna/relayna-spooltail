# /// script
# requires-python = ">=3.11"
# dependencies = ["websockets==15.0.1"]
# ///
"""Black-box acceptance tests for a real broker, filesystem and network clients."""

import argparse
import concurrent.futures
import json
import os
import sqlite3
import struct
import subprocess
import tempfile
import time
import zlib
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from websockets.sync.client import connect


class Broker:
    def __init__(self, binary, directory):
        self.binary = binary
        self.directory = directory
        self.process = None
        self.env = dict(
            os.environ, SPOOLTAIL_DATA=str(directory), SPOOLTAIL_LISTEN="127.0.0.1:0"
        )

    def start(self):
        self.process = subprocess.Popen(
            [self.binary], stdout=subprocess.PIPE, text=True, env=self.env
        )
        line = self.process.stdout.readline().split()
        assert len(line) == 3 and line[0] == "Listening", line
        self.address = line[1]
        return int(line[2].split("=")[1])

    def kill(self):
        if self.process is not None:
            self.process.kill()
            self.process.wait(timeout=10)
            self.process.stdout.close()
            self.process = None

    def request(self, route, body=None):
        data = None if body is None else json.dumps(body).encode()
        req = Request(
            f"http://{self.address}{route}",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        with urlopen(req, timeout=10) as res:
            return json.load(res)

    def sse(self, task, after=None):
        headers = {"Last-Event-ID": after} if after else {}
        req = Request(
            f"http://{self.address}/topics/status/tasks/{task}/events", headers=headers
        )
        return urlopen(req, timeout=10)

    def ws(self):
        return connect(
            f"ws://{self.address}/ws", open_timeout=5, close_timeout=1, proxy=None
        )

    def publish(self, task, eid, topic="status", **fields):
        return self.request(
            f"/topics/{topic}/records", {"task_id": task, "event_id": eid, **fields}
        )["offset"]


def sse_event(response):
    lines = {}
    while True:
        line = response.readline()
        assert line, "premature end of SSE response"
        line = line.decode().rstrip("\r\n")
        if not line and "event" in lines:
            return lines["event"], lines.get("id"), json.loads(lines["data"])
        if line and not line.startswith(":"):
            key, value = line.split(":", 1)
            lines[key] = value.lstrip(" ")


def subscribe(ws, task, after=None):
    ws.send(
        json.dumps(
            {"op": "subscribe", "topic": "status", "task_id": task, "after": after}
        )
    )


def ws_event(ws):
    return json.loads(ws.recv(timeout=8))


def fixture(directory, count):
    """Write published-format frames offline into a new temporary volume."""
    assert not list(directory.iterdir())
    segment = -1
    end = 0
    f = None
    try:
        for i in range(count):
            task, eid = (
                ("T7", "E41") if i == count // 2 else (f"other-{i % 16384}", f"F{i}")
            )
            event = {
                "topic": "status",
                "event": {"task_id": task, "event_id": eid, "status": "running"},
            }
            body = json.dumps(event, separators=(",", ":")).encode()
            frame = struct.pack("<II", len(body), zlib.crc32(body)) + body
            if f is None or end + len(frame) > 8 * 1024 * 1024:
                if f:
                    f.close()
                segment += 1
                f = (directory / f"{segment:020}.seg").open("xb")
                end = 0
            f.write(frame)
            end += len(frame)
    finally:
        if f:
            f.close()


def expect_http(code, fn):
    try:
        fn()
    except HTTPError as exc:
        try:
            assert exc.code == code, (exc.code, exc.read())
        finally:
            exc.close()
    else:
        raise AssertionError(f"expected HTTP {code}")


def cross_protocol(broker, prefix, offline):
    assert broker.start() == prefix
    with broker.sse("T7") as sse:
        assert sse_event(sse)[0] == "ready"
        _, eid, first = sse_event(sse)
        assert eid == first["event_id"] == "E41"

    for i in range(offline - 1):
        broker.publish("unrelated-offline", f"D{i}", status="running")
    offset = broker.publish("T7", "E42", status="running")
    assert offset == prefix + offline
    broker.kill()
    # A fully indexed prefix must not be decoded again during restart.
    assert broker.start() == 0
    started = time.monotonic()
    with broker.ws() as ws:
        subscribe(ws, "T7", "E41")
        assert ws_event(ws)["type"] == "ready"
        second = ws_event(ws)
        assert second["data"] == {
            "task_id": "T7",
            "event_id": "E42",
            "status": "running",
        }
        first_ms = (time.monotonic() - started) * 1000
        assert broker.request("/stats")["payload_reads"] == 1, (
            "rare-task seek decoded unrelated payloads"
        )
        broker.publish("other", "noise")
        broker.publish("T7", "E43")
        third = ws_event(ws)
        assert third["type"] == "status" and third["data"]["event_id"] == "E43"

    # A second transport switch, in the reverse direction, across another hard restart.
    broker.publish("T7", "E44", status="running")
    broker.kill()
    assert broker.start() == 0
    with broker.sse("T7", "E43") as sse:
        assert sse_event(sse)[0] == "ready"
        assert sse_event(sse)[1] == "E44"
        broker.publish("T7", "E45", status="completed")
        assert sse_event(sse)[1] == "E45"
        assert not sse.readline()
    assert (
        broker.request("/topics/status/tasks/T7/latest")["event"]["event_id"] == "E45"
    )
    assert broker.publish("T7", "E42", status="running") == offset
    expect_http(409, lambda: broker.publish("T7", "E42", status="changed"))
    return offset, first_ms


def subscriptions(broker):
    topic = "/topics/inference.tasks"
    work = topic + "/work/workers"
    audit = topic + "/streams/audit"
    metrics = topic + "/streams/metrics"
    first = broker.publish("X", "enqueue-X", topic="inference.tasks")
    second = broker.publish("Y", "enqueue-Y", topic="inference.tasks")
    a = broker.request(work + "/claim", {"lease_ms": 150})
    b = broker.request(work + "/claim", {"lease_ms": 30_000})
    assert (a["record"]["offset"], b["record"]["offset"]) == (first, second)
    assert broker.request(audit + "/next")["offset"] == first
    expect_http(409, lambda: broker.request(audit + "/commit", {"offset": second}))
    broker.request(audit + "/commit", {"offset": first})
    broker.kill()
    broker.start()
    time.sleep(0.17)
    new = broker.request(work + "/claim", {"lease_ms": 30_000})
    assert new["record"]["offset"] == first and new["generation"] > a["generation"]

    def settle(claim, action, delay=0):
        return broker.request(
            work + "/settle",
            {
                "offset": claim["record"]["offset"],
                "generation": claim["generation"],
                "action": action,
                "delay_ms": delay,
            },
        )

    expect_http(409, lambda: settle(a, "ack"))
    settle(new, "retry", 0)
    expect_http(409, lambda: settle(new, "ack"))
    retry = broker.request(work + "/claim", {"lease_ms": 30_000})
    settle(retry, "renew", 30_000)
    settle(retry, "ack")
    settle(b, "dead-letter")
    assert broker.request(work + "/claim", {"lease_ms": 30_000}) is None
    assert broker.request(audit + "/next")["offset"] == second
    assert broker.request(metrics + "/next")["offset"] == first


def kill_after_journal_write(broker, operation):
    """Kill after the complete frame reaches the journal and before SQLite commits."""
    journal = max(broker.directory.glob("*.seg"))
    before = journal.stat().st_size
    with (
        sqlite3.connect(broker.directory / "index.sqlite") as lock,
        concurrent.futures.ThreadPoolExecutor() as pool,
    ):
        lock.execute("BEGIN IMMEDIATE")
        pending = pool.submit(operation)
        deadline = time.monotonic() + 3
        while True:
            newest = max(broker.directory.glob("*.seg"))
            if (
                newest != journal and newest.stat().st_size > 8
            ) or newest.stat().st_size > before + 8:
                break
            assert time.monotonic() < deadline, "append failed to reach journal"
            time.sleep(0.002)
        # The index is still locked. Confirm the complete frame and flush before killing the process.
        with newest.open("rb+") as f:
            f.seek(before if newest == journal else 0)
            size, checksum = struct.unpack("<II", f.read(8))
            while True:
                f.seek((before if newest == journal else 0) + 8)
                body = f.read(size)
                if len(body) == size:
                    break
                assert time.monotonic() < deadline
            assert zlib.crc32(body) == checksum
            os.fsync(f.fileno())
        broker.kill()
        lock.rollback()
        assert pending.exception(timeout=5) is not None


def crash_after_write(broker):
    kill_after_journal_write(
        broker, lambda: broker.publish("crash-task", "unique", status="running")
    )
    assert broker.start() == 1
    off = broker.publish("crash-task", "unique", status="running")
    history = broker.request("/topics/status/tasks/crash-task/history")
    assert [r["offset"] for r in history] == [off]


def atomic_completion(broker):
    topic = "completion.tasks"
    work = f"/topics/{topic}/work/workers"
    task = "atomic-live"
    offset = broker.publish(task, "queued", topic)
    claim = broker.request(work + "/claim", {"lease_ms": 30_000})
    assert claim["record"]["offset"] == offset
    event = {"task_id": task, "event_id": "finished", "status": "completed"}
    body = {
        "offset": offset,
        "generation": claim["generation"],
        "status_topic": "status",
        "event": event,
    }

    def complete(**changes):
        return broker.request(work + "/complete", body | changes)

    expect_http(409, lambda: complete(generation=claim["generation"] + 1))
    expect_http(409, lambda: complete(event=event | {"task_id": "different"}))
    expect_http(400, lambda: complete(event=event | {"status": "running"}))
    assert broker.request(f"/topics/status/tasks/{task}/history") == []
    # Even identical public content cannot turn a plain publish into a completion receipt.
    collision = event | {"event_id": "published"}
    broker.publish(task, "published", status="completed")
    expect_http(409, lambda: complete(event=collision))
    with broker.ws() as ws, broker.sse(task, "published") as sse:
        subscribe(ws, task, "published")
        assert ws_event(ws)["type"] == "ready" and sse_event(sse)[0] == "ready"
        with concurrent.futures.ThreadPoolExecutor() as pool:
            retries = [pool.submit(complete) for _ in range(2)]
            receipt = retries[0].result()
            assert receipt == retries[1].result()
        assert sse_event(sse) == ("status", "finished", event)
        assert not sse.readline()
        assert ws_event(ws)["data"] == event
    assert broker.request(work + "/claim", {"lease_ms": 30_000}) is None
    assert broker.request(f"/topics/status/tasks/{task}/history?after=published") == [
        {"offset": receipt["offset"], "event": event}
    ]
    expect_http(409, lambda: complete(event=event | {"status": "failed"}))
    expect_http(409, lambda: complete(event=event | {"event_id": "second-id"}))
    expect_http(409, lambda: complete(status_topic="different.status"))
    other = f"/topics/{topic}/work/other"
    assert (
        broker.request(other + "/claim", {"lease_ms": 30_000})["record"]["offset"]
        == offset
    )
    expect_http(409, lambda: broker.request(other + "/complete", body))
    assert broker.request(f"/topics/{topic}/streams/audit/next")["offset"] == offset


def crash_during_completion(broker):
    task = "atomic-crash"
    topic = "crash.tasks"
    work = f"/topics/{topic}/work/workers"
    offset = broker.publish(task, "queued", topic)
    first = broker.request(work + "/claim", {"lease_ms": 1})
    time.sleep(0.01)
    event = {"task_id": task, "event_id": "finished", "status": "failed"}
    body = {
        "offset": offset,
        "generation": first["generation"],
        "status_topic": "status",
        "event": event,
    }
    expect_http(409, lambda: broker.request(work + "/complete", body))
    claim = broker.request(work + "/claim", {"lease_ms": 1_500})
    body["generation"] = claim["generation"]
    kill_after_journal_write(broker, lambda: broker.request(work + "/complete", body))
    # A completion admitted by a valid lease survives recovery after its deadline.
    time.sleep(max(0, claim["lease_until_ms"] / 1000 - time.time()) + 0.01)
    assert broker.start() == 1
    history = broker.request(f"/topics/status/tasks/{task}/history")
    assert len(history) == 1 and history[0]["event"] == event
    assert broker.request(work + "/claim", {"lease_ms": 30_000}) is None
    receipt = broker.request(work + "/complete", body)
    assert receipt == {"offset": history[0]["offset"]}
    expect_http(
        409,
        lambda: broker.request(
            work + "/complete", body | {"generation": first["generation"]}
        ),
    )
    broker.kill()
    assert broker.start() == 0
    assert broker.request(work + "/complete", body) == receipt
    assert broker.request(f"/topics/status/tasks/{task}/history") == history


def expired(broker, floor):
    broker.kill()
    subprocess.run(
        [broker.binary, "expire-before", "status", str(floor)],
        env=broker.env,
        check=True,
    )
    broker.start()
    with broker.ws() as ws, broker.sse("T7", "E41") as sse:
        subscribe(ws, "T7", "E41")
        gap = ws_event(ws)
        assert gap["type"] == "gap" and gap["data"]["earliest_event_id"] == "E42"
        kind, _, data = sse_event(sse)
        assert kind == "gap" and gap["data"] == data


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, default=Path("target/release/spooltail"))
    parser.add_argument("--events", type=int, default=1000)
    args = parser.parse_args()
    if args.events < 2:
        parser.error("--events must be at least 2")
    with tempfile.TemporaryDirectory() as path:
        directory = Path(path)
        offline = min(1000, max(1, args.events // 800))
        prefix = args.events - offline
        fixture(directory, prefix)
        broker = Broker(args.binary.resolve(), directory)
        try:
            floor, latency = cross_protocol(broker, prefix, offline)
            subscriptions(broker)
            crash_after_write(broker)
            atomic_completion(broker)
            crash_during_completion(broker)
            expired(broker, floor)
            print(
                f"PASS: {args.events:,} fixture events; sample WebSocket resume {latency:.2f} ms; "
                "both transport directions, independent groups, lease fencing, write/ack crash, "
                "atomic completion and retention gap"
            )
        finally:
            broker.kill()
