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
from contextlib import closing
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

    def request(self, route, body=None, *, method=None):
        data = None if body is None else json.dumps(body).encode()
        req = Request(
            f"http://{self.address}{route}",
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        with urlopen(req, timeout=10) as res:
            return json.load(res)

    def sse(self, task, after=None, *, topic="status"):
        headers = {"Last-Event-ID": after} if after else {}
        req = Request(
            f"http://{self.address}/topics/{topic}/tasks/{task}/events", headers=headers
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


def subscribe(ws, task, after=None, *, topic="status"):
    ws.send(
        json.dumps({"op": "subscribe", "topic": topic, "task_id": task, "after": after})
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


def partition_expansion(broker):
    assert broker.request("/partitions") == {"partitions": [0]}
    old = broker.publish("moved", "before", status="running")
    assert broker.request("/partitions/2", method="PUT") == {"partition": 2}
    assert broker.request("/partitions/2", method="PUT") == {"partition": 2}
    assert broker.request("/tasks/moved/partition") == {"partition": 0}
    move = {"from_partition": 0, "to_partition": 2}
    assert broker.request("/tasks/moved/move", move) == {"partition": 2}
    with broker.sse("moved", "before") as stream:
        assert sse_event(stream)[0] == "ready"
        new = broker.request(
            "/topics/status/records",
            {"task_id": "moved", "event_id": "after", "status": "completed"},
        )
        assert new["partition"] == 2 and new["offset"] > old
        assert sse_event(stream)[1] == "after"

    broker.kill()
    assert broker.start() == 0
    assert broker.request("/partitions") == {"partitions": [0, 2]}
    history = broker.request("/topics/status/tasks/moved/history")
    assert [(item.get("partition", 0), item["offset"]) for item in history] == [
        (0, old),
        (2, new["offset"]),
    ]
    assert broker.publish("moved", "before", status="running") == old


def kill_after_journal_write(broker, operation):
    """Kill after the complete frame reaches the journal and before SQLite commits."""
    journal = max(broker.directory.glob("*.seg"))
    before = journal.stat().st_size
    index = broker.directory / "partition-indexes" / "0.sqlite"
    if not index.exists():
        index = broker.directory / "index.sqlite"
    with (
        sqlite3.connect(index) as lock,
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


def atomic_completion(broker, terminal):
    topic = f"completion.{terminal}.tasks"
    work = f"/topics/{topic}/work/workers"
    task = f"atomic-live-{terminal}"
    offset = broker.publish(task, "queued", topic)
    claim = broker.request(work + "/claim", {"lease_ms": 30_000})
    assert claim["record"]["offset"] == offset
    event = {"task_id": task, "event_id": "finished", "status": terminal}
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
    broker.publish(task, "published", status=terminal)
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
    with broker.sse(task, "finished") as sse:
        assert sse_event(sse) == ("ready", None, {})
        assert not sse.readline()
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


def crash_after_partition_commit(broker):
    task = "split-completion"
    source = f"/topics/{task}/work/workers"
    offset = broker.publish(task, "queued", task)
    claim = broker.request(source + "/claim", {"lease_ms": 30_000})
    # An independent topic avoids scanning the large status fixture.
    audit = f"/topics/{task}.status/streams/audit"
    before = broker.publish(task, "pre-completion", topic=f"{task}.status")
    assert broker.request(audit + "/next")["offset"] == before
    broker.request(audit + "/commit", {"offset": before})
    broker.request("/partitions/3", method="PUT")
    source_partition = broker.request(f"/tasks/{task}/partition")["partition"]
    broker.request(
        f"/tasks/{task}/move", {"from_partition": source_partition, "to_partition": 3}
    )
    completion = {
        "offset": offset,
        "generation": claim["generation"],
        "status_topic": f"{task}.status",
        "event": {"task_id": task, "event_id": "done", "status": "completed"},
    }
    with (
        sqlite3.connect(broker.directory / "index.sqlite") as lock,
        sqlite3.connect(broker.directory / "partition-indexes" / "3.sqlite") as shard,
        concurrent.futures.ThreadPoolExecutor() as pool,
    ):
        lock.execute("BEGIN IMMEDIATE")
        pending = pool.submit(broker.request, source + "/complete", completion)
        deadline = time.monotonic() + 3
        receipt = None
        while receipt is None:
            receipt = shard.execute(
                "SELECT offset FROM records WHERE task=? AND event_id='done'", (task,)
            ).fetchone()
            assert time.monotonic() < deadline, (
                "completion never reached the shard index"
            )
            if receipt is None:
                time.sleep(0.002)
        broker.kill()
        lock.rollback()
        assert pending.exception(timeout=5) is not None

    assert broker.start() == 0
    assert broker.request(audit + "/next")["offset"] == receipt[0]
    assert broker.request(source + "/claim", {"lease_ms": 30_000}) is None
    assert broker.request(source + "/complete", completion) == {
        "offset": receipt[0],
        "partition": 3,
    }
    broker.kill()
    assert broker.start() == 0
    assert broker.request(source + "/complete", completion)["offset"] == receipt[0]


def background_index_recovery(broker):
    broker.env.update(
        SPOOLTAIL_INDEX_MODE="partitioned", SPOOLTAIL_INDEX_BATCH_SIZE="8"
    )
    assert broker.start() == 0
    shard = broker.directory / "partition-indexes" / "0.sqlite"
    work = "/topics/work/work/workers"
    audit = "/topics/status/streams/audit"
    with closing(sqlite3.connect(shard)) as index:
        index.execute("BEGIN IMMEDIATE")
        queued = broker.publish("T1", "queued", "work")
        claim = broker.request(work + "/claim", {"lease_ms": 30_000})
        assert claim["record"]["offset"] == queued
        started = broker.publish("T1", "started", status="running")
        assert broker.request(audit + "/next")["offset"] == started
        broker.request(audit + "/commit", {"offset": started})
        completion = {
            "offset": queued,
            "generation": claim["generation"],
            "status_topic": "status",
            "event": {"task_id": "T1", "event_id": "done", "status": "completed"},
        }
        finished = broker.request(work + "/complete", completion)["offset"]
        assert broker.request(audit + "/next")["offset"] == finished
        assert index.execute("SELECT count(*) FROM records").fetchone()[0] == 0
        broker.kill()
        index.rollback()

    assert broker.start() == 3
    assert broker.request(audit + "/next")["offset"] == finished
    broker.request(audit + "/commit", {"offset": finished})
    assert broker.request(work + "/claim", {"lease_ms": 30_000}) is None
    assert broker.request(work + "/complete", completion)["offset"] == finished
    assert broker.request(audit + "/next") is None

    # An idle writer flushes a partial index batch without more publishes.
    idle = broker.publish("T2", "isolated", status="running")
    with closing(sqlite3.connect(shard)) as index:
        deadline = time.monotonic() + 3
        while (
            index.execute("SELECT 1 FROM records WHERE offset=?", (idle,)).fetchone()
            is None
        ):
            assert time.monotonic() < deadline, "partial index batch was never flushed"
            time.sleep(0.002)


def document_summary_streams(broker):
    """Simulate a summary worker publishing progress as it processes each page."""
    broker.env.update(
        SPOOLTAIL_INDEX_MODE="partitioned",
        SPOOLTAIL_INDEX_BATCH_SIZE="32",
        SPOOLTAIL_DYNAMIC_PLACEMENT="1",
        SPOOLTAIL_SERVICE_PARTITIONS='{"summary":[1,2]}',
    )
    assert broker.start() == 0
    task = "document-42"
    topic = "documents.status"
    work = "/topics/documents.work/work/summary-workers"
    page_count = 73
    queued = broker.request(
        "/topics/documents.work/records?owner_service=summary",
        {"task_id": task, "event_id": "queued", "page_count": page_count},
    )
    source = queued["partition"]
    assert source in (1, 2)
    destination = 3 - source
    claim = broker.request(work + "/claim", {"lease_ms": 120_000, "partition": source})
    assert claim["record"]["offset"] == queued["offset"]

    events = []
    sizes = []
    paragraph = (
        "The quarterly inventory report compares goods received, shipped, and "
        "returned. The appendix reconciles totals by warehouse and explains "
        "the adjustments applied during the stock count.\n\n"
    )
    for page in range(1, page_count + 1):
        size_kib = (
            1000 if page in (2, page_count) else (8, 32, 128, 256)[(page - 1) % 4]
        )
        size = size_kib * 1024
        header = f"Page {page} · Café inventory 📄\nRésumé — คลังสินค้า\n\n"
        remaining = size - len(header.encode())
        summary = header + (paragraph * (remaining // len(paragraph) + 1))[:remaining]
        assert len(summary.encode()) == size
        sizes.append(size)
        events.append(
            {
                "task_id": task,
                "event_id": f"page-{page}",
                "status": "running",
                "page_number": page,
                "page_count": page_count,
                "summary": summary,
            }
        )

    previous_offset = queued["offset"]

    def publish_page(event, partition):
        nonlocal previous_offset
        receipt = broker.request(f"/topics/{topic}/records", event)
        assert receipt["partition"] == partition
        assert receipt["offset"] > previous_offset
        previous_offset = receipt["offset"]

    def check_delivery(sse, ws, event):
        event_id = event["event_id"]
        assert sse_event(sse) == ("status", event_id, event), f"SSE changed {event_id}"
        assert ws_event(ws) == {
            "type": "status",
            "topic": topic,
            "task_id": task,
            "data": event,
        }, f"WebSocket changed {event_id}"

    with broker.sse(task, topic=topic) as sse, broker.ws() as ws:
        subscribe(ws, task, topic=topic)
        assert sse_event(sse)[0] == "ready" and ws_event(ws)["type"] == "ready"
        for event in events[:2]:
            publish_page(event, source)
            check_delivery(sse, ws, event)

    # Miss 70 updates, enough to require a second replay page. Move the task
    # while the subscribers are offline, then restart with the work lease active.
    move_route = f"/tasks/{task}/move"
    partition = source
    for event in events[2:-1]:
        if event["page_number"] == 37:
            assert broker.request(
                move_route, {"from_partition": source, "to_partition": destination}
            ) == {"partition": destination}
            partition = destination
        publish_page(event, partition)
    broker.kill()
    broker.start()

    completed = {
        "task_id": task,
        "event_id": "completed",
        "status": "completed",
        "page_count": page_count,
    }
    completion = {
        "offset": queued["offset"],
        "generation": claim["generation"],
        "status_topic": topic,
        "event": completed,
    }
    cursor = events[1]["event_id"]
    with broker.sse(task, cursor, topic=topic) as sse, broker.ws() as ws:
        subscribe(ws, task, cursor, topic=topic)
        assert sse_event(sse)[0] == "ready" and ws_event(ws)["type"] == "ready"
        for event in events[2:-1]:
            check_delivery(sse, ws, event)
        publish_page(events[-1], destination)
        check_delivery(sse, ws, events[-1])
        receipt = broker.request(work + "/complete", completion)
        assert (
            receipt["partition"] == destination and receipt["offset"] > previous_offset
        )
        check_delivery(sse, ws, completed)
        assert not sse.readline()

    assert broker.request(work + "/claim", {"lease_ms": 120_000}) is None
    assert broker.request(work + "/complete", completion) == receipt
    print(
        f"PASS: summary worker streamed {page_count} page summaries over SSE and WebSocket "
        f"({sum(sizes) / 1024**2:.1f} MiB total, largest {max(sizes) // 1024} KiB), "
        "resumed across a partition move and restart, and completed its original lease"
    )


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
        broker.env["SPOOLTAIL_INDEX_BATCH_SIZE"] = "1"
        try:
            floor, latency = cross_protocol(broker, prefix, offline)
            subscriptions(broker)
            crash_after_write(broker)
            for terminal in ("completed", "cancelled"):
                atomic_completion(broker, terminal)
            crash_during_completion(broker)
            expired(broker, floor)
            partition_expansion(broker)
            if (directory / "partition-indexes").exists():
                crash_after_partition_commit(broker)
            print(
                f"PASS: {args.events:,} fixture events; sample WebSocket resume {latency:.2f} ms; "
                "both transport directions, independent groups, lease fencing, write/ack crash, "
                "atomic completion and cancellation, retention gap, and partition expansion"
            )
        finally:
            broker.kill()

    with tempfile.TemporaryDirectory() as path:
        broker = Broker(args.binary.resolve(), Path(path))
        try:
            background_index_recovery(broker)
            print(
                "PASS: background index catch-up and recovery after acknowledged appends"
            )
        finally:
            broker.kill()

    with tempfile.TemporaryDirectory() as path:
        broker = Broker(args.binary.resolve(), Path(path))
        try:
            document_summary_streams(broker)
        finally:
            broker.kill()
