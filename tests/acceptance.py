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

    def request(self, route, body=None, *, method=None, expected_status=200):
        data = None if body is None else json.dumps(body).encode()
        req = Request(
            f"http://{self.address}{route}",
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        with urlopen(req, timeout=10) as res:
            assert res.status == expected_status
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


def native_dead_letters(broker):
    topic = "dlq.tasks"
    work = f"/topics/{topic}/work/workers"
    dead = work + "/dead-letters"
    other = f"/topics/{topic}/work/other"
    broker.publish("invalid-run", "enqueue", topic, payload={"bad": True})
    broker.publish("second-run", "enqueue", topic)
    lease_args = {"lease_ms": 30_000}
    first = broker.request(work + "/claim", lease_args)
    second = broker.request(work + "/claim", lease_args)
    offset = first["record"]["offset"]
    detail = f"{dead}/{offset}"

    def settle(lease, action, group=work):
        return broker.request(
            group + "/settle",
            {
                "offset": lease["record"]["offset"],
                "generation": lease["generation"],
                "action": action,
            },
        )

    def redrive(generation, route=detail):
        return broker.request(route + "/redrive", {"generation": generation})

    settle(first, "dead-letter")
    settle(second, "dead-letter")
    assert broker.request(work + "/claim", lease_args) is None
    assert broker.request(other + "/dead-letters") == []
    other_lease = broker.request(other + "/claim", lease_args)
    assert other_lease["record"] == first["record"]
    settle(other_lease, "ack", group=other)
    expect_http(404, lambda: broker.request(f"{other}/dead-letters/{offset}"))
    expect_http(
        409, lambda: redrive(first["generation"], f"{other}/dead-letters/{offset}")
    )
    expect_http(400, lambda: broker.request(dead + "?limit=65"))
    expect_http(400, lambda: broker.request(dead + "?after=-1"))
    reads = broker.request("/stats")["payload_reads"]
    (entry,) = broker.request(dead + "?limit=1")
    assert entry["offset"] == offset and entry["task_id"] == "invalid-run"
    assert entry["event_id"] == "enqueue" and entry["generation"] == first["generation"]
    (next_entry,) = broker.request(f"{dead}?after={offset}&limit=1")
    assert next_entry["offset"] == second["record"]["offset"]
    assert broker.request(f"{dead}?after={next_entry['offset']}") == []
    assert broker.request("/stats")["payload_reads"] == reads

    source = first.get("partition", 0)
    destination = 2 if source == 0 else 0
    broker.request(
        "/tasks/invalid-run/move",
        {"from_partition": source, "to_partition": destination},
    )
    letter = broker.request(detail)
    assert (
        letter["record"] == first["record"]
        and letter["generation"] == first["generation"]
    )
    assert letter.get("partition", 0) == destination
    expect_http(409, lambda: redrive(first["generation"] + 1))
    with concurrent.futures.ThreadPoolExecutor() as pool:
        assert list(pool.map(redrive, [first["generation"]] * 2)) == [{"ok": True}] * 2
    expect_http(404, lambda: broker.request(detail))
    broker.kill()
    assert broker.start() == 0
    claim = broker.request(work + "/claim", lease_args | {"partition": destination})
    assert (
        claim["record"] == first["record"] and claim["generation"] > first["generation"]
    )
    expect_http(409, lambda: settle(first, "ack"))
    settle(claim, "dead-letter")
    assert redrive(first["generation"]) == {"ok": True}
    assert broker.request(detail)["generation"] == claim["generation"]
    assert broker.request(work + "/claim", lease_args) is None
    redrive(claim["generation"])
    final = broker.request(work + "/claim", lease_args)
    settle(final, "ack")
    assert broker.request(work + "/claim", lease_args) is None
    assert broker.request(dead) == [next_entry]
    assert broker.request(f"/topics/{topic}/tasks/invalid-run/history") == [
        first["record"]
    ]


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


def cancellation_requests(broker):
    task_topic = "cancellation.tasks"
    status_topic = "cancellation.status"
    cancel_topic = "cancellation.control"
    work = f"/topics/{task_topic}/work/workers"
    topics = {"status_topic": status_topic, "cancel_topic": cancel_topic}

    def cancel(task):
        return broker.request(
            f"/topics/{task_topic}/tasks/{task}/cancel", topics, expected_status=202
        )

    def complete(lease, task, status):
        return broker.request(
            work + "/complete",
            {
                "offset": lease["record"]["offset"],
                "generation": lease["generation"],
                "status_topic": status_topic,
                "event": {"task_id": task, "event_id": "result", "status": status},
            },
        )

    expect_http(404, lambda: cancel("unknown"))
    expect_http(
        400,
        lambda: broker.request(
            f"/topics/{task_topic}/tasks/unknown/cancel",
            topics | {"cancel_topic": task_topic},
        ),
    )
    task = "cancel-after-restart"
    broker.publish(task, "queued", task_topic)
    kill_after_journal_write(broker, lambda: cancel(task))
    assert broker.start() >= 1
    (intent,) = broker.request(f"/topics/{cancel_topic}/tasks/{task}/history")
    receipt = cancel(task)
    assert intent == receipt | {"event": {"task_id": task, "event_id": "cancel"}}
    assert broker.request(f"/topics/{status_topic}/tasks/{task}/history") == []
    lease = broker.request(work + "/claim", {"lease_ms": 30_000})
    complete(lease, task, "cancelled")
    assert cancel(task) == receipt
    assert broker.request(work + "/claim", {"lease_ms": 30_000}) is None

    task = "already-finished"
    broker.publish(task, "queued", task_topic)
    lease = broker.request(work + "/claim", {"lease_ms": 30_000})
    complete(lease, task, "completed")
    expect_http(409, lambda: cancel(task))
    assert broker.request(f"/topics/{cancel_topic}/tasks/{task}/history") == []

    task = "racing-completion"
    broker.publish(task, "queued", task_topic)
    lease = broker.request(work + "/claim", {"lease_ms": 30_000})
    with concurrent.futures.ThreadPoolExecutor() as pool:
        finish = pool.submit(complete, lease, task, "completed")
        requests = [pool.submit(cancel, task) for _ in range(2)]
        finished = finish.result()
        outcomes = []
        for request in requests:
            try:
                outcomes.append(request.result())
            except HTTPError as error:
                assert error.code == 409
                outcomes.append(None)
    assert outcomes[0] == outcomes[1]
    if outcomes[0] is not None:
        assert outcomes[0]["offset"] < finished["offset"]


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


def service_quotas_and_metrics(broker):
    broker.env["SPOOLTAIL_SERVICE_FAIRNESS"] = "1"
    broker.start()

    def publish_owned(topic, task, owner):
        return broker.request(
            f"/topics/{topic}/records?owner_service={owner}",
            {
                "task_id": task,
                "event_id": "enqueued",
            },
        )["offset"]

    def claim(topic, group="workers", lease_ms=30_000):
        return broker.request(
            f"/topics/{topic}/work/{group}/claim", {"lease_ms": lease_ms}
        )

    def settle(topic, lease, action="ack", delay_ms=0, group="workers"):
        return broker.request(
            f"/topics/{topic}/work/{group}/settle",
            {
                "offset": lease["record"]["offset"],
                "generation": lease["generation"],
                "action": action,
                "delay_ms": delay_ms,
            },
        )

    task_topic = "fair.tasks"
    a1 = publish_owned(task_topic, "a1", "alpha")
    a2 = publish_owned(task_topic, "a2", "alpha")
    broker.publish("unrelated", "enqueue", "different.topic")
    b1 = publish_owned(task_topic, "b1", "beta")
    b2 = publish_owned(task_topic, "b2", "beta")
    lease = claim(task_topic)
    assert lease["record"]["offset"] == a1
    settle(task_topic, lease)
    broker.kill()
    broker.env["SPOOLTAIL_SERVICE_QUOTAS"] = '{"limited":{"max_in_flight":1}}'
    broker.start()
    for offset in (b1, a2, b2):
        lease = claim(task_topic)
        assert lease["record"]["offset"] == offset
        settle(task_topic, lease)
    assert claim(task_topic) is None

    limited = publish_owned("quota.left", "limited-left", "limited")
    limited_right = publish_owned("quota.right", "limited-right", "limited")
    spare = publish_owned("quota.right", "spare", "beta")
    lease = claim("quota.left", lease_ms=1_000)
    assert lease["record"]["offset"] == limited
    assert claim("quota.left", group="other") is None
    other = claim("quota.right")
    assert other["record"]["offset"] == spare
    settle("quota.right", other)
    assert claim("quota.right") is None
    stats = "/topics/quota.left/work/workers/stats?owner_service=limited&partition=0"
    assert broker.request(stats)["leased"] == 1
    deadline = time.monotonic() + 5
    while (snapshot := broker.request(stats))["expired_leases"] == 0:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    assert snapshot["queue_depth"] == snapshot["ready"] == 1
    assert snapshot["oldest_ready_age_ms"] >= 0
    assert snapshot["expired_lease_redeliveries_total"] == 0
    new_lease = claim("quota.left")
    assert new_lease["generation"] > lease["generation"]
    settle("quota.left", new_lease, "retry", delay_ms=60_000)
    snapshot = broker.request(stats)
    assert snapshot["queue_depth"] == snapshot["delayed"] == 1
    assert snapshot["ready"] == snapshot["expired_leases"] == 0
    assert snapshot["oldest_ready_age_ms"] is None
    assert snapshot["retries_total"] == snapshot["retries_per_minute"] == 1
    assert snapshot["expired_lease_redeliveries_total"] == 1
    assert (
        broker.request("/topics/quota.left/work/workers/stats?owner_service=beta")[
            "queue_depth"
        ]
        == 0
    )
    assert claim("quota.right")["record"]["offset"] == limited_right

    stream = f"/topics/{task_topic}/streams/audit"
    expected = {"checkpoint": 0, "latest_offset": b2, "lag": 4, "retention_gap": False}
    assert broker.request(stream + "/lag?partition=0") == expected
    assert broker.request(stream + "/next")["offset"] == a1
    broker.request(stream + "/commit", {"offset": a1})
    assert broker.request(stream + "/lag") == expected | {"checkpoint": a1, "lag": 3}
    broker.kill()
    broker.env["SPOOLTAIL_SERVICE_FAIRNESS"] = "0"
    subprocess.run(
        [broker.binary, "expire-before", task_topic, str(b1)],
        env=broker.env,
        check=True,
    )
    broker.start()
    assert broker.request(stream + "/lag") == expected | {
        "checkpoint": a1,
        "lag": 2,
        "retention_gap": True,
    }
    assert broker.request(f"/topics/{task_topic}/streams/new-group/lag") == expected | {
        "checkpoint": b1 - 1,
        "lag": 2,
    }
    assert claim("quota.right", group="other")["record"]["offset"] == spare

    original = broker.publish("lag-moved", "first", "lag.multi")
    broker.request("/partitions/2", method="PUT")
    broker.request("/tasks/lag-moved/move", {"from_partition": 0, "to_partition": 2})
    moved = broker.publish("lag-moved", "second", "lag.multi")
    lag = "/topics/lag.multi/streams/audit/lag"
    assert broker.request(lag + "?partition=0")["latest_offset"] == original
    assert broker.request(lag + "?partition=2")["latest_offset"] == moved
    assert broker.request(lag)["lag"] == 2


def managed_stream_groups(broker):
    broker.start()
    topic = "managed.status"
    group = f"/topics/{topic}/streams/audit"
    members = group + "/members"

    def join(member_id, lease_ms=30_000):
        return broker.request(members, {"member_id": member_id, "lease_ms": lease_ms})

    def heartbeat(member):
        return broker.request(
            f"{members}/{member['member_epoch']}/heartbeat", method="POST"
        )

    def leave(member):
        return broker.request(f"{members}/{member['member_epoch']}", method="DELETE")

    def scope(member, partition):
        assignment = next(
            a for a in member["assignments"] if a["partition"] == partition
        )
        return (
            f"?partition={partition}&member_epoch={member['member_epoch']}"
            f"&assignment_epoch={assignment['assignment_epoch']}"
        )

    initial = broker.publish("moved", "initial", topic)
    assert broker.request(group + "/next")["offset"] == initial
    broker.request(group + "/commit", {"offset": initial})
    predecessor = broker.publish("moved", "source", topic)
    broker.request("/partitions/2", method="PUT")
    broker.request("/tasks/moved/move", {"from_partition": 0, "to_partition": 2})
    moved = broker.publish("moved", "destination", topic)

    first = join("first")
    assert join("first") == first
    source = scope(first, 0)
    original_destination = scope(first, 2)
    assert broker.request(group + "/next" + original_destination) is None
    expect_http(409, lambda: broker.request(group + "/next?partition=0"))
    expect_http(409, lambda: broker.request(group + "/commit", {"offset": initial}))
    assert broker.request(group + "/lag?partition=0")["checkpoint"] == initial

    second = join("second", lease_ms=1_000)
    destination = scope(second, 2)
    assert heartbeat(first)["assignments"] == [first["assignments"][0]]
    expect_http(409, lambda: broker.request(group + "/next" + original_destination))
    assert broker.request(group + "/next" + source)["offset"] == predecessor
    broker.request(group + "/commit" + source, {"offset": predecessor})
    assert broker.request(group + "/next" + destination)["offset"] == moved

    time.sleep(max(0, second["lease_until_ms"] / 1000 - time.time()) + 0.02)
    expect_http(409, lambda: broker.request(group + "/next" + destination))
    expect_http(409, lambda: heartbeat(second))
    reassigned = heartbeat(first)
    current_destination = scope(reassigned, 2)
    assert (
        current_destination != original_destination and scope(reassigned, 0) == source
    )
    expect_http(409, lambda: broker.request(group + "/next" + original_destination))
    assert broker.request(group + "/next" + current_destination)["offset"] == moved
    broker.request(group + "/commit" + current_destination, {"offset": moved})
    expect_http(
        409, lambda: broker.request(group + "/commit" + destination, {"offset": moved})
    )

    broker.kill()
    broker.start()
    assert broker.request(group + "/next" + current_destination) is None
    broker.request("/partitions/3", method="PUT")
    expanded = heartbeat(first)
    assert [a["partition"] for a in expanded["assignments"]] == [0, 2, 3]
    assert scope(expanded, 2) == current_destination
    assert leave(first) == leave(first) == {"ok": True}
    expect_http(409, lambda: broker.request(group + "/next" + current_destination))
    expect_http(409, lambda: broker.request(group + "/next?partition=2"))
    assert join("first")["member_epoch"] != first["member_epoch"]


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
            cancellation_requests(broker)
            crash_during_completion(broker)
            expired(broker, floor)
            partition_expansion(broker)
            if (directory / "partition-indexes").exists():
                crash_after_partition_commit(broker)
            native_dead_letters(broker)
            print(
                f"PASS: {args.events:,} fixture events; sample WebSocket resume {latency:.2f} ms; "
                "both transport directions, independent groups, lease fencing, write/ack crash, "
                "atomic completion and cancellation, dead-letter redrive, retention gap, and partition expansion"
            )
        finally:
            broker.kill()

    with tempfile.TemporaryDirectory() as path:
        broker = Broker(args.binary.resolve(), Path(path))
        try:
            background_index_recovery(broker)
            cancellation_requests(broker)
            print(
                "PASS: background index catch-up and recovery for appends and cancellation"
            )
        finally:
            broker.kill()

    with tempfile.TemporaryDirectory() as path:
        broker = Broker(args.binary.resolve(), Path(path))
        try:
            document_summary_streams(broker)
        finally:
            broker.kill()

    with tempfile.TemporaryDirectory() as path:
        broker = Broker(args.binary.resolve(), Path(path))
        try:
            service_quotas_and_metrics(broker)
            print("PASS: service fairness, lease quotas, work metrics, and stream lag")
        finally:
            broker.kill()

    with tempfile.TemporaryDirectory() as path:
        broker = Broker(args.binary.resolve(), Path(path))
        try:
            managed_stream_groups(broker)
            print("PASS: stream assignment, rebalance, fencing, and checkpoint handoff")
        finally:
            broker.kill()
