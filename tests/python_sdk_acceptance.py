"""Run the installed Python SDK against the TLS and SSE listeners of a real broker."""

import asyncio
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from contextlib import aclosing
from pathlib import Path

import grpc
import httpx
from grpc_acceptance import BROKER, env, identity, start
from relayna_spooltail import (
    AuditOutcome,
    AuditStore,
    AuditTimeline,
    ConflictError,
    CursorGapError,
    Record,
    ReplayBatch,
    SettlementAction,
    SpooltailClient,
    StaleLeaseError,
    SubscriptionError,
)


async def expect_error(expected, operation):
    try:
        await operation
    except expected as error:
        return error
    raise AssertionError(f"expected {expected.__name__}")


async def exercise(directory):
    cert, key = identity(directory)
    config = env(directory, cert, key)
    tls_roots = cert.read_bytes()
    task_id = "T1"
    queued = {"task_id": task_id, "event_id": "queued"}
    running = {"task_id": task_id, "event_id": "running", "status": "running"}
    done = {
        "task_id": task_id,
        "event_id": "done",
        "status": "completed",
        "result": {"integer": 2**60, "text": "déjà vu", "items": [None, False]},
    }

    async with (
        start(config) as (http_url, grpc_port),
        SpooltailClient(
            f"localhost:{grpc_port}", root_certificates=tls_roots
        ) as client,
        httpx.AsyncClient(base_url=http_url, trust_env=False, timeout=5) as http,
    ):
        async with SpooltailClient(
            f"127.0.0.1:{grpc_port}", root_certificates=tls_roots
        ) as wrong_name:
            error = await expect_error(grpc.aio.AioRpcError, wrong_name.ping())
            assert error.code() == grpc.StatusCode.UNAVAILABLE

        await client.ping()
        assert await client.latest("status", task_id) is None
        assert await client.claim("work", "workers") is None
        async with SpooltailClient(
            f"localhost:{grpc_port}", root_certificates=tls_roots
        ) as retrier:
            work_offset, retry_offset = await asyncio.gather(
                client.publish("work", queued), retrier.publish("work", queued)
            )
        assert retry_offset == work_offset
        assert await client.publish("work", queued) == work_offset
        await expect_error(
            ConflictError, client.publish("work", {**queued, "changed": True})
        )

        original = await client.claim("work", "workers")
        assert original is not None and original.record.offset == work_offset
        assert original.record.event == queued and original.lease_until_ms > 0
        await client.settle(original, SettlementAction.RENEW, delay_ms=30_000)
        await client.settle(original, SettlementAction.RETRY)
        abandoned = await client.claim("work", "workers", lease_ms=50)
        assert abandoned is not None and abandoned.generation > original.generation
        # No settlement arrives for this attempt. Another worker reclaims it.
        async with asyncio.timeout(5):
            while (lease := await client.claim("work", "workers")) is None:
                await asyncio.sleep(0.02)
        assert lease.record.offset == work_offset
        assert lease.generation > abandoned.generation
        await expect_error(
            StaleLeaseError, client.settle(original, SettlementAction.ACK)
        )
        await expect_error(StaleLeaseError, client.complete(abandoned, "status", done))

        progress_offset = await client.publish("status", running)
        async with http.stream("GET", f"/topics/status/tasks/{task_id}/events") as sse:
            sse.raise_for_status()
            lines = sse.aiter_lines()
            assert await anext(lines) == "event: ready"
            assert await anext(lines) == "data: {}"
            assert await anext(lines) == ""
            progress = [await anext(lines) for _ in range(4)]
            assert "id: running" in progress and "event: status" in progress
        assert (await client.next("status", "audit")).offset == progress_offset
        await client.commit("status", "audit", progress_offset)

        async with aclosing(
            client.subscribe("status", task_id, after="running", timeout=5)
        ) as deliveries:
            assert (await anext(deliveries)).kind == "ready"
            status_offset = await client.complete(lease, "status", done)
            delivery = await anext(deliveries)
            assert (delivery.kind, delivery.event_id, delivery.data) == (
                "status",
                "done",
                done,
            )
            assert [event async for event in deliveries] == []
        conflict = await expect_error(
            ConflictError, client.complete(lease, "status", {**done, "result": None})
        )
        assert type(conflict) is ConflictError
        assert await client.claim("work", "workers") is None

    async with (
        start(config) as (http_url, grpc_port),
        SpooltailClient(
            f"localhost:{grpc_port}", root_certificates=tls_roots
        ) as client,
        httpx.AsyncClient(base_url=http_url, trust_env=False, timeout=5) as http,
    ):
        assert await client.complete(lease, "status", done) == status_offset
        latest = await client.latest("status", task_id)
        assert latest is not None and (latest.offset, latest.event) == (
            status_offset,
            done,
        )
        assert await client.history("status", task_id, after="running") == [latest]
        assert len(await client.history("status", task_id)) == 2
        assert await client.history("status", task_id, after="done") == []
        batch = await client.next_batch("status", "batch-audit", limit=2)
        assert [record.offset for record in batch.records] == [progress_offset, status_offset]
        await client.commit_batch(batch, count=1)
        await client.commit_batch(batch, count=1)
        await expect_error(ConflictError, client.commit_batch(batch))
        remaining = await client.next_batch("status", "batch-audit")
        assert remaining.after == progress_offset and remaining.records == (latest,)
        await client.commit_batch(remaining)
        assert await client.next("status", "audit") == latest
        await client.commit("status", "audit", status_offset)
        assert await client.next("status", "audit") is None
        assert (await client.stats()).payload_reads > 0

        for topic, earliest in [("status", "running"), ("empty", None)]:
            error = await expect_error(
                CursorGapError, client.history(topic, task_id, after="missing")
            )
            assert (error.after, error.earliest_event_id) == ("missing", earliest)
            async with aclosing(
                client.subscribe(topic, task_id, after="missing")
            ) as stream:
                error = await expect_error(CursorGapError, anext(stream))
                assert (error.after, error.earliest_event_id) == ("missing", earliest)
        async with aclosing(client.subscribe("", task_id)) as stream:
            await expect_error(SubscriptionError, anext(stream))
        async with aclosing(
            client.subscribe("status", task_id, after="done", timeout=5)
        ) as stream:
            assert (await anext(stream)).kind == "ready"
            assert await anext(stream, None) is None

        async with http.stream(
            "GET",
            f"/topics/status/tasks/{task_id}/events",
            headers={"Last-Event-ID": "done"},
        ) as sse:
            sse.raise_for_status()
            assert [line async for line in sse.aiter_lines() if line] == [
                "event: ready",
                "data: {}",
            ]

        async with http.stream(
            "GET",
            f"/topics/status/tasks/{task_id}/events",
            headers={"Last-Event-ID": "running"},
        ) as sse:
            sse.raise_for_status()
            resumed = [line async for line in sse.aiter_lines()]
            assert [line for line in resumed if line.startswith("id: ")] == ["id: done"]
            assert [
                json.loads(line[6:]) for line in resumed if line.startswith("data: ")
            ] == [
                {},
                done,
            ]

    cancel_topic = "cancel.work"
    cancel_task = "cancel-task"
    cancelled = {
        "task_id": cancel_task,
        "attempt_id": "worker-attempt",
        "event_id": "cancelled",
        "status": "cancelled",
    }
    async with (
        start(config) as (_, grpc_port),
        SpooltailClient(f"localhost:{grpc_port}", root_certificates=tls_roots) as client,
    ):
        work = {"task_id": cancel_task, "event_id": "queued"}
        await client.publish(cancel_topic, work)
        lease_to_cancel = await client.claim(cancel_topic, "workers")
        assert lease_to_cancel is not None and lease_to_cancel.record.event == work
        async with aclosing(client.subscribe("cancel.status", cancel_task, timeout=5)) as feed:
            assert (await anext(feed)).kind == "ready"
            cancel_offset = await client.complete(lease_to_cancel, "cancel.status", cancelled)
            delivery = await anext(feed)
            assert (delivery.kind, delivery.event_id, delivery.data) == (
                "status",
                "cancelled",
                cancelled,
            )
            assert await anext(feed, None) is None

    async with (
        start(config) as (_, grpc_port),
        SpooltailClient(f"localhost:{grpc_port}", root_certificates=tls_roots) as client,
    ):
        latest_cancelled = await client.latest("cancel.status", cancel_task)
        assert latest_cancelled is not None
        assert (latest_cancelled.offset, latest_cancelled.event) == (cancel_offset, cancelled)
        assert await client.complete(lease_to_cancel, "cancel.status", cancelled) == cancel_offset
        await expect_error(
            ConflictError,
            client.complete(lease_to_cancel, "cancel.status", {**cancelled, "status": "completed"}),
        )
        assert await client.claim(cancel_topic, "workers") is None
        async with aclosing(
            client.subscribe("cancel.status", cancel_task, after="cancelled", timeout=5)
        ) as feed:
            assert (await anext(feed)).kind == "ready"
            assert await anext(feed, None) is None

    plaintext = {
        key: value
        for key, value in config.items()
        if key not in {"SPOOLTAIL_GRPC_TLS_CERT", "SPOOLTAIL_GRPC_TLS_KEY"}
    }
    plaintext["SPOOLTAIL_GRPC_TLS"] = "off"
    async with (
        start(plaintext, scheme="http") as (_, grpc_port),
        SpooltailClient(f"127.0.0.1:{grpc_port}", tls=False) as client,
    ):
        assert await client.latest("status", task_id) == latest
        batch = await client.next_batch("status", "batch-audit")
        assert batch.after == status_offset and not batch.records
        # This history page exceeds gRPC's default client receive limit.
        blob = "x" * (900 * 1024)
        for i in range(5):
            await client.publish(
                "large", {"task_id": task_id, "event_id": str(i), "blob": blob}
            )
        page = await client.history("large", task_id)
        assert [record.event["event_id"] for record in page] == [
            "0",
            "1",
            "2",
            "3",
            "4",
        ]
        assert all(record.event["blob"] == blob for record in page)
        batch = await client.next_batch("large", "bounded-audit", limit=64)
        assert batch.records == tuple(page[:2])
        await client.commit_batch(batch)
        batch = await client.next_batch("large", "bounded-audit", limit=1)
        assert batch.records == (page[2],)


async def partitioning(directory):
    cert, key = identity(directory)
    config = env(directory, cert, key)
    async with (
        start(config) as (_, grpc_port),
        SpooltailClient(
            f"localhost:{grpc_port}", root_certificates=cert.read_bytes()
        ) as client,
    ):
        queued = {"task_id": "T7", "event_id": "queued"}
        running = {"task_id": "T7", "event_id": "running", "status": "running"}
        terminal = {"task_id": "T7", "event_id": "done", "status": "completed"}
        work = await client.publish("work", queued)
        first = await client.publish("status", running)
        assert await client.list_partitions() == [0]
        await client.ensure_partition(2)
        assert await client.list_partitions() == [0, 2]
        assert await client.task_partition("T7") == 0
        lease = await client.claim("work", "workers", partition=0)
        assert lease is not None and lease.record.offset == work

        await client.move_task("T7", 0, 2)
        assert await client.task_partition("T7") == 2
        assert await client.claim("work", "workers", partition=2) is None
        final = await client.complete(lease, "status", terminal)
        assert await client.complete(lease, "status", terminal) == final
        assert await client.next("status", "audit", partition=2) is None
        progress = await client.next("status", "audit", partition=0)
        assert (progress.partition, progress.offset) == (0, first)
        await client.commit("status", "audit", first, partition=0)
        done = await client.next("status", "audit", partition=2)
        assert (done.partition, done.offset) == (2, final)
        await client.commit("status", "audit", final, partition=2)
        assert await client.history("status", "T7", after="running") == [done]


async def service_routing(directory):
    cert, key = identity(directory)
    config = env(directory, cert, key)
    config["SPOOLTAIL_SERVICE_PARTITIONS"] = json.dumps({"urgent": 2, "worker": 4})
    queued = {"task_id": "shared-task", "event_id": "queued"}
    status = {"task_id": "shared-task", "event_id": "running", "status": "running"}

    async with (
        start(config) as (http_url, grpc_port),
        SpooltailClient(
            f"localhost:{grpc_port}", root_certificates=cert.read_bytes()
        ) as client,
        httpx.AsyncClient(base_url=http_url, trust_env=False, timeout=5) as http,
    ):
        assert await client.list_partitions() == [0, 2, 4]
        work = await client.publish("work", queued, owner_service="urgent")
        assert await client.task_partition("shared-task") == 2
        assert await client.publish("work", queued, owner_service="worker") == work
        reply = await http.post(
            "/topics/status/records",
            params={"owner_service": "worker"},
            json=status,
        )
        assert reply.raise_for_status().json()["partition"] == 2
        latest = await client.latest("status", "shared-task")
        assert latest is not None and latest.event == status and latest.partition == 2

        results = await asyncio.gather(
            client.publish(
                "work",
                {"task_id": "race", "event_id": "queued"},
                owner_service="urgent",
            ),
            client.publish(
                "status",
                {"task_id": "race", "event_id": "running"},
                owner_service="worker",
            ),
        )
        placement = await client.task_partition("race")
        assert placement in {2, 4}
        for topic, offset in zip(("work", "status"), results, strict=True):
            latest = await client.latest(topic, "race")
            assert (latest.offset, latest.partition) == (offset, placement)

        for task, owner in [("general", None), ("unmapped", "unmapped")]:
            await client.publish(
                "work", {"task_id": task, "event_id": "queued"}, owner_service=owner
            )
            assert await client.task_partition(task) == 0

        reply = await http.post(
            "/topics/work/records",
            params={"owner_service": "worker"},
            json={"task_id": "worker-task", "event_id": "queued"},
        )
        assert reply.raise_for_status().json()["partition"] == 4

    config["SPOOLTAIL_SERVICE_PARTITIONS"] = json.dumps({"urgent": 3, "worker": 4})
    async with (
        start(config) as (_, grpc_port),
        SpooltailClient(
            f"localhost:{grpc_port}", root_certificates=cert.read_bytes()
        ) as client,
    ):
        await client.publish(
            "status",
            {**status, "event_id": "progress"},
            owner_service="urgent",
        )
        assert await client.task_partition("shared-task") == 2
        history = await client.history("status", "shared-task")
        assert [record.partition for record in history] == [2, 2]
        await client.publish(
            "work",
            {"task_id": "new-urgent", "event_id": "queued"},
            owner_service="urgent",
        )
        assert await client.task_partition("new-urgent") == 3


def init_consumer_state(path):
    with sqlite3.connect(path) as db:
        db.executescript("CREATE TABLE app(n INTEGER); INSERT INTO app VALUES (0);")


def app_count(path):
    with sqlite3.connect(path) as db:
        return db.execute("SELECT n FROM app").fetchone()[0]


def audit_crash_child(path, point):
    batch = ReplayBatch("status", 0, (Record(17, {"task_id": "T1", "event_id": "E1"}),))
    outcome = AuditOutcome(17, "policy", "rejected", "policy-service")

    def apply(execute):
        execute("UPDATE app SET n=n+1")

    with AuditStore(path) as store:
        if point == "before":
            def interrupt_commit(sql):
                if sql == "COMMIT":
                    os._exit(47)

            store._db.set_trace_callback(interrupt_commit)
        store.commit_batch("audit", batch, [outcome], apply_state=apply)
        os._exit(47)


def verify_audit_crashes(directory):
    for point, committed in [("before", False), ("after", True)]:
        path = directory / f"crash-{point}.sqlite"
        init_consumer_state(path)
        result = subprocess.run(
            [sys.executable, __file__, "--audit-crash", path, point], check=False
        )
        assert result.returncode == 47
        with AuditStore(path) as store, sqlite3.connect(path) as db:
            assert store.checkpoint("status", "audit") == (17 if committed else None)
            assert db.execute("SELECT count(*) FROM spooltail_audit_outcome").fetchone() == (
                int(committed),
            )
        assert app_count(path) == int(committed)


async def audit_pages(timeline, cursor=None, limit=1, topic="status"):
    entries = []
    while True:
        page = await timeline.read(topic, "T1", cursor=cursor, limit=limit)
        entries.extend(page.entries)
        if page.cursor is None:
            return entries
        assert page.entries
        assert page.cursor != cursor
        cursor = page.cursor


async def audit_timeline(directory):
    path = directory / "consumer.sqlite"
    init_consumer_state(path)
    cert, key = identity(directory)
    config = env(directory, cert, key)
    roots = cert.read_bytes()
    async with start(config) as (_, grpc_port), SpooltailClient(
        f"localhost:{grpc_port}", root_certificates=roots
    ) as client:
        first = await client.publish("status", {"task_id": "T1", "event_id": "E1"})
        await client.publish("other", {"task_id": "T1", "event_id": "E2"})
        second = await client.publish("status", {"task_id": "T1", "event_id": "E3"})
        batch = await client.read_after_batch("status", limit=2)
        assert [r.offset for r in batch.records] == [first, second]
        decision = AuditOutcome(first, "policy", "rejected", "policy-service", "policy-v2")
        notice = AuditOutcome(first, "notify", "queued", "policy-service")
        selected = [decision, notice]
        with AuditStore(path) as store:
            assert store.checkpoint("status", "audit") is None
            try:
                store.commit_batch(
                    "audit", batch, selected,
                    apply_state=lambda execute: (
                        execute("UPDATE app SET n=n+1"), execute("COMMIT")
                    ),
                )
            except sqlite3.DatabaseError:
                pass
            else:
                raise AssertionError("application committed the SDK transaction")
            assert app_count(path) == 0
            assert store.checkpoint("status", "audit") is None

            def apply(execute):
                execute("UPDATE app SET n=n+1")

            try:
                store.commit_batch("audit", batch, [decision, decision], apply_state=apply)
            except sqlite3.IntegrityError:
                pass
            else:
                raise AssertionError("duplicate operation was accepted")
            assert app_count(path) == 0
            assert store.checkpoint("status", "audit") is None
            assert store.commit_batch("audit", batch, selected, apply_state=apply)
            assert not store.commit_batch("audit", batch, selected, apply_state=apply)
            assert app_count(path) == 1
            try:
                store.commit_batch("audit", batch, [decision])
            except ConflictError:
                pass
            else:
                raise AssertionError("mismatched audit retry was accepted")

            timeline = AuditTimeline(client, store)
            initial = await timeline.read("status", "T1", limit=1)
            assert initial.cursor and initial.entries[0].event["event_id"] == "E1"
            assert not (await timeline.read("status", "missing")).entries
            for i in range(70):
                await client.publish("long-audit", {"task_id": "T1", "event_id": f"long-{i}"})
            long_entries = await audit_pages(timeline, limit=16, topic="long-audit")
            assert [entry.source_event_id for entry in long_entries] == [
                f"long-{i}" for i in range(70)
            ]
            other_file = directory / "another.sqlite"
            with AuditStore(other_file) as another:
                await expect_error(ValueError, AuditTimeline(client, another).read(
                    "status", "T1", cursor=initial.cursor,
                ))
            late = AuditOutcome(first, "review", "accepted", "review-service")
            first_batch = await client.read_after_batch("status", limit=1)
            store.commit_batch("review", first_batch, [late])
            third = await client.publish("status", {"task_id": "T1", "event_id": "E4"})
            tail = await client.read_after_batch(
                "status", after=store.checkpoint("status", "audit")
            )
            assert [r.offset for r in tail.records] == [third]
            assert store.commit_batch("audit", tail, [])
            frozen = [*initial.entries, *await audit_pages(timeline, initial.cursor)]
            assert [entry.source_offset for entry in frozen] == [first, first, first, second]
            assert [entry.outcome for entry in frozen if entry.outcome] == selected
            fresh = await audit_pages(timeline)
            assert [entry.outcome for entry in fresh if entry.outcome] == [*selected, late]
            assert await audit_pages(timeline, limit=64) == fresh
            expired = (await timeline.read("status", "T1", limit=1)).cursor
            assert expired

    subprocess.run([BROKER, "expire-before", "status", str(second)], env=config, check=True)
    with AuditStore(path) as store:
        async with start(config) as (_, grpc_port), SpooltailClient(
            f"localhost:{grpc_port}", root_certificates=roots
        ) as client:
            assert store.checkpoint("status", "audit") == third
            await expect_error(ConflictError, client.read_after_batch("status", after=first))
            assert [r.offset for r in (await client.read_after_batch("status")).records] == [
                second, third
            ]
            timeline = AuditTimeline(client, store)
            await expect_error(CursorGapError, timeline.read("status", "T1", cursor=expired))
            fresh = await audit_pages(timeline)
            assert [entry.source_offset for entry in fresh if entry.kind == "input"] == [second, third]
            assert [entry.outcome for entry in fresh if entry.input_before_retained_history] == [
                *selected, late
            ]

    subprocess.run([BROKER, "expire-before", "status", str(third + 1)], env=config, check=True)
    with AuditStore(path) as store:
        async with start(config) as (_, grpc_port), SpooltailClient(
            f"localhost:{grpc_port}", root_certificates=roots
        ) as client:
            fresh = await audit_pages(AuditTimeline(client, store))
            assert [entry.outcome for entry in fresh] == [*selected, late]
            assert all(entry.input_before_retained_history for entry in fresh)


async def archive_timeline(directory):
    cert, key = identity(directory)
    config = env(directory, cert, key)
    roots = cert.read_bytes()
    policy = {"hot_for": "1s", "retain_for": "1h", "paused": True}
    path = directory / "outcomes.sqlite"
    events = [
        {"task_id": "T1", "event_id": f"large-{i}", "payload": str(i) * 730_000}
        for i in range(3)
    ]
    config["SPOOLTAIL_RETENTION"] = json.dumps({"status": policy})
    with AuditStore(path) as store:
        async with start(config) as (_, grpc_port), SpooltailClient(
            f"localhost:{grpc_port}", root_certificates=roots
        ) as client:
            offsets = [await client.publish("status", event) for event in events]
            batch = await client.read_after_batch("status")
            assert [record.offset for record in batch.records] == offsets[:2]
            assert store.commit_batch("audit", batch, [])
            batch = await client.read_after_batch("status", after=offsets[1])
            decision = AuditOutcome(offsets[2], "review", "accepted", "review-service")
            assert store.commit_batch("audit", batch, [decision])
            initial = await AuditTimeline(client, store).read("status", "T1", limit=1)
            assert initial.cursor and initial.entries[0].source_offset == offsets[0]

        policy["paused"] = False
        config["SPOOLTAIL_RETENTION"] = json.dumps({"status": policy})
        async with start(config) as (_, grpc_port), SpooltailClient(
            f"localhost:{grpc_port}", root_certificates=roots
        ) as client:
            async with asyncio.timeout(10):
                while await client.latest("status", "T1") is not None:
                    await asyncio.sleep(0.1)
            assert not (await client.read_after_batch("status")).records
            await expect_error(ConflictError, client.read_after_batch("status", after=0))
            await expect_error(CursorGapError, client.history("status", "T1", after="large-0"))
            timeline = AuditTimeline(client, store)
            fresh = await audit_pages(timeline, limit=64)
            frozen = [*initial.entries, *await audit_pages(timeline, initial.cursor)]
            assert fresh == frozen
            assert [entry.kind for entry in fresh] == ["input", "input", "input", "outcome"]
            assert [entry.event for entry in fresh[:-1]] == events
            assert fresh[-1].outcome == decision
            assert not fresh[-1].input_before_retained_history

        # The broker resumes the saved policy without a configuration override.
        del config["SPOOLTAIL_RETENTION"]
        async with start(config) as (_, grpc_port), SpooltailClient(
            f"localhost:{grpc_port}", root_certificates=roots
        ) as client:
            assert await audit_pages(AuditTimeline(client, store), limit=64) == fresh


if __name__ == "__main__":
    if sys.argv[1:2] == ["--audit-crash"]:
        audit_crash_child(Path(sys.argv[2]), sys.argv[3])
    with tempfile.TemporaryDirectory(prefix="spooltail-python-sdk-") as temp:
        asyncio.run(asyncio.wait_for(exercise(Path(temp)), timeout=30))
    with tempfile.TemporaryDirectory(prefix="spooltail-partition-sdk-") as temp:
        asyncio.run(asyncio.wait_for(partitioning(Path(temp)), timeout=30))
    with tempfile.TemporaryDirectory(prefix="spooltail-service-sdk-") as temp:
        asyncio.run(asyncio.wait_for(service_routing(Path(temp)), timeout=30))
    with tempfile.TemporaryDirectory(prefix="spooltail-audit-sdk-") as temp:
        verify_audit_crashes(Path(temp))
        asyncio.run(asyncio.wait_for(audit_timeline(Path(temp)), timeout=30))
    with tempfile.TemporaryDirectory(prefix="spooltail-archive-sdk-") as temp:
        asyncio.run(asyncio.wait_for(archive_timeline(Path(temp)), timeout=30))
    print(
        "PASS: Python SDK publish, lease, completion, cancellation, replay gaps, SSE resume, "
        "partition moves, service placement, audit timeline recovery, and archive reads"
    )
