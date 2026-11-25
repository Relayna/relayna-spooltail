"""Exercise bounded SDK replay, optional accelerators, and recovery on a real broker."""

import argparse
import asyncio
import json
import os
import pickle
import sqlite3
import subprocess
import sys
import tempfile
import threading
from dataclasses import asdict, replace
from functools import partial
from pathlib import Path

from grpc_acceptance import env, identity, start
from relayna_spooltail import (
    AuditOutcome,
    AuditStore,
    Record,
    ReplayBatch,
    SpooltailClient,
)
from relayna_spooltail._wire import spooltail_pb2 as wire
from relayna_spooltail._wire.spooltail_pb2_grpc import (
    SpooltailServicer,
    add_SpooltailServicer_to_server,
)

import grpc


def outcomes(page):
    return [
        AuditOutcome(r.offset, "apply", "ok", "test", r.event_id) for r in page.records
    ]


async def crash_child(path, boundary):
    fetched = threading.Event()

    async def read(topic, *, after, limit):
        after = after or 0
        if after == 2:
            fetched.set()
        return ReplayBatch(
            topic,
            after,
            tuple(
                Record(i, {"event_id": str(i), "task_id": "t"})
                for i in range(after + 1, min(after + limit, 6) + 1)
            ),
        )

    def process(page):
        if page.after == 0 and boundary == "before":
            assert fetched.wait(3)
            os._exit(81)
        if page.after == 2 and boundary == "after":
            os._exit(82)
        return outcomes(page)

    assert (
        await AuditStore.catch_up(
            path, "topic", "c", read_page=read, process=process, limit=2
        )
        == 6
    )


async def legacy():
    class OldBroker(SpooltailServicer):
        async def ReadReplayPage(self, req, ctx):
            self.attempts += 1
            await ctx.abort(grpc.StatusCode.UNIMPLEMENTED, "old broker")

        async def ReadAfterBatch(self, req, ctx):
            return wire.StreamBatch(
                after=0,
                records=[
                    wire.Record(offset=1, event_json=b'{"task_id":"t","event_id":"e"}')
                ],
            )

    broker = OldBroker()
    broker.attempts = 0
    server = grpc.aio.server()
    add_SpooltailServicer_to_server(broker, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        async with SpooltailClient(f"127.0.0.1:{port}", tls=False) as client:
            for _ in range(2):
                assert (await client.read_after_batch("topic")).records[
                    0
                ].event_id == "e"
            assert broker.attempts == 1
    finally:
        await server.stop(None)


async def exercise(directory, native):
    if native:
        from relayna_spooltail.replay import decode_object

        assert decode_object.__module__ == "spooltail_replay_native"
    path = directory / "audit.sqlite"
    cert, key = identity(directory)
    config = env(directory, cert, key)
    nested = {"edge": [2**64 - 1, -(2**63), None, False, -0.0, [], {}, 'ไทย\n"']}
    values = [[], [1.25, -2], [], [3.5], [], [2**54], [2**53 + 1]]
    events = [
        dict(nested, event_id=str(i), task_id="t", samples=[{"rate": n} for n in row])
        for i, row in enumerate(values)
    ]
    events[0]["payload"] = "x"
    events[1]["payload"] = None
    events[2]["many"] = [0] * 8200
    events[3]["payload"] = {"user": "extension"}
    async with (
        start(config) as (_, port),
        SpooltailClient(
            f"localhost:{port}", root_certificates=cert.read_bytes()
        ) as client,
    ):
        for event in events:
            await client.publish("topic", event)
    async with (
        start(config) as (_, port),
        SpooltailClient(
            f"localhost:{port}", root_certificates=cert.read_bytes()
        ) as client,
    ):
        batch = await client.read_after_batch("topic")
        # ID-only access and the SQLite receipt must both leave event data deferred.
        assert [r.event_id for r in batch.records] == [e["event_id"] for e in events]
        with AuditStore(path) as store:
            assert store.commit_batch("lazy", batch, outcomes(batch))
        assert all(isinstance(r._data, wire.ReplayEvent) for r in batch.records)
        assert batch.records[2]._data.WhichOneof("attributes") == "object_json"
        for record, event in zip(batch.records, events, strict=True):
            assert json.dumps(record.event, sort_keys=True) == json.dumps(
                event, sort_keys=True
            )
            assert asdict(record)["event"] == event
            assert replace(record) == pickle.loads(pickle.dumps(record))
            assert record.event is record.event
            record.event["task_id"] = "changed"
            assert record.task_id == "changed"
        read_numeric = partial(
            client.read_numeric_batch, array_field="samples", value_field="rate"
        )
        col = await read_numeric("topic")
        assert col.sums() == (0, -0.75, 0, 3.5, 0, 2**54)
        if native:
            assert col.numpy_sums().tolist() == list(col.sums())
            assert not col.as_numpy().flags.writeable
        try:
            await read_numeric("topic", after=col.records[-1].offset)
        except grpc.aio.AioRpcError as exc:
            assert exc.code() == grpc.StatusCode.INVALID_ARGUMENT
        else:
            raise AssertionError("inexact integer accepted")
        checkpoint = await AuditStore.catch_up(
            path,
            "topic",
            "numeric",
            read_page=read_numeric,
            process=outcomes,
            max_records=6,
            limit=2,
        )
        assert checkpoint == col.records[-1].offset

        got_ahead = threading.Event()
        requests = []

        async def read(topic, *, after, limit):
            requests.append((after, limit))
            next_page = await client.read_after_batch(topic, after=after, limit=limit)
            if after is not None:
                got_ahead.set()
            return next_page

        with sqlite3.connect(path) as db:
            db.execute("CREATE TABLE applied(id TEXT PRIMARY KEY)")

        def fail(page, execute):
            assert got_ahead.wait(3)
            execute("INSERT INTO applied VALUES(?)", (page.records[0].event_id,))
            raise ValueError("rollback")

        try:
            await AuditStore.catch_up(
                path,
                "topic",
                "reader",
                read_page=read,
                process=outcomes,
                apply_state=fail,
                limit=2,
            )
        except ValueError as exc:
            assert str(exc) == "rollback"
        else:
            raise AssertionError("expected failed state write")
        assert len(requests) == 2
        with AuditStore(path) as store, sqlite3.connect(path) as db:
            assert store.checkpoint("topic", "reader") is None
            assert db.execute("SELECT count(*) FROM applied").fetchone() == (0,)

        started = asyncio.Event()
        release = threading.Event()
        loop = asyncio.get_running_loop()

        def delayed(page):
            loop.call_soon_threadsafe(started.set)
            assert release.wait(5)
            return outcomes(page)

        current = asyncio.create_task(
            AuditStore.catch_up(
                path, "topic", "reader", read_page=read, process=delayed, limit=2
            )
        )
        await asyncio.wait_for(started.wait(), 3)
        current.cancel()
        release.set()
        try:
            await current
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("expected cancellation")
        with AuditStore(path) as store:
            assert store.checkpoint("topic", "reader") == batch.records[1].offset
        requests.clear()
        saved = await AuditStore.catch_up(
            path,
            "topic",
            "reader",
            read_page=read,
            process=outcomes,
            limit=2,
            max_records=3,
        )
        assert requests == [(batch.records[1].offset, 2), (batch.records[3].offset, 1)]
        assert saved == batch.records[4].offset
        assert (
            await AuditStore.catch_up(
                path, "topic", "reader", read_page=read, process=outcomes
            )
            == batch.records[-1].offset
        )

    await legacy()


def crash_probes(directory):
    for boundary, code, expected in [("before", 81, None), ("after", 82, 2)]:
        audit = directory / f"{boundary}.sqlite"
        cmd = [sys.executable, __file__, "--crash", str(audit)]
        assert (
            subprocess.run(cmd + [boundary], timeout=10, check=False).returncode == code
        )
        with AuditStore(audit) as store:
            assert store.checkpoint("topic", "c") == expected
        subprocess.run(cmd + ["resume"], check=True, timeout=10)
        with sqlite3.connect(audit) as db:
            assert db.execute(
                "SELECT source_offset FROM spooltail_audit_outcome ORDER BY source_offset"
            ).fetchall() == [(i,) for i in range(1, 7)]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--native", action="store_true")
    parser.add_argument("--crash", nargs=2)
    args = parser.parse_args()
    if args.crash:
        asyncio.run(crash_child(*args.crash))
    else:
        with tempfile.TemporaryDirectory() as tmp:
            asyncio.run(exercise(Path(tmp), args.native))
            crash_probes(Path(tmp))
        print(
            "PASS: replay pages, deferred records, numeric projections, read-ahead and crash recovery"
        )
