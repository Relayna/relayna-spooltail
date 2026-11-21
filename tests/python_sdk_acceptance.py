"""Run the installed Python SDK against the TLS and SSE listeners of a real broker."""

import asyncio
import json
import tempfile
from contextlib import aclosing
from pathlib import Path

import grpc
import httpx
from grpc_acceptance import env, identity, start
from relayna_spooltail import (
    ConflictError,
    CursorGapError,
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
        work_offset = await client.publish("work", queued)
        assert await client.publish("work", queued) == work_offset
        await expect_error(
            ConflictError, client.publish("work", {**queued, "changed": True})
        )

        original = await client.claim("work", "workers")
        assert original is not None and original.record.offset == work_offset
        assert original.record.event == queued and original.lease_until_ms > 0
        await client.settle(original, SettlementAction.RENEW, delay_ms=30_000)
        await client.settle(original, SettlementAction.RETRY)
        lease = await client.claim("work", "workers")
        assert lease is not None and lease.generation > original.generation
        await expect_error(
            StaleLeaseError, client.settle(original, SettlementAction.ACK)
        )
        await expect_error(StaleLeaseError, client.complete(original, "status", done))

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
            client.subscribe("status", task_id, after="done")
        ) as stream:
            assert (await anext(stream)).kind == "ready"

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


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="spooltail-python-sdk-") as temp:
        asyncio.run(asyncio.wait_for(exercise(Path(temp)), timeout=30))
    print(
        "PASS: Python SDK publish, lease, completion, replay gaps, and SSE resume after restart"
    )
