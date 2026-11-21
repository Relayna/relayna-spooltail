# /// script
# requires-python = ">=3.11"
# dependencies = ["grpcio==1.80.0", "grpcio-tools==1.80.0", "httpx==0.28.1", "protobuf==6.33.5"]
# ///
"""Verify both listeners, certificate validation, and cross-transport broker behavior."""

import asyncio
import importlib
import json
import os
import socket
import subprocess
import sys
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path

import grpc
import httpx

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = ROOT / "proto/spooltail/v1"
BROKER = ROOT / "target/release/spooltail"


def identity(directory):
    cert, key = directory / "server.pem", directory / "server-key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
            "-keyout",
            key,
            "-out",
            cert,
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    return cert, key


def env(directory, cert, key):
    values = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("SPOOLTAIL_GRPC_", "SPOOLTAIL_BENCH_"))
    }
    values.update(
        SPOOLTAIL_DATA=str(directory / "data"),
        SPOOLTAIL_LISTEN="127.0.0.1:0",
        SPOOLTAIL_GRPC_LISTEN="127.0.0.1:0",
        SPOOLTAIL_GRPC_TLS_CERT=str(cert),
        SPOOLTAIL_GRPC_TLS_KEY=str(key),
    )
    return values


def assert_startup_rejected(values, message):
    result = subprocess.run(
        [BROKER], env=values, capture_output=True, text=True, timeout=5, check=False
    )
    assert result.returncode != 0 and message in result.stderr, result
    assert not result.stdout, result.stdout


@asynccontextmanager
async def start(values, *, scheme="https"):
    proc = await asyncio.create_subprocess_exec(
        BROKER,
        env=values,
        stdout=asyncio.subprocess.PIPE,
    )
    try:
        http_line = (await asyncio.wait_for(proc.stdout.readline(), 5)).decode()
        grpc_line = (await asyncio.wait_for(proc.stdout.readline(), 5)).decode()
        assert http_line.startswith("Listening 127.0.0.1:"), http_line
        assert grpc_line.startswith(f"gRPC listening on {scheme}://127.0.0.1:"), (
            grpc_line
        )
        yield f"http://{http_line.split()[1]}", grpc_line.strip().rsplit(":", 1)[1]
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()


async def rejected(code, call):
    try:
        await call
    except grpc.aio.AioRpcError as error:
        assert error.code() == code, error
        return error
    raise AssertionError(f"expected {code}")


async def exercise(directory, messages, stub):
    cert, key = identity(directory)
    config = env(directory, cert, key)
    plaintext = {
        k: v
        for k, v in config.items()
        if k not in ("SPOOLTAIL_GRPC_TLS_CERT", "SPOOLTAIL_GRPC_TLS_KEY")
    }
    assert_startup_rejected(plaintext, "SPOOLTAIL_GRPC_TLS_KEY together")
    assert_startup_rejected(
        {**config, "SPOOLTAIL_GRPC_TLS": "off"},
        "unset SPOOLTAIL_GRPC_TLS_CERT and SPOOLTAIL_GRPC_TLS_KEY",
    )
    assert_startup_rejected(
        {**config, "SPOOLTAIL_GRPC_TLS": "auto"}, "SPOOLTAIL_GRPC_TLS must be on or off"
    )
    plaintext["SPOOLTAIL_GRPC_TLS"] = "off"
    assert_startup_rejected(
        {k: v for k, v in config.items() if k != "SPOOLTAIL_GRPC_TLS_KEY"},
        "SPOOLTAIL_GRPC_TLS_KEY together",
    )
    assert_startup_rejected(
        {**config, "SPOOLTAIL_GRPC_TLS_CERT": str(directory / "absent.pem")},
        "read gRPC TLS certificate",
    )
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        reserved.listen()
        assert_startup_rejected(
            {
                **config,
                "SPOOLTAIL_GRPC_LISTEN": f"127.0.0.1:{reserved.getsockname()[1]}",
            },
            "bind gRPC TCP listener",
        )

    credentials = grpc.ssl_channel_credentials(root_certificates=cert.read_bytes())
    codes = grpc.StatusCode
    async with start(config) as (http_url, grpc_port):
        async with grpc.aio.insecure_channel(f"127.0.0.1:{grpc_port}") as cleartext:
            await rejected(
                codes.UNAVAILABLE, stub(cleartext).Ping(messages.Empty(), timeout=3)
            )
        async with grpc.aio.secure_channel(
            f"127.0.0.1:{grpc_port}", credentials
        ) as wrong_name:
            await rejected(
                codes.UNAVAILABLE, stub(wrong_name).Ping(messages.Empty(), timeout=3)
            )
        async with (
            grpc.aio.secure_channel(f"localhost:{grpc_port}", credentials) as channel,
            httpx.AsyncClient(base_url=http_url, trust_env=False, timeout=5) as http,
        ):
            api = stub(channel)
            await api.Ping(messages.Empty(), timeout=5)
            task = messages.Task(topic="status", task_id="T1")
            await rejected(codes.NOT_FOUND, api.Latest(task, timeout=5))
            await rejected(
                codes.INVALID_ARGUMENT,
                api.Append(
                    messages.Publish(topic="status", event_json=b"invalid"), timeout=5
                ),
            )

            first = {"task_id": "T1", "event_id": "queued", "status": "queued"}
            response = await http.post("/topics/work/records", json=first)
            response.raise_for_status()
            offset = response.json()["offset"]

            claim = messages.ClaimRequest(topic="work", group="workers", lease_ms=30000)
            lease = await api.Claim(claim, timeout=5)
            assert (
                lease.record.offset == offset
                and json.loads(lease.record.event_json) == first
            )
            settle = messages.SettleRequest(
                topic="work",
                group="workers",
                offset=offset,
                generation=lease.generation,
            )
            await rejected(codes.INVALID_ARGUMENT, api.Settle(settle, timeout=5))
            settle.action = messages.SETTLEMENT_ACTION_RENEW
            settle.delay_ms = 30000
            await api.Settle(settle, timeout=5)
            settle.action = messages.SETTLEMENT_ACTION_RETRY
            settle.delay_ms = 0
            await api.Settle(settle, timeout=5)
            await rejected(codes.FAILED_PRECONDITION, api.Settle(settle, timeout=5))
            new_lease = await api.Claim(claim, timeout=5)
            assert new_lease.generation > lease.generation

            initial = {"task_id": "T1", "event_id": "début", "status": "running"}
            progress = messages.Publish(
                topic="status", event_json=json.dumps(initial).encode()
            )
            progress_offset = (await api.Append(progress, timeout=5)).offset
            assert progress_offset == (await api.Append(progress, timeout=5)).offset
            earliest = await rejected(
                codes.FAILED_PRECONDITION,
                api.History(
                    messages.Task(topic="status", task_id="T1", after="absent"),
                    timeout=5,
                ),
            )
            assert (
                dict(earliest.trailing_metadata())["spooltail-earliest-event-id-bin"]
                == "début".encode()
            )

            terminal = {
                "task_id": "T1",
                "event_id": "done",
                "status": "completed",
                "extra": {"value": 2**60},
            }
            complete = messages.CompleteRequest(
                topic="work",
                group="workers",
                offset=offset,
                generation=new_lease.generation,
                status=messages.Publish(
                    topic="status", event_json=json.dumps(terminal).encode()
                ),
            )
            task.after = initial["event_id"]
            stream = api.Subscribe(task, timeout=5)
            async with http.stream(
                "GET",
                "/topics/status/tasks/T1/events",
                params={"after": initial["event_id"]},
            ) as sse:
                sse.raise_for_status()
                events = sse.aiter_lines()
                assert (await anext(events)) == "event: ready"
                assert (await stream.read()).kind == "ready"
                status_offset = (await api.Complete(complete, timeout=5)).offset
                delivery = await stream.read()
                assert delivery.kind == "status" and delivery.event_id == "done"
                assert json.loads(delivery.data_json) == terminal
                assert await stream.read() is grpc.aio.EOF
                received = [line async for line in events]
                assert "id: done" in received and "event: status" in received
                payload = next(
                    line.removeprefix("data: ")
                    for line in received
                    if line.startswith("data: ") and json.loads(line[6:]) == terminal
                )
                assert json.loads(payload) == terminal

            latest = (await http.get("/topics/status/tasks/T1/latest")).json()
            assert latest == {"offset": status_offset, "event": terminal}
            history = await api.History(task, timeout=5)
            assert [r.offset for r in history.records] == [status_offset]
            assert (await api.Stats(messages.Empty(), timeout=5)).payload_reads > 0
            assert not (await api.Claim(claim, timeout=5)).HasField("record")

            group = messages.StreamGroup(topic="status", group="audit")
            assert (await api.Next(group, timeout=5)).record.offset == progress_offset
            commit = messages.CommitRequest(
                topic="status", group="audit", offset=status_offset
            )
            await rejected(codes.FAILED_PRECONDITION, api.Commit(commit, timeout=5))
            commit.offset = progress_offset
            await api.Commit(commit, timeout=5)
            assert (await api.Next(group, timeout=5)).record.offset == status_offset
            commit.offset = status_offset
            await api.Commit(commit, timeout=5)
            assert not (await api.Next(group, timeout=5)).HasField("record")

    async with (
        start({**config, "SPOOLTAIL_GRPC_TLS": "on"}) as (_, grpc_port),
        grpc.aio.secure_channel(f"localhost:{grpc_port}", credentials) as channel,
    ):
        api = stub(channel)
        assert (await api.Complete(complete, timeout=5)).offset == status_offset
        assert not (await api.Next(group, timeout=5)).HasField("record")

    async with (
        start(plaintext, scheme="http") as (http_url, grpc_port),
        grpc.aio.insecure_channel(f"127.0.0.1:{grpc_port}") as channel,
        httpx.AsyncClient(base_url=http_url, trust_env=False, timeout=5) as http,
    ):
        api = stub(channel)
        # The same journal remains readable after switching listener modes.
        assert (await api.Latest(task, timeout=5)).offset == status_offset
        event = {"task_id": "T2", "event_id": "finished", "status": "completed"}
        async with http.stream("GET", "/topics/status/tasks/T2/events") as sse:
            sse.raise_for_status()
            events = sse.aiter_lines()
            assert await anext(events) == "event: ready"
            await api.Append(
                messages.Publish(topic="status", event_json=json.dumps(event).encode()),
                timeout=5,
            )
            lines = [line async for line in events]
            assert "event: status" in lines and "id: finished" in lines


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="spooltail-grpc-") as tmp:
        directory = Path(tmp)
        subprocess.run(
            [
                sys.executable,
                "-m",
                "grpc_tools.protoc",
                f"-I{SCHEMA}",
                f"--python_out={directory}",
                f"--grpc_python_out={directory}",
                str(SCHEMA / "spooltail.proto"),
            ],
            check=True,
        )
        sys.path.insert(0, tmp)
        asyncio.run(
            exercise(
                directory,
                importlib.import_module("spooltail_pb2"),
                importlib.import_module("spooltail_pb2_grpc").SpooltailStub,
            )
        )
    print(
        "PASS: TLS and plaintext gRPC, HTTP/SSE interoperability, leases, cursors, and restart"
    )
