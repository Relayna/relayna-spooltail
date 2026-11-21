"""Python clients for the measured variants, with a shared application result shape."""

import json
import os
import ssl
from contextlib import asynccontextmanager
from functools import lru_cache
from pathlib import Path

WIRES = ("http1", "http-uds", "http2", "grpc", "grpc-uds")


class Rejected(Exception):
    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


def json_bytes(event):
    return json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode()


class HttpClient:
    def __init__(self, http, version):
        self.http = http
        self.version = version

    async def ping(self):
        reply = await self.http.get("/bench/ping")
        if reply.status_code != 204 or reply.http_version != self.version:
            raise RuntimeError(
                f"expected {self.version} ping, got {reply.http_version} {reply.status_code}"
            )

    async def request(self, method, path, **kwargs):
        reply = await self.http.request(method, path, **kwargs)
        if reply.status_code == 404:
            raise Rejected("missing")
        if reply.status_code in (409, 410):
            raise Rejected("precondition")
        if reply.status_code == 400:
            raise Rejected("invalid")
        reply.raise_for_status()
        return reply.json()

    async def latest(self, topic, task):
        try:
            row = await self.request("GET", f"/topics/{topic}/tasks/{task}/latest")
        except Rejected as error:
            if error.reason == "missing":
                return None
            raise
        return row["offset"], row["event"]

    async def history(self, topic, task, after=None):
        params = {"after": after} if after is not None else None
        rows = await self.request(
            "GET", f"/topics/{topic}/tasks/{task}/history", params=params
        )
        return [(row["offset"], row["event"]) for row in rows]

    async def append(self, topic, event):
        return (await self.request("POST", f"/topics/{topic}/records", json=event))[
            "offset"
        ]

    async def claim(self, topic, group):
        claim = await self.request(
            "POST", f"/topics/{topic}/work/{group}/claim", json={"lease_ms": 30000}
        )
        if claim is None:
            return None
        return claim["record"]["offset"], claim["generation"], claim["record"]["event"]

    async def complete(self, topic, group, offset, generation, status_topic, event):
        return (
            await self.request(
                "POST",
                f"/topics/{topic}/work/{group}/complete",
                json={
                    "offset": offset,
                    "generation": generation,
                    "status_topic": status_topic,
                    "event": event,
                },
            )
        )["offset"]

    async def subscribe(self, topic, task, after=None):
        headers = {"Last-Event-ID": after} if after is not None else {}
        async with self.http.stream(
            "GET", f"/topics/{topic}/tasks/{task}/events", headers=headers
        ) as reply:
            reply.raise_for_status()
            fields = {}
            async for line in reply.aiter_lines():
                if line:
                    key, _, value = line.partition(":")
                    fields[key] = value.removeprefix(" ")
                elif "event" in fields:
                    yield fields["event"], fields.get("id"), json.loads(fields["data"])
                    fields.clear()
                else:
                    fields.clear()


class GrpcClient:
    def __init__(self, grpc, stub, message):
        self.grpc = grpc
        self.stub = stub
        self.message = message

    async def call(self, method, payload):
        try:
            return await method(payload, timeout=20)
        except self.grpc.aio.AioRpcError as error:
            codes = self.grpc.StatusCode
            reasons = {
                codes.NOT_FOUND: "missing",
                codes.FAILED_PRECONDITION: "precondition",
                codes.INVALID_ARGUMENT: "invalid",
            }
            if error.code() in reasons:
                raise Rejected(reasons[error.code()]) from error
            raise

    async def ping(self):
        await self.call(self.stub.Ping, self.message.Empty())

    @staticmethod
    def record(row):
        return row.offset, json.loads(row.event_json)

    async def latest(self, topic, task):
        try:
            return self.record(
                await self.call(
                    self.stub.Latest, self.message.Task(topic=topic, task_id=task)
                )
            )
        except Rejected as error:
            if error.reason == "missing":
                return None
            raise

    async def history(self, topic, task, after=None):
        page = await self.call(
            self.stub.History, self.message.Task(topic=topic, task_id=task, after=after)
        )
        return [self.record(row) for row in page.records]

    async def append(self, topic, event):
        return (
            await self.call(
                self.stub.Append,
                self.message.Publish(topic=topic, event_json=json_bytes(event)),
            )
        ).offset

    async def claim(self, topic, group):
        reply = await self.call(
            self.stub.Claim,
            self.message.ClaimRequest(topic=topic, group=group, lease_ms=30000),
        )
        if not reply.HasField("record"):
            return None
        return (
            reply.record.offset,
            reply.generation,
            json.loads(reply.record.event_json),
        )

    async def complete(self, topic, group, offset, generation, status_topic, event):
        return (
            await self.call(
                self.stub.Complete,
                self.message.CompleteRequest(
                    topic=topic,
                    group=group,
                    offset=offset,
                    generation=generation,
                    status=self.message.Publish(
                        topic=status_topic, event_json=json_bytes(event)
                    ),
                ),
            )
        ).offset

    async def subscribe(self, topic, task, after=None):
        call = self.stub.Subscribe(
            self.message.Task(topic=topic, task_id=task, after=after), timeout=20
        )
        try:
            async for item in call:
                yield (
                    item.kind,
                    item.event_id if item.HasField("event_id") else None,
                    json.loads(item.data_json),
                )
        finally:
            call.cancel()


@lru_cache
def tls_context(ca):
    return ssl.create_default_context(cafile=ca)


@lru_cache
def grpc_credentials(ca):
    import grpc

    return grpc.ssl_channel_credentials(root_certificates=Path(ca).read_bytes())


@asynccontextmanager
async def client(wire, readers=1, *, cold=False):
    ca = os.environ.get("SPOOLTAIL_BENCH_TLS_CA")
    endpoint = os.environ.get("SPOOLTAIL_BENCH_TARGET", f"{wire}:7474")
    if wire.startswith("grpc"):
        import grpc
        import spooltail_pb2
        from spooltail_pb2_grpc import SpooltailStub

        target = "unix:/sockets/grpc.sock" if wire == "grpc-uds" else endpoint
        options = [("grpc.max_receive_message_length", 70 * 1024**2)]
        if wire == "grpc-uds":
            options.append(("grpc.default_authority", "localhost"))
        if cold:
            options.append(("grpc.use_local_subchannel_pool", 1))
        channel = (
            grpc.aio.secure_channel(target, grpc_credentials(ca), options=options)
            if ca
            else grpc.aio.insecure_channel(target, options=options)
        )
        async with channel:
            yield GrpcClient(grpc, SpooltailStub(channel), spooltail_pb2)
    else:
        import httpx

        transport = httpx.AsyncHTTPTransport(
            uds="/sockets/http.sock" if wire == "http-uds" else None,
            http1=wire != "http2",
            http2=wire == "http2",
            verify=tls_context(ca) if ca else True,
            trust_env=False,
            limits=httpx.Limits(
                max_connections=readers, max_keepalive_connections=readers
            ),
        )
        async with httpx.AsyncClient(
            base_url=f"{'https' if ca else 'http'}://{endpoint}",
            transport=transport,
            timeout=20,
            trust_env=False,
        ) as http:
            yield HttpClient(http, "HTTP/2" if wire == "http2" else "HTTP/1.1")
