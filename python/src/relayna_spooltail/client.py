"""Async Python API for the Spooltail v1 service."""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from enum import IntEnum
from types import TracebackType
from typing import Any, Self

import grpc

from ._wire import spooltail_pb2 as wire
from ._wire.spooltail_pb2_grpc import SpooltailStub


class SpooltailError(Exception):
    """A broker response that the SDK handles explicitly."""


class ConflictError(SpooltailError):
    """An event ID, lease, or checkpoint conflicted with broker state."""


class StaleLeaseError(ConflictError):
    """The broker has no active lease matching this operation."""


class CursorGapError(SpooltailError):
    """The requested resume cursor is unavailable."""

    def __init__(self, after: str | None, earliest_event_id: str | None) -> None:
        super().__init__(
            f"Cursor {after!r} unavailable (earliest: {earliest_event_id!r})"
        )
        self.after = after
        self.earliest_event_id = earliest_event_id


class SubscriptionError(SpooltailError):
    """The broker ended a subscription with an error delivery."""


@dataclass(frozen=True, slots=True)
class Record:
    offset: int
    event: dict[str, Any]
    partition: int = 0


@dataclass(frozen=True, slots=True)
class StreamBatch:
    topic: str
    group: str
    after: int
    records: tuple[Record, ...]
    partition: int | None = None


@dataclass(frozen=True, slots=True)
class ReplayBatch:
    topic: str
    after: int
    records: tuple[Record, ...]


@dataclass(frozen=True, slots=True)
class Lease:
    topic: str
    group: str
    record: Record
    generation: int
    lease_until_ms: int
    partition: int = 0


class SettlementAction(IntEnum):
    ACK = wire.SETTLEMENT_ACTION_ACK
    RENEW = wire.SETTLEMENT_ACTION_RENEW
    RETRY = wire.SETTLEMENT_ACTION_RETRY
    DEAD_LETTER = wire.SETTLEMENT_ACTION_DEAD_LETTER


@dataclass(frozen=True, slots=True)
class Delivery:
    kind: str
    event_id: str | None
    data: dict[str, Any]


@dataclass(frozen=True, slots=True)
class BrokerStats:
    payload_reads: int
    recovered_frames: int
    free_bytes: int = 0
    disk_reserve_bytes: int = 0
    raw_journal_bytes: int = 0
    packed_journal_bytes: int = 0
    archive_pending_records: int = 0
    packing_space_pauses: int = 0


def _publish(
    topic: str, event: Mapping[str, Any], *, owner_service: str | None = None
) -> wire.Publish:
    data = json.dumps(
        dict(event), ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode("utf-8")
    return wire.Publish(topic=topic, event_json=data, owner_service=owner_service)


def _object(data: bytes) -> dict[str, Any]:
    value = json.loads(data)
    if not isinstance(value, dict):
        raise SpooltailError("Expected a JSON object from the broker")
    return value


def _record(message: wire.Record) -> Record:
    return Record(message.offset, _object(message.event_json), message.partition)


def _task(topic: str, task_id: str, after: str | None = None) -> wire.Task:
    request = wire.Task(topic=topic, task_id=task_id)
    if after is not None:
        request.after = after
    return request


@contextmanager
def _translate_cursor_gap(after: str | None) -> Iterator[None]:
    try:
        yield
    except grpc.aio.AioRpcError as error:
        if error.code() != grpc.StatusCode.FAILED_PRECONDITION:
            raise
        metadata = dict(error.trailing_metadata() or ())
        earliest = metadata.get("spooltail-earliest-event-id-bin")
        raise CursorGapError(
            after, earliest.decode("utf-8") if earliest is not None else None
        ) from error


@contextmanager
def _translate_conflict(stale_detail: str | None = None) -> Iterator[None]:
    try:
        yield
    except grpc.aio.AioRpcError as error:
        if error.code() != grpc.StatusCode.FAILED_PRECONDITION:
            raise
        # The v1 broker reports lease failures and other conflicts with the
        # same status code. Preserve other preconditions as generic conflicts.
        detail = error.details()
        if stale_detail is not None and detail == stale_detail:
            raise StaleLeaseError(detail) from error
        raise ConflictError(detail) from error


class SpooltailClient:
    """A reusable gRPC client. Create and use it on the same asyncio loop."""

    def __init__(
        self,
        target: str,
        *,
        tls: bool = True,
        root_certificates: bytes | None = None,
        timeout: float = 5.0,
    ) -> None:
        """Connect to a gRPC target such as localhost:7443.

        TLS uses the default trust roots unless given PEM-encoded roots.
        The timeout in seconds applies to unary RPCs.
        """
        if not tls and root_certificates is not None:
            raise ValueError("root_certificates requires TLS")
        options = (
            # Match the broker's limit for an encoded history page.
            ("grpc.max_receive_message_length", 70 * 1024 * 1024),
            ("grpc.enable_retries", 0),
        )
        if tls:
            credentials = grpc.ssl_channel_credentials(
                root_certificates=root_certificates
            )
            self._channel = grpc.aio.secure_channel(
                target, credentials, options=options
            )
        else:
            self._channel = grpc.aio.insecure_channel(target, options=options)
        self._stub = SpooltailStub(self._channel)
        self._timeout = timeout

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the channel and cancel outstanding calls."""
        await self._channel.close()

    async def ping(self) -> None:
        """Check that the broker can serve requests."""
        await self._stub.Ping(wire.Empty(), timeout=self._timeout)

    async def publish(
        self, topic: str, event: Mapping[str, Any], *, owner_service: str | None = None
    ) -> int:
        """Append an event, optionally selecting the owner service for a new task."""
        with _translate_conflict():
            response = await self._stub.Append(
                _publish(topic, event, owner_service=owner_service),
                timeout=self._timeout,
            )
        return response.offset

    async def latest(self, topic: str, task_id: str) -> Record | None:
        """Return the latest retained event for a task, if present."""
        try:
            response = await self._stub.Latest(
                _task(topic, task_id), timeout=self._timeout
            )
        except grpc.aio.AioRpcError as error:
            if error.code() == grpc.StatusCode.NOT_FOUND:
                return None
            raise
        return _record(response)

    async def history(
        self, topic: str, task_id: str, *, after: str | None = None
    ) -> list[Record]:
        """Return one page of retained records after the exclusive event ID."""
        with _translate_cursor_gap(after):
            response = await self._stub.History(
                _task(topic, task_id, after), timeout=self._timeout
            )
        return [_record(record) for record in response.records]

    async def audit_history(
        self,
        topic: str,
        task_id: str,
        *,
        after: str | None = None,
        through: int | None = None,
        limit: int = 32,
    ) -> tuple[list[Record], int | None]:
        """Return a bounded page of hot and archived inputs and its snapshot offset."""
        with _translate_cursor_gap(after):
            response = await self._stub.AuditHistory(
                wire.AuditHistoryRequest(
                    topic=topic, task_id=task_id, after=after, through=through, limit=limit
                ),
                timeout=self._timeout,
            )
        return (
            [_record(record) for record in response.records],
            response.through if response.HasField("through") else None,
        )

    async def subscribe(
        self,
        topic: str,
        task_id: str,
        *,
        after: str | None = None,
        timeout: float | None = None,
    ) -> AsyncGenerator[Delivery, None]:
        """Yield ready and status deliveries. Use aclosing when leaving early."""
        call = self._stub.Subscribe(_task(topic, task_id, after), timeout=timeout)
        try:
            async for message in call:
                data = _object(message.data_json)
                if message.kind == "gap":
                    raise CursorGapError(after, data.get("earliest_event_id"))
                if message.kind == "error":
                    raise SubscriptionError(data["error"])
                event_id = message.event_id if message.HasField("event_id") else None
                yield Delivery(message.kind, event_id, data)
        finally:
            call.cancel()

    async def claim(
        self,
        topic: str,
        group: str,
        *,
        lease_ms: int = 30_000,
        partition: int | None = None,
    ) -> Lease | None:
        """Lease the next available work record in a group, if present."""
        response = await self._stub.Claim(
            wire.ClaimRequest(
                topic=topic, group=group, lease_ms=lease_ms, partition=partition
            ),
            timeout=self._timeout,
        )
        if not response.HasField("record"):
            return None
        return Lease(
            topic,
            group,
            _record(response.record),
            response.generation,
            response.lease_until_ms,
            response.partition,
        )

    async def settle(
        self, lease: Lease, action: SettlementAction, *, delay_ms: int = 0
    ) -> None:
        """Acknowledge, renew, retry, or dead-letter a lease."""
        request = wire.SettleRequest(
            topic=lease.topic,
            group=lease.group,
            offset=lease.record.offset,
            generation=lease.generation,
            action=action,
            delay_ms=delay_ms,
        )
        with _translate_conflict("lease expired, settled or superseded"):
            await self._stub.Settle(request, timeout=self._timeout)

    async def complete(
        self, lease: Lease, status_topic: str, event: Mapping[str, Any]
    ) -> int:
        """Atomically acknowledge a lease and publish its task's terminal status."""
        request = wire.CompleteRequest(
            topic=lease.topic,
            group=lease.group,
            offset=lease.record.offset,
            generation=lease.generation,
            status=_publish(status_topic, event),
        )
        with _translate_conflict("no active matching lease for this task"):
            response = await self._stub.Complete(request, timeout=self._timeout)
        return response.offset

    async def next(
        self, topic: str, group: str, *, partition: int | None = None
    ) -> Record | None:
        """Read the next record after a stream group's durable checkpoint."""
        with _translate_conflict():
            response = await self._stub.Next(
                wire.StreamGroup(topic=topic, group=group, partition=partition),
                timeout=self._timeout,
            )
        return _record(response.record) if response.HasField("record") else None

    async def commit(
        self, topic: str, group: str, offset: int, *, partition: int | None = None
    ) -> None:
        """Advance a stream group's checkpoint through its next record."""
        request = wire.CommitRequest(
            topic=topic, group=group, offset=offset, partition=partition
        )
        with _translate_conflict():
            await self._stub.Commit(request, timeout=self._timeout)

    async def next_batch(
        self, topic: str, group: str, *, limit: int = 32, partition: int | None = None
    ) -> StreamBatch:
        """Fetch up to 64 records after a group's durable checkpoint."""
        if not 1 <= limit <= 64:
            raise ValueError("limit must be between 1 and 64")
        request = wire.StreamBatchRequest(
            topic=topic, group=group, partition=partition, limit=limit
        )
        with _translate_conflict():
            response = await self._stub.NextBatch(request, timeout=self._timeout)
        return StreamBatch(
            topic, group, response.after, tuple(map(_record, response.records)), partition
        )

    async def commit_batch(self, batch: StreamBatch, *, count: int | None = None) -> None:
        """Commit all records or a processed prefix of a fetched batch."""
        if count is None:
            count = len(batch.records)
        if not 1 <= count <= len(batch.records):
            raise ValueError("count must select a nonempty prefix of the batch")
        request = wire.StreamBatchCommit(
            topic=batch.topic,
            group=batch.group,
            partition=batch.partition,
            after=batch.after,
            through=batch.records[count - 1].offset,
            count=count,
        )
        with _translate_conflict():
            await self._stub.CommitBatch(request, timeout=self._timeout)

    async def read_after_batch(
        self, topic: str, *, after: int | None = None, limit: int = 32
    ) -> ReplayBatch:
        """Fetch topic records for a consumer that stores its own checkpoint."""
        if not 1 <= limit <= 64:
            raise ValueError("limit must be between 1 and 64")
        request = wire.TopicReplayRequest(topic=topic, after=after, limit=limit)
        with _translate_conflict():
            response = await self._stub.ReadAfterBatch(request, timeout=self._timeout)
        return ReplayBatch(topic, response.after, tuple(map(_record, response.records)))

    async def list_partitions(self) -> list[int]:
        """List the broker's durable partition IDs."""
        response = await self._stub.ListPartitions(wire.Empty(), timeout=self._timeout)
        return list(response.ids)

    async def ensure_partition(self, partition: int) -> None:
        """Create a partition if its ID is new."""
        await self._stub.EnsurePartition(
            wire.Partition(id=partition), timeout=self._timeout
        )

    async def task_partition(self, task_id: str) -> int:
        """Read a task's current placement."""
        response = await self._stub.TaskPartition(
            wire.TaskId(task_id=task_id), timeout=self._timeout
        )
        return response.id

    async def move_task(
        self, task_id: str, from_partition: int, to_partition: int
    ) -> None:
        """Change the placement of a task and its work claims."""
        request = wire.MoveTaskRequest(
            task_id=task_id, from_partition=from_partition, to_partition=to_partition
        )
        with _translate_conflict():
            await self._stub.MoveTask(request, timeout=self._timeout)

    async def stats(self) -> BrokerStats:
        """Read process counters and a snapshot of journal storage."""
        response = await self._stub.Stats(wire.Empty(), timeout=self._timeout)
        return BrokerStats(
            response.payload_reads,
            response.recovered_frames,
            response.free_bytes,
            response.disk_reserve_bytes,
            response.raw_journal_bytes,
            response.packed_journal_bytes,
            response.archive_pending_records,
            response.packing_space_pauses,
        )
