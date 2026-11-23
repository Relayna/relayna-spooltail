"""A local SQLite outcome store and a per-task timeline over it and Spooltail."""

from __future__ import annotations

import hashlib
import heapq
import inspect
import json
import sqlite3
from collections.abc import Callable, Mapping, Sequence
from dataclasses import astuple, dataclass
from os import PathLike
from types import TracebackType
from typing import Any, Protocol, Self
from uuid import uuid4

from .client import ConflictError, CursorGapError, Record, ReplayBatch, SpooltailClient


@dataclass(frozen=True, slots=True)
class AuditOutcome:
    """An application outcome linked to a source record in the batch."""

    source_offset: int
    operation: str
    outcome: str
    actor: str
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class AuditEntry:
    """An input or a committed outcome. Both use the broker's source offset."""

    kind: str
    source_offset: int
    source_event_id: str
    event: dict[str, Any] | None = None
    outcome: AuditOutcome | None = None
    consumer: str | None = None
    input_before_retained_history: bool = False


@dataclass(frozen=True, slots=True)
class AuditCursor:
    """A continuation for the same task and outcome store."""

    store_id: str
    topic: str
    task_id: str
    input_through: int | None
    outcome_through: int
    earliest_input_offset: int | None
    input_after: str | None = None
    input_done: bool = False
    outcome_after: tuple[int, int] = (0, 0)


@dataclass(frozen=True, slots=True)
class AuditPage:
    entries: tuple[AuditEntry, ...]
    cursor: AuditCursor | None


class StateStatement(Protocol):
    def __call__(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] = (), /
    ) -> list[tuple[Any, ...]]: ...


StateChanges = Callable[[StateStatement], None]

_STATE_READS = frozenset({
    sqlite3.SQLITE_READ, sqlite3.SQLITE_SELECT, sqlite3.SQLITE_FUNCTION,
    sqlite3.SQLITE_RECURSIVE,
})
_STATE_WRITES = frozenset({sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE})


def _authorize_state_sql(
    action: int,
    table: str | None,
    _column: str | None,
    database: str | None,
    _origin: str | None,
) -> int:
    # The callback can query and update application tables with the same transaction.
    # Transaction control, schema changes, and writes to SDK tables stay with the SDK.
    if action in _STATE_READS or (
        action in _STATE_WRITES and database == "main" and table is not None
        and not table.startswith(("spooltail_audit_", "sqlite_"))
    ):
        return sqlite3.SQLITE_OK
    return sqlite3.SQLITE_DENY


def _input_entry(record: Record) -> AuditEntry:
    return AuditEntry("input", record.offset, record.event["event_id"], event=record.event)


class AuditStore:
    """Store outcomes and replay checkpoints for one Spooltail data directory."""

    def __init__(self, path: str | PathLike[str]) -> None:
        self._db = sqlite3.connect(path)
        try:
            if self._db.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal":
                raise ValueError("an audit store requires an on-disk SQLite database in WAL mode")
            self._db.executescript("""
                PRAGMA synchronous=FULL;
                PRAGMA foreign_keys=ON;
                CREATE TABLE IF NOT EXISTS spooltail_audit_store (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1), id TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS spooltail_audit_checkpoint (
                    topic TEXT NOT NULL, consumer TEXT NOT NULL, offset INTEGER NOT NULL,
                    receipt TEXT NOT NULL, PRIMARY KEY(topic, consumer)
                );
                CREATE TABLE IF NOT EXISTS spooltail_audit_outcome (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL, task_id TEXT NOT NULL, consumer TEXT NOT NULL,
                    source_offset INTEGER NOT NULL, source_event_id TEXT NOT NULL,
                    operation TEXT NOT NULL, outcome TEXT NOT NULL, actor TEXT NOT NULL,
                    reason TEXT,
                    UNIQUE(topic, consumer, source_offset, operation)
                );
                CREATE INDEX IF NOT EXISTS spooltail_audit_task_order
                    ON spooltail_audit_outcome(topic, task_id, source_offset, id);
                CREATE INDEX IF NOT EXISTS spooltail_audit_task_head
                    ON spooltail_audit_outcome(topic, task_id, id);
            """)
            with self._db:
                self._db.execute(
                    "INSERT OR IGNORE INTO spooltail_audit_store VALUES (1,?)",
                    (uuid4().hex,),
                )
            self._id = self._db.execute(
                "SELECT id FROM spooltail_audit_store WHERE singleton=1"
            ).fetchone()[0]
        except BaseException:
            self.close()
            raise

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self._db.close()

    def _checkpoint(self, topic: str, consumer: str) -> tuple[int, str] | None:
        return self._db.execute(
            "SELECT offset, receipt FROM spooltail_audit_checkpoint WHERE topic=? AND consumer=?",
            (topic, consumer),
        ).fetchone()

    def checkpoint(self, topic: str, consumer: str) -> int | None:
        """Return the saved offset, or None for a consumer with no checkpoint."""
        saved = self._checkpoint(topic, consumer)
        return saved[0] if saved else None

    def _apply_state(self, callback: StateChanges) -> None:
        active = True

        def execute(
            sql: str, params: Sequence[Any] | Mapping[str, Any] = ()
        ) -> list[tuple[Any, ...]]:
            if not active or not self._db.in_transaction:
                raise RuntimeError("audit state transaction is closed")
            return self._db.execute(sql, params).fetchall()

        self._db.set_authorizer(_authorize_state_sql)
        try:
            result = callback(execute)
            if inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()
                raise TypeError("apply_state must be synchronous")
            if not self._db.in_transaction:
                raise RuntimeError("apply_state ended the audit transaction")
        finally:
            active = False
            self._db.set_authorizer(None)

    def commit_batch(
        self,
        consumer: str,
        batch: ReplayBatch,
        outcomes: Sequence[AuditOutcome],
        *,
        apply_state: StateChanges | None = None,
    ) -> bool:
        """Save a fetched batch. Return False if the same commit already succeeded.

        The optional synchronous callback uses the provided SQL executor to write
        application tables in this transaction. Initialize those tables separately.
        """
        if not consumer or not batch.records or batch.after < 0:
            raise ValueError("a consumer and a nonempty replay batch are required")
        records = {record.offset: record for record in batch.records}
        offsets = [record.offset for record in batch.records]
        if offsets != sorted(records) or offsets[0] <= batch.after:
            raise ValueError("batch offsets must be strictly increasing after the checkpoint")

        rows = []
        for outcome in outcomes:
            record = records.get(outcome.source_offset)
            if record is None:
                raise ValueError("outcome must reference a record in the batch")
            if not outcome.operation or not outcome.outcome or not outcome.actor:
                raise ValueError("operation, outcome, and actor are required")
            rows.append((batch.topic, record.event["task_id"], consumer, outcome.source_offset,
                         record.event["event_id"], outcome.operation, outcome.outcome,
                         outcome.actor, outcome.reason))
        through = offsets[-1]
        receipt = hashlib.sha256(json.dumps(
            [batch.after, [(r.offset, r.partition, r.event["task_id"], r.event["event_id"])
                           for r in batch.records], [astuple(o) for o in outcomes]],
            separators=(",", ":"),
        ).encode()).hexdigest()
        with self._db:
            self._db.execute("BEGIN IMMEDIATE")
            saved = self._checkpoint(batch.topic, consumer)
            if saved and saved[0] == through:
                if saved[1] != receipt:
                    raise ConflictError("different audit retry for the same checkpoint")
                return False
            if saved and saved[0] != batch.after:
                raise ConflictError("audit checkpoint changed since fetch")
            if apply_state is not None:
                self._apply_state(apply_state)
            self._db.executemany(
                """INSERT INTO spooltail_audit_outcome
                   (topic,task_id,consumer,source_offset,source_event_id,operation,outcome,actor,reason)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                rows,
            )
            self._db.execute(
                """INSERT INTO spooltail_audit_checkpoint VALUES (?,?,?,?)
                   ON CONFLICT(topic,consumer) DO UPDATE
                   SET offset=excluded.offset, receipt=excluded.receipt""",
                (batch.topic, consumer, through, receipt),
            )
        return True

    def _head(self, topic: str, task_id: str) -> int:
        return self._db.execute(
            "SELECT COALESCE(MAX(id),0) FROM spooltail_audit_outcome WHERE topic=? AND task_id=?",
            (topic, task_id),
        ).fetchone()[0]

    def _outcomes(self, cursor: AuditCursor, limit: int) -> list[tuple[int, AuditEntry]]:
        rows = self._db.execute(
            """SELECT id,source_offset,source_event_id,consumer,operation,outcome,actor,reason
               FROM spooltail_audit_outcome WHERE topic=? AND task_id=? AND id<=?
               AND (source_offset,id)>(?,?) ORDER BY source_offset,id LIMIT ?""",
            (cursor.topic, cursor.task_id, cursor.outcome_through, *cursor.outcome_after, limit),
        )
        return [
            (row_id, AuditEntry(
                "outcome", offset, event_id,
                outcome=AuditOutcome(offset, operation, outcome, actor, reason), consumer=consumer,
                input_before_retained_history=(
                    cursor.earliest_input_offset is None or offset < cursor.earliest_input_offset
                ),
            ))
            for row_id, offset, event_id, consumer, operation, outcome, actor, reason in rows
        ]


class AuditTimeline:
    """Read hot and archived inputs and local outcomes in source-offset order."""

    def __init__(self, client: SpooltailClient, store: AuditStore) -> None:
        self._client = client
        self._store = store

    async def read(
        self,
        topic: str,
        task_id: str,
        *,
        limit: int = 16,
        cursor: AuditCursor | None = None,
    ) -> AuditPage:
        """Read a page. Subsequent pages use the first page's input and outcome bounds."""
        if not 1 <= limit <= 64:
            raise ValueError("limit must be between 1 and 64")
        if cursor is None:
            watermark = self._store._head(topic, task_id)
            inputs, through = await self._client.audit_history(
                topic, task_id, limit=limit
            )
            cursor = AuditCursor(
                self._store._id, topic, task_id, through,
                watermark, inputs[0].offset if inputs else None,
                input_done=through is None,
            )
        else:
            if (cursor.store_id, cursor.topic, cursor.task_id) != (
                self._store._id, topic, task_id
            ):
                raise ValueError("audit cursor belongs to another task or store")
            inputs = []
            if not cursor.input_done:
                inputs, _ = await self._client.audit_history(
                    topic, task_id, after=cursor.input_after,
                    through=cursor.input_through, limit=limit,
                )
                if not inputs or (
                    cursor.input_after is None and inputs[0].offset != cursor.earliest_input_offset
                ):
                    earliest = inputs[0].event["event_id"] if inputs else None
                    raise CursorGapError(cursor.input_after, earliest)
        outcomes = self._store._outcomes(cursor, limit + 1)
        if inputs and inputs[-1].offset != cursor.input_through:
            # A byte-limited input page may end before the next outcome's source.
            outcomes = [row for row in outcomes if row[1].source_offset <= inputs[-1].offset]
        merged = list(heapq.merge(
            ((0, _input_entry(record)) for record in inputs), outcomes,
            key=lambda row: (row[1].source_offset, row[0]),
        ))
        selected = merged[:limit]
        input_after = cursor.input_after
        input_done = cursor.input_done
        outcome_after = cursor.outcome_after
        for row_id, entry in selected:
            if row_id:
                outcome_after = (entry.source_offset, row_id)
            else:
                input_after = entry.source_event_id
                input_done = entry.source_offset == cursor.input_through
        continuation = None
        if len(merged) > limit or not input_done:
            continuation = AuditCursor(
                cursor.store_id, topic, task_id, cursor.input_through, cursor.outcome_through,
                cursor.earliest_input_offset, input_after, input_done, outcome_after,
            )
        return AuditPage(tuple(entry for _, entry in selected), continuation)
