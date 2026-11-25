"""Replay records with deferred attribute decoding, and packed numeric projections."""

from __future__ import annotations

import json
import struct
from dataclasses import dataclass, field
from threading import Lock
from typing import Any

from ._wire import spooltail_pb2 as wire

try:
    from spooltail_replay_native import decode_object
except ImportError:

    def _value(atom: wire.ReplayValue) -> Any:
        kind = atom.WhichOneof("kind")
        if kind == "null_value":
            return None
        if kind == "array":
            return [_value(item) for item in atom.array.items]
        if kind == "object":
            return _fields(atom.object)
        if kind is None:
            raise ValueError("missing replay value")
        return getattr(atom, kind)

    def _fields(obj: wire.ReplayObject) -> dict[str, Any]:
        return {item.key: _value(item.value) for item in obj.fields}

    def decode_object(data: bytes) -> dict[str, Any]:
        return _fields(wire.ReplayObject.FromString(data))


class _EventStorage:
    __slots__ = ("_data", "_lock")
    _data: dict[str, Any] | wire.ReplayEvent

    @property
    def event(self) -> dict[str, Any]:
        data = self._data
        if isinstance(data, dict):
            return data
        # Concurrent first accesses must observe the same mutable dictionary.
        with self._lock:
            data = self._data
            if isinstance(data, dict):
                return data
            kind = data.WhichOneof("attributes")
            if kind == "object_proto":
                obj = decode_object(data.object_proto)
            elif kind == "object_json":
                obj = json.loads(data.object_json)
            else:
                obj = {}
            obj["task_id"] = data.task_id
            obj["event_id"] = data.event_id
            if data.HasField("payload"):
                obj["payload"] = data.payload
            object.__setattr__(self, "_data", obj)
            return obj


@dataclass(frozen=True, init=False)
class Record(_EventStorage):
    """A record's IDs are available before its mutable event dictionary is materialized."""

    __slots__ = ("offset", "partition")
    offset: int
    event: dict[str, Any] = field()
    partition: int

    def __init__(self, offset: int, event: dict[str, Any], partition: int = 0):
        object.__setattr__(self, "offset", offset)
        object.__setattr__(self, "partition", partition)
        object.__setattr__(self, "_data", event)

    @classmethod
    def _from_wire(cls, message: wire.ReplayEvent, lock) -> Record:
        record = object.__new__(cls)
        object.__setattr__(record, "offset", message.offset)
        object.__setattr__(record, "partition", message.partition)
        object.__setattr__(record, "_data", message)
        object.__setattr__(record, "_lock", lock)
        return record

    @property
    def task_id(self) -> str:
        data = self._data
        return data["task_id"] if isinstance(data, dict) else data.task_id

    @property
    def event_id(self) -> str:
        data = self._data
        return data["event_id"] if isinstance(data, dict) else data.event_id

    def __reduce__(self):
        return type(self), (self.offset, self.event, self.partition)

    def __setstate__(self, state):
        # Older frozen/slots dataclasses pickled a list of field values.
        self.__init__(*state)


def replay_records(page: wire.ReplayPage) -> tuple[Record, ...]:
    lock = Lock()
    return tuple(Record._from_wire(message, lock) for message in page.records)


@dataclass(frozen=True, slots=True)
class NumericRow:
    offset: int
    partition: int
    task_id: str
    event_id: str
    count: int


@dataclass(frozen=True, slots=True)
class NumericBatch:
    """Rows delimit consecutive groups in the little-endian float64 buffer."""

    topic: str
    after: int
    records: tuple[NumericRow, ...]
    values_f64_le: bytes

    def __post_init__(self) -> None:
        if any(row.count < 0 for row in self.records) or (
            len(self.values_f64_le) != sum(row.count for row in self.records) * 8
        ):
            raise ValueError("invalid numeric replay column length")

    def sums(self) -> tuple[float, ...]:
        """Return a sum for each row, including zero for an empty group."""
        start = 0
        result = []
        for row in self.records:
            values = struct.unpack_from(f"<{row.count}d", self.values_f64_le, start)
            result.append(sum(values, 0.0))
            start += row.count * 8
        return tuple(result)

    def as_numpy(self):
        """Return a read-only flat array view. Each row specifies its element count."""
        import numpy as np

        return np.frombuffer(self.values_f64_le, dtype="<f8")

    def numpy_sums(self):
        """Reduce the packed column by row, preserving empty groups."""
        import numpy as np

        values = self.as_numpy()
        counts = np.fromiter(
            (r.count for r in self.records), dtype=np.intp, count=len(self.records)
        )
        result = np.zeros(len(counts), dtype=np.float64)
        used = counts != 0
        if used.any():
            starts = counts.cumsum() - counts
            result[used] = np.add.reduceat(values, starts[used])
        return result
