# Relayna Spooltail Python SDK

This async gRPC client supports Python 3.11 and later.

## Install from GitHub Releases

Use a GitHub account that can read this private repository. After authenticating `gh`, download a pinned version and install the wheel into your application's environment. Run the `uv pip` command in the same environment where Relayna is installed if using it with Relayna.

```sh
sdk_version=0.0.1
sdk_wheel="relayna_spooltail-${sdk_version}-py3-none-any.whl"
gh release download "v${sdk_version}" --repo sarattha/relayna-spooltail \
  --pattern "$sdk_wheel"
uv pip install "./$sdk_wheel"
```

For automation in another repository, supply `GH_TOKEN` in the download step with a token that has read access to the Spooltail repository's contents. The consuming repository's default GitHub Actions token is scoped to that repository. The released wheel can be installed without a Spooltail checkout or an SDK build step. For a uv project that manages its own lockfile, keep the downloaded wheel in a reproducible location and add it as a path dependency with `uv add ./relayna_spooltail-0.0.1-py3-none-any.whl`.

## Use the client

Start the broker with [gRPC and TLS](../README.md#enable-grpc). Pass the broker CA certificate as PEM bytes for a private CA. Omitting `root_certificates` uses gRPC's default trust roots. For a loopback broker started in plaintext mode, pass `tls=False`. Save the example as `example.py` and run `python example.py` in the environment where the wheel is installed.

```python
import asyncio
from pathlib import Path

from relayna_spooltail import SpooltailClient


async def main():
    async with SpooltailClient(
        "localhost:7443", root_certificates=Path("ca.pem").read_bytes()
    ) as client:
        await client.publish("work", {"task_id": "T7", "event_id": "T7-queued"})
        lease = await client.claim("work", "workers")
        if lease is None:
            return
        task_id = lease.record.event["task_id"]
        await client.publish(
            "status",
            {"task_id": task_id, "event_id": f"{task_id}-running", "status": "running"},
        )
        await client.complete(
            lease,
            "status",
            {"task_id": task_id, "event_id": f"{task_id}-done", "status": "completed"},
        )


asyncio.run(main())
```

A browser that already received `T7-running` can resume the same task over SSE with `Last-Event-ID: T7-running`, including after restarting the broker with the same data directory.

- `publish` and `complete` return an offset. Unary calls default to a five-second timeout. Retries are caller-controlled. On an ambiguous result, retry with the same event ID and payload, and for completion reuse the same lease.
- `claim` returns a lease or `None`. Pass the lease to `complete` or to `settle` with a `SettlementAction`. An inactive lease raises `StaleLeaseError`. Other state conflicts raise `ConflictError`. Transport errors retain their gRPC status.
- `history` returns one page. Use the last record's `event["event_id"]` as the next page's `after` cursor. `subscribe` yields `ready` and `status` deliveries. Use `contextlib.aclosing` if leaving a subscription early. Both methods raise `CursorGapError` for an unavailable cursor and expose `earliest_event_id`, which can be `None`.
- `next` and `commit` let an independent consumer read and checkpoint status records in order.

For catch-up, use `next_batch(topic, group, limit=32)`. Process the returned `batch.records` in order, then call `await client.commit_batch(batch)`. Pass `count=n` to checkpoint only the first `n` records after their effects are durable. A page with no records needs no commit. The broker persists the cursor, so reconnecting with the same topic, group, and partition resumes from the last commit.

## Audit a task

The SDK joins retained journal inputs with outcomes from a local `AuditStore`. Use a separate on-disk `consumer.sqlite` for application state and audit records, and bind that file to one broker data directory. Several consumers can record outcomes in the file, each with a separate checkpoint per topic. Pass `None` to `read_after_batch` when you have no saved checkpoint and want to start at the earliest retained record.

```python
from relayna_spooltail import AuditOutcome, AuditStore, AuditTimeline


async def project_status(client):
    topic, consumer = "status", "policy-worker"
    with AuditStore("consumer.sqlite") as store:
        after = store.checkpoint(topic, consumer)
        batch = await client.read_after_batch(topic, after=after)
        if batch.records:
            outcomes = [
                AuditOutcome(record.offset, "policy_check", "rejected",
                             actor="policy-worker", reason="policy-v2")
                for record in batch.records
            ]
            store.commit_batch(consumer, batch, outcomes)

        timeline = AuditTimeline(client, store)
        page = await timeline.read(topic, "T7", limit=16)
        for entry in page.entries:
            print(entry.kind, entry.source_event_id, entry.event or entry.outcome)
        if page.cursor is not None:
            next_page = await timeline.read(topic, "T7", cursor=page.cursor)
            for entry in next_page.entries:
                print(entry.kind, entry.source_event_id, entry.event or entry.outcome)
```

For application state in the same file, initialize your application tables before processing. Pass a synchronous `apply_state(execute)` callback to `commit_batch` and use its executor for application queries and writes. The SDK commits those changes, the outcomes, and the cursor together. Pass an empty outcomes list to advance over records that produce no audit entries. Retrying the same committed batch returns `False` and skips the callback. External effects need their own idempotency handling.

The timeline orders entries by source offset, with inputs before outcomes at the same offset. The producer provides the event ID, and the consumer supplies the outcome actor. The timeline uses the broker's `AuditHistory` RPC to read originals from hot history and the Zstd archive, when a [topic retention policy](../README.md#configure-audit-input-retention) is configured. Deploy a broker that supports this RPC when upgrading the SDK.

Follow `page.cursor` with the same task and store to keep the first page's input and outcome bounds. A fresh read sees later arrivals. The cursor stays valid while its event moves into the archive. If retention removes the input cursor between pages, the SDK raises `CursorGapError`. A fresh read marks local outcomes whose original input is outside retained history.

## Partitions

Administrators can call `list_partitions()`, `ensure_partition(id)`, `task_partition(task_id)`, and `move_task(task_id, from_partition, to_partition)`. Pass `partition=id` to `claim()` to claim tasks assigned to that partition. For partition-scoped replay, pass it to both `next()` and `commit()`, or to `next_batch()` before committing the returned batch.

`Record.partition` identifies the journal that stored a record. `Lease.partition` identifies the placement at claim time. A moved task's old work record can have a different original partition. Numeric offsets are still globally unique, and `settle()` and `complete()` accept leases issued before the move. See [broker partition operations](../README.md#add-partitions-and-move-tasks) for checkpoint behavior.

For service-based placement, pass `owner_service="urgent"` to the first `publish()` for a task and configure that service in the broker's `SPOOLTAIL_SERVICE_PARTITIONS`. Other services can publish updates with the same task ID. The broker uses the saved placement.

## Development

Run these commands from the repository root.

```sh
uv sync --project python --locked
uv run --project python --locked python python/generate_wire.py
uv build --project python
```

The generator reads the broker's Protobuf schema. The build writes the source distribution and wheel to `python/dist`.

## Publish a version

Set `project.version` in `python/pyproject.toml` to the next three-part [semantic version](https://semver.org/), regenerate `python/uv.lock` with `uv lock --project python`, and merge the version change. On an up-to-date `main`, create and push an annotated `vMAJOR.MINOR.PATCH` tag matching that version.

```sh
sdk_version=0.0.2
git tag -a "v${sdk_version}" -m "Spooltail v${sdk_version}"
git push origin "refs/tags/v${sdk_version}"
```

GitHub Actions builds the source distribution and wheel, tests the installed wheel against a broker built from the tagged revision, and attaches both distributions to a versioned GitHub Release.

### Saved-offset catch-up with one page ahead

`AuditStore.catch_up` starts at the consumer's saved SQLite cursor. While one worker applies a page and commits its outcomes, the SDK fetches the next page. The default page size is 32 and the maximum is 64, subject to the broker's 2 MiB limit.

```python
from relayna_spooltail import AuditOutcome, AuditStore

def process(page):
    return [
        AuditOutcome(r.offset, "index", "applied", "worker", r.event_id)
        for r in page.records
    ]

saved = await AuditStore.catch_up(
    "audit.sqlite", "status", "indexer",
    read_page=client.read_after_batch, process=process, limit=32,
)
```

The call stops on an empty page. Set `max_records` to bound a run under continuous publishing. It returns the persisted offset, or `None` if the consumer has never committed. The synchronous `process(page)` runs on the writer thread and may be called again after interruption. Use `apply_state(page, execute)` for application SQL that must commit atomically with the outcomes and cursor. A failed commit discards the prefetched page. Cancellation waits for an already-running transaction to settle. On restart the SDK reads the durable cursor again.

### Reading Protobuf events

On a compatible broker, `read_after_batch` returns records with directly accessible `record.task_id` and `record.event_id`. Accessing `record.event` decodes the remaining fields once and returns the same mutable dictionary on subsequent accesses. Numeric precision, lists, and nulls are preserved. Older brokers automatically use the original JSON-based replay RPC. Both protocols read the existing journal format.

For applications that materialize deeply nested events, the optional `relayna-spooltail-native` companion decodes Protobuf attributes in C++. Install the matching native source distribution from the SDK release alongside the regular SDK, or build it from the repository root with `python -m pip install .`. Building the companion requires a C++17 toolchain, CMake, `protoc` and matching libprotobuf development libraries. Its wheel links to the system libprotobuf. The SDK automatically uses the extension when importable and otherwise decodes in Python.

### Numeric projections

For a consumer that only needs one numeric field, `read_numeric_batch` returns event IDs, task IDs, offsets and row counts, followed by one contiguous little-endian float64 column. The array and value selectors are literal object keys.

```python
from functools import partial

read_rates = partial(client.read_numeric_batch, array_field="samples", value_field="rate")
page = await read_rates("status", after=saved, limit=64)
per_event = page.sums()          # Python tuple, including 0.0 for an empty array
values = page.as_numpy()        # read-only flat NumPy view
per_event_array = page.numpy_sums()
```

Install the SDK's `numpy` extra or NumPy separately to use the array methods. `sums()` uses the standard library. Pass `read_page=read_rates` to `AuditStore.catch_up` to use the same read-ahead and checkpoint machinery. This RPC requires an upgraded broker. All selected samples must contain a float64-representable number. A page may end before an incompatible record, and a subsequent read at that boundary reports its offset. A numeric consumer can then use ordinary event replay to inspect the record. Floating-point reduction order can differ between NumPy and Python.
