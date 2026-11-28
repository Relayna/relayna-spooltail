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
- `await client.request_cancellation("work", "T7", status_topic="status", cancel_topic="control")` returns the durable intent offset. Configure the worker to watch the same control topic. Read the status topic for the worker's terminal result. A first request for an already-terminal task raises `ConflictError`.
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

## Choosing a replay reader

Choose based on what the consumer needs to **finish processing one event**. The native decoder builds Python objects on the consumer. A numeric projection selects values on the broker before it sends the page.

| What your handler needs | What to use | Example |
| --- | --- | --- |
| Only IDs, offsets, or partitions | Use `read_after_batch` and access `record.event_id`, `record.task_id`, `record.offset`, or `record.partition` directly. | An audit consumer records which event it processed. |
| Complete events, occasionally or with a simple shape | Start with the portable SDK. Access `record.event` when the handler needs its contents. | A router inspects the full event only for selected tasks. |
| Complete nested dictionaries and lists for most events | Use `read_after_batch`. Add the **optional native decoder** when profiling shows that building those objects takes significant consumer time. | A validator walks every nested sample, including flags, tags, and large integer IDs. |
| One numeric field across an array, plus each event’s identity | Use **`read_numeric_batch`**. Start with `sums()` for per-event totals. Add NumPy for large columns or downstream array operations. | A meter reader totals `samples[].rate` for each event. |

For example, a consumer that sums **only approved samples** needs both `rate` and `approved`. Use full records for that calculation. A rate-only projection would discard the flag needed to produce the correct result. Either reader works with the same SQLite checkpoint and one-page read-ahead.

### When to install the native decoder

Use a broker that supports `ReadReplayPage`. Reading `record.event_id` and `record.task_id` leaves the event dictionary unbuilt. The first access to `record.event` materializes it, and subsequent accesses return the same mutable dictionary. Installing the companion accelerates decoding of Protobuf attributes at that first access. Legacy brokers and the JSON fallback for unusually deep or dense attributes use the Python JSON decoder.

To decide whether the extra build dependency pays off, time your actual handler with a representative backlog, including its SQLite commits. Compare the portable SDK and the native companion with the same events and page size. An ID-only handler can use the portable SDK throughout.

Install the matching `relayna-spooltail-native` source distribution from the SDK release alongside the regular SDK, or build the companion from the repository root with `python -m pip install .`. Building requires a C++17 toolchain, CMake, `protoc` and matching libprotobuf development libraries. The wheel links to the system libprotobuf. The SDK selects the extension automatically when it can import it. To verify that your application interpreter can load it, run the following command.

```sh
python -c "import spooltail_replay_native; print(spooltail_replay_native.__file__)"
```

### When to use a numeric projection

Use a broker that supports `ReadNumericPage` when one numeric column and the event identifiers supply everything the handler needs. For an event with `"samples": [{"rate": 1.25}, {"rate": 2.0}]`, select the array key `samples` and the element key `rate`. These are literal keys. The broker returns the values as packed float64 data, with a count, offset, partition, task ID, and event ID for each record.

```python
from functools import partial

read_rates = partial(client.read_numeric_batch, array_field="samples", value_field="rate")
page = await read_rates("status", after=saved, limit=64)
for row, total in zip(page.records, page.sums(), strict=True):
    print(row.event_id, row.count, total)  # Two samples above give a total of 3.25.
```

**Dependencies.** Numeric projection and `page.sums()` work with the portable SDK alone. Install the SDK’s `numpy` extra or NumPy separately when you need `page.as_numpy()` (a flat, read-only array view) or `page.numpy_sums()` (one sum per event). Both sum methods give zero for an empty array. Floating-point summation order can differ.

**Data requirements.** Each event must have the selected array. Each element must contain a number exactly representable as float64. For example, the integer `9007199254740993` cannot be projected exactly. Use full records for exact integer IDs, mixed event shapes, or calculations that depend on other fields. A projection can return the valid prefix of a page. A read starting at the incompatible record reports its offset. To inspect it, call `read_after_batch` with the last successfully processed offset as `after`.

Pass `read_page=read_rates` to `AuditStore.catch_up` to use this projection with durable checkpoints.

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
sdk_version=0.0.5
git tag -a "v${sdk_version}" -m "Spooltail v${sdk_version}"
git push origin "refs/tags/v${sdk_version}"
```

GitHub Actions builds the source distribution and wheel, tests the installed wheel against a broker built from the tagged revision, and attaches both distributions to a versioned GitHub Release.
