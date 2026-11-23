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

## Partitions

Administrators can call `list_partitions()`, `ensure_partition(id)`, `task_partition(task_id)`, and `move_task(task_id, from_partition, to_partition)`. Pass `partition=id` to `claim()` to claim tasks assigned to that partition. For partition-scoped replay, pass it to both `next()` and `commit()`.

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
