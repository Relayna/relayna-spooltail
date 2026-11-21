"""Expose the released SDK status routes for the benchmark datasets."""

import os

from compare import MODES
from fastapi import FastAPI
from redis.asyncio import Redis
from relayna.api.status_routes import create_status_router
from relayna.status import RedisStatusStore, SSEStatusStream

app = FastAPI()
redis = Redis(host="redis")
for trial in range(int(os.environ["BENCH_TRIALS"])):
    for mode in MODES:
        namespace = f"trial{trial}-{mode}"
        stream = SSEStatusStream(store=RedisStatusStore(redis, prefix=namespace))
        app.include_router(
            create_status_router(sse_stream=stream), prefix="/" + namespace
        )


# Each router reads the same task histories.
routes = [
    ("sdk-default", redis, {}),
    ("sdk-once", redis, {"latest_retry_attempts": 1}),
]
if pool_size := int(os.getenv("BENCH_REDIS_POOL_SIZE", "0")):
    routes.append(
        (
            "sdk-wide",
            Redis(host="redis", max_connections=pool_size),
            {"latest_retry_attempts": 1},
        )
    )
for namespace, client, options in routes:
    store = RedisStatusStore(client, prefix="random-reads")
    app.include_router(
        create_status_router(
            sse_stream=SSEStatusStream(store=store),
            latest_status_store=store,
            **options,
        ),
        prefix="/" + namespace,
    )
