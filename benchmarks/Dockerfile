FROM rust:1.95-bookworm AS source
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY src/ src/
COPY proto/ proto/

FROM source AS build
ARG SPOOLTAIL_FEATURES=""
RUN cargo build --release --locked --features "$SPOOLTAIL_FEATURES"

FROM source AS transport-build
ARG SPOOLTAIL_TRANSPORT_FEATURES=bench-transports
RUN cargo build --release --locked --features "$SPOOLTAIL_TRANSPORT_FEATURES"

FROM debian:bookworm-slim AS transport-broker
COPY --from=transport-build /build/target/release/spooltail /usr/local/bin/spooltail
ENV SPOOLTAIL_LISTEN=0.0.0.0:7474 SPOOLTAIL_DATA=/data
ENTRYPOINT ["spooltail"]

FROM debian:bookworm-slim AS broker
COPY --from=build /build/target/release/spooltail /usr/local/bin/spooltail
ENV SPOOLTAIL_LISTEN=0.0.0.0:7474 SPOOLTAIL_DATA=/data
ENTRYPOINT ["spooltail"]

FROM python:3.13-slim-bookworm AS driver
WORKDIR /sdk
COPY --from=sdk pyproject.toml README.md uv.lock ./
COPY --from=sdk src/ src/
RUN pip install --no-cache-dir uv==0.13.9 && uv sync --locked --no-dev --no-editable
ENV PATH=/sdk/.venv/bin:$PATH PYTHONUNBUFFERED=1
WORKDIR /bench
RUN uv pip install --python /sdk/.venv/bin/python uvicorn==0.44.0 click==8.3.3
COPY benchmarks/compare.py benchmarks/metrics.py benchmarks/read_resources.py benchmarks/random_reads.py benchmarks/load_generators.py benchmarks/sdk_api.py ./
ENTRYPOINT ["python", "compare.py"]

FROM driver AS fanout-sdk-driver
RUN uv pip install --python /sdk/.venv/bin/python grpcio==1.80.0 protobuf==6.33.5
COPY python/src/relayna_spooltail /bench/relayna_spooltail

FROM python:3.13-slim-bookworm AS transport-driver
WORKDIR /bench
RUN pip install --no-cache-dir "httpx[http2]==0.28.1" "grpcio==1.80.0" "grpcio-tools==1.80.0" "protobuf==6.33.5"
COPY benchmarks/metrics.py ./
COPY benchmarks/protocols/ ./protocols/
COPY proto/spooltail/v1/spooltail.proto ./protocols/
ENV PYTHONPATH=/bench:/bench/protocols PYTHONUNBUFFERED=1
RUN python -m grpc_tools.protoc -Iprotocols --python_out=protocols --grpc_python_out=protocols protocols/spooltail.proto
ENTRYPOINT ["python", "protocols/driver.py"]
