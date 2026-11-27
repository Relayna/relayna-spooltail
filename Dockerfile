FROM rust:1.95-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs ./
COPY src/ src/
COPY proto/ proto/
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /build/target/release/spooltail /usr/local/bin/spooltail
RUN install -d -o 10001 -g 10001 /data
USER 10001:10001
ENV SPOOLTAIL_DATA=/data SPOOLTAIL_LISTEN=0.0.0.0:7474
EXPOSE 7474 7443
ENTRYPOINT ["spooltail"]
