# syntax=docker/dockerfile:1
# SPDX-License-Identifier: AGPL-3.0-only

FROM rust:1-bookworm AS builder

ARG SUPERBANK_RPC_FEATURES=""

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    clang \
    cmake \
    libclang-dev \
    libssl-dev \
    pkg-config \
    protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY crates/ crates/

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked -p superbank \
    && cp target/release/superbank /app/superbank

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    if [ -n "${SUPERBANK_RPC_FEATURES}" ]; then \
      cargo build --release --locked -p superbank-rpc --features "${SUPERBANK_RPC_FEATURES}"; \
    else \
      cargo build --release --locked -p superbank-rpc; \
    fi \
    && cp target/release/superbank-rpc /app/superbank-rpc

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates libssl3 libstdc++6 \
  && rm -rf /var/lib/apt/lists/* \
  && useradd --system --create-home --home-dir /var/lib/superbank --shell /usr/sbin/nologin superbank

COPY --from=builder /app/superbank /usr/local/bin/superbank
COPY --from=builder /app/superbank-rpc /usr/local/bin/superbank-rpc

USER superbank

EXPOSE 8899 9900 9901

ENTRYPOINT ["/usr/local/bin/superbank-rpc"]
