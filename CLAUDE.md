# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Superbank?

Superbank is a Rust workspace that ingests Solana ledger data into ClickHouse and serves Solana-compatible JSON-RPC endpoints backed by that data. Licensed under AGPL-3.0-only.

## Architecture

```
Source [Fumarole / gRPC / RPC / Bigtable] --> superbank (ingestor) --> ClickHouse --> superbank-rpc (JSON-RPC + optional gRPC server)
```

Two main crates in the workspace:
- **`crates/superbank`** — Ingestor binary. Pulls Solana data from Yellowstone Fumarole, Yellowstone gRPC (DragonsMouth), Solana JSON-RPC (`getBlock`), or Solana Bigtable and writes to ClickHouse.
- **`crates/superbank-rpc`** — Axum-based JSON-RPC server. Reads from ClickHouse and serves Solana-compatible RPC. Has optional `grpc-head-cache`, `disk-cache`, `grpc-streaming`, and `pyroscope` features.

Other key paths:
- `ddl/` — ClickHouse schemas (local, cluster, replicated variants)
- `tests/k6/` — k6 load/validation tests for superbank-rpc
- `scripts/` — Helper scripts (dev, test, deploy, analysis)
- `ingest/jetstreamer` and `ingest/jetstreamer-clickhouse-plugin` — Standalone workspaces (not in root Cargo workspace)

## Build & Development Commands

```bash
# Build both crates
cargo build -p superbank -p superbank-rpc

# Run ingestor
cargo run -p superbank -- --config superbank.yaml

# Run RPC server
RPC_HOST=0.0.0.0 RPC_PORT=8899 \
CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_DATABASE=default \
cargo run -p superbank-rpc --

# Local smoke test helper
scripts/dev/run-local-rpc.sh
```

## Linting & Testing

```bash
# Format check
cargo fmt --all -- --check

# Clippy (CI-style)
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo clippy -p superbank-rpc --all-targets --all-features --locked -- -D warnings

# Tests (minimum for PRs)
cargo test --workspace --locked

# Tests with optional features
cargo test -p superbank-rpc --all-features --locked

# k6 load test (basic)
k6 run tests/k6/scenarios/basic/superbank-rpc-get-signatures.js -e RPC_URL=http://localhost:8899

# k6 full suite
scripts/test/run-k6.sh
```

## Conventions

- **Rust stable toolchain** with `rustfmt` and `clippy` components (see `rust-toolchain.toml`)
- **Conventional Commits** required for PR titles (e.g. `fix: ...`, `feat: ...`, `chore: ...`). CI enforces this via `Lint PR title` check.
- PRs are squash-merged to `main`; releases are published by GoReleaser from `vX.Y.Z` tags.
- Keep diffs scoped — avoid drive-by refactors.
- When changing CLI flags, env vars, config fields, or scripts, update docs (`README.md`, `superbank.example.yaml`, `crates/*/README.md`) in the same PR.
- Config precedence: CLI flags > env vars > config file > defaults.
- Never commit `superbank.yaml` or secrets; use env vars or local config.

## Build Dependencies

Requires `protoc` (v25.x recommended) and libclang/LLVM dev libraries on the system. The Nix flake dev shell (`nix develop`) provides all needed tooling.
