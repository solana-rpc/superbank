# Contributing to Superbank

This document covers the contributor workflows used in this repo: setting up a dev environment,
running ClickHouse locally, building/running the ingestor and RPC server, and running tests.

If you just want to run Superbank locally, start with `README.md`.

## Getting the repo

```bash
git clone --recurse-submodules https://github.com/solana-rpc/superbank.git
cd superbank
```

`ingest/jetstreamer` is a git submodule. Cloning without `--recurse-submodules` leaves it empty
and causes build errors when compiling the Jetstreamer ClickHouse plugin. If you already cloned
without the flag, populate it with:

```bash
git submodule update --init
```

## Who This Is For

Contributors to:

- Rust code in `crates/superbank` (ingestor) and `crates/superbank-rpc` (JSON-RPC server)
- ClickHouse schemas under `ddl/`
- Load/validation tests under `tests/k6/`
- Helper scripts under `scripts/`
- Documentation

## Recommended Dev Environment (Nix / flakes)

This repo includes a Nix flake dev shell (`flake.nix`). Recommended:

```bash
nix develop
```

If flakes are not enabled globally, use:

```bash
nix --extra-experimental-features 'nix-command flakes' develop
```

Sanity-check tooling:

```bash
cargo --version
rustc --version
k6 version
docker version
```

Notes:

- The dev shell provides the Docker CLI, but you still need a running Docker daemon on your machine.
- If you hit build errors about missing native deps (for example `protoc` or `libclang`), see
  "Non-Nix Setup (Supported)" below.

## Non-Nix Setup (Supported)

You can contribute without Nix. Ensure these tools are installed and on `PATH`:

- Rust stable toolchain (repo uses `rust-toolchain.toml`, plus `rustfmt` and `clippy`)
- `protoc` (CI installs 25.3; a recent 25.x is recommended)
- libclang/LLVM development libraries (needed by crates that use bindgen)
- Docker (for local ClickHouse, or point at a remote ClickHouse)
- k6 (for RPC load/validation scenarios)

Also used by scripts:

- `bash`, `curl`, `python3`, and standard Unix utilities (for example `awk`)

Package names vary by OS/distribution. CI's Linux install list is a useful hint for what you may
need (for example: `clang`, `libclang`, `llvm`, `pkg-config`, `libudev`, `libusb`).

## Common Workflows

### ClickHouse (local)

Start a local ClickHouse container:

```bash
docker run -d --name clickhouse \
  --ulimit nofile=262144:262144 \
  -p 8123:8123 -p 9000:9000 \
  clickhouse/clickhouse-server:26.1.2.11
```

Create the dev tables (single-node) using the schemas under `ddl/local/`:

```bash
cat ddl/local/transactions.sql | docker exec -i clickhouse clickhouse-client --multiquery
cat ddl/local/blocks_metadata.sql | docker exec -i clickhouse clickhouse-client --multiquery
cat ddl/local/entries.sql | docker exec -i clickhouse clickhouse-client --multiquery
cat ddl/local/gsfa.sql | docker exec -i clickhouse clickhouse-client --multiquery
cat ddl/local/signatures.sql | docker exec -i clickhouse clickhouse-client --multiquery
cat ddl/local/token_owner_activity.sql | docker exec -i clickhouse clickhouse-client --multiquery
```

Optional:
- If you configure hot address routing (`CLICKHOUSE_GSFA_HOT_ADDRESSES`), also apply:
  `cat ddl/local/gsfa_hot.sql | docker exec -i clickhouse clickhouse-client --multiquery`

### Ingestor (`superbank`)

Create a local config:

```bash
cp superbank.example.yaml superbank.yaml
```

Run with config:

```bash
cargo run -p superbank -- --config superbank.yaml
```

See `crates/superbank/README.md` for the full option reference (config file, env vars, and flags).

### RPC server (`superbank-rpc`)

Run against local ClickHouse:

```bash
RPC_HOST=0.0.0.0 RPC_PORT=8899 \
CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_DATABASE=default \
cargo run -p superbank-rpc --
```

See `crates/superbank-rpc/README.md` for required DDL and optional features (for example
`--features grpc-head-cache`).

### Local RPC helper

`scripts/dev/run-local-rpc.sh` builds and runs the RPC server against a configured ClickHouse.
It defaults to the local Docker ClickHouse settings used above and accepts overrides via
environment variables.

For a single-node local ClickHouse using the schemas under `ddl/local/`, use distributed HTTP
routing and point at your container:

```bash
CLICKHOUSE_URL=http://localhost:8123 \
CLICKHOUSE_DATABASE=default \
CLICKHOUSE_USER=default \
CLICKHOUSE_TRANSPORT=http \
CLICKHOUSE_SCOPE=distributed \
scripts/dev/run-local-rpc.sh
```

### k6 load/validation tests

Basic quick check:

```bash
k6 run tests/k6/scenarios/basic/superbank-rpc-get-signatures.js -e RPC_URL=http://localhost:8899
```

Run the full suite runner (basic + validation + fuzz + replay; optional stress/soak/spike):

```bash
scripts/test/run-k6.sh
```

Validation scenarios compare Superbank RPC responses against a reference endpoint. Example:

```bash
REFERENCE_RPC_URL=https://api.mainnet-beta.solana.com scripts/test/run-k6.sh
```

`getTransactionsForAddress` is a Superbank-specific method, so its validation scenario uses
`TFA_REFERENCE_RPC_URL` instead of the standard Solana reference endpoint:

```bash
REFERENCE_RPC_URL=https://api.mainnet-beta.solana.com \
TFA_REFERENCE_RPC_URL=http://localhost:8898 \
scripts/test/run-k6.sh
```

See `tests/k6/README.md` for scenario docs and all environment variables.

## Coding Standards

Keep changes focused and avoid drive-by refactors unless they are part of the same change.

### Conventional Commits (Required)

Superbank uses **Conventional Commits** (semantic commits) to:

- enforce consistent commit messages on `main`
- keep release history and GitHub release notes readable

Requirements:

- PR titles must match Conventional Commits, e.g. `fix: ...`, `feat: ...`, `chore: ...`
- Breaking changes must be indicated with `!` in the PR title (for example `feat!: ...`), since PR titles are one line.
- `main` should only receive changes via **squash merge**, and squash commits must default to the PR title.

CI enforces PR titles via the `Lint PR title` GitHub Actions check.

### Releases

Releases are tag-driven and published by GoReleaser:

1. Update the workspace and member versions in the `Cargo.toml` files.
2. Create and push an annotated `vX.Y.Z` tag.
3. The release workflow runs a Tilt-backed E2E gate that starts ClickHouse, applies local DDL,
   runs `superbank` ingestion, starts `superbank-rpc`, and runs the k6 release suite before assets are
   published.
4. After E2E passes, GoReleaser builds both binaries for Linux amd64 and Linux arm64,
   then publishes a GitHub Release with `.tar.gz` archives, release notes, and `SHA256SUMS.txt`.

Example:

```bash
git tag -a v0.4.0 -m "Release v0.4.0"
git push origin v0.4.0
```

Repository settings required (GitHub UI):

- Enable squash merging and set squash commit messages to use the PR title by default.
- Protect the `main` branch to require PRs and required status checks (at minimum: `CI` and `Lint PR title`).

Formatting and linting:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
```

Local optional stricter checks (used by the repo's pre-commit hooks):

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Do not commit secrets. Use environment variables or `superbank.yaml` locally (do not commit `superbank.yaml`).

## Testing Expectations

Minimum for PRs:

```bash
cargo test --workspace --locked
```

CI also exercises optional features for `superbank-rpc`:

```bash
cargo test -p superbank-rpc --features grpc-head-cache,pyroscope,disk-cache --locked
```

If you change RPC behavior, ClickHouse queries, or response formats:

- Run at least the basic k6 scenario: `tests/k6/scenarios/basic/superbank-rpc-get-signatures.js`
- Prefer running the suite runner (`scripts/test/run-k6.sh`) when feasible
- Include k6 results (what you ran, pass/fail, and any relevant latency/error deltas) in the PR description

## Docs Expectations

If you change CLI flags, environment variables, config fields, scripts, or default behavior, update the
relevant docs in the same PR:

- `README.md` for user-facing quick start changes
- `superbank.example.yaml` for config schema changes
- `crates/*/README.md` for component-specific changes

## Using Codex Agents

If you use Codex (or other agents) on this repo:

- `AGENTS.md` is the agent contract: it contains the repo-specific rules and canonical commands agents should follow.
- See `docs/agents/codex.md` for agent workflow guidance (planning vs execution, expected evidence, and output conventions).

See also:
- `docs/development.md` for local dev workflows (scripts, Tilt).
- `docs/troubleshooting.md` for common build/run issues.
