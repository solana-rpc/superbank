# Development

This repo is Nix-first for local development. Most commands below assume you are running from the
repo root.

## Nix dev shell (recommended)

Enter the dev shell:

```bash
nix develop
```

If you do not want to enable flakes globally, you can run:

```bash
nix --extra-experimental-features 'nix-command flakes' develop
```

Run a single command inside the shell (no interactive session):

```bash
nix develop -c cargo build -p superbank -p superbank-rpc
```

Notes:
- The dev shell provides tools used by scripts and Tilt (`tilt`, `kind`, `kubectl`, `docker`, `k6`, Rust tooling).
- You still need a running Docker daemon on the host (the shell provides the Docker CLI, not the daemon).

## Local ClickHouse (Docker)

The root quick start has the canonical local ClickHouse steps: [README.md](../README.md).

Minimal workflow:

1. Start ClickHouse:

```bash
docker run -d --name clickhouse \
  --ulimit nofile=262144:262144 \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  -p 8123:8123 -p 9000:9000 \
  clickhouse/clickhouse-server:26.1.2.11
```

`CLICKHOUSE_SKIP_USER_SETUP=1` is a local-development shortcut that lets host-side clients connect
as the image's `default` user through the mapped ports. Do not use it for production ClickHouse.

2. Apply local (single-node) DDL:

```bash
for f in \
  ddl/local/transactions.sql \
  ddl/local/blocks_metadata.sql \
  ddl/local/entries.sql \
  ddl/local/gsfa.sql \
  ddl/local/signatures.sql \
  ddl/local/token_owner_activity.sql
do
  cat "$f" | docker exec -i clickhouse clickhouse-client --multiquery
done
```

Apply `transactions.sql` before the materialized-view schemas (`gsfa*.sql`, `signatures.sql`, and
`token_owner_activity.sql`) because those views read from the transactions table.

The default loop uses `gsfa.sql`. For hot-address routing, use `gsfa_nohot.sql` plus
`gsfa_hot.sql` instead:

```bash
for f in \
  ddl/local/transactions.sql \
  ddl/local/blocks_metadata.sql \
  ddl/local/entries.sql \
  ddl/local/gsfa_nohot.sql \
  ddl/local/gsfa_hot.sql \
  ddl/local/signatures.sql \
  ddl/local/token_owner_activity.sql
do
  cat "$f" | docker exec -i clickhouse clickhouse-client --multiquery
done
```

These examples use the ClickHouse image defaults (`default` user, empty password). If you are using
a different local ClickHouse config, pass `--user/--password` to `clickhouse-client`.

## Local RPC helper: scripts/dev/run-local-rpc.sh

`../scripts/dev/run-local-rpc.sh` is a convenience wrapper for running `superbank-rpc` against an
existing ClickHouse:

- Builds `superbank-rpc` in release mode (binary: `target/release/superbank-rpc`).
  - If `SUPERBANK_RPC_FEATURES` is set, it builds with `--features "$SUPERBANK_RPC_FEATURES"`.
- Exports a set of env vars (with defaults) used by `superbank-rpc`.
- Starts the RPC server with explicit flags for host/port/metrics/clickhouse connection.
- It does not start ClickHouse and does not apply DDL. You must do those separately.

### Running against local Docker ClickHouse

The script is local-friendly by default:
- `CLICKHOUSE_URL=http://localhost:8123`
- `CLICKHOUSE_USER=default`
- `CLICKHOUSE_TRANSPORT=http`
- `CLICKHOUSE_SCOPE=distributed`

For a default local Docker ClickHouse (from the setup above), this is usually enough:

```bash
scripts/dev/run-local-rpc.sh
```

Override as needed for custom local settings:

```bash
CLICKHOUSE_URL=http://localhost:8123 \
CLICKHOUSE_USER=default \
CLICKHOUSE_PASSWORD= \
scripts/dev/run-local-rpc.sh
```

### Key environment variables

Common:
- `SUPERBANK_RPC_FEATURES` (optional Cargo feature list used when building `superbank-rpc`, for
  example `grpc-head-cache`, `disk-cache`, `grpc-streaming`, or `pyroscope`)
- `RUST_LOG` (default: `info,clickhouse_rs=warn`)
- `LOG_FORMAT` (default: `plain`, supports `json`)
- `RPC_HOST` / `RPC_PORT` (defaults: `0.0.0.0` / `8899`)
- `METRICS_HOST` / `METRICS_PORT` (defaults: `0.0.0.0` / `9900`)
- `RPC_MAX_BODY_BYTES` (default: `1048576`)
- `RPC_REQUEST_TIMEOUT_MS` (default: `10000`)
- `RPC_CONCURRENCY_LIMIT` (default: `512`)
- `HYDRATION_CPU_CONCURRENCY` (default: `8`)
- `CLICKHOUSE_URL` / `CLICKHOUSE_DATABASE` / `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_QUERY_TIMEOUT_MS` (default: `8000`; when query `SETTINGS` are enabled, read queries set ClickHouse `max_execution_time` to this budget so abandoned queries do not keep running, so keep this below `RPC_REQUEST_TIMEOUT_MS`)
- `MAX_SIGNATURES_LIMIT` (default: `1000`)
- `CLICKHOUSE_QUERY_CACHE_ENABLED` (default: `true` in this helper script)
- `CLICKHOUSE_QUERY_CACHE_TTL_SECONDS` (default: `1`)
- `CLICKHOUSE_QUERY_CACHE_SHARE_BETWEEN_USERS` (default: `false`)

Routing and shard options:
- `CLICKHOUSE_TRANSPORT=http|tcp` (default: `http`)
- `CLICKHOUSE_SCOPE=distributed|shard-direct` (default: `distributed`)
- `CLICKHOUSE_TCP_ACCESS_CHECK_TIMEOUT_MS` (default: `2000`)
- `CLICKHOUSE_CLUSTER` (default: `{cluster}`)
- `CLICKHOUSE_TOPOLOGY_CONFIG` (optional authoritative shard topology YAML; see
  `../crates/superbank-rpc/README.md`)
- `CLICKHOUSE_SHARD_FANOUT_CONCURRENCY` (default: `8`)
- `CLICKHOUSE_HTTP_MAX_CONCURRENCY` (default: `512`; caps concurrent direct/scalar ClickHouse HTTP queries server-wide so HTTP connection demand does not track request/batch concurrency)
- `CLICKHOUSE_HTTP_CONNECT_TIMEOUT_MS` (default: `2000`; TCP connect timeout for ClickHouse HTTP connections so connects fail fast under backpressure)
- `CLICKHOUSE_TCP_POOL_MIN` (default: `10`) / `CLICKHOUSE_TCP_POOL_MAX` (default: `20`) — per-shard native (TCP) pool sizing; total native connections per instance ≈ `CLICKHOUSE_TCP_POOL_MAX` × shards
- `CLICKHOUSE_KILL_QUERY_MAX_CONCURRENCY` (default: `16`; hard cap on concurrent best-effort `KILL QUERY` cleanup connections so a burst of timeouts/cancellations cannot storm ClickHouse with extra connections)
- `CLICKHOUSE_IN_CLAUSE_CHUNK` (default: `512`)
- `CLICKHOUSE_STARTUP_TABLE_CHECK=exists|count` (default: `exists`)
- `CLICKHOUSE_GSFA_LOCAL_TABLE`, `CLICKHOUSE_SIGNATURES_LOCAL_TABLE`,
  `CLICKHOUSE_TOKEN_OWNER_ACTIVITY_LOCAL_TABLE`, `CLICKHOUSE_TRANSACTIONS_LOCAL_TABLE`,
  `CLICKHOUSE_BLOCKS_METADATA_LOCAL_TABLE` (auto-derived from distributed table names when unset)
- `CLICKHOUSE_SHARD_HTTP_PORT` (optional override)

Validation behavior:
- `CLICKHOUSE_TRANSPORT=tcp` requires `CLICKHOUSE_SCOPE=shard-direct`.
- `CLICKHOUSE_SCOPE=shard-direct` requires a GSFA local table (auto-derived by default).

### Optional gRPC head cache (Yellowstone DragonsMouth)

The head cache is a compile-time feature and a runtime toggle:

```bash
SUPERBANK_RPC_FEATURES=grpc-head-cache \
HEAD_CACHE_ENABLED=true \
DRAGONSMOUTH_ENDPOINT=https://YOUR_DRAGONSMOUTH_ENDPOINT \
DRAGONSMOUTH_X_TOKEN=YOUR_OPTIONAL_TOKEN \
CLICKHOUSE_URL=http://localhost:8123 \
CLICKHOUSE_USER=default \
scripts/dev/run-local-rpc.sh
```

Notes:
- `HEAD_CACHE_ENABLED` accepts common truthy values (`true`, `1`, `yes`, `on`).
- `HEAD_CACHE_ENABLED` defaults to `false`.
- If `HEAD_CACHE_ENABLED=true` but `DRAGONSMOUTH_ENDPOINT` is empty, the script prints a warning and
  the head cache remains disabled.

For the full `superbank-rpc` configuration surface (including additional ClickHouse table env vars),
see `../crates/superbank-rpc/README.md`.

### Optional Superbank gRPC streaming

The Superbank gRPC streaming API is also a compile-time feature plus a runtime toggle:

```bash
SUPERBANK_RPC_FEATURES=grpc-streaming \
SUPERBANK_GRPC_ENABLED=true \
SUPERBANK_GRPC_PORT=10000 \
CLICKHOUSE_URL=http://localhost:8123 \
CLICKHOUSE_USER=default \
scripts/dev/run-local-rpc.sh
```

This starts the JSON-RPC server on `RPC_PORT` and the gRPC endpoint on `SUPERBANK_GRPC_PORT`.
See `../crates/superbank-rpc/README.md` for the stream methods and gRPC-specific limits.

## Tilt (Kind + Kubernetes)

Tilt provides a local Kubernetes workflow (Kind cluster) that runs:
- ClickHouse (StatefulSet) + a DDL Job that applies the local schemas from `ddl/local/*.sql`
- `superbank-rpc` (Deployment) with a local port-forward to `localhost:8899`
- `superbank` ingestion, either:
  - `SUPERBANK_INGEST_MODE=rpc` (default): one-shot Job that ingests a bounded RPC range, or
  - `SUPERBANK_INGEST_MODE=grpc`: Deployment that tails a DragonsMouth gRPC stream

### Prerequisites

- Docker daemon running and accessible (Kind runs on Docker).
- Tilt, Kind, and kubectl.
  - Recommended: use the Nix dev shell (`nix develop`) so the toolchain is consistent.

### Setup and run

Run the setup script:

```bash
scripts/dev/setup-tilt.sh
```

This script:
- Creates/selects a Kind cluster (default name: `superbank`) unless disabled.
- Selects the matching kubectl context (`kind-superbank`).
- Optionally configures a Kind local registry (default port: `5001`) and prints the env needed for Tilt.
- Does not start Tilt; it prints the `tilt up --stream` command to run next.
- If you are not already in a Nix shell, it re-execs itself via `nix develop -c ...`.

Then run the printed command, typically:

```bash
tilt up --stream
```

If you do not have Tilt installed globally, run it via Nix:

```bash
nix develop -c tilt up --stream
```

### Tilt with Docker Compose

Use the Compose shim when you want Tilt's dashboard and resource controls without creating a Kind
cluster:

```bash
tilt up -f Tiltfile.compose
```

This loads `docker-compose.yaml` through Tilt's Docker Compose support. The default run starts
ClickHouse, applies the local DDL, and runs `superbank-rpc`. To include the optional RPC ingestor,
start Tilt with `COMPOSE_PROFILES=ingest-rpc`; `superbank-ingest-rpc` is manual in Tilt so it behaves
like the optional Compose profile.

### Tilt E2E CI profile

The E2E workflow uses `../scripts/ci/release-e2e.sh` to run the same Tilt stack in GitHub CI.
It can be run manually from GitHub Actions against any branch, and the tag release workflow calls
it before GoReleaser publishes release assets. The script:

- creates a Kind cluster and configures the local registry via `../scripts/dev/setup-tilt.sh`
- runs `tilt ci` so ClickHouse, the DDL Job, `superbank-rpc`, and the RPC ingest Job are all exercised
- checks ClickHouse row counts, generates k6 pool files from ingested data, port-forwards
  `superbank-rpc`, and runs `../scripts/test/run-k6.sh --stress --soak --spike`
- stores diagnostics and the Tilt exit snapshot under `artifacts/release-e2e/`

By default the validation suite uses the Tilt endpoint as its own reference, which exercises the
validation scenarios without depending on public RPC availability. Set `SUPERBANK_E2E_REFERENCE_RPC_URL`
to a real reference endpoint when you specifically want cross-endpoint validation.
The E2E profile disables `validate:getLatestBlockhash` by default because that validation needs
`isBlockhashValid` on a live reference endpoint, while the release ingest window is intentionally
historical and bounded. The stress suite still exercises `getLatestBlockhash`.

The CI script also sets `SUPERBANK_INGEST_RPC_FROM_SLOT` and `SUPERBANK_INGEST_SLOT_COUNT` (default:
`64`) so the public-RPC ingest Job is bounded for CI validation.
It gives the full Tilt run 65 minutes by default and active resources 35 minutes to become ready,
which leaves enough room for cold Rust release builds in CI.

Manual branch run:

```bash
gh workflow run e2e.yml --ref <branch-name>
```

### Release flow

Releases are tag-driven. Before cutting a release, update the shared package version under
`[workspace.package]` in the root `Cargo.toml`, then create and push an annotated `vX.Y.Z` tag:

```bash
git tag -a v0.5.0 -m "Release v0.5.0"
git push origin v0.5.0
```

The release workflow first verifies that the tag version matches the Cargo package versions. It
then runs the Tilt E2E profile before GoReleaser builds both binaries for Linux amd64 and Linux
arm64, publishes GitHub release notes, uploads `.tar.gz` archives, and generates
`SHA256SUMS.txt`.

### Key environment variables

Setup script (`../scripts/dev/setup-tilt.sh`):
- `SUPERBANK_KIND_CLUSTER` (default: `superbank`)
- `SUPERBANK_NAMESPACE` (default: `superbank-dev`)
- `SUPERBANK_INGEST_MODE` (default: `rpc`, supported: `rpc` or `grpc`)
- `SUPERBANK_SETUP_KIND=0|1` (default: `1`)
- `SUPERBANK_USE_LOCAL_REGISTRY=0|1` (default: `1`)
- `SUPERBANK_KIND_REGISTRY_NAME` (default: `kind-registry`)
- `SUPERBANK_KIND_REGISTRY_PORT` (default: `5001`)

Tiltfile (`../Tiltfile`):
- `SUPERBANK_NAMESPACE` (namespace override; defaults to `superbank-dev`)
- `SUPERBANK_INGEST_MODE` (`rpc` or `grpc`; defaults to `rpc`)
- `SUPERBANK_INGEST_RPC_URL` / `SUPERBANK_INGEST_RPC_FROM_SLOT` / `SUPERBANK_INGEST_SLOT_COUNT`
  - Optional overrides for the default RPC ingest Job when `SUPERBANK_INGEST_MODE=rpc`
- `SUPERBANK_CLICKHOUSE_USER` / `SUPERBANK_CLICKHOUSE_PASSWORD`
  - Defaults: `default` / `superbank` (used for both ClickHouse itself and clients)
- `SUPERBANK_E2E_TILT_TIMEOUT` / `SUPERBANK_E2E_TILT_READINESS_TIMEOUT`
  - Defaults: `65m` / `35m` for the CI profile
- `DRAGONSMOUTH_ENDPOINT` (required when `SUPERBANK_INGEST_MODE=grpc`)
- `DRAGONSMOUTH_X_TOKEN` (optional when `SUPERBANK_INGEST_MODE=grpc`)
- `LOCAL_REGISTRY_HOST` (set by the setup script when using the Kind local registry)
- `SUPERBANK_IMAGE_REPO` (override image name/tag; default: `superbank-dev`)

### Nix-built binaries and patchelf (Tiltfile requirement)

The Tilt workflow builds the Rust binaries locally, copies them into `dist/`, and then builds an
Ubuntu runtime image (`deploy/docker/Dockerfile.superbank-dev-runtime`). When the binaries are built
inside a Nix environment, they can reference a `/nix/store/...` dynamic linker, which will not
exist in the Ubuntu image.

`../Tiltfile` detects this and uses `patchelf` to rewrite the interpreter to
`/lib64/ld-linux-x86-64.so.2`. If `patchelf` is missing and the binaries look Nix-linked, Tilt will
fail fast with an error. The simplest fix is to run Tilt inside the Nix dev shell (which includes
`patchelf`).
