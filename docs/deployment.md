# Deployment

This repo includes deployment-oriented artifacts under `deploy/`. These files are used by the local
Tilt workflow (`../Tiltfile`) and are intentionally minimal. Treat them as dev-focused building
blocks, not a complete production recipe.

## Directory layout

- `deploy/k8s/`: Kubernetes manifests (applied by Tilt; reusable with `kubectl` if you provide the
  missing Secrets/ConfigMaps that Tilt generates).
- `deploy/docker/Dockerfile.superbank-dev-runtime`: Runtime image used by Tilt that packages the
  `superbank` ingestor and `superbank-rpc` server binaries.

## Kubernetes manifests (`deploy/k8s/`)

All manifests default to:
- Namespace: `superbank-dev`
- Image: `superbank-dev`

Tilt can override both via environment variables (see "How Tilt uses deploy/" below).

### Namespace: `deploy/k8s/00-namespace.yaml`

Creates the default namespace:
- Namespace: `superbank-dev`

### ClickHouse: `deploy/k8s/10-clickhouse.yaml`

Runs ClickHouse as a single-node StatefulSet plus a Service:

- Service `clickhouse` exposes:
  - HTTP: `8123`
  - Native/TCP: `9000`
- StatefulSet `clickhouse`:
  - Image: `clickhouse/clickhouse-server:26.1.2.11`
  - Credentials are provided via the Secret `superbank-clickhouse` (`CLICKHOUSE_USER`,
    `CLICKHOUSE_PASSWORD`).
  - Data storage uses `emptyDir` (ephemeral).
  - Liveness/readiness probes hit `/ping` on port `8123`.

Dev vs production notes (facts about this manifest):
- There is no PersistentVolumeClaim; ClickHouse data is lost when the Pod is rescheduled.
- There are no resource requests/limits, backups, or HA/sharding/replication settings.

### ClickHouse DDL job: `deploy/k8s/11-clickhouse-ddl-job.yaml`

Applies the local ClickHouse schemas as a Kubernetes Job:

- Job `clickhouse-ddl` waits for ClickHouse to accept queries, then runs `clickhouse-client
  --multiquery` to apply schemas from files under `/ddl`.
- The DDL files come from a ConfigMap named `clickhouse-ddl` mounted at `/ddl`.
- Credentials are provided via the Secret `superbank-clickhouse`.

Important:
- `deploy/k8s/11-clickhouse-ddl-job.yaml` does not define the `clickhouse-ddl` ConfigMap; Tilt
  generates it from the repo's `ddl/local/*.sql` files.

### superbank-rpc: `deploy/k8s/20-superbank-rpc.yaml`

Runs the JSON-RPC server (`crates/superbank-rpc`) as a Deployment plus a Service:

- Service `superbank-rpc` exposes:
  - JSON-RPC: `8899`
  - Metrics: `9900`
- Deployment `superbank-rpc`:
  - Image: `superbank-dev` (the Tilt-built runtime image)
  - Command: `/usr/local/bin/superbank-rpc`
  - Connects to ClickHouse at `http://clickhouse:8123` (database: `default`)
  - Credentials are provided via the Secret `superbank-clickhouse`

The Tilt manifest builds `superbank-rpc` without optional Cargo features and exposes only JSON-RPC
and metrics. To use the optional Superbank gRPC streaming API in Kubernetes, build
`superbank-rpc` with `--features grpc-streaming`, set the `SUPERBANK_GRPC_*` environment variables,
and add the gRPC container/service port (default `10000`) plus any desired port-forward.

### Ingest: RPC job vs gRPC daemon

There are two ingestion variants under `deploy/k8s/`, selected by Tilt via `SUPERBANK_INGEST_MODE`.

#### RPC ingest job: `deploy/k8s/30-superbank-ingest-rpc-job.yaml`

Runs the ingestor (`crates/superbank`) once as a Kubernetes Job:

- Job `superbank-ingest-rpc`:
  - Command: `/usr/local/bin/superbank`
  - `SUPERBANK_SOURCE=rpc`
  - Defaults to Solana mainnet RPC (`RPC_URL=https://api.mainnet-beta.solana.com`)
  - Ingests a bounded range (via env vars like `RPC_FROM_SLOT` and `RPC_SLOT_COUNT`)
  - Writes to ClickHouse at `http://clickhouse:8123` (database: `default`)
  - Credentials are provided via the Secret `superbank-clickhouse`

Tilt can override the RPC ingest URL/range with `SUPERBANK_INGEST_RPC_URL`,
`SUPERBANK_INGEST_RPC_FROM_SLOT`, and `SUPERBANK_INGEST_SLOT_COUNT`.

This is dev-friendly in the sense that it terminates when the configured range is complete.

#### gRPC ingest daemon: `deploy/k8s/31-superbank-ingest-grpc.yaml`

Runs the ingestor continuously as a Kubernetes Deployment:

- Deployment `superbank-ingest-grpc`:
  - Command: `/usr/local/bin/superbank`
  - `SUPERBANK_SOURCE=grpc`
  - Reads `DRAGONSMOUTH_ENDPOINT` and `DRAGONSMOUTH_X_TOKEN` from the Secret `superbank-dragonsmouth`
  - Uses `DRAGONSMOUTH_COMMITMENT=finalized` by default
  - Uses `DRAGONSMOUTH_FROM_SLOT=*` by default so seeded deployments resume from the latest durable
    slot in `blocks_metadata` after restarts
  - Writes to ClickHouse using the Secret `superbank-clickhouse`

Important:
- `deploy/k8s/31-superbank-ingest-grpc.yaml` assumes the Secret `superbank-dragonsmouth` exists; Tilt
  creates it when `SUPERBANK_INGEST_MODE=grpc`.

## How Tilt uses `deploy/` (high level)

The local Tilt workflow is implemented in `../Tiltfile` and uses the artifacts in `deploy/` like
this:

1. Build Rust binaries locally:
   - Tilt runs a `local_resource` named `superbank-build` that builds `superbank` and `superbank-rpc` in release
     mode, then stages them into `dist/` as `dist/superbank` and `dist/superbank-rpc`.
   - The checked-in Tilt build command does not enable optional `superbank-rpc` features.
   - When the binaries are built inside a Nix environment, they can reference a `/nix/store/...`
     dynamic linker. Tilt may use `patchelf` to rewrite the interpreter so they can run inside the
     Ubuntu-based runtime image.

2. Build a runtime image:
   - Tilt builds an image (default name: `superbank-dev`, override via `SUPERBANK_IMAGE_REPO`) using
     `deploy/docker/Dockerfile.superbank-dev-runtime` and the staged `dist/` binaries.

3. Generate runtime-only Kubernetes config:
   - Tilt generates and applies:
     - A Secret `superbank-clickhouse` with `CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD`
       (from `SUPERBANK_CLICKHOUSE_USER`/`SUPERBANK_CLICKHOUSE_PASSWORD`).
     - A ConfigMap `clickhouse-ddl` containing the contents of the repo's `ddl/local/*.sql` files.
     - When `SUPERBANK_INGEST_MODE=grpc`, a Secret `superbank-dragonsmouth` with DragonsMouth gRPC creds
       (from `DRAGONSMOUTH_ENDPOINT`/`DRAGONSMOUTH_X_TOKEN`).

4. Apply Kubernetes manifests:
   - Tilt loads `deploy/k8s/*.yaml` and applies simple string replacements to support:
     - `SUPERBANK_NAMESPACE` (defaults to `superbank-dev`)
     - `SUPERBANK_IMAGE_REPO` (defaults to `superbank-dev`)
     - RPC ingest overrides (`SUPERBANK_INGEST_RPC_URL`, `SUPERBANK_INGEST_RPC_FROM_SLOT`,
       `SUPERBANK_INGEST_SLOT_COUNT`)
   - Tilt applies:
     - `deploy/k8s/00-namespace.yaml`
     - `deploy/k8s/10-clickhouse.yaml`
     - `deploy/k8s/11-clickhouse-ddl-job.yaml`
     - `deploy/k8s/20-superbank-rpc.yaml`
     - Exactly one ingest manifest:
       - `deploy/k8s/30-superbank-ingest-rpc-job.yaml` (default), or
       - `deploy/k8s/31-superbank-ingest-grpc.yaml` when `SUPERBANK_INGEST_MODE=grpc`

5. Order, dependencies, and port-forwards:
   - Tilt sequences resources so ClickHouse comes up first, then the DDL Job, then `superbank-rpc` and
     ingestion.
   - Tilt port-forwards:
     - ClickHouse: `8123`, `9000`
     - superbank-rpc JSON-RPC: `8899`

6. Re-applying DDL:
   - Tilt defines a manual `local_resource` named `apply-clickhouse-schema` that re-applies the DDL
     by `kubectl exec`-ing into the ClickHouse Pod and running `clickhouse-client`.

For "how to run Tilt locally", see [docs/development.md](development.md). This page is specifically
about what lives under `deploy/` and how Tilt consumes it.

## Docker runtime image (`deploy/docker/Dockerfile.superbank-dev-runtime`)

`deploy/docker/Dockerfile.superbank-dev-runtime` is a small Ubuntu-based runtime image used for
local Kubernetes development:

- Base image: `ubuntu:24.04`
- Installs: `ca-certificates`, `libssl3`
- Copies in prebuilt binaries:
  - `dist/superbank` -> `/usr/local/bin/superbank`
  - `dist/superbank-rpc` -> `/usr/local/bin/superbank-rpc`

This Dockerfile does not build Rust code itself. Something (Tilt's `superbank-build` step, or a
manual build) must produce the `dist/` artifacts before building the image.
