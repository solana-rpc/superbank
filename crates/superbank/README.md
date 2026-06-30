# superbank

Ingest Solana blocks from Yellowstone Fumarole, a Yellowstone gRPC stream
(DragonsMouth), Solana JSON-RPC `getBlock`, or Solana Bigtable and write to
ClickHouse `transactions` + `blocks_metadata` tables. When using Yellowstone
Fumarole or gRPC, Superbank writes live PoH entries to an `entries` table by
default.

## Prereqs

- ClickHouse with the matching schema set under `ddl/`: use `ddl/local/transactions.sql` +
  `ddl/local/blocks_metadata.sql` for single-node development, plus `ddl/local/entries.sql`
  for the default Fumarole/gRPC source configuration. Use `ddl/cluster/transactions.sql` +
  `ddl/cluster/blocks_metadata.sql` (+ `ddl/cluster/entries.sql` for Fumarole/gRPC source defaults) for
  clustered non-replicated storage, or `ddl/replicated/transactions.sql` +
  `ddl/replicated/blocks_metadata.sql` (+ `ddl/replicated/entries.sql` for Fumarole/gRPC source defaults)
  for clustered replicated storage.
- Fumarole endpoint + consumer group + optional `x-token` **or**
- DragonsMouth (Yellowstone gRPC) endpoint + optional `x-token` **or**
- Solana JSON-RPC endpoint that supports `getBlock` **or**
- Solana Bigtable instance credentials

## Build

```bash
cargo build -p superbank
```

## Run

Minimal example (Fumarole source):

```bash
SUPERBANK_SOURCE=fumarole \
FUMAROLE_ENDPOINT=https://your.fumarole.endpoint:443 \
FUMAROLE_X_TOKEN=your-token \
FUMAROLE_CONSUMER_GROUP=superbank-mainnet \
CLICKHOUSE_URL=http://localhost:8123 \
CLICKHOUSE_DATABASE=default \
CLICKHOUSE_ENTRIES_TABLE=default.entries \
cargo run -p superbank --
```

To initialize a new Fumarole consumer group from a specific slot, add
`FUMAROLE_CREATE_CONSUMER_GROUP=true` and `FUMAROLE_FROM_SLOT=<slot>`.
For existing consumer groups, Fumarole's stored offset is authoritative and
`fumarole-from-slot` is ignored.

Minimal example (gRPC source):

```bash
SUPERBANK_SOURCE=grpc \
DRAGONSMOUTH_ENDPOINT=https://your.dragonsmouth.endpoint:443 \
DRAGONSMOUTH_X_TOKEN=your-token \
CLICKHOUSE_URL=http://localhost:8123 \
CLICKHOUSE_DATABASE=default \
CLICKHOUSE_ENTRIES_TABLE=default.entries \
cargo run -p superbank --
```

Prometheus metrics are served at `/metrics` and a liveness health check at `/health` on
`METRICS_HOST:METRICS_PORT` (defaults: `0.0.0.0:9901`). `/health` returns `200 OK` while the
ingestor is flushing normally, and `503 Service Unavailable` when no successful ClickHouse flush
has occurred within `HEALTH_STALE_SECS` seconds (default: 120). Set `HEALTH_STALE_SECS=0` to
disable staleness checking. Set `METRICS_CLUSTER_LABEL` to attach a static `cluster="..."` label
to every ingestor metric.

Minimal example (RPC source):

```bash
SUPERBANK_SOURCE=rpc \
RPC_URL=https://api.mainnet-beta.solana.com \
RPC_FROM_SLOT=200000000 \
RPC_SLOT_COUNT=1000 \
CLICKHOUSE_URL=http://localhost:8123 \
CLICKHOUSE_DATABASE=default \
cargo run -p superbank --
```

Minimal example (Bigtable source, epoch range):

```bash
SUPERBANK_SOURCE=bigtable \
BIGTABLE_RANGE=1-10 \
RPC_URL=https://api.mainnet-beta.solana.com \
BIGTABLE_CREDENTIAL_PATH=/path/to/gcp-credentials.json \
CLICKHOUSE_URL=http://localhost:8123 \
CLICKHOUSE_DATABASE=default \
cargo run -p superbank --
```

Minimal example (Bigtable source, slot list):

```bash
SUPERBANK_SOURCE=bigtable \
BIGTABLE_SLOT_FILE=/path/to/slots.txt \
BIGTABLE_CREDENTIAL_PATH=/path/to/gcp-credentials.json \
CLICKHOUSE_URL=http://localhost:8123 \
CLICKHOUSE_DATABASE=default \
cargo run -p superbank --
```

## Configuration (config file, env, flags)

All options can be passed via YAML config file, `--flag`, or environment variable.
Config file keys use kebab-case by default; snake_case aliases are accepted.

Precedence: CLI flags > environment variables > config file > defaults.

Config file:

```yaml
source: fumarole
fumarole-endpoint: "https://your.fumarole.endpoint:443"
fumarole-x-token: "your-token"
fumarole-consumer-group: "superbank-mainnet"
commitment: "finalized"
clickhouse-url: "http://localhost:8123"
clickhouse-database: "default"
transactions-table: "default.transactions"
blocks-table: "default.blocks_metadata"
entries-table: "default.entries"
```

Full example: `superbank.example.yaml` (repo root)

Run with config:

```bash
cargo run -p superbank -- --config path/to/superbank.yaml
```

### Options

- `--config` / `SUPERBANK_CONFIG` (optional YAML config path)
- `--source` / `SUPERBANK_SOURCE` (required: `fumarole`, `grpc`, `rpc`, or `bigtable`)
- `--fumarole-endpoint` / `FUMAROLE_ENDPOINT` (required for fumarole source)
- `--fumarole-x-token` / `FUMAROLE_X_TOKEN` (optional)
- `--fumarole-consumer-group` / `FUMAROLE_CONSUMER_GROUP` (required for fumarole source)
- `--fumarole-create-consumer-group[=true|false]` / `FUMAROLE_CREATE_CONSUMER_GROUP` (default: false)
- `--fumarole-data-plane-tcp-connections` / `FUMAROLE_DATA_PLANE_TCP_CONNECTIONS` (default: 4; maximum: 20)
- `--fumarole-concurrent-download-limit-per-tcp` / `FUMAROLE_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP` (default: 2)
- `--fumarole-data-channel-capacity` / `FUMAROLE_DATA_CHANNEL_CAPACITY` (default: 4096; Fumarole client data channel capacity)
- `--fumarole-memory-soft-limit-bytes` / `FUMAROLE_MEMORY_SOFT_LIMIT_BYTES` (default: 25769803776; Fumarole backpressure guard soft limit, set 0 to disable)
- `--fumarole-commit-interval-secs` / `FUMAROLE_COMMIT_INTERVAL_SECS` (default: 10)
- `--fumarole-no-commit[=true|false]` / `FUMAROLE_NO_COMMIT` (default: false)
- `--endpoint` / `DRAGONSMOUTH_ENDPOINT` (required for grpc source)
- `--x-token` / `DRAGONSMOUTH_X_TOKEN` (optional)
- `--commitment` / `DRAGONSMOUTH_COMMITMENT` (default: `finalized`)
- `--dragonsmouth-from-slot` / `DRAGONSMOUTH_FROM_SLOT` (optional for grpc source; use `*`
  for latest slot in `blocks_metadata`, `0` to start from earliest available slot)
- `--fumarole-from-slot` / `FUMAROLE_FROM_SLOT` (optional for fumarole source; only used when
  `fumarole-create-consumer-group` is true)
- `--grpc-max-decoding-bytes` / `GRPC_MAX_DECODING_BYTES` (default: 67108864)
- `--grpc-http2-adaptive-window[=true|false]` / `GRPC_HTTP2_ADAPTIVE_WINDOW` (default: false)
- `--grpc-idle-timeout-secs` / `GRPC_IDLE_TIMEOUT_SECS` (default: 30; grpc source exits if no messages arrive before the timeout)
- `--grpc-health-watch-enabled[=true|false]` / `GRPC_HEALTH_WATCH_ENABLED` (default: true; grpc source exits if health is not `SERVING`)
- `--grpc-slot-notifications[=true|false]` / `GRPC_SLOT_NOTIFICATIONS` (default: true; subscribe to slot notifications on the gRPC stream to populate `superbank_ingest_chain_tip_lag`)
- `--rpc-url` / `RPC_URL` (required for rpc source)
- `--rpc-from-slot` / `RPC_FROM_SLOT` (required for rpc source; use `*` for latest slot in
  `blocks_metadata`, `0` to start from earliest available slot)
- `--rpc-to-slot` / `RPC_TO_SLOT` (required for rpc source if `--rpc-slot-count` not set)
- `--rpc-slot-count` / `RPC_SLOT_COUNT` (required for rpc source if `--rpc-to-slot` not set)
- `--rpc-timeout-secs` / `RPC_TIMEOUT_SECS` (default: 30)
- `--rpc-retry-backoff-ms` / `RPC_RETRY_BACKOFF_MS` (default: 500)
- `--rpc-max-inflight` / `RPC_MAX_INFLIGHT` (default: 64)
- `--rpc-max-supported-tx-version` / `RPC_MAX_SUPPORTED_TX_VERSION` (default: 0)
- `--rpc-flush-every-slots` / `RPC_FLUSH_EVERY_SLOTS` (default: 500)
- `--rpc-progress-every-slots` / `RPC_PROGRESS_EVERY_SLOTS` (default: 100)
- `--rpc-discovery-chunk-slots` / `RPC_DISCOVERY_CHUNK_SLOTS` (default: 10000)
- `--bigtable-range` / `BIGTABLE_RANGE` (required unless using `BIGTABLE_SLOT_FILE`; `123:456` slots, `1-10` epochs, or `5` epoch)
- `--bigtable-slot-file` / `BIGTABLE_SLOT_FILE` (optional; whitespace-separated slot list, mutually exclusive with `BIGTABLE_RANGE`)
- `--bigtable-instance` / `BIGTABLE_INSTANCE` (default: `solana-ledger`)
- `--bigtable-app-profile` / `BIGTABLE_APP_PROFILE` (default: `default`)
- `--bigtable-timeout-secs` / `BIGTABLE_TIMEOUT_SECS` (optional)
- `--bigtable-max-message-bytes` / `BIGTABLE_MAX_MESSAGE_BYTES` (default: 67108864)
- `--bigtable-credential-path` / `BIGTABLE_CREDENTIAL_PATH` (optional)
- `--bigtable-credential-json` / `BIGTABLE_CREDENTIAL_JSON` (optional)
- `--bigtable-discovery-limit` / `BIGTABLE_DISCOVERY_LIMIT` (default: 10000)
- `--bigtable-fetch-batch-size` / `BIGTABLE_FETCH_BATCH_SIZE` (default: 500)
- `--bigtable-fetch-concurrency` / `BIGTABLE_FETCH_CONCURRENCY` (default: 4)
- `--bigtable-insert-concurrency` / `BIGTABLE_INSERT_CONCURRENCY` (default: 1)
- `--bigtable-decode-concurrency` / `BIGTABLE_DECODE_CONCURRENCY` (default: available CPU threads)
- `--bigtable-progress-every-slots` / `BIGTABLE_PROGRESS_EVERY_SLOTS` (default: 10000)
- `--clickhouse-url` / `CLICKHOUSE_URL` (default: `http://localhost:8123`)
- `--metrics-host` / `METRICS_HOST` (default: `0.0.0.0`)
- `--metrics-port` / `METRICS_PORT` (default: `9901`)
- `--health-stale-secs` / `HEALTH_STALE_SECS` (default: 120; `/health` returns 503 when no successful ClickHouse flush has occurred within this many seconds; set 0 to disable)
- `--metrics-cluster-label` / `METRICS_CLUSTER_LABEL` (optional static `cluster` label on all metrics)
- `--clickhouse-database` / `CLICKHOUSE_DATABASE` (default: `default`)
- `--clickhouse-user` / `CLICKHOUSE_USER` (default: `default`)
- `--clickhouse-password` / `CLICKHOUSE_PASSWORD` (default: empty)
- `--clickhouse-async-insert` / `CLICKHOUSE_ASYNC_INSERT` (default: `false`)
- `--transactions-table` / `CLICKHOUSE_TRANSACTIONS_TABLE` (default: `default.transactions`)
- `--blocks-table` / `CLICKHOUSE_BLOCKS_TABLE` (default: `default.blocks_metadata`)
- `--entries-table` / `CLICKHOUSE_ENTRIES_TABLE` (default: `default.entries`; Fumarole and gRPC ingest write live PoH entries to this table)
- `--transactions-flush-rows` / `TRANSACTIONS_FLUSH_ROWS` (default: 25000)
- `--blocks-flush-rows` / `BLOCKS_FLUSH_ROWS` (default: 2000)
- `--flush-interval-secs` / `FLUSH_INTERVAL_SECS` (default: 5)
- `--flush-every-block` / `FLUSH_EVERY_BLOCK` (default: false; Fumarole/gRPC/Bigtable only)
- `--insert-max-retries` / `CLICKHOUSE_INSERT_MAX_RETRIES` (default: 5; stateless sources only: gRPC, RPC, Bigtable)
- `--insert-retry-base-ms` / `CLICKHOUSE_INSERT_RETRY_BASE_MS` (default: 1000; initial ClickHouse insert retry backoff)
- `--insert-retry-max-ms` / `CLICKHOUSE_INSERT_RETRY_MAX_MS` (default: 30000; maximum ClickHouse insert retry backoff)

## Notes

- For Fumarole and gRPC ingest, `meta_cost_units` is written when Yellowstone provides `cost_units`; rows ingested before this behavior may still have `NULL`.
- For Fumarole and gRPC ingest, apply `entries.sql` or set `CLICKHOUSE_ENTRIES_TABLE` to a table that exists before starting Superbank.
- `/metrics` includes Fumarole backpressure gauges/counters such as
  `superbank_ingest_fumarole_memory_soft_limit_bytes`,
  `superbank_ingest_fumarole_buffered_bytes`, `superbank_ingest_fumarole_pending_slots`,
  `superbank_ingest_fumarole_rss_bytes`, and
  `superbank_ingest_fumarole_pressure_flushes_total`.
- The ingestor writes **distributed** tables by default. Set table names if you want shard-local writes.
- Fumarole ingest commits consumer-group progress only after pending ClickHouse rows have been flushed. Set `fumarole-no-commit: true` only for diagnostics.
- Fumarole ingest applies a memory soft limit guard by default. When sampled RSS or Superbank's
  estimated Fumarole assembler bytes reach `fumarole-memory-soft-limit-bytes`, Superbank stops
  polling new Fumarole events while it flushes completed rows to ClickHouse, which backpressures
  upstream downloads instead of letting memory grow unbounded.
- `fumarole-from-slot` only initializes a Fumarole consumer group when
  `fumarole-create-consumer-group: true`; existing groups resume from the Fumarole-managed offset.
- `dragonsmouth-from-slot: 0` attempts slot 0; if unavailable, superbank parses the gRPC error to
  find the earliest available slot.
- `dragonsmouth-from-slot: "*"` uses the highest slot in `blocks_metadata` before subscribing; if
  rejected, superbank falls back to the gRPC-reported available slot.
- For seeded live-tail gRPC deployments, prefer `dragonsmouth-from-slot: "*"` so process restarts
  resume from the latest durable ClickHouse slot instead of replaying from the earliest available
  upstream slot.
- gRPC ingest is fail-fast: stream EOF, stream errors, health degradation, or an idle stream beyond `grpc-idle-timeout-secs` all cause superbank to flush once and exit nonzero.
- `grpc-http2-adaptive-window` is available for DragonsMouth deployments that need more resilient HTTP/2 flow control under large or bursty blocks.
- RPC mode uses `getBlock` and stores empty metadata rows when historical transactions lack metadata.
- RPC mode discovers available slots via `getBlocks` before calling `getBlock`, to avoid per-slot misses.
- RPC and Bigtable modes do not currently populate the `entries` table because those sources do not expose the per-entry payload Superbank needs.
- Bigtable epoch ranges use the RPC epoch schedule to resolve epochs to slots; provide `RPC_URL` when using `1-10` or single-epoch ranges.
- Bigtable slot lists do not require `RPC_URL` because slots are explicit.
- Superbank forces `async_insert=0` by default for ClickHouse writes; enable `--clickhouse-async-insert`
  only when your ClickHouse profile and dependent materialized views support it.
