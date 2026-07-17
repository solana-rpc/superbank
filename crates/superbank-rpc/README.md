# superbank-rpc

Solana-compatible JSON-RPC server backed by ClickHouse tables produced by `superbank` (or any
writer that matches the same schemas).

## Supported methods

- `getSignaturesForAddress`
- `getSignatureStatuses`
- `getTransaction`
- `getBlock`
- `getBlockHeight`
- `getSlot`
- `getTransactionCount`
- `getLatestBlockhash`
- `isBlockhashValid`
- `getBlockTime`
- `getBlocks`
- `getBlocksWithLimit`
- `getHealth`
- `getFirstAvailableBlock`
- `minimumLedgerSlot`
- `getInflationReward`
- `getEpochSchedule`
- `getTransactionsForAddress` (custom)

Notes:
- JSON-RPC batch envelopes are supported. Batch execution is bounded by
  `RPC_MAX_BATCH_SIZE` and `RPC_BATCH_CONCURRENCY_LIMIT`.
- Requests without an `id` are normalized to `id: null` and still return
  JSON-RPC response bodies (compatibility behavior; not strict notification semantics).
- `processed` commitment is rejected by default; use `confirmed` or `finalized`.
- `processed` commitment is supported for a subset of methods when compiled with
  `--features grpc-head-cache` and enabled at runtime with `HEAD_CACHE_ENABLED=true`
  (see "Optional gRPC head cache" below).
- `getBlockHeight`, `getSlot`, and `getTransactionCount` accept an optional single config object as the sole param:
  - `commitment`: `processed|confirmed|finalized` (defaults to `finalized`; `processed` requires the head cache)
  - `minContextSlot`: optional `u64`; if the server's current context slot is below this value, the
    call fails with JSON-RPC error `-32016` ("Minimum context slot has not been reached") and
    includes `contextSlot` in the error `data`.
- `minimumLedgerSlot` currently reports the lowest slot retained in Superbank's ClickHouse-backed
  block storage. This is a pragmatic approximation of Solana's validator-local ledger metadata,
  not an exact blockstore-equivalent implementation.
- `getHealth` returns Solana-compatible `"ok"` only when superbank-rpc can resolve a latest
  finalized slot from ClickHouse. ClickHouse query failures or empty block metadata return
  JSON-RPC error `-32005` (`Node is unhealthy`) with `numSlotsBehind: null`; this is not Agave's
  validator-local cluster-tip distance check.
- Methods that need a latest finalized context return a backend/internal JSON-RPC error when
  ClickHouse has no finalized slot available. This includes `getSlot`, `getBlockHeight`,
  `getTransactionCount`, `getLatestBlockhash`, `getSignatureStatuses`, and min-context checks.
- `getTransaction` accepts the standard Solana config fields plus an optional Superbank extension:
  - `slot`: optional `u64`; when supplied, ClickHouse is queried directly for that exact slot and
    the response is `null` if the signature is not present in that slot.
- `getSignaturesForAddress` accepts standard Solana config fields plus optional Superbank extensions:
  - `beforeSlot`: optional `u64`; exclusive whole-slot upper bound (`slot < beforeSlot`).
  - `untilSlot`: optional `u64`; exclusive whole-slot lower bound (`slot > untilSlot`).
    These are whole-slot cursors, not signature-position cursors; `beforeSlot` cannot be combined
    with `before`, and `untilSlot` cannot be combined with `until`.
  - Missing `before` or `until` signatures return JSON-RPC error `-32020` (`Transaction <signature> not found`).
- `getTransactionsForAddress` supports `transactionDetails=signatures|full`, `sortOrder=asc|desc`,
  `paginationToken`, and filters (`slot`, `blockTime`, `signature`, `status`, `tokenAccounts`).
  With `transactionDetails=full`, each item carries the transaction's `version` when `maxSupportedTransactionVersion` is set.
  It also accepts `beforeSlot`/`untilSlot` as aliases for `filters.slot.lt`/`filters.slot.gt`;
  these aliases cannot be combined with same-side slot filters (`lt`/`lte` for `beforeSlot`,
  `gt`/`gte` for `untilSlot`).
  Token account filters require the token-owner activity table (see below).

## ClickHouse schemas

Choose one schema set under `ddl/`:
- `ddl/local/*.sql` for single-node ClickHouse.
- `ddl/cluster/*.sql` for clustered non-replicated shard-local tables.
- `ddl/replicated/*.sql` for clustered replicated shard-local tables.

Required files in the chosen set:
- `transactions.sql`
- `blocks_metadata.sql`
- `gsfa.sql` or `gsfa_nohot.sql`
- `signatures.sql`

Optional:
- `gsfa_hot.sql` when using hot-address routing.
- `token_owner_activity.sql` when using token-owner filters in `getTransactionsForAddress`.

Apply `transactions.sql` before the materialized-view schemas (`gsfa*.sql`, `signatures.sql`, and
`token_owner_activity.sql`) because those views read from the transactions table. If you use
`gsfa_hot.sql`, apply `gsfa_nohot.sql` instead of `gsfa.sql`, then apply `gsfa_hot.sql`.

## Run

```bash
RPC_HOST=0.0.0.0 RPC_PORT=8899 \
CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_DATABASE=default \
cargo run -p superbank-rpc --
```

## Optional Superbank gRPC streaming (`grpc-streaming`)

When compiled with `--features grpc-streaming` and enabled at runtime, superbank-rpc serves a
tonic gRPC endpoint alongside JSON-RPC. The v1 implementation supports historical
`StreamBlocks` and `StreamTransactions` over bounded inclusive slot ranges from ClickHouse.
Unary methods and bidirectional `Get` are present in the proto for compatibility but return
`UNIMPLEMENTED`.

`StreamBlocks` returns one message per matching block with block metadata, rewards, and
transaction payloads. `StreamTransactions` returns one message per matching transaction. Filters
support account include/exclude/required matching, transaction vote filtering, and failed/success
filtering where present in the request.

Run example:

```bash
RPC_HOST=0.0.0.0 RPC_PORT=8899 \
CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_DATABASE=default \
SUPERBANK_GRPC_ENABLED=true SUPERBANK_GRPC_PORT=10000 \
cargo run -p superbank-rpc --features grpc-streaming --
```

Configuration:

| Option | Environment | Default | Notes |
| --- | --- | --- | --- |
| `--superbank-grpc-enabled` | `SUPERBANK_GRPC_ENABLED` | `false` | Enables the gRPC endpoint at runtime. |
| `--superbank-grpc-host` | `SUPERBANK_GRPC_HOST` | `0.0.0.0` | Bind host. |
| `--superbank-grpc-port` | `SUPERBANK_GRPC_PORT` | `10000` | Bind port. |
| `--superbank-grpc-max-slot-range` | `SUPERBANK_GRPC_MAX_SLOT_RANGE` | `100` | Maximum inclusive slots per stream request. |
| `--superbank-grpc-query-timeout-ms` | `SUPERBANK_GRPC_QUERY_TIMEOUT_MS` | `30000` | Per-chunk ClickHouse range-query timeout. |
| `--superbank-grpc-chunk-slots` | `SUPERBANK_GRPC_CHUNK_SLOTS` | `8` | Slots fetched from ClickHouse per chunk. |
| `--superbank-grpc-max-send-bytes` | `SUPERBANK_GRPC_MAX_SEND_BYTES` | `104857600` | Max encoded gRPC response message size. |
| `--superbank-grpc-max-concurrent-streams` | `SUPERBANK_GRPC_MAX_CONCURRENT_STREAMS` | `20` | HTTP/2 concurrent stream limit. |

## HTTP status behavior

By default, JSON-RPC responses use HTTP `200 OK`, including JSON-RPC error bodies. To make
infrastructure/server-side failures visible to HTTP-aware load balancers and clients, enable:

| Option | Environment | Default | Notes |
| --- | --- | --- | --- |
| `--emit-http-errors` | `SUPERBANK_RPC_EMIT_HTTP_ERRORS` | `false` | Returns HTTP `503 Service Unavailable` when the JSON-RPC response contains a server-side failure; JSON-RPC response bodies are unchanged. |

Only internal error (`-32603`), server-generated request timeout (`-32000`), node unhealthy
(`-32005`), and long-term storage unreachable (`-32019`) are promoted to HTTP `503`.
Client, malformed-request, and data-condition errors remain HTTP `200 OK`. For batches, any
eligible item promotes the whole HTTP response to `503`.

## Optional gRPC head cache (`grpc-head-cache`)

When compiled with `--features grpc-head-cache` and enabled at runtime, superbank-rpc subscribes to
a Yellowstone DragonsMouth gRPC stream via `yellowstone-block-machine` and keeps a small
in-memory cache of the most recent slots. RPC handlers can merge this "head" data with ClickHouse
to hide the typical ingestion lag.

For slot context resolution (`getSlot` and min-context checks), superbank-rpc prefers head-cache
slots for the requested commitment and falls back to ClickHouse only when the head cache has no
qualifying slot.

`processed` commitment is supported only when the head cache is enabled, and only for:
- `getSignaturesForAddress`
- `getSignatureStatuses`
- `getTransaction`
- `getTransactionsForAddress`
- `getBlockHeight`
- `getSlot`
- `getTransactionCount`
- `getLatestBlockhash`
- `isBlockhashValid`
- `getBlocks`
- `getBlocksWithLimit`

`getBlock` still requires `confirmed`/`finalized` commitment.

When the head cache is disabled (or not compiled), requests with `commitment=processed` are
rejected with JSON-RPC error `-32602` and include `requestedCommitment` in the error `data`.

`HEAD_CACHE_MIN_COMMITMENT` acts as an exposure floor: if set to `confirmed` or `finalized`,
`commitment=processed` will not be fresher than that minimum.

Run example:

```bash
RPC_HOST=0.0.0.0 RPC_PORT=8899 \
CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_DATABASE=default \
HEAD_CACHE_ENABLED=true \
DRAGONSMOUTH_ENDPOINT=https://YOUR_DRAGONSMOUTH_ENDPOINT \
DRAGONSMOUTH_X_TOKEN=YOUR_OPTIONAL_TOKEN \
cargo run -p superbank-rpc --features grpc-head-cache --
```

Configuration:

| Option | Environment | Default | Notes |
| --- | --- | --- | --- |
| `--head-cache-enabled` | `HEAD_CACHE_ENABLED` | `false` | Enables the feature at runtime. |
| `--dragonsmouth-endpoint` | `DRAGONSMOUTH_ENDPOINT` | — | Required when enabled. |
| `--dragonsmouth-x-token` | `DRAGONSMOUTH_X_TOKEN` | — | Optional auth header. |
| `--head-cache-retain-slots` | `HEAD_CACHE_RETAIN_SLOTS` | `32` | How many slots to retain in memory. |
| `--head-cache-min-commitment` | `HEAD_CACHE_MIN_COMMITMENT` | `processed` | `processed|confirmed|finalized` exposure gate for head reads. |
| `--grpc-max-decoding-bytes` | `GRPC_MAX_DECODING_BYTES` | `67108864` | Max gRPC decoding message size. |

License note: superbank-rpc is licensed under AGPL-3.0-only (see `../../LICENSE`).
The optional `grpc-head-cache` feature pulls in `yellowstone-block-machine` (also AGPL-3.0).

## Optional RocksDB disk cache (`disk-cache`)

When compiled with `--features disk-cache` (which implies `grpc-head-cache`) and enabled at
runtime, superbank-rpc keeps a RocksDB-backed cache of recent **finalized** slots on local disk
and serves it *in place of* ClickHouse. The read tiering becomes:

```
head cache (memory, unfinalized tip) -> disk cache (finalized, recent slots) -> ClickHouse (full history)
```

The cache is hydrated FROM ClickHouse ("backfill") on startup, kept current by copying slots out
of the head cache as the DragonsMouth stream finalizes them, and self-repairs gaps from
ClickHouse. It never writes to ClickHouse. Coverage is tracked per slot and claimed atomically
with the data, so the cache never serves a slot it holds partially; any miss, hole, or decode
failure silently degrades to a ClickHouse read.

Served from disk when covered: `getBlock` (all `transactionDetails` levels), `getBlocks`,
`getBlocksWithLimit`, `getBlockTime`, `getTransaction`, `getSignatureStatuses`,
`getSignaturesForAddress`, and `getTransactionsForAddress` (including `tokenAccounts` filters via
an on-disk port of the `gsfa` and `token_owner_activity` materialized views). Address queries are
answered from the contiguous covered span; ClickHouse is consulted only for the remainder strictly
below the coverage floor.

> [!WARNING]
> **Sizing:** at mainnet volume the default 10-epoch window (4,320,000 slots) needs on the order
> of **15–20 TB** of NVMe even with compression (~1.5–2 TB per epoch across the record and index
> column families). Set `DISK_CACHE_MAX_BYTES` to bound disk usage — the retention window shrinks
> to fit, with the tighter of the slot window and the byte budget winning.

Requires `HEAD_CACHE_ENABLED=true` with a usable `DRAGONSMOUTH_ENDPOINT` (the stream is the live
ingestion source). `HEAD_CACHE_RETAIN_SLOTS` is clamped to at least `64` when the disk cache is
enabled: the default `32` roughly equals the mainnet finalization lag, so finalized slots would
already be evicted from the head cache when the disk snapshot hook fires; `150` is a comfortable
setting (~1.5–3 GB of head-cache memory at mainnet rates).

Run example:

```bash
RPC_HOST=0.0.0.0 RPC_PORT=8899 CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_DATABASE=default HEAD_CACHE_ENABLED=true HEAD_CACHE_RETAIN_SLOTS=150 DRAGONSMOUTH_ENDPOINT=https://YOUR_DRAGONSMOUTH_ENDPOINT DISK_CACHE_ENABLED=true DISK_CACHE_PATH=/var/lib/superbank/disk-cache DISK_CACHE_MAX_BYTES=2199023255552 cargo run -p superbank-rpc --features disk-cache --
```

Configuration:

| Option | Environment | Default | Notes |
| --- | --- | --- | --- |
| `--disk-cache-enabled` | `DISK_CACHE_ENABLED` | `false` | Enables the feature at runtime. |
| `--disk-cache-path` | `DISK_CACHE_PATH` | — | RocksDB directory; required when enabled. |
| `--disk-cache-retain-slots` | `DISK_CACHE_RETAIN_SLOTS` | `4320000` | Finalized slots to retain (~10 epochs). |
| `--disk-cache-max-bytes` | `DISK_CACHE_MAX_BYTES` | `0` | Disk byte budget; `0` = unlimited. Tighter of window/budget wins. |
| `--disk-cache-block-cache-bytes` | `DISK_CACHE_BLOCK_CACHE_BYTES` | `4294967296` | RocksDB block cache shared across column families. |
| `--disk-cache-write-queue-slots` | `DISK_CACHE_WRITE_QUEUE_SLOTS` | `64` | Live write queue depth; overflow defers slots to repair. |
| `--disk-cache-read-concurrency` | `DISK_CACHE_READ_CONCURRENCY` | `64` | Max concurrent blocking disk reads. |
| `--disk-cache-backfill-enabled` | `DISK_CACHE_BACKFILL_ENABLED` | `true` | ClickHouse->disk backfill/repair task. |
| `--disk-cache-backfill-slots-per-query` | `DISK_CACHE_BACKFILL_SLOTS_PER_QUERY` | `8` | Slots per ClickHouse range query. |
| `--disk-cache-backfill-max-slots-per-sec` | `DISK_CACHE_BACKFILL_MAX_SLOTS_PER_SEC` | `50` | Backfill rate limit (the default fills 10 epochs in ~24h). |
| `--disk-cache-backfill-query-timeout-ms` | `DISK_CACHE_BACKFILL_QUERY_TIMEOUT_MS` | `30000` | Range scans need more than the interactive query timeout. |
| `--disk-cache-repair-interval-ms` | `DISK_CACHE_REPAIR_INTERVAL_MS` | `5000` | Idle wait between repair/backfill planning rounds. |
| `--disk-cache-repair-min-lag-slots` | `DISK_CACHE_REPAIR_MIN_LAG_SLOTS` | `75` | Never backfill slots ClickHouse ingest may not have landed. |

Observability: `superbank_disk_cache_*` metrics cover coverage span, hit/miss per operation,
write/backfill/repair/eviction activity, and the route metrics gain a `disk_cache_read` label.
The `X-Superbank-Sources` response header reports `disk-cache` combinations.

Parity validation against a reference target (e.g. the same build with the disk cache disabled):

```bash
k6 run tests/k6/scenarios/validation/superbank-rpc-disk-cache-parity.js \
  -e RPC_URL=http://disk-enabled:8899 -e REFERENCE_RPC_URL=http://reference:8899 \
  -e ADDRESS_FILE=tests/k6/data/pools/addresses.txt
```

Performance comparison against the same build without disk cache:

```bash
k6 run tests/k6/scenarios/performance/superbank-rpc-disk-cache-compare.js \
  -e RPC_URL=http://disk-enabled:8899 -e REFERENCE_RPC_URL=http://disk-disabled:8899 \
  -e ADDRESS_FILE=tests/k6/data/pools/addresses.txt \
  -e VUS=10 -e DURATION=60s
```

The performance scenario pre-probes the target and only keeps workload items whose
`X-Superbank-Sources` header reports a disk-cache hit, then reports per-method latency deltas and
speedup ratios versus the reference.

License note: the `disk-cache` feature implies `grpc-head-cache` and therefore also pulls in
`yellowstone-block-machine` (AGPL-3.0).

## Optional Pyroscope profiling (`pyroscope`)

When compiled with `--features pyroscope` and enabled at runtime, superbank-rpc captures CPU
profiles and uploads them to a Pyroscope server.

Local run example:

```bash
docker run --rm -p 4040:4040 grafana/pyroscope:latest

RPC_HOST=0.0.0.0 RPC_PORT=8899 \
CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_DATABASE=default \
PYROSCOPE_URL=http://localhost:4040 PYROSCOPE_APP_NAME=superbank-rpc PYROSCOPE_TAGS=env=dev \
cargo run -p superbank-rpc --features pyroscope -- --pyroscope
```

Configuration (only available when built with `--features pyroscope`):

| Option | Environment | Default | Notes |
| --- | --- | --- | --- |
| `--pyroscope` | `PYROSCOPE_ENABLED` | `false` | Enables profiling at runtime. |
| `--pyroscope-url` | `PYROSCOPE_URL` | — | Required when enabled. |
| `--pyroscope-app-name` | `PYROSCOPE_APP_NAME` | `superbank-rpc` | — |
| `--pyroscope-sample-rate` | `PYROSCOPE_SAMPLE_RATE` | `100` | CPU samples per second. |
| `--pyroscope-report-thread-name` | `PYROSCOPE_REPORT_THREAD_NAME` | `true` | Include thread names. |
| `--pyroscope-report-thread-id` | `PYROSCOPE_REPORT_THREAD_ID` | `false` | Include thread IDs. |
| `--pyroscope-tags` | `PYROSCOPE_TAGS` | empty | Repeatable; env accepts comma-separated `k=v`. |
| `--pyroscope-report-encoding` | `PYROSCOPE_REPORT_ENCODING` | `pprof` | `pprof|folded`. |
| `--pyroscope-compression` | `PYROSCOPE_COMPRESSION` | `gzip` | `gzip|off`. |
| `--pyroscope-auth-token` | `PYROSCOPE_AUTH_TOKEN` | — | Bearer token; preferred over basic auth. |
| `--pyroscope-basic-auth-user` | `PYROSCOPE_BASIC_AUTH_USER` | — | Requires `PYROSCOPE_BASIC_AUTH_PASS`. |
| `--pyroscope-basic-auth-pass` | `PYROSCOPE_BASIC_AUTH_PASS` | — | — |
| `--pyroscope-tenant-id` | `PYROSCOPE_TENANT_ID` | — | Sent as `X-Scope-OrgID`. |
| `--pyroscope-http-header` | `PYROSCOPE_HTTP_HEADERS` | empty | Repeatable; env accepts comma-separated `Header=Value`. |

Production note: for better stack traces, consider building with frame pointers, e.g.
`RUSTFLAGS="-C force-frame-pointers=yes"`.

## Configuration

CLI flags and environment variables (see `crates/superbank-rpc/src/config.rs`):

| Option | Environment | Default | Notes |
| --- | --- | --- | --- |
| `--rpc-max-body-bytes` | `RPC_MAX_BODY_BYTES` | `1048576` | Maximum accepted JSON-RPC request body size (bytes). |
| `--rpc-request-timeout-ms` | `RPC_REQUEST_TIMEOUT_MS` | `10000` | End-to-end timeout for a request envelope (single or batch). |
| `--rpc-concurrency-limit` | `RPC_CONCURRENCY_LIMIT` | `512` | Maximum number of in-flight HTTP JSON-RPC envelopes. |
| `--rpc-max-batch-size` | `RPC_MAX_BATCH_SIZE` | `64` | Maximum number of JSON-RPC calls in a single batch envelope. |
| `--rpc-batch-concurrency-limit` | `RPC_BATCH_CONCURRENCY_LIMIT` | `8` | Max concurrent item execution within one batch envelope. |
| `--emit-http-errors` | `SUPERBANK_RPC_EMIT_HTTP_ERRORS` | `false` | Return HTTP `503 Service Unavailable` for selected server-side JSON-RPC failures; response bodies are unchanged. |
| `--host` | `RPC_HOST` | `0.0.0.0` | — |
| `--port` | `RPC_PORT` | `8899` | — |
| `--metrics-host` | `METRICS_HOST` | `0.0.0.0` | — |
| `--metrics-port` | `METRICS_PORT` | `9900` | — |
| `--metrics-capture-header` | `METRICS_CAPTURE_HEADERS` | empty | Repeatable; env accepts comma-separated values. Supported: `X-Endpoint`, `X-RPC-Node`, `X-Subscription-ID`, `X-Account-ID`. Empty entries are ignored. Warning: Capturing unbounded header values can lead to high metric cardinality (for example in Prometheus). `X-Subscription-ID` and `X-Account-ID` are emitted as raw label values when enabled, so treat them as sensitive metadata and only capture trusted, bounded values. |
| `--superbank-grpc-enabled` | `SUPERBANK_GRPC_ENABLED` | `false` | Only available with `--features grpc-streaming`; enables the gRPC endpoint at runtime. |
| `--superbank-grpc-host` | `SUPERBANK_GRPC_HOST` | `0.0.0.0` | Only available with `--features grpc-streaming`. |
| `--superbank-grpc-port` | `SUPERBANK_GRPC_PORT` | `10000` | Only available with `--features grpc-streaming`. |
| `--superbank-grpc-max-slot-range` | `SUPERBANK_GRPC_MAX_SLOT_RANGE` | `100` | Only available with `--features grpc-streaming`. |
| `--superbank-grpc-query-timeout-ms` | `SUPERBANK_GRPC_QUERY_TIMEOUT_MS` | `30000` | Only available with `--features grpc-streaming`. |
| `--superbank-grpc-chunk-slots` | `SUPERBANK_GRPC_CHUNK_SLOTS` | `8` | Only available with `--features grpc-streaming`. |
| `--superbank-grpc-max-send-bytes` | `SUPERBANK_GRPC_MAX_SEND_BYTES` | `104857600` | Only available with `--features grpc-streaming`. |
| `--superbank-grpc-max-concurrent-streams` | `SUPERBANK_GRPC_MAX_CONCURRENT_STREAMS` | `20` | Only available with `--features grpc-streaming`. |
| `--clickhouse-url` | `CLICKHOUSE_URL` | `http://localhost:8123` | — |
| `--clickhouse-database` | `CLICKHOUSE_DATABASE` | `default` | — |
| `--clickhouse-user` | `CLICKHOUSE_USER` | `default` | — |
| `--clickhouse-password` | `CLICKHOUSE_PASSWORD` | empty | — |
| `--max-signatures-limit` | `MAX_SIGNATURES_LIMIT` | `1000` | — |
| `--clickhouse-query-timeout-ms` | `CLICKHOUSE_QUERY_TIMEOUT_MS` | `8000` | Per-query ClickHouse timeout (ms). When query `SETTINGS` are enabled, superbank-rpc injects this budget as `max_execution_time` on read queries so ClickHouse abandons a query (instead of leaving it running and holding a connection) once superbank-rpc stops awaiting it. In shard-direct TCP mode it additionally uses a shorter internal TCP-attempt timeout inside this budget to trigger best-effort cleanup of abandoned shard-local TCP reads. Keep the parent value below `RPC_REQUEST_TIMEOUT_MS`. |
| `--clickhouse-http-max-concurrency` | `CLICKHOUSE_HTTP_MAX_CONCURRENCY` | `512` | Max concurrent direct (scalar/lookup) ClickHouse HTTP queries in flight server-wide. Bounds HTTP connections to ClickHouse independently of shard fanout and JSON-RPC batching; excess queries wait (and may time out, shedding load) rather than opening more connections. Set at or below the ClickHouse per-user connection/query budget. |
| `--clickhouse-http-connect-timeout-ms` | `CLICKHOUSE_HTTP_CONNECT_TIMEOUT_MS` | `2000` | TCP connect timeout (ms) for ClickHouse HTTP connections, so a new connection attempt fails fast during ClickHouse backpressure instead of hanging. |
| `--clickhouse-query-cache-enabled` | `CLICKHOUSE_QUERY_CACHE_ENABLED` | `false` | Enables ClickHouse query cache settings for historical read queries. |
| `--clickhouse-query-cache-ttl-seconds` | `CLICKHOUSE_QUERY_CACHE_TTL_SECONDS` | `1` | TTL for cached historical read query results (seconds). |
| `--clickhouse-get-transaction-query-cache-ttl-seconds` | `CLICKHOUSE_GET_TRANSACTION_QUERY_CACHE_TTL_SECONDS` | `300` | TTL override applied only to historical `getTransaction` point lookups when query cache is enabled. |
| `--clickhouse-get-transaction-query-cache-min-query-runs` | `CLICKHOUSE_GET_TRANSACTION_QUERY_CACHE_MIN_QUERY_RUNS` | `2` | Minimum identical `getTransaction` point-lookups required before ClickHouse writes them into cache. |
| `--clickhouse-query-cache-share-between-users` | `CLICKHOUSE_QUERY_CACHE_SHARE_BETWEEN_USERS` | `false` | Controls `query_cache_share_between_users` for historical read queries. |
| `--clickhouse-query-condition-cache-enabled` | `CLICKHOUSE_QUERY_CONDITION_CACHE_ENABLED` | `false` | Enables `use_query_condition_cache=1` for selected historical address-filtered read queries. |
| `--clickhouse-transport` | `CLICKHOUSE_TRANSPORT` | `http` | `tcp` or `http` (`tcp` requires `CLICKHOUSE_SCOPE=shard-direct`). |
| `--clickhouse-scope` | `CLICKHOUSE_SCOPE` | `distributed` | `distributed` or `shard-direct`. |
| `--clickhouse-tcp-access-check-timeout-ms` | `CLICKHOUSE_TCP_ACCESS_CHECK_TIMEOUT_MS` | `2000` | Startup TCP access-check timeout (ms). |
| `--clickhouse-tcp-pool-min` | `CLICKHOUSE_TCP_POOL_MIN` | `10` | Minimum connections retained per shard in each ClickHouse native (TCP) connection pool. |
| `--clickhouse-tcp-pool-max` | `CLICKHOUSE_TCP_POOL_MAX` | `20` | Maximum connections per shard in each ClickHouse native (TCP) connection pool. Total native connections per instance are bounded by this value times the number of shards, so size it against the ClickHouse connection budget. |
| `--clickhouse-cluster` | `CLICKHOUSE_CLUSTER` | `{cluster}` | — |
| `--clickhouse-topology-config` | `CLICKHOUSE_TOPOLOGY_CONFIG` | — | Optional authoritative YAML shard topology. When set, superbank-rpc skips `system.clusters` discovery at startup and uses the YAML shard/IP/port mapping directly for shard-local connections. |
| `--clickhouse-gsfa-local-table` | `CLICKHOUSE_GSFA_LOCAL_TABLE` | — | Required for shard-direct GSFA routing. |
| `--clickhouse-hot-address` | `CLICKHOUSE_GSFA_HOT_ADDRESSES` | empty | Repeatable; env accepts comma-separated values. |
| `--clickhouse-gsfa-hot-table` | `CLICKHOUSE_GSFA_HOT_TABLE` | `default.gsfa_hot` | Distributed hot table used for active hot-address reads. |
| `--clickhouse-gsfa-hot-local-table` | `CLICKHOUSE_GSFA_HOT_LOCAL_TABLE` | `default.gsfa_hot_local` | Shard-local backing table behind `CLICKHOUSE_GSFA_HOT_TABLE`. |
| `--clickhouse-signatures-local-table` | `CLICKHOUSE_SIGNATURES_LOCAL_TABLE` | — | — |
| `--clickhouse-token-owner-activity-local-table` | `CLICKHOUSE_TOKEN_OWNER_ACTIVITY_LOCAL_TABLE` | — | — |
| `--clickhouse-transactions-local-table` | `CLICKHOUSE_TRANSACTIONS_LOCAL_TABLE` | — | — |
| `--clickhouse-blocks-metadata-local-table` | `CLICKHOUSE_BLOCKS_METADATA_LOCAL_TABLE` | — | — |
| `--clickhouse-shard-http-port` | `CLICKHOUSE_SHARD_HTTP_PORT` | — | — |

Table selection (environment variables, read at startup):

| Environment | Default | Notes |
| --- | --- | --- |
| `CLICKHOUSE_TRANSACTION_TABLE` | `default.transactions` | — |
| `CLICKHOUSE_SIGNATURE_TABLE` | — | Legacy alias for `CLICKHOUSE_TRANSACTION_TABLE`. |
| `CLICKHOUSE_BLOCKS_METADATA_TABLE` | `default.blocks_metadata` | — |
| `CLICKHOUSE_GSFA_TABLE` | `default.gsfa` | — |
| `CLICKHOUSE_GSFA_HOT_TABLE` | `default.gsfa_hot` | — |
| `CLICKHOUSE_SIGNATURE_STATUSES_TABLE` | `default.signatures` | — |
| `CLICKHOUSE_TOKEN_OWNER_ACTIVITY_TABLE` | `default.token_owner_activity` | — |

Shard routing:
When `CLICKHOUSE_SCOPE=shard-direct`, superbank-rpc discovers shards from `system.clusters` and
validates local table schemas. Local tables default to `{table}_local` when not provided
explicitly. `CLICKHOUSE_TRANSPORT` selects the shard-direct transport (`tcp` or `http`).
Set `CLICKHOUSE_TOPOLOGY_CONFIG` (or `--clickhouse-topology-config`) to make a YAML topology file
authoritative for shard-local connection targets and skip `system.clusters` discovery at startup.
When multiple YAML nodes are listed for the same shard, the first node listed for that shard is
used as the shard-local TCP/HTTP connection target, and the remaining nodes are ignored.
The YAML `ip-address` field is the authoritative connection address and does not need to match
ClickHouse `host_address`. Shard-local TCP uses YAML `ip-address` and `tcp-port`; shard-local HTTP
uses the same YAML `ip-address` plus `CLICKHOUSE_SHARD_HTTP_PORT` or the port from
`CLICKHOUSE_URL`.
YAML keys support both kebab-case and snake_case:

```yaml
nodes:
  - shard-id: 1
    hostname: ch-bhs1
    ip-address: 10.43.86.5
    tcp-port: 9000
    shard-weight: 1
```

For GSFA shard routing, clustered schemas must define `CLICKHOUSE_GSFA_TABLE` (normally
`default.gsfa`) as the materialized view itself, with `ENGINE = Distributed(...,
CLICKHOUSE_GSFA_LOCAL_TABLE, cityHash64(address))`. Superbank-rpc fails startup in shard-direct mode
if it detects the legacy split layout with a separate `gsfa_mv` object or any other incompatible
GSFA writer shape.

Shard-direct TCP reads:
- superbank-rpc now assigns real ClickHouse `query_id` values to shard-local TCP reads even when SQL comment annotation is disabled.
- If a shard-local TCP read times out or the request future is dropped, superbank-rpc issues a best-effort `KILL QUERY ... ASYNC` over shard-local HTTP to reduce orphaned work that would otherwise surface later as `210 NETWORK_ERROR` broken pipes.
- Transient shard-local TCP failures on GSFA, signature-status, signature-slot, and transactions-for-address reads fall back to the existing shard-local HTTP path by default before any distributed-table fallback.

Additional env flags:

| Environment | Default | Notes |
| --- | --- | --- |
| `LOG_FORMAT` | `plain` | `plain` or `json`. |
| `CLICKHOUSE_QUERY_ID_PREFIX` | `superbank` | `auto` or `off`/`0`/`false` disables. |
| `CLICKHOUSE_GSFA_STRICT_PAGINATION` | `true` | — |
| `CLICKHOUSE_GSFA_FALLBACK_TRANSACTIONS` | disabled | `empty`/`true` for empty-only fallback; `force`/`always` for incomplete fallback. |
| `CLICKHOUSE_DISABLE_QUERY_SETTINGS` | `false` | Disables per-query ClickHouse `SETTINGS` overrides (including shard optimization, query-cache, and query-condition-cache settings) when truthy. |

### ClickHouse query cache (read queries)

- Scope: `superbank-rpc` applies query-cache settings only on historical `SELECT` paths.
- Tip-sensitive reads (`latest`-style queries) bypass query cache to reduce stale-head risk.
- This feature does not write to application tables. It only controls ClickHouse read-query cache behavior.
- When enabled for historical reads, superbank-rpc sets:
  - `use_query_cache=1`
  - `enable_reads_from_query_cache=1`
  - `enable_writes_to_query_cache=1`
  - `query_cache_ttl=<CLICKHOUSE_QUERY_CACHE_TTL_SECONDS>`
  - `query_cache_share_between_users=<0|1>`
- Historical `getTransaction` point lookups can override the general historical cache settings with:
  - `query_cache_ttl=<CLICKHOUSE_GET_TRANSACTION_QUERY_CACHE_TTL_SECONDS>`
  - `query_cache_min_query_runs=<CLICKHOUSE_GET_TRANSACTION_QUERY_CACHE_MIN_QUERY_RUNS>`
- These `getTransaction` overrides still honor `CLICKHOUSE_QUERY_CACHE_ENABLED`; if the general query cache is disabled, superbank-rpc does not force it on for any method.
- Query-cache capacity and eviction behavior remain ClickHouse server configuration. superbank-rpc does not modify cluster-level cache capacity or `system.query_cache`.
- Superbank-side metrics:
  - `superbank_rpc_clickhouse_query_cache_total{operation,cache}` (`cache=eligible|bypassed`)
  - `superbank_rpc_clickhouse_query_cache_settings_total{operation,reads,writes,ttl}`
- Note: these superbank metrics track cache eligibility/settings application, not true ClickHouse cache hit/miss outcomes.

### ClickHouse query condition cache (selected reads)

- Scope: `superbank-rpc` applies `use_query_condition_cache=1` only on selected historical address-filtered reads:
  - `getTransactionsForAddress`
  - the transactions-table fallback path for `getSignaturesForAddress`
- Point lookups and slot-range reads do not opt in.
- This setting is enabled separately from the query-result cache via:
  - `--clickhouse-query-condition-cache-enabled`
  - `CLICKHOUSE_QUERY_CONDITION_CACHE_ENABLED`
- `CLICKHOUSE_DISABLE_QUERY_SETTINGS=true` or ClickHouse readonly mode disables this override along with the other per-query `SETTINGS`.

## GSFA hot addresses

Use hot addresses to route specific accounts to a dedicated GSFA table (for heavily queried
addresses like USDC). Configure one or more hot addresses with `--clickhouse-hot-address` or
`CLICKHOUSE_GSFA_HOT_ADDRESSES` (comma-separated).

Entries are trimmed before use. Empty or invalid pubkeys are ignored and do not enable hot
routing by themselves.

| Option | Environment | Default | Notes |
| --- | --- | --- | --- |
| `--clickhouse-hot-address` | `CLICKHOUSE_GSFA_HOT_ADDRESSES` | empty | Repeatable; env accepts comma-separated values. |
| `--clickhouse-gsfa-hot-table` | `CLICKHOUSE_GSFA_HOT_TABLE` | `default.gsfa_hot` | Distributed hot table used for active hot-address reads. |
| `--clickhouse-gsfa-hot-local-table` | `CLICKHOUSE_GSFA_HOT_LOCAL_TABLE` | `default.gsfa_hot_local` | Shard-local backing table behind `CLICKHOUSE_GSFA_HOT_TABLE`. |

Startup checks:
- The hot distributed table must exist and contain rows for each configured address.
- If a hot address has no rows (or the hot table is unavailable), that address falls back to
  the standard GSFA table and a warning is logged.

Routing behavior:
- Active hot addresses always query `CLICKHOUSE_GSFA_HOT_TABLE`, even when
  `CLICKHOUSE_SCOPE=shard-direct` and `CLICKHOUSE_TRANSPORT=tcp`.
- `CLICKHOUSE_GSFA_HOT_LOCAL_TABLE` remains the shard-local backing table for the distributed hot
  table, but superbank-rpc no longer queries it directly.

Hot table schema expectations:
- Same columns as `default.gsfa_local` (`addr_bucket`, `address`, `signature`, `slot`,
  `slot_idx`, `memo`, `err`, `block_time`).
- Partitioning and ordering should favor the access pattern (latest-first reads).

## Metrics

Prometheus metrics are served at `/metrics` on `METRICS_HOST:METRICS_PORT`.

Route normalization metric:

- `superbank_rpc_route_total{method,transport,scope,source,head_cache_read,disk_cache_read,outcome,x_endpoint,x_rpc_node,x_subscription_id,x_account_id}`
  - `method`: supported JSON-RPC method name.
  - `transport`: `tcp|http` (active ClickHouse routing transport policy).
  - `scope`: `distributed|shard_direct` (active ClickHouse routing scope policy).
  - `source`: `clickhouse|head_cache|disk_cache|none` (primary source used for the returned response).
  - `head_cache_read`: `true|false` (whether handler read from head cache on that request).
  - `disk_cache_read`: `true|false` (whether handler read from the RocksDB disk cache on that request).
  - `outcome`: `success|not_found|invalid_params|rpc_error|backend_error|timeout`.
  - `x_endpoint`: omitted when capture is disabled; otherwise `missing|<value>` (`<value>` is the raw `X-Endpoint` header value).
  - `x_rpc_node`: omitted when capture is disabled; otherwise `missing|<value>`.
  - `x_subscription_id`: omitted when capture is disabled; otherwise `missing|<value>` (`<value>` is the raw `X-Subscription-ID` header value).
  - `x_account_id`: omitted when capture is disabled; otherwise `missing|<value>` (`<value>` is the raw `X-Account-ID` header value).

Request-scoped metric families:

- `rpc_requests`, `rpc_response_time_seconds`, `rpc_inflight_requests`, `rpc_timeouts`, `rpc_response_overhead_seconds`, `rpc_blocks_slots_returned`, `rpc_batch_requests`, `rpc_batch_items`, `rpc_batch_size`, `rpc_batch_rejected_total`, `rpc_backend_errors`, `rpc_clickhouse_duration_seconds`, `rpc_clickhouse_received_bytes`, `rpc_clickhouse_decoded_bytes`, `rpc_clickhouse_timeouts`, `rpc_clickhouse_query_cache_total`, `rpc_clickhouse_query_cache_settings_total` can include `x_endpoint`, `x_rpc_node`, `x_subscription_id`, and `x_account_id` when each capture option is enabled.
- For those labels, values are `missing|<raw-value>` for enabled capture; disabled capture omits the label.

Superbank gRPC streaming metrics, emitted only with `--features grpc-streaming`:

- `superbank_grpc_stream_requests_total{method}`
- `superbank_grpc_stream_chunks_total{method}`
- `superbank_grpc_stream_messages_total{method}`
- `superbank_grpc_stream_errors_total{method,stage}`

Response metric headers:

- `X-Superbank-Sources`: downstream sources consulted while serving the JSON-RPC response. Values
  are `none`, `clickhouse`, `head-cache`, `both` (legacy head-cache + ClickHouse), `disk-cache`,
  `disk-cache,clickhouse`, `head-cache,disk-cache`, or `all`. For batch responses, this is the
  aggregate source footprint across batch items.
- `X-Superbank-Metrics`: aggregate ClickHouse timing/volume counters for the response envelope. Format:
  - `rows_read=<u64>|unknown;rows_returned=<u64>;data_read_bytes=<u64>`
  - For batch responses, counters are aggregated across batch items.
- `X-Downstream-Timings` is removed and is no longer emitted.
  - This is a breaking change for clients that relied on that header.
  - Migrate to `X-Superbank-Metrics` and/or Prometheus metrics for downstream timing and volume data.
  - Check GitHub release notes for rollout timing in your deployment version.

Head cache activation metric:

- `head_cache_active{x_rpc_node}`
  - Value is `1` for the active head-cache upstream node label, else `0`.
  - `x_rpc_node="none"`: head cache is disabled.
  - `x_rpc_node="unknown"`: head cache is enabled, but upstream metadata did not include `x-rpc-node`.
  - `x_rpc_node="<value>"`: concrete upstream node identifier reported by DragonsMouth metadata.
