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
- `getBlock` requires the returned transaction count to match block metadata for `full`,
  `accounts`, and `signatures` responses. Incomplete cache data falls back to storage;
  inconsistent source data returns internal error (`-32603`) without populating the serialized
  response cache. A subsequent request can succeed once storage is repaired. Metadata-only
  (`transactionDetails: "none"`) requests and valid empty blocks remain supported.
- JSON-RPC batch envelopes are supported. Batch execution is bounded by
  `RPC_MAX_BATCH_SIZE` and `RPC_BATCH_CONCURRENCY_LIMIT`.
- Requests without an `id` are normalized to `id: null` and still return
  JSON-RPC response bodies (compatibility behavior; not strict notification semantics).
- `processed` commitment is rejected by default; use `confirmed` or `finalized`.
- `getInflationReward` reads the payout epoch boundary, serves boundary vote rewards immediately,
  and queries only the partition block heights required by unresolved requested addresses. A stake
  reward becomes available as soon as that address's partition block lands; the method does not
  wait for later partitions or expand reward arrays across a complete epoch. If the payout boundary
  does not exist yet, it returns `-32004` (`Block not available`) over HTTP `200`. If a requested
  partition is still pending, it returns `-32017` (`Epoch rewards period still active`) over HTTP
  `200`, with `slot`, `currentBlockHeight`, and `rewardsCompleteBlockHeight` in `error.data`.
  Missing rewards are returned as `null` only after the address's required partition is available.
  Dedicated address, concurrency, timeout, thread, memory, and read-byte limits are enabled by
  default.
  Historical non-partitioned rewards do not require block height metadata. Partitioned rewards
  still require it to locate payout blocks and determine reward availability.
- Reward objects expose the optional Agave `commissionBps` field when the ingested source supplied
  it. Legacy rows ingested before the basis-point columns were deployed omit the field; Superbank
  does not infer it from the legacy percentage `commission` value.
- Transaction v1 (SIMD-0385) is supported. Requests must set
  `maxSupportedTransactionVersion: 1`; JSON encodings report `version: 1` and expose the inline
  `message.transactionConfig`, while binary encodings preserve the signed v1 wire bytes.
- Rewards accept both `DeactivatedStake` and the historical producer spelling
  `deactivated-stake`, and are emitted as Agave's typed `DeactivatedStake` JSON value.
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
- `getSignatureStatuses` looks up recent statuses in the enabled cache tiers: the short-lived head
  cache and, when compiled with `--features disk-cache` and enabled, the finalized disk cache. The
  ClickHouse-backed history tier is searched only when the second param sets
  `searchTransactionHistory: true`; without it, a signature outside the enabled cache retention
  still returns `null`.
- `getTransaction` accepts the standard Solana config fields plus an optional Superbank extension:
  - `slot`: optional `u64`; when supplied, ClickHouse is queried directly for that exact slot and
    the response is `null` if the signature is not present in that slot.
- `getSignaturesForAddress` accepts standard Solana config fields plus optional Superbank extensions:
  - Each returned entry includes `transactionIndex`, the transaction's zero-based position in the
    block's flattened transaction list.
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

### HTTP SELECT lifetime and cancellation

HTTP SELECT reads share one lifetime wrapper across RPC methods, including both
`getTransaction` lookup stages, signature/address lookups, blocks and reward queries,
local disk-cache reads, cache/index/background reads, and shard-local HTTP reads.
Each read receives a required query ID with a random process namespace and monotonic
counter, preventing collisions across instances sharing a prefix or container PID.
Each read also receives HTTP parameters `readonly=2` and
`cancel_http_readonly_queries_on_client_close=1`. These settings apply to the individual
read; the shared application client and write profile remain writable. Disabling optional
SQL tuning does not disable these required HTTP settings, and unsupported protection
fails the read without an unprotected retry.

Cancellation capability is initialized before an endpoint can serve protected reads.
Primary startup validates topology and process-inspection capability before serving RPC
traffic; a shard HTTP endpoint whose preflight fails cannot submit protected reads.
Initialization uses a separate control connection pool, validates unique `hostName()`
identities, and executes the actual process-inspection probe before caching success.
Ready reads perform no discovery or process probes on their successful path.
Failed or cancelled initialization shares a one-second retry backoff across client clones;
internal logs include the failing phase and selected timeout.

Configure `CLICKHOUSE_CLUSTER=rbx2` for RBX2, or an empty string for standalone ClickHouse.
Existing `{cluster}` macros remain supported. Distributed verification uses the configured
gateway and `clusterAllReplicas` to inspect the coordinator and every expected replica;
it does not require application access to individual cluster nodes. Standalone and
shard-local HTTP endpoints verify their own server. The application account must be able
to read `system.clusters`, `system.one`, and `system.processes` as required by the endpoint,
and use the required query settings. Cached topology is fixed for the endpoint lifetime;
a topology change requires restart and successful preflight.

Successful reads consume the complete response, including EOF after a single-row result,
before releasing admission. A valid first row does not hide a later stream or decoding
error. Successful EOF generates no verification probes and permits connection reuse.
Dropping a query before submission releases its admission immediately. Once submitted,
abandonment closes the data response before scheduling verification and retains its read
and associated workflow permits until termination is confirmed.

One verifier is shared by clones of each endpoint and uses its separate control pool.
It probes pending IDs in batches of at most **128**, with one probe in flight per verifier.
Two consecutive fully covered observations must show neither the query ID nor its
`initial_query_id` on any expected node. Checks run at a 250 ms interval with one-thread probes.
The startup and runtime verification budgets each default to **10 seconds**. Set
`--clickhouse-startup-verification-timeout-ms` / `CLICKHOUSE_STARTUP_VERIFICATION_TIMEOUT_MS`
for each macro-resolution, topology-discovery, and capability-probe query, including retries.
This is a per-query budget, not an overall startup deadline. Set
`--clickhouse-runtime-verification-timeout-ms` / `CLICKHOUSE_RUNTIME_VERIFICATION_TIMEOUT_MS`
for each runtime batch that verifies abandoned reads. Both options require positive integer
milliseconds; CLI flags override the environment, and neither option disables verification.
The client enforces the exact millisecond budget. ClickHouse `max_execution_time` and
`max_execution_time_leaf` round that budget up to whole seconds. Both budgets propagate to
primary, shard, and local-cache endpoints, including clones and background readers.
The independent HTTP connection timeout remains **2 seconds** by default; raising a verification
budget does not extend connection establishment or change normal query deadlines.

After a probe completes, reads still unconfirmed after five seconds record `unconfirmed`,
retain their permits, and retry at a one-second interval. A slow 10-second probe can delay
that warning beyond five seconds. The 250 ms polling delay, one-second unconfirmed retry,
one-second idle-worker wakeup, and one-second initialization backoff are independent of the
verification budgets. Longer runtime probes hold admission longer and delay later batches.
Probe errors, incomplete coverage, or changed topology reset the absence evidence;
none establishes termination. Later successful verification restores capacity. A failed
verifier can therefore hold all capacity in the affected admission lane and make new
reads wait or time out, rather than release capacity while source work may remain active.

Existing RPC, primary-status, HTTP, fanout, and background concurrency settings keep
their configured values. Background maintenance and index readers use persistent
admission lanes separate from interactive cache reads; their explicit longer deadlines
are preserved. This change does not increase production concurrency or change response
lookup semantics.

Exceptions are explicit: bootstrap/discovery/control probes and bounded health probes
use their control paths to avoid recursive verification; writes, inserts, DDL, and native
TCP reads do not use the HTTP SELECT wrapper. Native TCP reads retain their existing
best-effort cleanup. Protected HTTP SELECT cleanup never uses `KILL QUERY` or the
distributed DDL queue.

Absence observations establish termination, not its cause. The production gateway must
close the upstream request when the downstream closes and discard buffered requests
rather than forwarding them after abandonment. Otherwise a query can appear after two
quiet observations. Validate this contract on the deployed route; a local proxy fixture
is not deployment evidence.

Shared metrics have bounded `operation` and `target` labels, never query IDs:

| Metric | Meaning |
| --- | --- |
| `superbank_clickhouse_read_disconnect_pending{operation,target}` | Abandoned HTTP reads retaining admission. |
| `superbank_clickhouse_read_disconnect_verification_total{operation,target,outcome}` | `confirmed_absent` and `unconfirmed` outcomes; the latter is recorded once per abandoned read. |
| `superbank_clickhouse_read_disconnect_probe_seconds{target,outcome}` | Control-probe latency histogram; targets are `cluster` or `local`, outcomes `success` or `error`. |

Pending/verification target classes are `primary`, `cache`, `shard`, and `background`.
Probe targets describe verification scope (`cluster` or `local`), rather than the
admission lane. See the [HTTP cancellation protocol gate](../../tests/k6/README.md#clickhouse-http-cancellation-protocol-gate)
for lifecycle, correctness, and matched-baseline performance requirements.

### Primary signature-status overload protection

History requests may contain up to 256 signatures. Cache-negative membership skips only
local cache work: unresolved signatures still require a primary lookup when
`searchTransactionHistory` is true. Primary status admission remains shared across client
clones and precedes HTTP admission within `CLICKHOUSE_QUERY_TIMEOUT_MS`; keep this timeout
below `RPC_REQUEST_TIMEOUT_MS`.

`GET_SIGNATURE_STATUSES_MAX_CONCURRENCY` and `GET_SIGNATURE_STATUSES_MAX_THREADS` retain
their existing meanings and configured values. Primary status queries keep their explicit
thread/index-thread, replica, and remaining execution-time limits, including when optional
query tuning is disabled. The shared HTTP wrapper retains the status source permit during
abandonment verification; unrelated RPC methods do not acquire the status semaphore.

Existing status metrics remain available:
`superbank_rpc_signature_status_batch_size{stage="input"|"primary_fallback"}` counts accepted
input arrays (including duplicates) and unresolved history candidates before admission.
Fallback observations include zero when caches resolve the candidates; they are not counts
of submitted queries. `superbank_rpc_signature_status_admission_seconds` includes completed
and cancelled admission waits. `superbank_rpc_signature_status_disconnect_pending` and
`superbank_rpc_signature_status_disconnect_verification_total{outcome="confirmed_absent"|"unconfirmed"}`
remain compatible with status-specific monitoring. Native TCP legacy cleanup outcomes remain
under `superbank_rpc_clickhouse_shard_query_cleanup_total_total{operation="signature_statuses"}`.

See the [miss replay and cancellation gate](../../tests/k6/README.md#signature-status-miss-replay-and-cancellation-evidence)
for the matched-rate 256-signature workload. Preserve fast-negative behavior during warm
and cold cache validation; cache warming delays are not backend protection.

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

For the Agave 4.2 rollout, apply transaction-column and materialized-view DDL first, deploy
`superbank-rpc` next (disk-cache schema 3 intentionally rebuilds existing caches), and only then
deploy ingestion with transaction version 1 enabled. Rolling back the RPC binary is safe only
before v1 or `DeactivatedStake` rows have arrived. The optional ClickHouse forward cache detects
later source schema changes and rebuilds its owned local database automatically.
## Run

```bash
RPC_HOST=0.0.0.0 RPC_PORT=8899 \
CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_DATABASE=default \
cargo run -p superbank-rpc --
```

### Cluster genesis for epoch schedules and inflation rewards

`getInflationReward` needs the epoch schedule for the same Solana cluster represented by the
ClickHouse data. Operators of a cluster with warmup epochs must mount that cluster's exact
`genesis.bin` read-only into the RPC container or host and set `GENESIS_PATH` to the mounted path.
Do not reuse a genesis file from another network: doing so calculates payout-epoch bounds for the
wrong slots. If `GENESIS_PATH` is unset, superbank-rpc uses the production no-warmup fallback,
which is appropriate for mainnet and devnet.

For example, an operator-managed container mount can be configured as:

```bash
docker run --rm \
  -v /srv/solana/mainnet-beta/genesis.bin:/etc/superbank/genesis.bin:ro \
  -e GENESIS_PATH=/etc/superbank/genesis.bin \
  superbank:0.5.0
```

This setting controls both `getEpochSchedule` responses and internal `getInflationReward`
epoch math. For testnet, supply its exact current genesis file: testnet uses warmup epochs,
so the no-warmup fallback is incorrect even for recent payout boundaries.

## Exact method and parameter filters

`superbank-rpc` can reject configured method and parameter combinations before they enter handler
dispatch or use any cache or ClickHouse resources. Pass the shared YAML configuration with
`--config superbank.yaml` or `SUPERBANK_CONFIG=superbank.yaml` and add:

```yaml
rpc-parameter-filters:
  - [getTransactionsForAddress, So11111111111111111111111111111111111111112]
  - [getTransactionsForAddress, So11111111111111111111111111111111111111112, {transactionDetails: signatures}]
```

Each entry contains the case-sensitive method followed by its complete parameter array. Method
names cannot have leading or trailing whitespace. Matching is structural and exact: parameter
count, array order, JSON types, and values must match; mapping key order does not matter. Extra
parameters do not match. A method-only entry matches an explicitly empty `params: []` array;
omitted `params` is distinct.

A matching call preserves the request ID in a JSON-RPC error with code `-32602` and message
`Invalid params: request blocked by parameter filter`. Its error `data` echoes the matched
`method` and complete `params` array. It remains HTTP `200 OK`, including when
`--emit-http-errors` is enabled, because it is a client error rather than a server-side failure.
In a mixed batch, allowed calls still execute and matched calls receive individual errors in their
original positions. Filters are validated and indexed once at startup, so changing the file
requires restarting `superbank-rpc`.

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
For `getInflationReward`, both boundary-unavailable (`-32004`) and rewards-period-active (`-32017`)
are data-condition errors and remain HTTP `200`; ClickHouse query, metadata, and integrity failures
continue to use internal error (`-32603`) and are eligible for HTTP `503`.

The `JSON-RPC HTTP response` log event reports the final envelope `status` after promotion,
including batch responses, and `http_elapsed_ms`. It is emitted at INFO for server errors or
slow responses and DEBUG otherwise. Per-method timing and slow-request logs use `handler_status`
for the status before envelope promotion; that field is not the HTTP status seen by the client.
For a mixed batch, successful items can have `handler_status=200` while the envelope has
`status=503`. Queries for final HTTP status should select the envelope event. INFO logs omit fast successful
responses and cannot provide a total-request denominator. Per-method request metrics continue
to describe handler outcomes before envelope promotion.

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

### `getBlocks` coverage and latest-slot authority

`getBlocks` plans the complete requested range from the in-memory block index, head cache,
and local disk coverage before reading primary ClickHouse. Parent slots and hashes from
one head subscription prove both produced blocks and intervening skipped slots. Missing
metadata, conflicting forks, and concurrent invalidation leave coverage unknown. The
primary is queried only for remaining gaps; a failed gap query returns internal error
(`-32603`), never a partial successful list. The index can answer without a local disk query.

When head cache is enabled, an omitted `endSlot` uses only its commitment-specific tip.
There is **no primary ClickHouse latest-slot fallback**. Tip trust requires a connected
subscription, a verified parent edge (or the genesis root), and advancement at the requested commitment within
one second (inclusive), measured with a monotonic clock. Duplicate or older updates and
heartbeats do not renew freshness. Finalized progress can satisfy confirmed requests;
processed progress cannot keep confirmed or finalized tips fresh. A reconnect clears
proof state and requires new evidence. An uninitialized, disconnected, stale, or
unverifiable newest tip returns `-32603`; the server never selects an older complete tip.
This measures observed progress, not upstream distance from the network tip.

Explicit-end requests do not require fresh latest-slot discovery. With head cache disabled,
the existing primary latest-slot cache remains in use. `getBlocksWithLimit` shares the
coverage helper and retains its existing slot-window interpretation of the limit.

Existing source labels retain their values (`clickhouse`, `disk_cache`, `head_cache`,
`none`); the memory block index is classified as `disk_cache`. The histogram
`superbank_rpc_blocks_range_seconds{method,path,reason,outcome}` distinguishes `local`,
`partial_cache`, and `primary` paths, with `success`/`error` outcomes. Reasons are bounded
by `none`, `coverage_gap`, `commitment_unavailable`, `untrusted_tip`, and `cache_changed`.
Its count measures requests, not submitted queries. Downstream elapsed timing includes
latest-slot refresh/wait time and range reads; primary latest-slot row counts are marked
unknown. Source headers describe touched sources, not completeness proofs.

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

## Optional local ClickHouse forward cache (`disk-cache`)

When compiled with `--features disk-cache` and enabled at runtime, superbank-rpc forwards recent **finalized** slots from the configured source ClickHouse cluster into a separate ClickHouse instance on localhost. The feature is independent from `grpc-head-cache`. The local instance is a near cache, not a second source of truth. The read tiering is:

`DISK_CACHE_BLOCK_INDEX_ENABLED=true` adds a durable full-history `(slot, block_time)` store for `getBlockTime`, `getBlocks`, and `getBlocksWithLimit` in the separately owned local database `<cache_database>_block_index`. Its in-process read tier is deliberately bounded by the required `DISK_CACHE_BLOCK_INDEX_MAX_MEMORY_BYTES`: one-million-slot segments are retained only while they fit that budget, rounded down to whole segments. Startup remains asynchronous: ranges outside the hydrated window continue through the recent cache and source fallbacks. The historical worker reads finalized source metadata only, fills newest ranges first, and then proceeds toward slot zero. This feature cannot be combined with the ClickHouse Memory-engine mode.

```
head cache (optional, unfinalized tip) -> local ClickHouse cache (finalized, recent slots) -> source ClickHouse (full history)
```

At startup, superbank-rpc inspects the source tables through `system.tables` and `system.columns`. It copies columns, types, defaults, codecs, indexes, primary keys, sorting keys, and materialized-view queries into a cache-specific schema. Local MergeTree tables use slot-range partitions so retention can drop complete old partitions. The forwarder streams the source `transactions` and `blocks_metadata` tables in ClickHouse Native format over HTTP. Local materialized views build `signatures`, `gsfa`, optional `gsfa_hot`, and optional `token_owner_activity` from each transaction insert.

Coverage is published only after the base rows, dependent materialized views, and transaction-count validation complete. A hole, partial slot, local query error, or unavailable cache falls through to the source cluster. Address queries use only the contiguous covered tip span and ask the source cluster for any older remainder. The source credentials remain in superbank-rpc; the local ClickHouse instance does not connect to the source cluster.

The source ClickHouse user needs read access to the copied tables and their `system.tables` and `system.columns` metadata. The local ClickHouse user needs permission to create and drop the dedicated database, create and alter its tables, and read and insert its data.

The cache serves `getBlock` (all `transactionDetails` levels), `getBlocks`, `getBlocksWithLimit`, `getBlockTime`, `getTransaction`, `getSignatureStatuses`, `getSignaturesForAddress`, and `getTransactionsForAddress` when the required rows are covered. `DISK_CACHE_RETAIN_SLOTS` is required. The forwarder fills backward to the configured retention floor when the cache starts partially filled or the retention window increases. `DISK_CACHE_MAX_BYTES` is enforced after each successful fill against active MergeTree parts in the primary cache database: it evicts complete old partitions to a 90% low-water mark, and purges the newest partition too if that is necessary to get below the budget. If the database cannot get below the budget even after that purge, the cache marks itself unready and returns to source fallback. The limit does not cover the separately owned block-index database, ClickHouse server overhead, or transient in-flight writes.

`GET_BLOCK_RESPONSE_CACHE_MAX_BYTES` adds a separate lazy in-process cache of serialized finalized `getBlock` results. Confirmed requests never read or populate it. Its key includes the commitment and every response-shaping option, but not the JSON-RPC request ID. Concurrent finalized requests for the same result share hydration and serialization work. Use `RPC_RESPONSE_GZIP_ENABLED=true` to negotiate gzip for large responses. Clients that can consume binary transaction encoding can request `base64` to reduce full-block encoding cost.

The configured cache database is exclusively owned by this feature. A nonempty database without the Superbank ownership marker is rejected and never modified. A source schema fingerprint change rebuilds only a correctly marked cache database. Treat the instance and database as semi-ephemeral.

By default, local initialization failures do not block RPC startup. Reads continue against the source cluster while a background supervisor retries local initialization. `DISK_CACHE_REQUIRED=true` makes initialization a startup requirement and makes `/health` return HTTP 503 when the local cache is not ready or cannot answer a health query.

Signature and address reads use an in-process Bloom membership index to exclude unrelated slot
partitions before querying ClickHouse. This preserves whole-partition eviction without making
key lookups search every retained partition. Signature status batches, pagination-bound signature
lookups, regular/hot address history, and token-owner history use the same routing mechanism.
Candidate partitions are queried in result order, one at a time, until the answer is complete or
the shared `DISK_CACHE_QUERY_TIMEOUT_MS` deadline expires. Admission waiting and transaction
hydration count against that deadline. Incomplete address pages use source fallback.

Partition-scoped interactive reads enable ClickHouse's uncompressed-block cache
(`use_uncompressed_cache=1`) while keeping the query-result cache disabled. The cache reuses
decompressed MergeTree blocks; its capacity remains controlled by the local ClickHouse server's
`uncompressed_cache_size`. Background index scans and source-cluster reads retain their existing
settings. Transaction payload reads use the resolved slot and transaction index, with a same-slot
fallback if the signature index points to a missing transaction position. Both reads share the
existing admission permit and cache-attempt deadline.

Signature membership covers every retained partition, including the active and partially
retained edges and gaps between covered ranges. Each signature is hashed once and checked under
one index lock. A complete negative returns before ClickHouse admission, client cloning, or
signature encoding. Positive candidates still require a database lookup: Bloom filters can
produce false positives.

Fills add all signature keys, including secondary transaction signatures, before publishing
coverage. The bounded local transaction projection uses the source signatures view's expressions.
Ordinary appends and partial eviction preserve existing bits. Repairs remain unknown until their
update completes; failed or cancelled updates invalidate completeness. Missing and incomplete
filters rebuild asynchronously from actual materialized-table keys, newest partitions first.
Signature and address maintenance run in independent bounded loops. Signature sweeps retry
incomplete partitions after a five-second delay between sweeps; scan duration and other signature
builds add to recovery time. Before each address-partition build, maintenance checks live signature
completeness and cache readiness. New address builds pause while any signature partition is unknown;
an already-running address scan can finish alongside one signature rebuild. Persistent signature
failures therefore pause address warming, while queries retain their existing safe fallbacks.
Both workers stop with the existing cache task. This scheduling change preserves cache format 5,
its schema fingerprint, and existing disk data; only the in-memory indexes rebuild on restart.
Stale builds cannot publish across invalidation or schema reset. Address filters continue to
rebuild on complete historical partitions and invalidate on mutation. This assumes the owned
cache has no independent external writers.

The memory budget reserves 64 MiB for buffers/metadata and allocates the remaining space across
the retention window. Two-thirds of each partition's bitmap allowance is reserved for signatures
with seven probes; the remainder serves address filters. Signature selectivity depends on the
number of keys per partition and partition count: validate the **aggregate** false-positive rate,
targeting at most 1%, rather than a per-partition rate. Limited memory reduces selectivity rather
than correctness. The default budget remains 4 GiB. Background scans and fill updates each use
one ClickHouse execution thread and a separate 64 MiB server query-memory limit. Initialization
and index failures preserve source fallback; a cold index can have higher latency than a fully
built index.

Cache format **5** preserves the source's portable MergeTree index/mark settings and reverse
sort directions from canonical DDL, except for the local transactions payload layout. Its effective
settings are `index_granularity=64`, `index_granularity_bytes=10485760`,
`min_compress_block_size=16384`, and `max_compress_block_size=65536`. The same effective
settings feed table creation and the schema fingerprint; upstream payload settings cannot override them. Forwarding views project only insertable columns so the
cache recomputes materialized bucket columns. Upgrading uses
an ownership-checked table rebuild and temporarily refills through source fallback.
A payload-layout fingerprint change rebuilds all tables in the owned cache and clears their coverage. The filler repopulates the retention window, and signature indexes rebuild from the new data. Refill can take hours at full retention; a healthy RPC does not prove a warm cache. Restarting with matching settings reuses the data. Rolling back to a build with the previous fingerprint can cause another rebuild.

The rebuild preserves `_cache_meta` until replacement DDL succeeds, so interrupted rebuilds
can retry. Drops of verified owned tables set `max_table_size_to_drop=0` for that query only;
server-wide drop protection remains unchanged. The separately owned full-history block index is preserved. Before deployment, run the full-size
key-routing workload described in `tests/k6/README.md`; small-fixture tests do not establish its
latency targets.

If an older build partially dropped the cache database and removed `_cache_meta`, startup
continues to reject the remaining tables. After confirming the target is the disposable cache
ClickHouse instance and pausing its RPC task, an operator can remove the remaining cache with
`DROP DATABASE IF EXISTS superbank_disk_cache SYNC SETTINGS max_table_size_to_drop=0`
(substitute the configured cache database). This deletes the remaining cache data; restart RPC
to recreate and refill it. Do not recreate an ownership marker over unidentified tables.

`getTransaction` retains the cached `(slot, slot_idx)` for payload lookup, with a
slot-only retry for stale/legacy positions. A pinned-slot null requires a successful
signature miss and coverage valid throughout the attempt. Admission timeouts, query
errors, invalidation, and slots first covered during the lookup fall back to the primary.
An index entry whose payload is unavailable also falls back. Cache format and retention
are unchanged.

The `superbank_disk_cache_reads_total` outcomes distinguish misses, query errors, and timeouts.
`superbank_disk_cache_key_seconds` records complete attempts, admission waits, and index builds;
`superbank_disk_cache_key_index_bytes` reports reserved index memory, and
`superbank_disk_cache_key_index_partitions` / `superbank_disk_cache_key_index_unknown_partitions`
show address index coverage and refresh during builds and paused maintenance.
`superbank_disk_cache_key_seconds{operation="signature_index_build"}` distinguishes `success`,
`error`, `timeout`, and `superseded` attempts. Allocation or conflicting-writer deferrals use
`superbank_disk_cache_reads_total{operation="signature_index_build",outcome="deferred"}`.
Partition IDs appear only in diagnostic logs, not metric labels.
`superbank_disk_cache_signature_index_partitions` and
`superbank_disk_cache_signature_index_unknown_partitions` separately show signature completeness.
`superbank_disk_cache_signature_membership_seconds` has microsecond buckets and `absent`,
`possible`, and `unknown` outcomes. Partition probe/skip counters use
`operation="key_partition"` with bounded labels; no keys or partition IDs are metric labels.

Query-facing tables use `ReplacingMergeTree` by default. `blocks_metadata` can opt into the ClickHouse `Memory` engine with `DISK_CACHE_MEMORY_TABLES=blocks_metadata`. This mode requires explicit row and byte caps. Memory-engine coverage is reset after a local ClickHouse restart because those rows are not durable. No other query-facing table is accepted in the Memory allowlist in this release.

Run example:

```bash
RPC_HOST=0.0.0.0 RPC_PORT=8899 \
CLICKHOUSE_URL=http://source-clickhouse:8123 CLICKHOUSE_DATABASE=default \
DISK_CACHE_ENABLED=true DISK_CACHE_RETAIN_SLOTS=432000 \
DISK_CACHE_CLICKHOUSE_URL=http://127.0.0.1:8123 \
cargo run -p superbank-rpc --features disk-cache --
```

Configuration:

| Option | Environment | Default | Notes |
| --- | --- | --- | --- |
| `--disk-cache-enabled` | `DISK_CACHE_ENABLED` | `false` | Enables the feature at runtime. |
| `--disk-cache-clickhouse-url` | `DISK_CACHE_CLICKHOUSE_URL` | `http://127.0.0.1:8123` | Local ClickHouse HTTP endpoint. The host must be localhost or a loopback IP. |
| `--disk-cache-clickhouse-database` | `DISK_CACHE_CLICKHOUSE_DATABASE` | `superbank_disk_cache` | Dedicated database owned by the cache. |
| `--disk-cache-clickhouse-user` | `DISK_CACHE_CLICKHOUSE_USER` | `default` | Local ClickHouse user. |
| `--disk-cache-clickhouse-password` | `DISK_CACHE_CLICKHOUSE_PASSWORD` | empty | Local ClickHouse password. |
| `--disk-cache-required` | `DISK_CACHE_REQUIRED` | `false` | Fail startup and strict health when the local cache is unavailable. |
| `--disk-cache-retain-slots` | `DISK_CACHE_RETAIN_SLOTS` | — | Finalized slots to retain. Required when enabled. |
| `--disk-cache-max-bytes` | `DISK_CACHE_MAX_BYTES` | `0` | Enforced active-part byte budget for the primary cache database; `0` means unlimited. May purge the newest partition and mark the cache unready when one partition cannot fit. |
| `--disk-cache-partition-slots` | `DISK_CACHE_PARTITION_SLOTS` | automatic | Width of local slot partitions. The automatic value targets at most 128 active partitions. |
| `--disk-cache-query-timeout-ms` | `DISK_CACHE_QUERY_TIMEOUT_MS` | `2000` | Timeout for one local cache read. |
| `--disk-cache-key-index-max-memory-bytes` | `DISK_CACHE_KEY_INDEX_MAX_MEMORY_BYTES` | `4294967296` | In-process partition membership budget, including builder buffers and metadata; minimum 64 MiB. Separate from ClickHouse and the historical block index. |
| `--disk-cache-query-concurrency` | `DISK_CACHE_QUERY_CONCURRENCY` | `8` | Concurrent local interactive queries, range 1–64. |
| `--disk-cache-query-max-threads` | `DISK_CACHE_QUERY_MAX_THREADS` | `2` | ClickHouse execution threads per local interactive query, range 1–16. |
| `--disk-cache-schema-check-interval-secs` | `DISK_CACHE_SCHEMA_CHECK_INTERVAL_SECS` | `300` | Source schema fingerprint check interval. |
| `--disk-cache-memory-tables` | `DISK_CACHE_MEMORY_TABLES` | empty | Comma-separated Memory-engine allowlist. Only `blocks_metadata` is accepted. |
| `--disk-cache-memory-retain-slots` | `DISK_CACHE_MEMORY_RETAIN_SLOTS` | — | Required row cap when `blocks_metadata` uses Memory; must not exceed the main retention window. |
| `--disk-cache-memory-max-bytes` | `DISK_CACHE_MEMORY_MAX_BYTES` | — | Required byte cap when `blocks_metadata` uses Memory. |
| `--disk-cache-block-index-enabled` | `DISK_CACHE_BLOCK_INDEX_ENABLED` | `false` | Enables the durable full-history block-time store and bounded in-process index. Requires `DISK_CACHE_BLOCK_INDEX_MAX_MEMORY_BYTES`. |
| `--disk-cache-block-index-max-memory-bytes` | `DISK_CACHE_BLOCK_INDEX_MAX_MEMORY_BYTES` | — | Required in-process block-index segment budget. Must fit at least one 8,125,000-byte segment; values are rounded down to whole segments. |
| `--disk-cache-block-index-slots-per-query` | `DISK_CACHE_BLOCK_INDEX_SLOTS_PER_QUERY` | `250000` | Slots copied per historical metadata query. |
| `--disk-cache-block-index-max-slots-per-sec` | `DISK_CACHE_BLOCK_INDEX_MAX_SLOTS_PER_SEC` | `25000` | Read-only source scan rate limit for the historical index. |
| `--disk-cache-block-index-query-timeout-ms` | `DISK_CACHE_BLOCK_INDEX_QUERY_TIMEOUT_MS` | `300000` | Timeout for one historical metadata range query. |
| `--disk-cache-backfill-enabled` | `DISK_CACHE_BACKFILL_ENABLED` | `true` | Enables the unified source-to-local forward and repair task. |
| `--disk-cache-backfill-slots-per-query` | `DISK_CACHE_BACKFILL_SLOTS_PER_QUERY` | `8` | Slots per ClickHouse range query. Larger ranges reduce fixed query overhead but need a longer timeout. |
| `--disk-cache-backfill-concurrency` | `DISK_CACHE_BACKFILL_CONCURRENCY` | `4` | Independent validated ranges forwarded concurrently. Accepted range: 1–64. |
| `--disk-cache-backfill-max-slots-per-sec` | `DISK_CACHE_BACKFILL_MAX_SLOTS_PER_SEC` | `50` | Source forwarding rate limit. |
| `--disk-cache-backfill-query-timeout-ms` | `DISK_CACHE_BACKFILL_QUERY_TIMEOUT_MS` | `30000` | Range scans need more than the interactive query timeout. |
| `--disk-cache-repair-interval-ms` | `DISK_CACHE_REPAIR_INTERVAL_MS` | `5000` | Idle wait between forward/repair planning rounds. |
| `--disk-cache-repair-min-lag-slots` | `DISK_CACHE_REPAIR_MIN_LAG_SLOTS` | `75` | Do not claim source slots that ingestion may not have landed. |

The forwarder admits all concurrent ranges through one slots-per-second token bucket. Each range streams and validates independently. Coverage is published only for a range that completes both base-table writes and transaction-count validation. For a fast initial fill on capable source and local ClickHouse instances, increase concurrency and the rate limit first, then increase slots per query if fixed query overhead dominates. Watch `superbank_disk_cache_backfill_inflight_ranges`, write latency, fill errors, and source-cluster load while tuning.

The removed RocksDB settings `DISK_CACHE_PATH`, `DISK_CACHE_BLOCK_CACHE_BYTES`, `DISK_CACHE_WRITE_QUEUE_SLOTS`, and `DISK_CACHE_READ_CONCURRENCY` produce a configuration error instead of being ignored.

Observability: `superbank_disk_cache_*` metrics cover readiness, local ClickHouse bytes, coverage span, read outcomes, forwarding, errors, rebuilds, and partition eviction. `superbank_block_index_*` metrics report the full-history worker, hydrated floor/head, allocated memory, and errors. Route metrics retain the `disk_cache_read` label. The `X-Superbank-Sources` response header reports `disk-cache` combinations.

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

The `disk-cache` feature does not pull in or require `grpc-head-cache`.

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
| `--get-block-response-cache-max-bytes` | `GET_BLOCK_RESPONSE_CACHE_MAX_BYTES` | `0` | Serialized finalized `getBlock` result byte budget. `0` disables the in-process cache. Actual RSS also includes cache metadata and in-flight values. |
| `--rpc-response-gzip-enabled` | `RPC_RESPONSE_GZIP_ENABLED` | `false` | Compress JSON-RPC responses of at least 4 KiB when the client advertises gzip support. |
| `--rpc-max-batch-size` | `RPC_MAX_BATCH_SIZE` | `64` | Maximum number of JSON-RPC calls in a single batch envelope. |
| `--rpc-batch-concurrency-limit` | `RPC_BATCH_CONCURRENCY_LIMIT` | `8` | Max concurrent item execution within one batch envelope. |
| `--get-inflation-reward-max-addresses` | `GET_INFLATION_REWARD_MAX_ADDRESSES` | `100` | Maximum addresses accepted by one `getInflationReward` call. `0` disables this admission check; values above 100 are rejected at startup. |
| `--get-inflation-reward-max-concurrency` | `GET_INFLATION_REWARD_MAX_CONCURRENCY` | `20` | Maximum active `getInflationReward` ClickHouse workflows per RPC instance. Excess calls fail fast with node-unhealthy (`-32005`); `0` disables this method-level admission check. |
| `--get-inflation-reward-query-timeout-ms` | `GET_INFLATION_REWARD_QUERY_TIMEOUT_MS` | `5000` | End-to-end ClickHouse budget for HTTP-permit admission plus the targeted boundary and partition lookup. Must be below `RPC_REQUEST_TIMEOUT_MS`. |
| `--get-inflation-reward-max-threads` | `GET_INFLATION_REWARD_MAX_THREADS` | `2` | ClickHouse `max_threads` applied to every reward lookup query. |
| `--get-signature-statuses-max-concurrency` | `GET_SIGNATURE_STATUSES_MAX_CONCURRENCY` | `4` | Maximum primary signature-status workflows per RPC process, including pending cleanup. Must be positive. Admission waits consume the ClickHouse operation timeout; local disk-cache reads use their existing limits. |
| `--get-signature-statuses-max-threads` | `GET_SIGNATURE_STATUSES_MAX_THREADS` | `2` | Required `max_threads` and `max_threads_for_indexes` for distributed primary signature-status queries. Must be positive. These queries also disable hedging and parallel replicas. |
| `--get-inflation-reward-max-memory-bytes` | `GET_INFLATION_REWARD_MAX_MEMORY_BYTES` | `536870912` | ClickHouse `max_memory_usage` applied to every reward lookup query. |
| `--get-inflation-reward-max-bytes-to-read` | `GET_INFLATION_REWARD_MAX_BYTES_TO_READ` | `536870912` | ClickHouse `max_bytes_to_read` applied to every reward lookup query. |
| `--emit-http-errors` | `SUPERBANK_RPC_EMIT_HTTP_ERRORS` | `false` | Return HTTP `503 Service Unavailable` for selected server-side JSON-RPC failures; response bodies are unchanged. |
| `--host` | `RPC_HOST` | `0.0.0.0` | — |
| `--port` | `RPC_PORT` | `8899` | — |
| `--metrics-host` | `METRICS_HOST` | `0.0.0.0` | — |
| `--metrics-port` | `METRICS_PORT` | `9900` | — |
| `--genesis-path` | `GENESIS_PATH` | unset | Path to the target cluster's mounted `genesis.bin`. The server fails startup if a configured file cannot be read or decoded. Leave unset only for the no-warmup fallback. |
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
| `--clickhouse-query-timeout-ms` | `CLICKHOUSE_QUERY_TIMEOUT_MS` | `8000` | ClickHouse operation timeout (ms), including admission and response consumption. HTTP abandonment closes the data response and retains read/workflow admission until termination verification succeeds; optional query `SETTINGS` also carry `max_execution_time`. Explicit method and background range deadlines remain supported. Shard-direct TCP retains its shorter internal attempt timeout and best-effort cleanup. Keep this parent timeout below `RPC_REQUEST_TIMEOUT_MS`. |
| `--clickhouse-http-max-concurrency` | `CLICKHOUSE_HTTP_MAX_CONCURRENCY` | `512` | Concurrency budget for direct ClickHouse HTTP work, shared across client clones. Abandoned reads retain associated admission through termination verification. Existing shard fanout and method limits remain active; background readers have dedicated lanes, and verifier probes use a separate control pool. Excess reads wait within the applicable operation timeout. Set at or below the ClickHouse per-user connection/query budget. |
| `--clickhouse-http-connect-timeout-ms` | `CLICKHOUSE_HTTP_CONNECT_TIMEOUT_MS` | `2000` | TCP connect timeout (ms) for ClickHouse HTTP connections, so a new connection attempt fails fast during ClickHouse backpressure instead of hanging. |
| `--clickhouse-startup-verification-timeout-ms` | `CLICKHOUSE_STARTUP_VERIFICATION_TIMEOUT_MS` | `10000` | Positive per-query cancellation initialization budget (ms), including macro resolution, topology discovery, capability probes and retries. Independent of connection and normal query timeouts. |
| `--clickhouse-runtime-verification-timeout-ms` | `CLICKHOUSE_RUNTIME_VERIFICATION_TIMEOUT_MS` | `10000` | Positive per-batch abandoned-query verification budget (ms). Slow probes retain admission and can delay the five-second unconfirmed warning until the probe completes. |
| `--clickhouse-query-cache-enabled` | `CLICKHOUSE_QUERY_CACHE_ENABLED` | `false` | Enables ClickHouse query cache settings for historical read queries. |
| `--clickhouse-query-cache-ttl-seconds` | `CLICKHOUSE_QUERY_CACHE_TTL_SECONDS` | `1` | TTL for cached historical read query results (seconds). |
| `--clickhouse-get-transaction-query-cache-ttl-seconds` | `CLICKHOUSE_GET_TRANSACTION_QUERY_CACHE_TTL_SECONDS` | `300` | TTL override applied only to historical `getTransaction` point lookups when query cache is enabled. |
| `--clickhouse-get-transaction-query-cache-min-query-runs` | `CLICKHOUSE_GET_TRANSACTION_QUERY_CACHE_MIN_QUERY_RUNS` | `2` | Minimum identical `getTransaction` point-lookups required before ClickHouse writes them into cache. |
| `--clickhouse-query-cache-share-between-users` | `CLICKHOUSE_QUERY_CACHE_SHARE_BETWEEN_USERS` | `false` | Controls `query_cache_share_between_users` for historical read queries. |
| `--clickhouse-query-condition-cache-enabled` | `CLICKHOUSE_QUERY_CONDITION_CACHE_ENABLED` | `false` | Enables `use_query_condition_cache=1` for selected historical address-filtered read queries. |
| `--clickhouse-transport` | `CLICKHOUSE_TRANSPORT` | `http` | `tcp` or `http` (`tcp` requires `CLICKHOUSE_SCOPE=shard-direct`). |
| `--clickhouse-scope` | `CLICKHOUSE_SCOPE` | `distributed` | `distributed` sends all queries through `CLICKHOUSE_URL`; `shard-direct` enables local-table routing. |
| `--clickhouse-tcp-access-check-timeout-ms` | `CLICKHOUSE_TCP_ACCESS_CHECK_TIMEOUT_MS` | `2000` | Shard-direct only. Startup TCP access-check timeout (ms). |
| `--clickhouse-replica-health-check-interval-ms` | `CLICKHOUSE_REPLICA_HEALTH_CHECK_INTERVAL_MS` | `10000` | Background health-check interval for shard-direct replicas. Unavailable replicas are restored to the failover pool after recovery. |
| `--clickhouse-tcp-pool-min` | `CLICKHOUSE_TCP_POOL_MIN` | `10` | Shard-direct only. Minimum connections retained per shard in each ClickHouse native (TCP) connection pool. |
| `--clickhouse-tcp-pool-max` | `CLICKHOUSE_TCP_POOL_MAX` | `20` | Shard-direct only. Maximum connections per shard in each ClickHouse native (TCP) connection pool. Total native connections per instance are bounded by this value times the number of shards, so size it against the ClickHouse connection budget. |
| `--clickhouse-cluster` | `CLICKHOUSE_CLUSTER` | `{cluster}` | Cluster identity in both scopes: topology discovery in shard-direct mode and HTTP SELECT termination verification across coordinator/replicas in distributed mode. Supports ClickHouse macros. Set explicitly to `rbx2` for RBX2, or an empty string for standalone ClickHouse with no cluster. |
| `--clickhouse-topology-config` | `CLICKHOUSE_TOPOLOGY_CONFIG` | — | Shard-direct only. Optional authoritative YAML shard topology. When set, superbank-rpc skips `system.clusters` discovery, uses the YAML shard/IP/port mapping for shard-local connections, and routes `getTransactionsForAddress` to the address-owner shard. |
| `--clickhouse-gsfa-local-table` | `CLICKHOUSE_GSFA_LOCAL_TABLE` | — | Shard-direct only. Local GSFA table used by shard-direct reads and owner-shard `getTransactionsForAddress` routing. |
| `--clickhouse-hot-address` | `CLICKHOUSE_GSFA_HOT_ADDRESSES` | empty | Repeatable; env accepts comma-separated values. |
| `--clickhouse-gsfa-hot-table` | `CLICKHOUSE_GSFA_HOT_TABLE` | `default.gsfa_hot` | Distributed hot table used for active hot-address reads. |
| `--clickhouse-gsfa-hot-local-table` | `CLICKHOUSE_GSFA_HOT_LOCAL_TABLE` | `default.gsfa_hot_local` | Shard-direct only. Local hot table queried by hot-address fanout. |
| `--clickhouse-signatures-local-table` | `CLICKHOUSE_SIGNATURES_LOCAL_TABLE` | — | Shard-direct only. |
| `--clickhouse-token-owner-activity-local-table` | `CLICKHOUSE_TOKEN_OWNER_ACTIVITY_LOCAL_TABLE` | — | Shard-direct only. |
| `--clickhouse-transactions-local-table` | `CLICKHOUSE_TRANSACTIONS_LOCAL_TABLE` | — | Shard-direct only. |
| `--clickhouse-blocks-metadata-local-table` | `CLICKHOUSE_BLOCKS_METADATA_LOCAL_TABLE` | — | Shard-direct only. |
| `--clickhouse-shard-http-port` | `CLICKHOUSE_SHARD_HTTP_PORT` | — | Shard-direct only. |

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
When `CLICKHOUSE_SCOPE=distributed`, superbank-rpc sends every ClickHouse query through `CLICKHOUSE_URL`. HTTP SELECT termination verification inspects `system.clusters` and replica processes through that gateway. It does not read `CLICKHOUSE_TOPOLOGY_CONFIG`, connect directly to shard endpoints, query shard-local application tables, or validate local schemas. Explicit shard-local settings are ignored with a startup warning.

When `CLICKHOUSE_SCOPE=shard-direct`, superbank-rpc discovers shards from `system.clusters` and validates local table schemas. Local tables default to `{table}_local` when not provided explicitly. `CLICKHOUSE_TRANSPORT` selects the shard-direct transport (`tcp` or `http`). When a shard has multiple replicas, startup selects the first reachable replica, warns about unavailable replicas, and fails only if no replica is reachable for a shard. Background health checks move traffic away from failed replicas and restore recovered replicas to the failover pool.

In shard-direct scope, set `CLICKHOUSE_TOPOLOGY_CONFIG` (or `--clickhouse-topology-config`) to make a YAML topology file authoritative for shard-local connection targets and skip `system.clusters` discovery at startup. When multiple YAML nodes are listed for the same shard, file order defines failover priority and all replicas must use the same shard weight. The first reachable node is selected at startup. The YAML `ip-address` field is the authoritative connection address and does not need to match ClickHouse `host_address`. Shard-local TCP uses YAML `ip-address` and `tcp-port`; shard-local HTTP uses the same YAML `ip-address` plus `CLICKHOUSE_SHARD_HTTP_PORT` or the port from `CLICKHOUSE_URL`.
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
| `CLICKHOUSE_QUERY_ID_PREFIX` | `superbank` | `auto` selects a PID-based prefix; `off`/`0`/`false` disables the configured annotation prefix. Required query IDs always include a random process namespace and counter. |
| `CLICKHOUSE_GSFA_STRICT_PAGINATION` | `true` | — |
| `CLICKHOUSE_GSFA_FALLBACK_TRANSACTIONS` | disabled | `empty`/`true` for empty-only fallback; `force`/`always` for incomplete fallback. |
| `CLICKHOUSE_DISABLE_QUERY_SETTINGS` | `false` | Disables optional per-query ClickHouse `SETTINGS` overrides (including `getInflationReward` thread, memory, read-byte, and execution-time caps) when truthy. Required HTTP SELECT disconnect settings, local-cache settings, and primary `getSignatureStatuses` limits still apply; unsupported safety settings fail the operation without an uncapped retry. RPC admission limits remain active. |

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
| `--clickhouse-gsfa-hot-local-table` | `CLICKHOUSE_GSFA_HOT_LOCAL_TABLE` | `default.gsfa_hot_local` | Shard-direct local table queried by hot-address fanout. |

Startup checks:
- The hot distributed table must exist and contain rows for each configured address.
- If a hot address has no rows (or the hot table is unavailable), that address falls back to the standard GSFA table and a warning is logged.
- Shard-direct scope also validates the local hot table schema and verifies that its bucket modulus matches the distributed hot table.

Routing behavior:
- In distributed scope, active hot addresses query `CLICKHOUSE_GSFA_HOT_TABLE` through `CLICKHOUSE_URL` and do not require shard topology.
- In shard-direct scope, active hot addresses fan out across `CLICKHOUSE_GSFA_HOT_LOCAL_TABLE` using the configured or discovered topology.

Hot table schema expectations:
- Same columns as `default.gsfa_local` (`addr_bucket`, `address`, `signature`, `slot`,
  `slot_idx`, `memo`, `err`, `block_time`).
- Partitioning and ordering should favor the access pattern (latest-first reads).

## Local getTransaction diagnostics

The ignored `get_transaction_local_diagnostics` test exercises the disk-cache reader against a disposable loopback ClickHouse server. Use the same ClickHouse version as the deployment under investigation. The fixture creates unique source/cache databases and removes them after success; failed assertions can leave those databases for inspection. Do not point this command at an existing service through a forwarded loopback port.

```bash
DISK_CACHE_TEST_URL=http://127.0.0.1:18193 \
GETTX_DIAGNOSTIC_OUTPUT=/tmp/gettx-diagnostic.json \
cargo test -p superbank-rpc --all-features --locked --lib \
  get_transaction_local_diagnostics -- --ignored --nocapture
```

The JSON artifact contains twenty samples per combination of unknown/complete signature membership, legacy/v0 hits or misses, and signature-only/slot-pinned requests. It also records guarded signature and payload query IDs, workflow admission, read-endpoint setup/admission, first-row time, and the subsequent wait for successful EOF. Phase queries use the application reader, transaction column projection, and cache query settings. Signature SQL mirrors the production lookup, so keep that diagnostic projection aligned when changing the lookup. `first_row_ms` includes endpoint setup/admission; `complete_ms` includes first-row time. These overlapping measurements must not be added together.

Separate handler samples include fixture state creation, hydration, response construction, and body collection. They are not an isolated serialization benchmark. Assertions verify legacy/v0 response parity, stale-position fallback, invalidated reads, and fallback after a local cache payload error. Synthetic debug-build timings diagnose query sequencing; they do not establish live-server latency or throughput targets. Neither the test nor its timing helper is compiled into the RPC server binary.

## Local cache payload layout benchmark

`scripts/test/benchmark-cache-payload-layout.py` compares eight payload-table layouts using an owned native ClickHouse 26.1.2.11 process. It tests row granularity 8192, 1024, 256 and 64 with default compression blocks or 16 KiB minimum / 64 KiB maximum blocks, keeping the byte granularity limit at 10 MiB. Signature-table settings remain fixed. No live endpoint is accepted.

Build the ignored Rust harness and use the `superbank_rpc` library-test executable path printed by Cargo:

```bash
cargo test -p superbank-rpc --all-features --locked --lib --no-run
python3 scripts/test/benchmark-cache-payload-layout.py \
  --clickhouse /path/to/clickhouse-26.1.2.11 \
  --harness /path/to/target/debug/deps/superbank_rpc-HASH \
  --output /tmp/superbank-payload-layout-results
```

The default experiment inserts one million deterministic synthetic legacy/v0 rows per layout, one layout at a time, then measures payload-only and two-query reads using disjoint signature sets. Each mode has three batches of 200 previously unqueried signatures followed by three repeated-signature batches. Previously unqueried does not mean cold disk: insertion, the OS page cache, and shared compressed blocks can warm data. Compare each mode across layouts: the two-query mode follows payload-only reads, which can warm shared blocks. Query-result caching is disabled. Payload digests and hydrated responses are checked outside measured read intervals.

Use `--smoke --rows 2500` with a different output directory to check the fixture and interface first. The owned server binds loopback ports 18195 and 19095, caps ClickHouse tracked memory at 16 GiB, and the experiment stops if its output directory exceeds a 100 GiB disk budget. Existing listeners or output server-data directories cause refusal. The script creates and merges only its own fixture tables, removes fixture databases, and stops its server on completion. Read the raw storage, query-log, part-log, settings and harness artifacts together when comparing latency with insertion, merging and storage costs. Insert timing includes deterministic fixture generation. The memory setting is not an RSS limit, and the disk watchdog checks every three seconds. Small synthetic fixtures and repeated keys do not establish production latency or throughput.

## Metrics

Prometheus metrics are served at `/metrics` on `METRICS_HOST:METRICS_PORT`.

Route normalization metric:

- `superbank_rpc_route_total_total{method,transport,scope,source,head_cache_read,disk_cache_read,outcome,x_endpoint,x_rpc_node,x_subscription_id,x_account_id}`
  - `method`: supported JSON-RPC method name.
  - `transport`: `tcp|http` (active ClickHouse routing transport policy).
  - `scope`: `distributed|shard_direct` (active ClickHouse routing scope policy).
  - `source`: `clickhouse|head_cache|disk_cache|response_cache|none` (primary source used for the returned response).
  - `head_cache_read`: `true|false` (whether handler read from head cache on that request).
  - `disk_cache_read`: `true|false` (whether the handler read from the local ClickHouse disk cache on that request).
  - `outcome`: `success|not_found|invalid_params|rpc_error|backend_error|timeout`.
  - `x_endpoint`: omitted when capture is disabled; otherwise `missing|<value>` (`<value>` is the raw `X-Endpoint` header value).
  - `x_rpc_node`: omitted when capture is disabled; otherwise `missing|<value>`.
  - `x_subscription_id`: omitted when capture is disabled; otherwise `missing|<value>` (`<value>` is the raw `X-Subscription-ID` header value).
  - `x_account_id`: omitted when capture is disabled; otherwise `missing|<value>` (`<value>` is the raw `X-Account-ID` header value).

Request-scoped metric families:

- `rpc_requests`, `rpc_response_time_seconds`, `rpc_inflight_requests`, `rpc_timeouts`, `rpc_response_overhead_seconds`, `rpc_blocks_slots_returned`, `rpc_batch_requests`, `rpc_batch_items`, `rpc_batch_size`, `rpc_batch_rejected_total`, `rpc_backend_errors`, `rpc_clickhouse_duration_seconds`, `rpc_clickhouse_received_bytes`, `rpc_clickhouse_decoded_bytes`, `rpc_clickhouse_timeouts`, `rpc_clickhouse_query_cache_total`, `rpc_clickhouse_query_cache_settings_total` can include `x_endpoint`, `x_rpc_node`, `x_subscription_id`, and `x_account_id` when each capture option is enabled.
- For those labels, values are `missing|<raw-value>` for enabled capture; disabled capture omits the label.

`getBlock` response-cache metrics:

- `superbank_get_block_response_cache_access_total{operation,outcome}` with `hit`, `miss`, `insert`, and `coalesced` outcomes.
- `superbank_get_block_response_cache_entries`, `superbank_get_block_response_cache_weighted_bytes`, and `superbank_get_block_response_cache_max_bytes`.
- `superbank_get_block_phase_seconds{operation}` for hydration and serialization work.

Superbank gRPC streaming metrics, emitted only with `--features grpc-streaming`:

- `superbank_grpc_stream_requests_total{method}`
- `superbank_grpc_stream_chunks_total{method}`
- `superbank_grpc_stream_messages_total{method}`
- `superbank_grpc_stream_errors_total{method,stage}`

Response metric headers:

- `X-Superbank-Sources`: downstream sources consulted while serving the JSON-RPC response. Values combine `response-cache`, `head-cache`, `disk-cache`, and `clickhouse` in that order. `both` remains the legacy value for head-cache plus ClickHouse, and `all` remains the value for head-cache, disk-cache, and ClickHouse without the response cache. For batch responses, this is the aggregate source footprint across batch items.
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


## getBlock completeness regression

The regular Rust tests cover incomplete payload rejection and response-cache recovery.
To also exercise real ClickHouse reads, use a local ClickHouse instance with the default
user and permission to create databases:

```bash
BLO576_CLICKHOUSE_TEST_URL=http://127.0.0.1:8123 \
cargo test -p superbank-rpc --locked \
  get_block_clickhouse_partial_payload_repair -- --ignored

BLO576_CLICKHOUSE_TEST_URL=http://127.0.0.1:8123 \
cargo test -p superbank-rpc --all-features --locked \
  get_block_clickhouse_partial_payload_repair -- --ignored
```

This test creates uniquely named `blo576_*` databases and drops them on success. A failed
run can leave those test databases for inspection. With optional cache features compiled,
it also exercises incomplete head-cache and disk-cache fallback, including disk slot poisoning.
Both configurations initialize ClickHouse read cancellation and verify successful source
metadata and projection reads before testing partial-block rejection and recovery after repair.
