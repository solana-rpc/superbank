# superbank-rpc

Solana-compatible JSON-RPC server backed by ClickHouse tables produced by `superbank` (or any
writer that matches the same schemas).

See the [Agave 4.3 compatibility and rollout notes](../../docs/agave-4.3-compatibility.md) for request validation, parsed JSON changes, VAT rewards, and ingestion boundaries. Build with the pinned Rust 1.98.1 toolchain.

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
- `getAgGenesisCert`
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
  default. Address-limit rejections use code `-32602` and message `Too many inputs provided; max N`, where N is the configured limit. The default remains 100; set `GET_INFLATION_REWARD_MAX_ADDRESSES=32` for Agave limit parity.
  Historical non-partitioned rewards do not require block height metadata. Partitioned rewards
  still require it to locate payout blocks and determine reward availability.
  Validated payout boundaries and, once every partition block exists, the epoch's partition
  block-height-to-slot map are cached in process (`GET_INFLATION_REWARD_EPOCH_CACHE_MAX_BYTES`).
  A repeated epoch then needs one ClickHouse query that reads boundary vote rewards and partition
  stake rewards together; responses are identical to the uncached path.
- Reward objects expose the optional Agave `commissionBps` field when the ingested source supplied
  it. Legacy rows ingested before the basis-point columns were deployed omit the field; Superbank
  does not infer it from the legacy percentage `commission` value.
- `getBlock` and `getTransaction` reject `base58` or `binary` with `maxSupportedTransactionVersion >= 1` using code `-32602`, before cache/storage access. `getTransactionsForAddress` applies the same check to its supported `base58` encoding.
- `VATDebit` rewards preserve negative lamports; stored values also accept the producer spelling `validator-admission-ticket-debit`. Inflation reward queries exclude these debits.
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
read; the shared application client and write profile remain writable. Primary and
shard reads also receive HTTP parameters `max_execution_time` and `receive_timeout`, both
equal to their deadline rounded up to whole seconds; a statement's own
`SETTINGS max_execution_time` (derived from the same or a shorter budget) takes precedence.
`receive_timeout` bounds an initiator blocked on a hung leaf, which never checks
`max_execution_time`; working leaves send progress every `interactive_delay`, so the idle
gap only fires once the read has missed its deadline. Disabling optional SQL tuning
(`CLICKHOUSE_DISABLE_QUERY_SETTINGS`) does not disable these required HTTP settings, and
unsupported protection fails the read without an unprotected retry.

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
and associated workflow permits until termination is confirmed, or for primary and shard
reads until one of the liveness bounds below releases them. The local disk cache's
interactive reads are the exception: they release admission as soon as the response is closed,
recorded as `released_unverified`, because every local read carries
`cancel_http_readonly_queries_on_client_close` and partition-scoped reads also carry a
`max_execution_time` of their remaining budget. Holding their few permits for verification (at least two 250 ms polls) starved live
requests. Disk-cache background scans still wait for verification.

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
none establishes termination. Later successful verification restores capacity.

Primary and shard reads have two liveness bounds, because a verifier that cannot succeed
(for example while cluster replicas restart) would otherwise hold every permit in the lane
and make new reads wait until their deadline:

- **Execution limit.** Once `abandoned_at + max_execution_time + 5 s` has passed, the read
  releases admission as `released_expired`, whatever the probes report. The client deadline
  starts before submission, so a server that enforces its limit has normally ended the
  query by then; the margin covers request bytes still in flight and a short
  `max_concurrent_queries` queue wait (`queue_max_wait_ms`), both of which start the
  server's clock later. The release happens at the next sweep: after the current probe
  batch, so at most one runtime verification budget late. Such a read has usually recorded
  `unconfirmed` first.
- **Pending share.** Abandoned primary and shard reads of one client share one budget: at
  most a quarter of `CLICKHOUSE_HTTP_MAX_CONCURRENCY` (128 of the default 512) stay pending
  across the primary endpoint and every shard-direct replica endpoint, because all of them
  keep leases on the same HTTP semaphore. Beyond that the lane enqueuing a new abandoned
  read releases its own oldest as `released_over_cap`, so abandoned work cannot hold more
  than a quarter of the HTTP permits. Concurrent abandons on different lanes can overshoot
  by one entry each until their own enqueue trims it. Pending reads also hold smaller
  permits that the share does not protect: the primary signature-status source permits
  (`GET_SIGNATURE_STATUSES_MAX_CONCURRENCY`) and shard fanout permits
  (`CLICKHOUSE_SHARD_FANOUT_CONCURRENCY`). Only the execution limit bounds those.

Both trade strict accounting for liveness: a released read may still be running on the
server, so one node's server-side concurrency can exceed its admission by the released
reads. A server that enforces its limits ends them at `max_execution_time`. An initiator
idle-waiting on a hung leaf (a frozen replica mid-stream) ends only at its
`receive_timeout` gap after the leaf's last packet, which can be up to one more deadline
after the limit, so about twice the deadline after submission in the worst case; that
waiting does no work. A server that is itself frozen runs nothing until it resumes.
Background scans keep waiting for verification without a bound, and local-cache reads keep
releasing on close. A failed background verifier can therefore still hold its own lane.
The cached topology is not rediscovered; with a changed topology every abandoned primary
read waits for its execution limit.

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
| `superbank_clickhouse_read_disconnect_verification_total{operation,target,outcome}` | `confirmed_absent` and `unconfirmed` outcomes; the latter is recorded once per abandoned read. `released_unverified` counts abandoned local-cache reads that released admission without verification. `released_expired` and `released_over_cap` count primary/shard reads released by the execution limit or the pending share. |
| `superbank_clickhouse_read_disconnect_probe_seconds{target,outcome}` | Control-probe latency histogram; targets are `cluster` or `local`, outcomes `success` or `error`. |

Pending/verification target classes are `primary`, `cache`, `shard`, and `background`.
Probe targets describe verification scope (`cluster` or `local`), rather than the
admission lane.

Suggested alerts:
`sum by (nodename) (superbank_clickhouse_read_disconnect_pending{target=~"primary|shard"}) > 64`
for 5 minutes (abandoned reads are not being confirmed gone), and a sustained nonzero rate of
`released_expired` or `released_over_cap` (verification failing, or the primary running
reads past their limit). See the [HTTP cancellation protocol gate](../../tests/k6/README.md#clickhouse-http-cancellation-protocol-gate)
for lifecycle, correctness, and matched-baseline performance requirements.

### Primary signature-status overload protection

History requests may contain up to 256 signatures. Cache-negative membership skips only
local cache work: unresolved signatures still require a primary lookup when
`searchTransactionHistory` is true, unless the optional history absence cache below proves them
absent. Primary status admission remains shared across client
clones and precedes HTTP admission within `CLICKHOUSE_QUERY_TIMEOUT_MS`; keep this timeout
below `RPC_REQUEST_TIMEOUT_MS`.

`SIGNATURE_STATUS_HISTORY_CACHE_ENTRIES` (default `0`, disabled) enables an in-process absence
cache for builds with both `grpc-head-cache` and `disk-cache`. When the primary returns no row for
a signature that the same request's disk read proved absent, the signature is remembered with the
disk tip of that read for `SIGNATURE_STATUS_HISTORY_CACHE_TTL_SECS` (default `300`, at most
`300`). A later history request skips the primary for that signature only when all of these hold:
the head cache does not hold it; the disk read completed within its deadline under an unchanged
index epoch over one contiguous covered span and did not match it (a row outside coverage never
counts as absent); the head cache is connected, its finalized tip is fresh, and its
parent-verified chain covers every slot from the disk tip to that tip; no primary slot this process
last read is newer than the head's finalized tip taken before its status lookup; and the disk floor has not moved past the remembered tip. The primary history search covers all slots it held when it
answered, the disk cache is filled from the primary, and newer transactions can land only in the
slots the head and disk tiers still cover, so the response is the same `null` the primary would
return. The residual exposure is a primary backfill of already-covered history within the TTL.
Errored primary answers and found records are never remembered. `HEAD_CACHE_RETAIN_SLOTS` must
exceed the disk cache's lag behind the finalized tip, or the head proof never reaches the disk tip
and every lookup bypasses the cache; startup warns when the cache is enabled but a tier is missing
or `HEAD_CACHE_RETAIN_SLOTS` is below 256. The design assumes the primary ingests finalized data
that trails the head stream. `SIGNATURE_STATUS_HISTORY_CACHE_MAX_BYTES`
(default 64 MiB, budgeted at 384 bytes per entry) also bounds the entry count. Requests whose unresolved signatures
are all skipped carry no `X-Superbank-Metrics` primary timings and route as `head_cache`/`disk_cache`;
`superbank_rpc_signature_status_history_cache_total{outcome}` counts per-signature `hit`, `miss`,
`bypass` (no local proof), and `inserted`.

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
`superbank_rpc_signature_status_disconnect_verification_total{outcome="confirmed_absent"|"unconfirmed"|"released_expired"|"released_over_cap"}`
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

### Alpenglow genesis certificate source

[`getAgGenesisCert`](https://solana.com/docs/rpc/http/getaggenesiscert) accepts no parameters (omitted, `null`, or `[]`). It returns an authoritative `null` before migration and the certificate afterward, preserving `block.slot` as a JSON `u64`, `block.blockId` as 32 byte values, `signature.signature` as 192 byte values, and `signature.bitmap` as a variable-length byte array. There is no commitment parameter or context wrapper. The certificate slot is passed through exactly as supplied.

Set `AG_GENESIS_CERT_RPC_URL` to an operator-trusted HTTP(S) RPC endpoint on the **same cluster as the ClickHouse data**, supporting the Agave 4.3+ method. This endpoint is the authority for migration evidence: Superbank validates the response shape but does not independently verify the aggregate BLS signature or cluster identity. Do not point it back at this Superbank instance or through a route that forwards the call back here. Store credentialed URLs in the environment, outside git. For example, run with `--ag-genesis-cert-rpc-url https://your-cluster-rpc.example`. An empty value counts as unset. While the source is unset, the method returns `-32019` with reason `source_not_configured`. Under `--emit-http-errors` that is HTTP 503, so keep health checks and load balancers away from this method on deployments without a source.

The first request bootstraps the source lazily; startup requires valid configuration but does not require a reachable provider. Requests share one in-flight fetch with a total `AG_GENESIS_CERT_RPC_TIMEOUT_MS` budget (default 2000 ms) covering admission, connection, and response body. There are no automatic retries or redirects, and responses are capped at 64 KiB, including streamed bodies. A successful `null` is cached for `AG_GENESIS_CERT_REFRESH_INTERVAL_SECS` (default 5 seconds, range 1–300); the next request after expiry refreshes it. Expired evidence is never served when refresh fails. Failures, including unsupported upstream methods, are cached for one second before retrying on demand. Once obtained, the immutable finalized-bank certificate is cached for the process lifetime and remains available during provider outages. A restart bootstraps again; there is no durable certificate copy or background polling. Changing cluster/source requires restarting with the correct endpoint.

An unconfigured, unavailable, timed-out, malformed, or unsupported source returns JSON-RPC `-32019` with `error.data.reason` equal to `source_not_configured`, `upstream_unavailable`, `source_timeout`, `invalid_upstream_response`, or `upstream_unsupported`, respectively. Other upstream RPC errors use `upstream_error`. Upstream RPC errors include `upstreamCode` but do not expose provider messages or URLs. These errors never become `null`; a pre-4.3 upstream's `-32601` means unsupported evidence, not TowerBFT. With `--emit-http-errors`, source failures return HTTP 503; otherwise they return HTTP 200 with the JSON-RPC error body. Nonempty or named parameters return `-32602` without fetching the source.

This source is independent of `GENESIS_PATH`, which controls epoch schedules, and of finalized block storage and speculative head buffering. The existing ClickHouse schemas hold no genesis certificate, and the Yellowstone [footer message](https://docs.rs/yellowstone-grpc-proto/14.0.1/yellowstone_grpc_proto/geyser/struct.SubscribeUpdateBlockFooter.html) carries no genesis certificate. Dates, validator versions, local latest slots, and missing footer fields are not used to infer migration. Agave's [implementation](https://github.com/anza-xyz/agave/blob/v4.3.0/rpc/src/rpc.rs) reads the certificate from its finalized bank.

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
a Yellowstone DragonsMouth gRPC stream of complete bank-tagged blocks and keeps a small
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

`getBlock` accepts the boolean `footer` option from [SIMD-0307](https://simd.live/simd/0307-add-block-footer). The default is `false`, which deliberately differs from the SIMD's default of including footer fields, so existing callers see the previous response. With `footer: true` the response carries a `footer` object with `blockProducerTimeNanos` (a JSON number) and `blockUserAgent` (a string). The value is `null` for a block with no complete stored footer, such as a block before Alpenglow activation, a gap in footer ingestion, or a partly stored footer. An omitted option and `footer: false` return the same response. The fields come from `blocks_metadata` and are untrusted producer data. A non-boolean value returns `-32602`. The head cache has no footer data, so a block it serves returns `footer: null`, and that response is never inserted into the `getBlock` response cache. The response cache key includes the `footer` flag.

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

Explicit-end requests do not require fresh latest-slot discovery. When the head tip at the
requested commitment is trusted (the same rules as above), an explicit `endSlot` (and the
`getBlocksWithLimit` window end) is clamped to that tip, as Agave clamps to its bank slot:
slots above it are not at that commitment yet, so the primary is not asked to prove their
absence. A start above the trusted tip returns `[]`. A stale, unverified, or disconnected
tip keeps the explicit end unchanged, and gaps below the tip still read the primary.
`endSlot < startSlot` and the `500000`-slot range cap are checked on the raw parameters
before clamping. Clamps are counted as
`superbank_rpc_slot_source_total{operation="get_blocks_end"|"get_blocks_with_limit_end",source="head_clamped"}`.
With head cache disabled, the existing primary latest-slot cache remains in use.
`getBlocksWithLimit` shares the coverage helper and retains its existing slot-window
interpretation of the limit.

Primary latest-slot discovery (`get_latest_finalized_slot`, used by the latest-slot cache, `getHealth`,
the disk-cache forwarder, and the block-index worker) passes the caller's previous result as
a hint and reads only `blocks_metadata` slots `>= hint - 10000`, which prunes to the newest
partitions. An empty bounded result falls back to the unbounded
`ORDER BY slot DESC LIMIT 1` query, so the answer is unchanged; without a hint (or with
a hint `<= 10000`) the unbounded query is sent directly. Outcomes are counted as
`superbank_rpc_slot_source_total{operation="get_latest_finalized_slot",source="hinted"|"hint_fallback"}`.

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
| `--get-blocks-clamp-to-head-tip=<bool>` | `GET_BLOCKS_CLAMP_TO_HEAD_TIP` | `true` | Clamps an explicit `getBlocks`/`getBlocksWithLimit` end to the trusted head tip. `false` keeps the requested end and asks the primary for slots above the tip. |
| `--dragonsmouth-endpoint` | `DRAGONSMOUTH_ENDPOINT` | — | Required when enabled. |
| `--dragonsmouth-x-token` | `DRAGONSMOUTH_X_TOKEN` | — | Optional auth header. |
| `--head-cache-retain-slots` | `HEAD_CACHE_RETAIN_SLOTS` | `32` | How many slots to retain in memory. |
| `--head-cache-min-commitment` | `HEAD_CACHE_MIN_COMMITMENT` | `processed` | `processed|confirmed|finalized` exposure gate for head reads. |
| `--grpc-max-decoding-bytes` | `GRPC_MAX_DECODING_BYTES` | `67108864` | Max gRPC decoding message size. |

License note: superbank-rpc is licensed under AGPL-3.0-only (see `../../LICENSE`).
The optional `grpc-head-cache` feature pulls in `yellowstone-block-machine`, which is AGPL-3.0. The Yellowstone gRPC client and protobuf crates it also uses are Apache-2.0. A 4.3 producer must supply bank IDs. Bank replacement evicts the replaced slot and invalidates the coverage proof of descendants that name the replaced bank's hash as their parent. Descendants built on the winning bank keep their proof.

## Optional local ClickHouse forward cache (`disk-cache`)

When compiled with `--features disk-cache` and enabled at runtime, superbank-rpc forwards recent **finalized** slots from the configured source ClickHouse cluster into a separate ClickHouse instance on localhost. The feature is independent from `grpc-head-cache`. The local instance is a near cache, not a second source of truth. The read tiering is:

`DISK_CACHE_BLOCK_INDEX_ENABLED=true` adds a durable full-history `(slot, block_time)` store for `getBlockTime`, `getBlocks`, and `getBlocksWithLimit` in the separately owned local database `<cache_database>_block_index`. Its in-process read tier is deliberately bounded by the required `DISK_CACHE_BLOCK_INDEX_MAX_MEMORY_BYTES`: one-million-slot segments are retained only while they fit that budget, rounded down to whole segments. Startup remains asynchronous: ranges outside the hydrated window continue through the recent cache and source fallbacks. The historical worker reads finalized source metadata only, fills newest ranges first, and then proceeds toward slot zero. This feature cannot be combined with the ClickHouse Memory-engine mode.

```
head cache (optional, unfinalized tip) -> local ClickHouse cache (finalized, recent slots) -> source ClickHouse (full history)
```

At startup, superbank-rpc inspects the source tables through `system.tables` and `system.columns`. It copies columns, types, defaults, codecs, indexes, primary keys, sorting keys, and materialized-view queries into a cache-specific schema. Local MergeTree tables use slot-range partitions so retention can drop complete old partitions. The forwarder streams the source `transactions` and `blocks_metadata` tables in ClickHouse Native format over HTTP. Local materialized views build `signatures`, `gsfa`, optional `gsfa_hot`, and optional `token_owner_activity` from each transaction insert.

Coverage is published only after the base rows, dependent materialized views, and transaction-count validation complete. A hole, partial slot, local query error, or unavailable cache falls through to the source cluster. Address queries use only the contiguous covered tip span. `getTransactionsForAddress` asks the source cluster for any older remainder; `getSignaturesForAddress` races the source cluster's full page instead (see below). The source credentials remain in superbank-rpc; the local ClickHouse instance does not connect to the source cluster.

The source ClickHouse user needs read access to the copied tables and their `system.tables` and `system.columns` metadata. The local ClickHouse user needs permission to create and drop the dedicated database, create and alter its tables, and read and insert its data.

The cache serves `getBlock` (all `transactionDetails` levels), `getBlocks`, `getBlocksWithLimit`, `getBlockTime`, `getTransaction`, `getSignatureStatuses`, `getSignaturesForAddress`, and `getTransactionsForAddress` when the required rows are covered. `DISK_CACHE_RETAIN_SLOTS` is required. The forwarder fills backward to the configured retention floor when the cache starts partially filled or the retention window increases. `DISK_CACHE_MAX_BYTES` is enforced after each successful fill against active MergeTree parts in the primary cache database: it evicts complete old partitions to a 90% low-water mark, and purges the newest partition too if that is necessary to get below the budget. If the database cannot get below the budget even after that purge, the cache marks itself unready and returns to source fallback. The limit does not cover the separately owned block-index database, ClickHouse server overhead, or transient in-flight writes.

`GET_BLOCK_RESPONSE_CACHE_MAX_BYTES` adds a separate lazy in-process cache of serialized finalized `getBlock` results. Its key includes every response-shaping option, but not the JSON-RPC request ID. Entries are built only from finalized data: finalized requests from any tier, and confirmed requests served by the local disk cache or source ClickHouse (both read identically for either commitment). A confirmed request served by the head cache is never inserted, because that block may not be finalized yet. Because an entry proves its slot is finalized, and a confirmed request for a finalized slot returns the same block, confirmed and finalized requests both read it. `GET_BLOCK_RESPONSE_CACHE_SHARE_CONFIRMED=false` restores the finalized-only behavior (confirmed requests neither read nor populate the cache). Concurrent requests for the same result share hydration and serialization work. The same budget switch also enables a bounded (65,536-entry) record of `-32015` unsupported-transaction-version results for finalized blocks, keyed the same way: that error depends only on the immutable, count-validated block and `maxSupportedTransactionVersion`
(`GET_BLOCK_RESPONSE_CACHE_UNSUPPORTED_VERSION=false` turns that record off). Other errors (skipped or unavailable slots, backend failures) are never remembered.

Full and accounts `getBlock` responses with at least 256 transactions are hydrated, encoded, and serialized in up to `GET_BLOCK_HYDRATION_PARALLELISM` contiguous chunks on the blocking pool, one `HYDRATION_CPU_CONCURRENCY` permit per chunk. Only the first permit is awaited; extra permits are taken only when immediately free, so a busy pool falls back to one thread. The chunks are spliced into the response in block order, so the bytes and the reported error match the single-threaded path. Use `RPC_RESPONSE_GZIP_ENABLED=true` to negotiate gzip for large responses. Clients that can consume binary transaction encoding can request `base64` to reduce full-block encoding cost.

The configured cache database is exclusively owned by this feature. A nonempty database without the Superbank ownership marker is rejected and never modified. A source schema fingerprint change rebuilds only a correctly marked cache database. Treat the instance and database as semi-ephemeral.

By default, local initialization failures do not block RPC startup. Reads continue against the source cluster while a background supervisor retries local initialization. `DISK_CACHE_REQUIRED=true` makes initialization a startup requirement and makes `/health` return HTTP 503 when the local cache is not ready or cannot answer a health query.

Signature and address reads use an in-process Bloom membership index to exclude unrelated slot
partitions before querying ClickHouse. This preserves whole-partition eviction without making
key lookups search every retained partition. Signature status batches, pagination-bound signature
lookups, regular/hot address history, and token-owner history use the same routing mechanism.
`getSignaturesForAddress` reads every candidate partition in one local query over a single slot
range from the oldest to the newest candidate, so a full page and a partial page that falls
through to the source each cost one admission and one round trip. Skipped partitions inside the
range are definite index negatives; a range, unlike an `intDiv(slot, width) IN (...)` set, keeps
primary-key analysis on binary search. The local page runs concurrently with the source query for
the request's own bounds and limit. A complete local page (full, or bounded by an `until` inside the
covered span) answers if it arrives first, and the source read is left to finish and is discarded,
because cancelling a source read holds its admission until termination is verified. Otherwise the
source page answers alone and a pending local read is cancelled. If the source fails, the request
still waits for a complete local page before it returns the error. A request whose `before`/`until`
cursor is left to the source page query (`CLICKHOUSE_GSFA_INLINE_CURSOR`, below) is not raced: the
local page cannot be bounded by a cursor it does not know.

The local tip trails the source's finalized tip by the forwarder lag (at least
`DISK_CACHE_REPAIR_MIN_LAG_SLOTS`), so a newest-first address page whose request window extends above
the local tip (no `before`/`beforeSlot`/pagination cursor at or below it, or an `until`/`untilSlot` at
or above it) owes rows the cache has not read. Such a page counts as complete only when the head
cache merged into the response holds every row of the address from the first owed slot up. The
head proves this with the chain of its current stream session, the same proof block ranges use:
the stream is connected, the requested commitment's tip advanced within the last second, and
verified parent links reach from that tip down to the first owed slot, each linked slot having been
received and ingested whole in this session (a restart or reconnect starts a new chain, so slots
the head never received are never inside it). The slot must also be inside head retention
(`HEAD_CACHE_RETAIN_SLOTS`), and the address's head index must not have been truncated at or above
it by the per-address cap (`MAX_SIGNATURES_LIMIT`). For `getTransactionsForAddress` the head merge
must also apply to the whole window (no token-account, signature, block-time, slot or status
filter), and the local page must be full: a page that reached the floor queries the source anyway.
The source is assumed to hold no finalized slot the head has not yet seen as finalized. Otherwise
`getSignaturesForAddress` waits for the source page it already sent, and a descending
`getTransactionsForAddress` page is read from the source in full rather than as a `slot < floor`
remainder. `superbank_disk_cache_tip_gap_total{operation="signatures_for_address"|"transactions_for_address"}`
counts these pages by `outcome`: `head` (covered by the head) or `primary` (sent to the source).
It is separate from `superbank_disk_cache_reads_total`, where the same reads count as `hit`.

With `DISK_CACHE_GSFA_EMPTY_WATERMARK_TTL_SECS` above zero (default `0`, off) and the head cache
enabled, `getSignaturesForAddress` keeps an empty-address watermark: when a request without any
cursor (`before`, `until`, `beforeSlot`, `untilSlot`) gets an empty source page and no local page,
the address is recorded as having no source row at or below `W` = the local contiguous tip read
before the source query, minus 4,500 slots (replica lag and recent repairs; the filler copied every
slot up to that tip from the source). A later request of the same kind for that address, within the
TTL, reads only the local page. It answers without the source when the local page is complete, or
when it starts at or below `W + 1` and the head cache's chain proof (the one `getBlocks` uses)
covers every slot from the local tip + 1 up to its tip for the request's commitment, and the head
cache has not filled its per-address list for the address (a full list may have evicted older rows). These conditions are checked against
the current local span before the local read; when they fail (a coverage floor above `W + 1` after
a wipe, a head gap or stale head tip) the request takes the normal local/source race. If they hold
but the local read then fails, times out, or its span moved, the source page answers alone after
it. The head proof must reach down to the local contiguous tip, so `HEAD_CACHE_RETAIN_SLOTS` must
exceed the distance between the head tip and the local tip (the filler lag plus the finalization
lag); with the default 32 slots the proof fails and the watermark never answers. A non-empty source page removes the entry; only the TTL bounds how long a source
backfill at or below `W` stays unseen. `DISK_CACHE_GSFA_EMPTY_WATERMARK_MAX_ENTRIES` (default
100,000, about 128 bytes each, so about 13 MB) bounds memory with oldest-insert eviction. Outcomes
are counted in `superbank_disk_cache_reads_total{operation="gsfa_empty_watermark"}` (`hit`,
`complete`, `floor_gap`, `head_unproven`, `local_unavailable`). The plain (no head cache) path never
uses it, because nothing proves the slots above the local tip.

`getSignatureStatuses` batches read their candidate
partitions the same way, in one query over the candidates' slot range, while that range costs at
most 1,024 signature-partition lookups or four times the lookups of per-partition queries (after a
restart, unknown partitions make every signature a candidate); larger sparse batches query
candidate partitions newest first. Local tables merge each partition to one part once it has
had no writes for 10 minutes (`min_age_to_force_merge_seconds`, applied at startup outside the
cache fingerprint), because every local query pays CPU per part it opens. With
`DISK_CACHE_COMPACT_TRANSACTIONS_PARTS=true` (default `false`), new local `transactions` parts
are Compact (`min_bytes_for_wide_part = 1 TiB` and, where the server supports it,
`write_marks_for_substreams_in_compact_parts = 0`, applied the same way): a payload lookup then
reads one file with a few reads instead of one per column stream (~150), and marks memory roughly
halves. Compact parts add about 13.5% to `transactions` bytes, so enable the flag only together
with a `DISK_CACHE_MAX_BYTES` raise (about 13.5% above the budget that held the retention window with Wide parts; startup
warns when the flag is on under a byte budget): a budget below the retention window's size makes
the forwarder refill and re-evict the oldest partitions continuously. Existing Wide parts stay
readable; ClickHouse keeps any merge that includes a Wide part Wide, so partitions written before
the change stay Wide until eviction (one retention window) while later partitions are Compact.
With the flag off, startup runs `ALTER TABLE <database>.transactions RESET SETTING` for the
layout settings the server reports, so turning the flag off returns new parts to the server
default layout; Compact parts already written stay readable and age out with retention. Neither
direction changes the cache fingerprint or rebuilds the cache. If the server rejects either
`ALTER`, startup logs a warning and continues. Other reads query candidate partitions in
result order, one at a time, until the answer is complete or their deadline expires. `getSignaturesForAddress` and `getTransactionsForAddress` share one
`DISK_CACHE_ADDRESS_QUERY_TIMEOUT_MS` deadline (default 100 ms) across cursor/bound resolution,
address scans, and full transaction hydration, including admission waiting. An address-request
read waits for local admission at most one tenth of its remaining budget; a saturated cache then
reports `busy` and the source answers at once instead of after the full deadline. This is separate from
`DISK_CACHE_QUERY_TIMEOUT_MS` (default 2000 ms), which still governs other reads and index work.
The 100 ms default leaves room above the observed roughly 3 ms mean cache hit while limiting the
historical 2-second timeout penalty; it is a latency policy, not a measured tail-latency guarantee.
Each address scan or cursor lookup probes at most two unknown partitions, allowing useful recent
hits and both intentionally unindexed retention edges. If another unknown partition is required,
or the deadline expires, the incomplete page is discarded and the source answers the original
bounds. Unknown membership never proves absence. Complete Bloom candidates do not consume the
unknown-probe allowance.

`getTransactionsForAddress` resolves each distinct pagination/filter signature once per request
and reuses numeric bounds on every tier and refill. A successfully missing bound remains
unbounded; a lookup failure remains an error. Ordinary-table predicates retain their computed-key
workaround for reverse-key ClickHouse tables. Full pages hydrate exact `(slot, slot_idx, signature)`
keys in batches of at most 100, locally and on the primary. Only unresolved identities retry
without `slot_idx`, under the same hydration deadline. Cache eviction falls back to the primary;
unsupported transaction versions still produce the encoder's error.

Partition-scoped interactive reads enable ClickHouse's uncompressed-block cache
(`use_uncompressed_cache=1`) while keeping the query-result cache disabled. They keep
data-skipping indexes enabled: reads that span several partitions (the fused getTransaction lookup and
single-query signature-status reads) rely on `bf_signature` to skip the one primary-key-selected
granule in parts that do not hold the signature. The cache reuses
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

A Bloom-positive `getTransaction` first runs one local query over the slot span of its
candidate partitions: it resolves the newest indexed position and returns that payload
under a single admission. A row is used only when its slot is covered and lies in a
candidate partition. No row, or any other row, is not a miss: the attempt then runs the
separate position and payload reads above with the same candidates, and only they can
prove absence. When the fused query returns no row and there are several candidate
partitions, the fallback first runs the fused query's position lookup alone over the same
span; if that finds no position, every per-partition position read would find none too, so
they are skipped and the attempt goes straight to the pinned-slot check (reason
`span_empty`). A position anywhere in the span (stale, a secondary signature, a missing
payload, outside coverage) still takes the per-partition reads. This bounds a miss while
signature membership is unknown, such as during index builds after a restart, to two local
queries instead of one plus one per candidate partition. `DISK_CACHE_GET_TX_SPAN_CHECK=false`
turns the span lookup off, so the fallback always runs the per-partition reads.
`superbank_disk_cache_key_seconds{operation="get_tx_fused"}` records `hit`,
`fallback`, or the failure outcome of that query; `operation="get_tx_fallback"` records
the two-step read after it as `hit`, `miss`, `absent`, or its failure outcome. The
`get_tx` attempt outcomes are unchanged. Each fused query counts every candidate partition
as probed, and a fallback counts the partitions it reads again.

One local `getTransaction` attempt, fused query and fallback included, has its own budget,
`DISK_CACHE_GET_TX_TIMEOUT_MS` (default 1000 ms, capped at `DISK_CACHE_QUERY_TIMEOUT_MS`). The
primary read starts only after the local attempt gives up, so a stalled local read costs this
budget rather than the 2 s shared timeout; an expired budget is `Unavailable` (primary fallback),
never proof of absence. Status and other reads keep `DISK_CACHE_QUERY_TIMEOUT_MS`. The default
is chosen to sit above the tail latency (p99.9) of local hits on a healthy node.

While the signature index is being built (after a process restart or an index reset), every
unbuilt partition has unknown membership, so every signature is a candidate in each of them and a
local miss probes them one after another. When more than 4 partitions in the cached span are
unknown, the attempt uses `DISK_CACHE_GET_TX_UNKNOWN_TIMEOUT_MS` instead (default 150 ms, capped
at the getTransaction budget above). Fast local hits are still served; a slow attempt falls back
to the primary sooner. The one or two partitions a repairing or failed fill leaves unknown do not
trigger it. Setting it equal to `DISK_CACHE_GET_TX_TIMEOUT_MS` disables the shorter budget.

A local read is discarded when the cache is invalidated while it runs. Eviction only moves the
retention floor, so it discards in-flight negatives but keeps a found transaction whose slot is
still covered when the read completes (`DISK_CACHE_EVICTION_SAFE_HITS=false` makes eviction
discard every in-flight read again). Poisoned or repaired slots, schema rebuilds, and an
unready cache still discard every in-flight result.
`superbank_disk_cache_get_tx_reason_total{reason}` counts each `get_tx` attempt once by reason:
`found`, `found_revalidated` (found across an eviction), `absent`, `bloom_absent` (no candidate
partition, no query), `probe_empty`, `span_empty` (the fused span held no position; see
`DISK_CACHE_GET_TX_SPAN_CHECK`),
`index_without_payload`, `invalidated`, `not_ready`, `timeout`, `unknown_timeout` (the shorter
unknown-membership budget expired), and `error`. It is a separate counter family (no histogram),
so sums over `superbank_disk_cache_reads_total` count each attempt once, as `operation="get_tx"`,
whose outcomes are unchanged; both timeout reasons are `get_tx` `timeout`.

The `superbank_disk_cache_reads_total` outcomes distinguish misses, query errors, and timeouts;
address reads also report `busy` (bounded admission wait expired) and `probe_budget` (a third
unknown partition was required). `superbank_disk_cache_key_seconds{operation="admission"}`
records `acquired` and `busy` waits, and `superbank_disk_cache_address_partitions` is the number
of candidate partitions each address read covered.
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
| `--disk-cache-get-tx-timeout-ms` | `DISK_CACHE_GET_TX_TIMEOUT_MS` | `1000` | Budget for one local `getTransaction` attempt before primary fallback; capped at `DISK_CACHE_QUERY_TIMEOUT_MS`. Positive integer. |
| `--disk-cache-fused-get-tx=<bool>` | `DISK_CACHE_FUSED_GET_TX` | `true` | Reads a local `getTransaction` position and payload in one query before the two-step lookup. `false` uses only the two-step lookup (position, then payload). |
| `--disk-cache-get-tx-span-check=<bool>` | `DISK_CACHE_GET_TX_SPAN_CHECK` | `true` | After an empty fused `getTransaction` read over more than one candidate partition, asks the whole span for a position once and skips the per-partition probes when it holds none (reason `span_empty`). `false` always runs the per-partition probes. |
| `--disk-cache-eviction-safe-hits=<bool>` | `DISK_CACHE_EVICTION_SAFE_HITS` | `true` | Serves a local `getTransaction` row found across an eviction while its slot is still covered. `false` makes eviction discard every in-flight read. |
| `--disk-cache-status-span-query=<bool>` | `DISK_CACHE_STATUS_SPAN_QUERY` | `true` | Looks up local signature statuses with one query over the candidates' slot span (within the lookup cap). `false` queries candidate partitions one at a time, newest first. |
| `--disk-cache-compact-transactions-parts` | `DISK_CACHE_COMPACT_TRANSACTIONS_PARTS` | `false` | Writes new local `transactions` parts as Compact; raise `DISK_CACHE_MAX_BYTES` (~13.5% more bytes) with it. `false` resets the layout settings at startup so new parts use the server default. Never rebuilds the cache. |
| `--gsfa-race-primary=<bool>` | `GSFA_RACE_PRIMARY` | `true` | Races the local `getSignaturesForAddress` page against the primary's full page. `false` awaits the local page first and asks the primary only for the remainder below the coverage floor. |
| `--disk-cache-get-tx-unknown-timeout-ms` | `DISK_CACHE_GET_TX_UNKNOWN_TIMEOUT_MS` | `150` | Budget for one local `getTransaction` attempt while more than 4 signature-index partitions have unknown membership (index build after a restart); capped at the getTransaction budget. Positive integer. |
| `--disk-cache-address-query-timeout-ms` | `DISK_CACHE_ADDRESS_QUERY_TIMEOUT_MS` | `100` | Shared address-request cache budget in milliseconds; includes cursors, scans and full hydration. Positive integer. |
| `--disk-cache-gsfa-empty-watermark-ttl-secs` | `DISK_CACHE_GSFA_EMPTY_WATERMARK_TTL_SECS` | `0` | TTL of the `getSignaturesForAddress` empty-address watermark; `0` disables it. Needs the head cache. See the disk-cache section. |
| `--disk-cache-gsfa-empty-watermark-max-entries` | `DISK_CACHE_GSFA_EMPTY_WATERMARK_MAX_ENTRIES` | `100000` | Addresses kept in the watermark cache (about 128 bytes each), range 1–10,000,000. |
| `--disk-cache-key-index-max-memory-bytes` | `DISK_CACHE_KEY_INDEX_MAX_MEMORY_BYTES` | `4294967296` | In-process partition membership budget, including builder buffers and metadata; minimum 64 MiB. Separate from ClickHouse and the historical block index. |
| `--disk-cache-query-concurrency` | `DISK_CACHE_QUERY_CONCURRENCY` | `8` | Concurrent local interactive queries, range 1–64. |
| `--disk-cache-background-query-concurrency` | `DISK_CACHE_BACKGROUND_QUERY_CONCURRENCY` | min(query concurrency, 8) | Concurrent local background reads (coverage reloads, fill count validation, signature-membership scans), range 1–64. A separate lane from interactive queries; the default does not grow when query concurrency is raised above 8. Index builds keep their own single-reader lanes. |
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

Observability: `superbank_disk_cache_*` metrics cover readiness, local ClickHouse bytes, coverage span, read outcomes, forwarding, errors, rebuilds, and partition eviction. `superbank_disk_cache_bootstraps_total{outcome}` counts startups by what schema initialization did to the local cache: `reused` (fingerprint matched), `created` (the cache database had no tables, e.g. reset storage), `rebuilt` (format or fingerprint changed; also counted in `superbank_disk_cache_wipes_total`), or `coverage_reset` (Memory-table coverage reset after a local ClickHouse restart; also counted as a wipe). Startup logs the same outcome. `superbank_disk_cache_get_tx_reason_total{reason}` counts each local `getTransaction` attempt once by reason (see above). `superbank_block_index_*` metrics report the full-history worker, hydrated floor/head, allocated memory, and errors. Route metrics retain the `disk_cache_read` label. The `X-Superbank-Sources` response header reports `disk-cache` combinations.

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
| `--get-transaction-primary-cache-max-bytes` | `GET_TRANSACTION_PRIMARY_CACHE_MAX_BYTES` | `0` | Approximate byte budget for `getTransaction` records served by primary ClickHouse. `0` disables the in-process cache. |
| `--get-transaction-primary-cache-ttl-secs` | `GET_TRANSACTION_PRIMARY_CACHE_TTL_SECS` | `600` | Seconds a primary-served `getTransaction` record stays cached after insertion (1..=86400). |
| `--get-block-response-cache-share-confirmed` | `GET_BLOCK_RESPONSE_CACHE_SHARE_CONFIRMED` | `true` | Let confirmed `getBlock` requests read the finalized response cache and populate it from disk-cache or source ClickHouse results. Set the env var to `false` to disable. |
| `--get-block-response-cache-unsupported-version=<bool>` | `GET_BLOCK_RESPONSE_CACHE_UNSUPPORTED_VERSION` | `true` | While the response cache is enabled, remembers finalized `getBlock` `-32015` (unsupported transaction version) answers per request shape (up to 65,536 entries). `false` refetches and rehydrates the block for every such request. |
| `--hydration-cpu-concurrency` | `HYDRATION_CPU_CONCURRENCY` | `8` | Blocking-pool permits for CPU-heavy transaction and block hydration. |
| `--get-block-hydration-parallelism` | `GET_BLOCK_HYDRATION_PARALLELISM` | `4` | Maximum hydration permits (threads) one large full/accounts `getBlock` build may use; extra permits are taken only when free. `1` disables chunking. |
| `--signature-status-history-cache-entries` | `SIGNATURE_STATUS_HISTORY_CACHE_ENTRIES` | `0` | Signatures whose empty primary `searchTransactionHistory` answer is remembered (see "Primary signature-status overload protection"). `0` disables the cache. Requires `grpc-head-cache` and `disk-cache`. |
| `--signature-status-history-cache-max-bytes` | `SIGNATURE_STATUS_HISTORY_CACHE_MAX_BYTES` | `67108864` | Memory bound for that cache, budgeted at 384 bytes per entry. |
| `--signature-status-history-cache-ttl-secs` | `SIGNATURE_STATUS_HISTORY_CACHE_TTL_SECS` | `300` | How long a remembered absence may skip the primary (1-300 seconds). |
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
| `--get-inflation-reward-epoch-cache-max-bytes` | `GET_INFLATION_REWARD_EPOCH_CACHE_MAX_BYTES` | `16777216` | Byte budget for the in-process cache of validated payout-epoch boundary blocks and complete partition block-height-to-slot maps. Entries expire after one hour. Lookup outcomes (`-32004`, `-32017`), errors, and reward rows are never cached. `0` disables the cache and is a full rollback: every lookup takes the pre-cache queries, including the exact-height partition lookup instead of the whole declared partition range. |
| `--emit-http-errors` | `SUPERBANK_RPC_EMIT_HTTP_ERRORS` | `false` | Return HTTP `503 Service Unavailable` for selected server-side JSON-RPC failures; response bodies are unchanged. |
| `--host` | `RPC_HOST` | `0.0.0.0` | — |
| `--port` | `RPC_PORT` | `8899` | — |
| `--metrics-host` | `METRICS_HOST` | `0.0.0.0` | — |
| `--metrics-port` | `METRICS_PORT` | `9900` | — |
| `--genesis-path` | `GENESIS_PATH` | unset | Path to the target cluster's mounted `genesis.bin`. The server fails startup if a configured file cannot be read or decoded. Leave unset only for the no-warmup fallback. |
| `--ag-genesis-cert-rpc-url` | `AG_GENESIS_CERT_RPC_URL` | unset | Trusted same-cluster Agave 4.3+ certificate RPC source. Unset or empty keeps startup optional but `getAgGenesisCert` returns an unavailable-source error. |
| `--ag-genesis-cert-rpc-timeout-ms` | `AG_GENESIS_CERT_RPC_TIMEOUT_MS` | `2000` | Positive total source budget, including admission; must be below `RPC_REQUEST_TIMEOUT_MS` when a source is configured. |
| `--ag-genesis-cert-refresh-interval-secs` | `AG_GENESIS_CERT_REFRESH_INTERVAL_SECS` | `5` | Authoritative null TTL (1–300 seconds); certificates stay cached until restart, failures for 1 second. |
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
| `--clickhouse-query-timeout-ms` | `CLICKHOUSE_QUERY_TIMEOUT_MS` | `8000` | ClickHouse operation timeout (ms), including admission and response consumption. HTTP abandonment closes the data response and retains read/workflow admission until termination verification succeeds, for primary and shard reads at most the rounded budget plus 5 s; those reads carry HTTP `max_execution_time` and `receive_timeout` of the rounded budget, and optional query `SETTINGS` also carry `max_execution_time`. Explicit method and background range deadlines remain supported. Shard-direct TCP retains its shorter internal attempt timeout and best-effort cleanup. Keep this parent timeout below `RPC_REQUEST_TIMEOUT_MS`. |
| `--clickhouse-http-max-concurrency` | `CLICKHOUSE_HTTP_MAX_CONCURRENCY` | `512` | Concurrency budget for direct ClickHouse HTTP work, shared across client clones. Abandoned reads retain associated admission through termination verification. Existing shard fanout and method limits remain active; background readers have dedicated lanes, and verifier probes use a separate control pool. Excess reads wait within the applicable operation timeout. Set at or below the ClickHouse per-user connection/query budget. |
| `--clickhouse-http-connect-timeout-ms` | `CLICKHOUSE_HTTP_CONNECT_TIMEOUT_MS` | `2000` | TCP connect timeout (ms) for ClickHouse HTTP connections, so a new connection attempt fails fast during ClickHouse backpressure instead of hanging. |
| `--clickhouse-startup-verification-timeout-ms` | `CLICKHOUSE_STARTUP_VERIFICATION_TIMEOUT_MS` | `10000` | Positive per-query cancellation initialization budget (ms), including macro resolution, topology discovery, capability probes and retries. Independent of connection and normal query timeouts. |
| `--clickhouse-runtime-verification-timeout-ms` | `CLICKHOUSE_RUNTIME_VERIFICATION_TIMEOUT_MS` | `10000` | Positive per-batch abandoned-query verification budget (ms). Slow probes retain admission and can delay the five-second unconfirmed warning until the probe completes. |
| `--clickhouse-query-cache-enabled` | `CLICKHOUSE_QUERY_CACHE_ENABLED` | `false` | Enables ClickHouse query cache settings for historical read queries. |
| `--clickhouse-query-cache-ttl-seconds` | `CLICKHOUSE_QUERY_CACHE_TTL_SECONDS` | `1` | TTL for cached historical read query results (seconds). |
| `--clickhouse-get-transaction-query-cache-ttl-seconds` | `CLICKHOUSE_GET_TRANSACTION_QUERY_CACHE_TTL_SECONDS` | `300` | TTL override applied only to historical `getTransaction` point lookups when query cache is enabled. |
| `--clickhouse-get-transaction-query-cache-min-query-runs` | `CLICKHOUSE_GET_TRANSACTION_QUERY_CACHE_MIN_QUERY_RUNS` | `2` | Minimum identical `getTransaction` point-lookups required before ClickHouse writes them into cache. |
| `--clickhouse-latest-slot-hint=<bool>` | `CLICKHOUSE_LATEST_SLOT_HINT` | `true` | Bounds latest-finalized-slot queries by the caller's previous answer (an empty bounded result falls back to the unbounded query). `false` sends the unbounded query every time. |
| `--clickhouse-get-transaction-single-round-trip` | `CLICKHOUSE_GET_TRANSACTION_SINGLE_ROUND_TRIP` | `false` | Resolves an uncached `getTransaction` signature position and reads its payload in one primary query instead of two sequential queries. Distributed scope only. See [getTransaction single round trip](#gettransaction-single-round-trip). |
| `--clickhouse-transactions-for-address-position-tokens` | `CLICKHOUSE_TRANSACTIONS_FOR_ADDRESS_POSITION_TOKENS` | `false` | Returns `slot:idx` `paginationToken`s for ClickHouse-sourced `getTransactionsForAddress` rows so the next page skips the primary signature lookup. See [getTransactionsForAddress position tokens](#gettransactionsforaddress-position-tokens). |
| `--clickhouse-transactions-for-address-cursor-cache` | `CLICKHOUSE_TRANSACTIONS_FOR_ADDRESS_CURSOR_CACHE` | `false` | Remembers the position of each ClickHouse-sourced `getTransactionsForAddress` page's last row in process, so a follow-up page on the same node that sends that signature as its cursor skips the primary signature lookup. See [getTransactionsForAddress position tokens](#gettransactionsforaddress-position-tokens). |
| `--clickhouse-transactions-for-address-union-pushdown=<bool>` | `CLICKHOUSE_TRANSACTIONS_FOR_ADDRESS_UNION_PUSHDOWN` | `true` | For `getTransactionsForAddress` with `tokenAccounts` other than `none`, applies the request filters, `ORDER BY` and `LIMIT` inside each `gsfa`/`token_owner_activity` UNION branch (same result; each shard ships at most `limit` rows per branch). Unresolved signature bounds keep the outer filter. Also applies to local disk-cache reads. `false` applies the filter outside the union, as before. |
| `--clickhouse-gsfa-inline-cursor` | `CLICKHOUSE_GSFA_INLINE_CURSOR` | `false` | Resolves a `getSignaturesForAddress` `before`/`until` cursor that the head and local tiers missed inside the primary page query instead of a separate lookup first. Distributed scope only. See [getSignaturesForAddress inline cursor](#getsignaturesforaddress-inline-cursor). |
| `--clickhouse-signatures-owner-shard-routing=<bool>` | `CLICKHOUSE_SIGNATURES_OWNER_SHARD_ROUTING` | `false` | Sends primary signature lookups (signature position, `getSignatureStatuses` history, the single-round-trip position scalar) only to the shard that owns the signature instead of every shard. Verified at startup. See [Signature owner-shard routing](#signature-owner-shard-routing). |
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
| `--clickhouse-signatures-local-table` | `CLICKHOUSE_SIGNATURES_LOCAL_TABLE` | — | Shard-direct, and the local table read by `CLICKHOUSE_SIGNATURES_OWNER_SHARD_ROUTING`. Defaults to `CLICKHOUSE_SIGNATURE_STATUSES_TABLE` + `_local`. |
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

### getTransaction single round trip

- Without a `slot` parameter, a primary `getTransaction` fallback resolves the signature's latest `(slot, slot_idx)` on `signatures` and then reads the payload from `transactions`: two sequential queries, so two network round trips when the in-process signature-slot cache misses.
- With `CLICKHOUSE_GET_TRANSACTION_SINGLE_ROUND_TRIP=true` and `CLICKHOUSE_SCOPE=distributed`, a cache miss sends one query instead. A scalar subquery resolves the position and the outer query reads the payload at that exact `(slot, slot_idx, signature)`, so it selects one granule like the two-query payload read.
  - The scalar always returns one row. An unknown signature yields a sentinel slot (`u64::MAX`) that the outer `WHERE` rejects, so the query returns zero rows and the response is `null` as before.
  - With the analyzer (`enable_analyzer=1`; verified locally on 26.8.11.7), ClickHouse folds the scalar to a constant before `optimize_skip_unused_shards`: the payload read goes only to the shard that owns the slot's epoch, and an unknown signature reads no `transactions` shard. With `enable_analyzer=0` an unknown signature sends one empty `transactions` shard read.
  - The query carries `use_query_cache=0`: every lookup is unique per signature, so the query cache would only take writes.
  - There is no legacy fallback. A signature whose payload row has a different `slot_idx` than its `signatures` row returns `null` while the flag is on; the two-query path finds it by retrying without `slot_idx`.
- The signature-slot cache, its singleflight and the single HTTP permit are kept. A cached position reads the payload only (with the usual legacy fallback), and a cached miss returns `null` without a query. The query's leader caches the returned row's position, or a miss (`SIGNATURE_SLOT_CACHE_TTL_MISSING_SECS`) when no row comes back. As a result, a signature with a `signatures` row but no payload row at that position caches a miss rather than its position, and `getSignaturesForAddress`/`getTransactionsForAddress` `before`/`until` cursors that consult the same cache report it as not found until the miss expires.
- Shard-direct scope and requests with an explicit `slot` keep their existing queries.
- ClickHouse `query_id` label: `get_transaction_single_rt`.
- Default `false`. Enable it per node, and compare `rpc_clickhouse_duration_seconds{method="getTransaction"}` and the primary `system.query_log` shard sub-query counts against nodes that still send two queries: a found signature should cost at most the two-query path's shard sub-queries, and an unknown signature no `transactions` shard sub-query.

### getTransactionsForAddress position tokens

- Token-account requests read a `gsfa` UNION `token_owner_activity` subquery. By default each branch carries the request filter plus `ORDER BY slot, slot_idx, base58Encode(signature)` and `LIMIT n` in the request's direction, and the outer query still orders and limits the union, so the page is unchanged while each branch stops at `n` rows. It relies on the outer `signature` alias resolving to the base58 value (`prefer_column_name_to_alias=0`, the server default). `CLICKHOUSE_TRANSACTIONS_FOR_ADDRESS_UNION_PUSHDOWN=false` restores the previous shape (filter outside the union, no per-branch limit).
- `paginationToken` is either a signature or a `slot:idx` position. In `grpc-head-cache` builds, head-cache and disk-cache rows already return positions; in builds without `grpc-head-cache`, every row (disk-cache rows included) returns its signature unless position tokens are enabled. A ClickHouse-sourced last row returns its signature by default, so the next page resolves that signature to a position first: head cache, then disk cache, then a primary `signatures` query (a sequential primary round trip for rows older than the local caches).
- With `CLICKHOUSE_TRANSACTIONS_FOR_ADDRESS_POSITION_TOKENS=true`, ClickHouse-sourced rows (and, without `grpc-head-cache`, disk-cache rows) return `slot:idx` from the address row (`gsfa` or `token_owner_activity`) in both `signatures` and `full` modes, and the next page uses it directly. Signature tokens stay accepted, so tokens issued before the switch keep working.
- Behavior change: the position comes from the address row, while a signature token resolves through `signatures`. They agree whenever both tables carry the same `slot_idx` for the transaction; on a historical index mismatch the page continues from the address row's position, which is the order the page itself was sorted by. A signature token missing from `signatures` is a confirmed miss that drops the cursor, so that page restarts from the newest row; a position token (or a cursor-cache hit) continues after the previous page instead. Clients that parse the token as a signature (rather than echoing it) see the `slot:idx` shape that `grpc-head-cache` builds already return for head- and disk-sourced rows.
- The gain needs clients that echo the returned token. Default `false`; enable per node and compare `rpc_clickhouse_duration_seconds{method="getTransactionsForAddress"}` and the `signature_slot` query count per gTFA request.
- `CLICKHOUSE_TRANSACTIONS_FOR_ADDRESS_CURSOR_CACHE=true` keeps responses unchanged (signature tokens) and instead remembers, per served page, `last row signature -> (slot, slot_idx)` from the same address row (bounded in-process cache: 50,000 entries, 6 h TTL, roughly 12-15 MB, gTFA only). A signature `paginationToken` that the head and disk caches cannot place is looked up there before the primary `signatures` query. It helps when the next page lands on the same node, whether the client echoes the token or sends its last signature. The position semantics match position tokens (same caveats on historical index mismatches and confirmed misses). Signature filters (`filters.signature.*`) never use it and always resolve through the head cache, disk cache, or `signatures`, including a filter that names the same signature as the token. Outcomes are counted in `superbank_transactions_for_address_cursor_cache_access_total{operation="get_transactions_for_address",outcome}` (`hit`, `miss`, `insert`); roll out by comparing the hit share against the `signature_slot` query count per gTFA request.
### getSignaturesForAddress inline cursor

- A `before`/`until` signature cursor is resolved from the head cache, then the local cache, then the primary: a separate `signatures` lookup (cached in the signature-slot cache) before the page query, so two sequential round trips.
- With `CLICKHOUSE_GSFA_INLINE_CURSOR=true`, `CLICKHOUSE_SCOPE=distributed`, and the GSFA transaction-table fallback disabled, the primary step first consults the signature-slot cache without querying. On a miss the page query resolves the cursor itself: `WITH (SELECT if(count() = 0, (u64::MAX, 0), max((slot, slot_idx))) FROM signatures PREWHERE sig_bucket = … AND signature = …) AS before_pos`, the same aggregate scalar as the getTransaction single round trip, so an unknown signature never raises error 125 and the newest `(slot, slot_idx)` equals the separate lookup's.
  - The bound keeps the `(slot < s OR (slot = s AND slot_idx < i))` shape, so ClickHouse folds the scalar into the same key condition (verified locally on 26.8.11.7: same granules as the literal bound).
  - Each inline cursor adds one `UNION ALL` cursor row (empty signature, `memo` `before`/`until`) carrying the resolved `(slot, slot_idx)`. A missing cursor (`u64::MAX`) matches no page row and maps to the usual -32020 `Transaction … not found`; `before` wins when both are missing, as in the sequential order. `until` equal to `before` reuses one scalar.
  - On the head-cache path, head rows are read after the page query and bounded by the resolved positions, so the head-only shortcut (the head alone fills the limit) is skipped for these requests; the merged, truncated page is the same.
  - The query uses the normal historical settings (query cache included when enabled), like the separate lookup it replaces. It does not write the signature-slot cache.
  - The request is not raced against the local page (see the disk-cache section): a cursor the local tier missed is below its floor, in the filler gap, or timed out, so a local page could not be bounded by it.
- Shard-direct scope, local-cache clients and `CLICKHOUSE_GSFA_FALLBACK_TRANSACTIONS` (transaction-table fallback) keep the separate lookup.
- Default `false`. Enable it per node and compare `rpc_clickhouse_duration_seconds{method="getSignaturesForAddress"}` and primary `get_signature_slot` query counts against nodes without it.

### Signature owner-shard routing

- `default.signatures` is a materialized view whose storage is a `Distributed` table sharded by `cityHash64(signature)`. A read through the view does not prune shards, even with `optimize_skip_unused_shards=1`: every primary signature lookup queries every shard and waits for the slowest one.
- The view's inner table (`.inner_id.<uuid>`) does prune, but its name can differ between replicas, so it cannot be named behind a load-balanced `CLICKHOUSE_URL`.
- With `CLICKHOUSE_SIGNATURES_OWNER_SHARD_ROUTING=true`, primary lookups read `cluster('<CLICKHOUSE_CLUSTER>', <signatures local table>, cityHash64(signature))` instead. It is the same `Distributed` read over the same local table and sharding key, built on whichever host receives the query, so it returns the same rows. The queries add `optimize_skip_unused_shards=1, force_optimize_skip_unused_shards=0`, so ClickHouse sends each one only to the owner shard and a filter it cannot prune still fans out and works.
  - Covered reads: the signature position lookup (`getTransaction` without `slot`, `before`/`until` cursors), the primary `getSignatureStatuses` history read, and the `CLICKHOUSE_GET_TRANSACTION_SINGLE_ROUND_TRIP` position scalar.
  - Batched status reads add a redundant `signature IN (...)` to the `(sig_bucket, signature) IN (...)` filter, because ClickHouse cannot prune on the tuple filter alone. A batch reaches only the shards that own its signatures.
  - Local-cache (disk-cache) reads, shard-direct reads and gSFA are unchanged. The `CLICKHOUSE_GSFA_INLINE_CURSOR` cursor scalar also stays on the view: it is part of the gSFA page query and uses that query's source and settings. A signature-slot cache hit it consults may come from a routed lookup.
  - With `SIGNATURE_STATUS_HISTORY_CACHE_*` enabled, an empty routed history read is remembered as an absence like an empty view read, so a row that breaks the placement invariant below stays hidden for up to the cache TTL.
- Requirements: `CLICKHOUSE_CLUSTER` names the cluster the view's `Distributed` engine uses, or a cluster with identical `system.clusters` rows (the default `{cluster}` macro is expanded by ClickHouse). `CLICKHOUSE_CLUSTER` is also the query-cancellation cluster: do not point it at an all-hosts or all-replicas cluster while this flag is on. The local table is `CLICKHOUSE_SIGNATURES_LOCAL_TABLE` or `CLICKHOUSE_SIGNATURE_STATUSES_TABLE` + `_local` and must be a plain `[db.]table` name. The ClickHouse user needs the `REMOTE` grant (table functions), per-query `SETTINGS`, and read access to `system.tables`, `system.macros` and `system.clusters` for the startup layout check.
- Placement invariant: routed reads find a row only on the shard that `cityHash64(signature)` selects under the current `CLICKHOUSE_CLUSTER` layout (shard order and weights). The view reads every shard, so it still finds rows that sit elsewhere; routed lookups silently return not-found for them. This holds only while every `signatures_local` row sits on its owner shard. Resharding, reordering `remote_servers`, changing shard weights, adding a shard after data landed, or a backfill that inserted into `signatures_local` directly on the wrong shard all break it, and nothing at runtime detects that.
- Startup and failure modes:
  - An empty `CLICKHOUSE_CLUSTER` or an invalid table name fails startup.
  - When query `SETTINGS` are unavailable (`CLICKHOUSE_DISABLE_QUERY_SETTINGS` or a readonly user), routing turns itself off with a warning and lookups read the view.
  - Layout check (on the host that answers the startup queries): the signatures table must be a materialized view with `ENGINE = Distributed(...)` (or a `Distributed` table) over the configured local table, sharded by `cityHash64(signature)`. `CLICKHOUSE_CLUSTER` and the view's cluster, with `system.macros` expanded, must be the same name or have identical `system.clusters` rows (shard number, weight, replica number, host, port). A mismatch fails startup with an error naming both clusters. This catches a cluster that exists but lists the same hosts in another order, which the probe below accepts. The check reads the view's `create_table_query`, which ClickHouse shows only to a user with a privilege on the view; without it startup fails with "not a materialized view with ENGINE = Distributed".
  - Probe: one lookup of an all-zero signature against the `cluster()` source with `force_optimize_skip_unused_shards=1`, using the same `toFixedString(unhex('<hex>'), 64)` literal as real lookups. A missing cluster, grant or table, or a literal ClickHouse cannot constant-fold for pruning, fails startup with an error that names the source.
  - What the checks do not prove: that stored rows follow the placement invariant (run the pre-enable placement check below), or that hosts other than the one that answered have the same view, macros and `remote_servers` (behind a load balancer only one host answers).
  - Pruning depends on the signature literal being constant-folded. `toFixedString(unhex('<hex>'), 64)` folds; an expression such as `unhex(lpad(hex(...)))` does not, and with `force_optimize_skip_unused_shards=0` such a read would silently fan out to every shard. Change the literal only through the shared helper, which the probe also uses.
  - Unset the flag to go back to the view. At runtime a failed read fails like a read of the view; there is no automatic fallback to the view.
- Before enabling on a cluster (bounded placement check, run once per cluster, off-peak):
  1. `SELECT shard_weight, count() FROM system.clusters WHERE cluster = '<cluster>' AND replica_num = 1 GROUP BY shard_weight` must return one row with weight 1 (the check below assumes uniform weight 1; otherwise use `scripts/analysis/check-shard-key-consistency.sh`).
  2. For one `sig_bucket` and a signature-prefix range (the primary key is `(sig_bucket, signature, ...)`, so the range bounds the read; signatures are hash-distributed, so the sample covers every ingest, rebuild and backfill era), count misplaced rows. `misplaced` must be 0 on every shard. Add `AND slot BETWEEN <a> AND <b>` to look at a specific rebuild era.

     ```sql
     SELECT shardNum() AS shard, count() AS rows_checked,
            countIf(cityHash64(signature) % <shards> != shardNum() - 1) AS misplaced,
            min(slot), max(slot)
     FROM cluster('<cluster>', default.signatures_local)
     WHERE sig_bucket = 7 AND signature < toFixedString(unhex('01'), 64)
     GROUP BY shard ORDER BY shard
     SETTINGS optimize_skip_unused_shards = 0, max_rows_to_read = 200000000, read_overflow_mode = 'throw'
     ```

     `<shards>` is the cluster's shard count. `scripts/analysis/check-shard-key-consistency.sh` runs the same check over whole tables (full scan).
  3. Confirm `CLICKHOUSE_CLUSTER` on the canary node is unset, `{cluster}`, or the name of the cluster the view's `Distributed` engine uses, and that `SELECT getMacro('cluster')` returns that name on every host.
- Canary: enable it on one node and compare with a control node that keeps the flag off.
  - Rollback triggers (unset the flag and restart):
    - `getTransaction` `not_found` share (`superbank_rpc_route_total_total{method="getTransaction",outcome="not_found"}` over all outcomes, at least 10 minutes) on the canary more than 0.1 percentage point above the control node.
    - Any known-present signature that the canary reports as missing. There is no per-signature null metric for `getSignatureStatuses`, so before and during the canary run `tests/k6/scenarios/validation/superbank-rpc-get-transaction-parity.js` with `RPC_URL=<canary>`, `REFERENCE_RPC_URL=<control>` and a `SIGNATURE_FILE` of known-present signatures (it fails on the first difference), and send the same signatures through `getSignatureStatuses` on both nodes: any extra `null` on the canary triggers a rollback.
    - A `superbank_rpc_clickhouse_duration_seconds` p99 for `getTransaction` or `getSignatureStatuses` above the control node's.
  - Slot lookups: on the primary, count shard sub-queries per signature lookup (bounded window). On an N-shard cluster the view averages up to N-1 remote sub-queries per lookup (the coordinator reads its own shard locally with `prefer_localhost_replica=1`, and query-cache hits count as zero, so the measured average can be slightly lower). Routed lookups average at most one:

    ```sql
    SELECT position(i.query, 'FROM cluster(') > 0 AS routed,
           count() AS lookups,
           avg(s.subqueries) AS remote_subqueries_per_lookup,
           quantiles(0.5, 0.9, 0.99)(i.query_duration_ms) AS coordinator_ms
    FROM (
        SELECT query_id, query, query_duration_ms
        FROM clusterAllReplicas('<cluster>', system.query_log)
        WHERE event_date = today() AND event_time > now() - INTERVAL 10 MINUTE
          AND type = 'QueryFinish' AND is_initial_query = 1 AND user = '<rpc_user>'
          AND query LIKE '%sig_bucket = % AND signature = %LIMIT 1%'
    ) AS i
    LEFT JOIN (
        SELECT initial_query_id, count() AS subqueries
        FROM clusterAllReplicas('<cluster>', system.query_log)
        WHERE event_date = today() AND event_time > now() - INTERVAL 10 MINUTE
          AND type = 'QueryFinish' AND is_initial_query = 0 AND initial_user = '<rpc_user>'
          AND query LIKE '%`signatures_local`%'
        GROUP BY initial_query_id
    ) AS s ON s.initial_query_id = i.query_id
    GROUP BY routed
    ```

  - Status batches: the batch shape (`(sig_bucket, signature) IN (...) AND signature IN (...)`) was shown to prune on 26.8.11.7 only; confirm it on the version you run. Each routed batch must reach at most its owner shards (`batches_beyond_owners` = 0 for `routed = 1`; the view reaches every remote shard). `<shards>` is the cluster's shard count:

    ```sql
    SELECT position(i.query, 'FROM cluster(') > 0 AS routed,
           count() AS batches,
           avg(i.owner_shards) AS owner_shards_per_batch,
           avg(s.subqueries) AS remote_subqueries_per_batch,
           countIf(s.subqueries > i.owner_shards) AS batches_beyond_owners,
           quantiles(0.5, 0.9, 0.99)(i.query_duration_ms) AS coordinator_ms
    FROM (
        SELECT query_id, query, query_duration_ms,
               length(arrayDistinct(arrayMap(h -> cityHash64(unhex(h)) % <shards>,
                   extractAll(query, 'unhex\\(''([0-9A-F]{128})''\\)')))) AS owner_shards
        FROM clusterAllReplicas('<cluster>', system.query_log)
        WHERE event_date = today() AND event_time > now() - INTERVAL 10 MINUTE
          AND type = 'QueryFinish' AND is_initial_query = 1 AND user = '<rpc_user>'
          AND query LIKE '%(sig_bucket, signature) IN (%'
    ) AS i
    LEFT JOIN (
        SELECT initial_query_id, count() AS subqueries
        FROM clusterAllReplicas('<cluster>', system.query_log)
        WHERE event_date = today() AND event_time > now() - INTERVAL 10 MINUTE
          AND type = 'QueryFinish' AND is_initial_query = 0 AND initial_user = '<rpc_user>'
          AND query LIKE '%`signatures_local`%'
        GROUP BY initial_query_id
    ) AS s ON s.initial_query_id = i.query_id
    GROUP BY routed
    ```

- Default `false`.

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

The JSON artifact contains twenty samples per combination of unknown/complete signature membership, legacy/v0 hits or misses, and signature-only/slot-pinned requests. It also records guarded signature and payload query IDs, workflow admission, read-endpoint setup/admission, first-row time, and the subsequent wait for successful EOF. `get_tx` samples exercise the fused position-and-payload read and, for candidates without a row, its two-step fallback; `open_loop` holds ready-membership hits arriving at fixed rates. Phase queries use the application reader, transaction column projection, and cache query settings. Signature SQL mirrors the production lookup, so keep that diagnostic projection aligned when changing the lookup. `first_row_ms` includes endpoint setup/admission; `complete_ms` includes first-row time. These overlapping measurements must not be added together.

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
  - `source`: `clickhouse|head_cache|disk_cache|response_cache|none` (primary source used for the returned response; `response_cache` is an in-process `getBlock` response or `getTransaction` primary record cache hit).
  - `head_cache_read`: `true|false` (whether handler read from head cache on that request).
  - `disk_cache_read`: `true|false` (whether the handler read from the local ClickHouse disk cache on that request).
  - `outcome`: `success|not_found|invalid_params|rpc_error|backend_error|timeout|abandoned`. `abandoned` means the handler future was dropped before returning and before its own request timeout: the client disconnected, or a batch envelope timed out while the item was still running.
  - `x_endpoint`: omitted when capture is disabled; otherwise `missing|<value>` (`<value>` is the raw `X-Endpoint` header value).
  - `x_rpc_node`: omitted when capture is disabled; otherwise `missing|<value>`.
  - `x_subscription_id`: omitted when capture is disabled; otherwise `missing|<value>` (`<value>` is the raw `X-Subscription-ID` header value).
  - `x_account_id`: omitted when capture is disabled; otherwise `missing|<value>` (`<value>` is the raw `X-Account-ID` header value).

Request-scoped metric families:

- `rpc_requests`, `rpc_response_time_seconds`, `rpc_inflight_requests`, `rpc_timeouts`, `rpc_response_overhead_seconds`, `rpc_blocks_slots_returned`, `rpc_batch_requests`, `rpc_batch_items`, `rpc_batch_size`, `rpc_batch_rejected_total`, `rpc_backend_errors`, `rpc_clickhouse_duration_seconds`, `rpc_clickhouse_received_bytes`, `rpc_clickhouse_decoded_bytes`, `rpc_clickhouse_timeouts`, `rpc_clickhouse_query_cache_total`, `rpc_clickhouse_query_cache_settings_total` can include `x_endpoint`, `x_rpc_node`, `x_subscription_id`, and `x_account_id` when each capture option is enabled.
- For those labels, values are `missing|<raw-value>` for enabled capture; disabled capture omits the label.
- `rpc_requests` and `rpc_response_time_seconds` use `status="abandoned"` for requests dropped before a response existed, so cancelled requests keep their latency.
- `rpc_clickhouse_timeouts` carries `target`: `primary`, or `cache`/`background` for the local disk cache, which reuses primary operation names.
- `rpc_get_transaction_primary_slot_age_slots` (histogram): the finalized tip minus the slot of each `getTransaction` result served by primary ClickHouse. Buckets are 0.5, 1, 2, 3, 4, 5, 10 and 50 epochs (x432,000 slots), for sizing disk-cache retention. The tip is the head cache's latest finalized slot (its processed tip when no finalized slot is retained), or, without a head cache tip, the latest-slot cache's value (refreshed by other ClickHouse tip reads, not by this metric). A result is not recorded when neither tip is available or the latest-slot cache is more than 10 minutes old; `rpc_get_transaction_primary_slot_age_skipped_total{reason="no_tip|stale_tip"}` counts those.
- `rpc_response_time_seconds`, `rpc_clickhouse_duration_seconds` and `disk_cache_key_seconds` use `le` 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5, 0.75, 1, 2.5, 5, 10 s. Only these three families use that set. `clickhouse_read_disconnect_probe_seconds`, `rpc_response_overhead_seconds`, `rpc_blocks_range_seconds`, `get_block_phase_seconds`, `rpc_signature_status_admission_seconds`, `disk_cache_write_latency_seconds` and `block_index_lookup_seconds` keep the 13 `le` values without 0.15, 0.2, 0.3, 0.4 and 0.75. Histograms of counts and sizes have their own buckets.

`getBlock` response-cache metrics:

- `superbank_get_block_response_cache_access_total{operation,outcome}` with `hit`, `miss`, `insert`, and `coalesced` outcomes, plus `error_hit` and `error_insert` for remembered `-32015` results (an `error_hit` also counts as a `miss` of the success cache).
- `superbank_get_block_response_cache_entries`, `superbank_get_block_response_cache_weighted_bytes`, and `superbank_get_block_response_cache_max_bytes`.
- `superbank_get_block_phase_seconds{operation}` for hydration and serialization work.

`getTransaction` primary record cache (`GET_TRANSACTION_PRIMARY_CACHE_MAX_BYTES`, default `0` = off):

- Holds decoded transaction records that primary ClickHouse returned, keyed by signature and the optional pinned `slot`
  (pinned and unpinned requests never share an entry). It is consulted after head-cache and disk-cache misses and before
  the primary, for every accepted commitment, because the primary serves finalized history without regard to commitment.
  Encoding and the `maxSupportedTransactionVersion` check run on every hit, so responses, including the
  `UnsupportedTransactionVersion` (`-32015`) error, match a primary read of the same record.
- Not-found results are never cached (the 1 s signature-slot miss TTL is unchanged). Head-cache and disk-cache records
  are never inserted.
- Eviction is LRU (moka's default TinyLFU admission would reject first-seen keys, the common case for this cache).
- Entries expire `GET_TRANSACTION_PRIMARY_CACHE_TTL_SECS` (default 600) after insertion, not after last use. The budget is
  an estimate of each record's heap buffers plus a fixed per-entry overhead; allocator rounding and records being
  hydrated add to RSS. Records average a few tens of KB, so 512 MiB holds on the order of ten thousand records.
- A hit is labelled `source="response_cache"` on the route metrics and `response-cache` in `X-Superbank-Sources`.
- With the cache enabled, primary cache hits are not observed by
  `superbank_rpc_get_transaction_primary_slot_age_slots`; that histogram then counts unique primary fetches, not every
  request the primary tier answered.
- Follow-up: a hit still runs full hydration in `spawn_blocking` (under `HYDRATION_CPU_CONCURRENCY`) before the
  `maxSupportedTransactionVersion` check, so a client that retries a versioned transaction without that parameter gets
  `-32015` in about a millisecond and can drive many hydrations per second, where each retry used to wait for a primary
  round trip. The CPU per request is unchanged. The request rate that CPU allows is what goes up. Rejecting on
  `tx_version` before hydration is not byte-identical: hydration builds the metadata and the message first, so a
  malformed stored record answers `-32603`, where a pre-check would answer `-32015`. An exact fix has to remember each
  entry's hydration verdict after the first hydration and reuse it on later hits.
- `superbank_get_transaction_primary_cache_access_total{operation="get_transaction",outcome}` with `hit`, `miss` and
  `insert` outcomes (counted only while enabled), plus `superbank_get_transaction_primary_cache_entries`,
  `superbank_get_transaction_primary_cache_weighted_bytes` and `superbank_get_transaction_primary_cache_max_bytes`.

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


## Address latency regressions

The normal Rust suite checks bound deduplication, resolved SQL and unknown-partition probe limits.
For end-to-end coverage against a disposable loopback ClickHouse (26.1.2.11), run:

```bash
DISK_CACHE_TEST_URL=http://127.0.0.1:18195 \
cargo test -p superbank-rpc --all-features --locked address_latency_clickhouse_integration -- --ignored
DISK_CACHE_TEST_URL=http://127.0.0.1:18195 \
cargo test -p superbank-rpc --all-features --locked gsfa_handler_cursor_budget_and_missing_bounds -- --ignored
DISK_CACHE_TEST_URL=http://127.0.0.1:18195 \
cargo test -p superbank-rpc --all-features --locked gsfa_handler_races_local_page_against_primary -- --ignored
DISK_CACHE_TEST_URL=http://127.0.0.1:18195 \
cargo test -p superbank-rpc --all-features --locked address_pages_never_skip_rows_above_the_local_tip -- --ignored
cargo test -p superbank-rpc --all-features --locked gsfa_inline_cursor_matches_the_separate_lookup -- --ignored
DISK_CACHE_TEST_URL=http://127.0.0.1:18195 \
cargo test -p superbank-rpc --all-features --locked gsfa_empty_watermark_skips_the_primary_only_when_proven -- --ignored
```

These tests create uniquely named fixture databases and drop them on success. They exercise the
shared cursor/page budget, the getSignaturesForAddress local/source race, pages owing rows above a
local tip that trails the source (with and without a covering head cache), missing bounds, unknown edge partitions, exact batches spanning many
slots or one slot, historical position mismatch, eviction fallback, ordering, encodings and
unsupported versions. Failed runs can leave fixture databases for inspection. Small synthetic
fixtures establish regression behavior, not production latency or capacity.

### Bank-aware head streams

The head cache buffers `(slot, bank_id)` inside one subscription generation, including modern bank ID zero. It seals complete transactions and entry ranges, chooses the bank named by confirmed/finalized status, and removes losing-bank indexes. Each transaction retains its own bank's commitment token. Metadata comes from the same subscription and must match the sealed blockhash. Reconnect clears old data and fences callbacks from the prior generation. Legacy streams are identified by a CreatedBank notification without a bank ID; ambiguous protocol changes reconnect.

The gRPC head-cache minimum commitment applies when publishing a frozen bank, including to concurrent requests for `processed`. A frozen block is held until its commitment token reaches the session's configured minimum; the subsequent block-machine status event initializes that token before indexes are exposed. Reconnect resets the bank session and never carries node-local IDs or proofs from the previous subscription.


The head subscriber currently uses the pinned client's ordinary `subscribe_with_request` and an outer retry loop. A transport interruption drops its `CoverageSession`, clears all cached slots (including finalized slots), and starts a fresh protocol adapter, block machine and proof chain. Old returned records keep their original immutable content and bank token; replacement status cannot promote them, even if the node reuses the same bank ID. Requests use the configured ClickHouse fallback while the head rebuilds; recent data not yet in ClickHouse can temporarily be unavailable.

Until ClickHouse ingestion passes the slots the head held before the drop, usually tens of seconds on TowerBFT, clients can see more than missing data. `isBlockhashValid` at `processed` or `confirmed` returns `false` for a blockhash newer than the ClickHouse finalized tip. `getSlot`, `getBlockHeight`, `getTransactionCount` and `getLatestBlockhash` regress to the finalized tip, and `minContextSlot` callers can get -32016. Head-only `getSignaturesForAddress` cursors return an error, and `getBlocks` without `endSlot` returns -32603. Watch `superbank_head_cache_reconnects_total` and retry these calls during that window.

The client's `subscribe_with_reconnect`/`DiscardBanks` API is not integrated. Preserving cached results with that API requires handling connection generations and replacement finality/discard decisions before both the metadata tap and block machine, resetting legacy protocol evidence and invalidating range proofs at each boundary. Feeding only its replacement updates into the existing adapter would join unrelated node-local banks. The current full reset deliberately trades cache continuity for this explicit session fence; it does not provide retained finalized results or seamless replay across reconnects.
