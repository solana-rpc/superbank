# Agave 4.4 compatibility

The main workspace uses Agave **4.4.0** and Rust **1.98.1**. The behavior reference is the tagged [Agave v4.4.0 source](https://github.com/anza-xyz/agave/tree/v4.4.0). Compatibility covers Superbank's existing ingestion, storage, archive, verification, and RPC interfaces. It does not add validator execution, account subscriptions, or leader schedules. The [4.3 contracts and rollout requirements](agave-4.3-compatibility.md) also apply.

## Request context

`getTransaction` accepts `minContextSlot` in its config object. It checks the current context at the requested commitment before consulting transaction caches or storage. A context below the minimum returns JSON-RPC `-32016`, with message `Minimum context slot has not been reached` and `data.contextSlot`. This is a freshness requirement on the server, not a lower bound on the transaction's slot. Old transactions remain available when the context satisfies the minimum. The optional `slot` filter and head-cache `processed` extension retain their existing behavior.

`getSignatureStatuses` accepts `commitment` and `minContextSlot` alongside `searchTransactionHistory`. Commitment defaults to `processed`. The selected commitment controls the head-cache lookup and response context; confirmed or finalized requests do not expose processed-only transactions. The minimum-context error has the same shape as `getTransaction`. Without a usable head tip, the context falls back to finalized ClickHouse data. This fallback does not claim knowledge of a validator's processed tip. The finalized disk cache remains part of the recent-status lookup, while `searchTransactionHistory: true` enables primary ClickHouse history queries.

A cached transaction or absence cannot bypass a request's context check. Context is resolved per request and is not stored as part of a reusable transaction response.

## Data compatibility

Transaction v1 signed bytes and inline configuration, nullable CPI stack heights, commission basis points, negative VAT rewards, `DeactivatedStake` rewards, and Alpenglow footer fields use the existing schemas. No 4.4-specific ClickHouse migration or Parquet manifest bump is required. Apply all existing schema migrations before starting a reader or writer.

Superbank preserves execution results for deeper CPI calls, sBPF deployment restrictions, loader changes, and syscalls. It does not execute these rules itself. Reward ingestion preserves the amounts supplied by the source; the RPC server does not calculate a validator's inflation rate or maintain its stake cache. Parsed instructions use the pinned Agave transaction-status encoder.

## Producer qualification

Qualify the exact Yellowstone and Fumarole producer builds separately from the Superbank Cargo graph. Replay captured transactions, bank-status events, replacement events, and footers before rollout. Include reconnects and competing banks. Agave's Geyser plugin interface version does not establish Yellowstone wire compatibility on its own.

The standalone Jetstreamer plugin requires an evidenced genesis or preactivation bound. Its upstream block callback does not expose the bank/footer data needed for post-Alpenglow backfills, so the plugin rejects those ranges. Use a qualified source for them. Updating the main workspace does not remove this restriction.

Feature activation is cluster-specific. Use the [Agave feature tracker](https://github.com/anza-xyz/agave/wiki/Feature-Gate-Tracker-Schedule) and same-cluster evidence; do not infer activation from a release number or date. Preserve the certificate trust boundary and footer-gap handling described in the 4.3 rollout guide.

## Validation

Run the workspace and all-feature RPC checks listed in the repository README, the standalone `grpc-streaming` check from CI, and the site tests. Regression coverage must include context boundaries, all cache tiers, bank replacement, large v1 messages, deep CPI metadata, reward fields, and archive restoration. The local archive regression is `scripts/test/agave43-archive-roundtrip.py`; it exercises rewards, 4096-byte v1 transactions, and deep CPI metadata through the shared storage contract with the current binaries.

Run the basic k6 transaction scenario and `tests/k6/scenarios/validation/superbank-rpc-validate-agave44.js`. Set `RPC_URL` to a target with a populated context. Set `REFERENCE_RPC_URL` to compare the affected methods with an endpoint that reports Agave 4.4.0. Captured producer tests and live reference comparisons are separate rollout gates; synthetic unit tests do not qualify a deployed producer. Monitor ingestion errors, context lag, RPC errors, footer completeness, and cache fallback during a canary.

The disk-cache integration fixtures require a disposable ClickHouse server with query logging enabled (`system.query_log`). Use a larger test-thread stack for the large debug fixture:

```sh
RUST_MIN_STACK=16777216 DISK_CACHE_TEST_URL=http://127.0.0.1:18123 cargo test -p superbank-rpc --all-features --locked key_routing_clickhouse_integration -- --ignored --test-threads=1
RUST_MIN_STACK=16777216 DISK_CACHE_TEST_URL=http://127.0.0.1:18123 cargo test -p superbank-rpc --all-features --locked disk_cache::key_tests::agave43 -- --ignored --test-threads=1
```
