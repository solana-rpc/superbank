# Agave 4.3 compatibility

The main ingestor and RPC server target Agave **v4.3.0** and Rust **1.97.1**.
The reference is the tagged [Agave source](https://github.com/anza-xyz/agave/tree/v4.3.0),
not whichever version happens to serve a public endpoint. This is compatibility
for Superbank's existing methods, not an implementation of every validator RPC.

## Behavior changes

| Contract | Superbank behavior |
| --- | --- |
| `getInflationReward` address limit | Retains `GET_INFLATION_REWARD_MAX_ADDRESSES` (default 100; zero disables it). Set 32 for Agave's limit. Over-limit requests return HTTP 200, code `-32602`, message `Too many inputs provided; max N`, and no error data, before pubkey validation or backend access. Duplicate inputs count toward the limit. |
| Transaction encoding | `getBlock` and `getTransaction` reject `base58`/`binary` with `maxSupportedTransactionVersion >= 1` before any cache/storage lookup, even for legacy data or metadata-only blocks. Error: `-32602`, `base58 encoding is not supported with maxSupportedTransactionVersion >= 1`. `getTransactionsForAddress` applies this rule to its supported `base58` encoding; it does not add a `binary` alias. |
| Confidential-token `jsonParsed` | Uses Agave 4.3's corrected proof-account and signer mapping, including the `proofContextStateAccount` name for confidential supply-key rotation. Consumers must accommodate corrected JSON keys. |
| Bigtable v1 | The 4.3 storage-proto decoder preserves v1, empty/nonempty inline configuration, lifetime specifiers, and signed bytes. Previously ingested rows are not automatically repaired. |
| VAT rewards | Preserves negative lamports, balances, and `VATDebit`. Readers accept `VATDebit` and `validator-admission-ticket-debit`; JSON emits `VATDebit` and gRPC emits reward enum value 6. Inflation-reward queries continue to select only staking/voting rewards. |

The existing String reward-type columns and Int64 reward amounts accommodate VAT
without a DDL migration. Old commission rows continue to omit unavailable
`commissionBps`; no basis-point value is inferred from a percentage.

## Ingestion and streaming boundaries

JSON-RPC and Bigtable ingestion use Agave 4.3 types. Yellowstone gRPC ingestion,
Fumarole block assembly, and the head cache explicitly preserve raw reward value
6 because the published Yellowstone protobuf enum does not yet name it. Prost
retains this i32 value when decoding and re-encoding. The `grpc-streaming` feature
uses Agave's `solana-storage-proto = 4.3.0`
generated output types, including `VATDebit = 6`, without changing existing field
numbers or values. Only the Superbank service envelope is compiled locally.

Synthetic wire fixtures exercise these conversions. They do **not** certify a
deployed Yellowstone/Fumarole producer: record its exact version and replay
captured payloads before qualifying a production rollout. Never infer producer
correctness or cluster feature activation from the Agave version alone.

The standalone Old Faithful workspace remains on its historical Agave 3
dependency graph. The Jetstreamer ClickHouse plugin uses the separate upstream
Agave 4 workspace, but its block callback does not expose bank ID or footer data;
postmigration backfills require an independently qualified source. `getHealth` retains its documented
ClickHouse availability semantics rather than measuring cluster-tip distance.

## Validation

Run serially with the pinned toolchain:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo clippy -p superbank-rpc --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
cargo test -p superbank-rpc --all-features --locked
python3 scripts/test/check-rust-complexity.py --base <implementation-base-commit>
```

The complexity gate requires `rust-code-analysis-cli 0.0.25`: new functions must
have McCabe complexity <=10 and changed existing functions must not increase it.
Run the basic k6 scenario and affected method comparisons described in
[`tests/k6/README.md`](../tests/k6/README.md) against an isolated Superbank fixture
and a version-verified Agave 4.3 reference. Record missing external coverage;
passing unit tests does not establish live parity.

## Local acceptance (2026-09-18)

Implemented on `feat/agave-4.3` after pulling main to `382acdd`. The ten commits
separate request errors, encoding validation, dependency/VAT support, Bigtable
fixtures, confidential parsers, cached encoding matrices, disk reward preservation,
archive preservation, inflation exclusion, and acceptance tooling/documentation.

| Planned acceptance | Evidence |
| --- | --- |
| gIR configuration/error contract | Limits 32/100, boundaries, duplicate counting, disabled limit, exact HTTP/code/message/data assertions; configuration remains unchanged. |
| Encoding across supported cache paths | Head and real disk cache fixtures cover legacy, v0, populated v1 and empty-config v1; all five encodings, omitted/0/1/255 maxima and all block projections. Successful reads use an unavailable source endpoint. Missing-data rejection remains covered separately. |
| Bigtable v1 preservation | Production protobuf conversion preserves version, configuration, lifetime specifier and exact signed bytes. |
| Confidential-token parsing | Fixtures exercise all four affected parser families, mixed proof sources, multisig signers and renamed proof fields. |
| VAT wire/hydration | Upstream generated metadata bytes, all reward enum values, fixed wire tags, canonical JSON, signed lamports, commissions/basis points and partition counts. |
| Disk-cache preservation | Production Native fill and cache reopen preserve raw reward arrays and canonical transaction/block responses for both stored VAT spellings. |
| Archive preservation | Production Solparq local-bundle export and ingestor restore preserve raw columns and hydrated blocks, including historical absent basis points. |
| Inflation exclusion | Real query and RPC handler retain staking/voting rewards when the same address also has VAT; a VAT-only address returns null. |

Workspace tests passed (718 passed, 4 ignored); all-feature RPC tests passed
(612 passed, 11 ignored). Separately executed and passed the three new ClickHouse
integrations, existing key-routing integration, and isolated final-HTTP logging
test. The other ignored diagnostics are not claimed as executed.

Both Clippy configurations, formatting, the streaming-only build and the
complexity gate passed (75 changed Rust functions). Basic batch k6 passed 7,765
checks over 1,553 requests; the request-error scenario passed all 96 checks over
12 requests. These used disposable ClickHouse **26.1.2.11**. ClickHouse 25.6 could
not create the existing reverse-key disk-cache schema and is unsuitable for these
integration tests. The local throughput figures are smoke-test results, not
capacity measurements or live Agave endpoint comparisons.

Reproduce the integration checks against a disposable loopback server:

```sh
DISK_CACHE_TEST_URL=http://127.0.0.1:18196 cargo test -p superbank-rpc --all-features --locked disk_cache::key_tests::agave43 -- --ignored --test-threads=1
DISK_CACHE_TEST_URL=http://127.0.0.1:18196 cargo test -p superbank-rpc --all-features --locked key_routing_clickhouse_integration -- --ignored --test-threads=1
cargo build -p superbank -p superbank-solparq -p superbank-rpc --all-features --locked
DISK_CACHE_TEST_URL=http://127.0.0.1:18196 python3 scripts/test/agave43-archive-roundtrip.py
```

The archive script creates uniquely named databases and removes them in its cleanup
path. Logs and bundles remain in a printed temporary artifact directory. It uses
a deterministic local produced-slot reference to check archive completeness;
this does not qualify a deployed producer.

The normal k6 runner includes the request-contract scenario. Its optional
`AGAVE43_REFERENCE_RPC_URL` comparison refuses references outside reported
`solana-core` 4.3.x. Harness checks verify rejection of wrong reference versions,
wrong error codes, malformed JSON and invalid limit configuration. CI also checks
the standalone `grpc-streaming` feature build.

## Deployment and historical audit

1. Deploy VAT-capable RPC readers before enabling writers that store VAT rows.
   Keep compatible readers running if rolling writers back: old readers cannot
   hydrate the new reward type.
2. Canary ingestion and serving, comparing response bodies, decode failures,
   ingestion lag, and existing latency metrics with the prior baseline.
3. Identify slot ranges actually ingested through Bigtable with storage-proto
   4.2.1. Compare authoritative v1 transactions in those ranges with stored
   versions, configuration, lifetime specifiers, and reconstructed signed bytes.
   A stored v0 tag alone cannot distinguish a real v0 transaction from a v1
   downgrade. Missing inline configuration cannot be recovered from the row.
4. Record affected slots/signatures and source provenance in a read-only audit
   report. Schedule any reingestion separately; do not automatically overwrite
   historical data or infer that a dependency upgrade repairs existing rows.

Live producer qualification, production canaries, and historical audits require
the target endpoints and ingestion provenance. They are release gates, not
claims established by the local source change.
