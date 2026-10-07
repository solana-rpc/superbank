# Agave 4.3 compatibility

The main ingestor and RPC server target Agave **v4.3.0** and Rust **1.97.1**. The reference is the tagged [Agave source](https://github.com/anza-xyz/agave/tree/v4.3.0), not whichever version happens to serve a public endpoint. This is compatibility for Superbank's existing methods, not an implementation of every validator RPC.

## Behavior changes

| Contract | Superbank behavior |
| --- | --- |
| `getInflationReward` address limit | Retains `GET_INFLATION_REWARD_MAX_ADDRESSES` (default 100; zero disables it). Set 32 for Agave's limit. Over-limit requests return HTTP 200, code `-32602`, message `Too many inputs provided; max N`, and no error data, before pubkey validation or backend access. Duplicate inputs count toward the limit. |
| Transaction encoding | `getBlock` and `getTransaction` reject `base58`/`binary` with `maxSupportedTransactionVersion >= 1` before any cache/storage lookup, even for legacy data or metadata-only blocks. Error: `-32602`, `base58 encoding is not supported with maxSupportedTransactionVersion >= 1`. `getTransactionsForAddress` applies this rule to its supported `base58` encoding; it does not add a `binary` alias. |
| `getBlock` footer | Accepts the SIMD-0307 boolean `footer` option. The default is `false`, a deliberate deviation from the SIMD default, so responses keep the previous shape unless `footer: true` is set. Apply the `blocks_metadata.sql` footer columns before upgrading the RPC, because every `getBlock` metadata read selects them. |
| Confidential-token `jsonParsed` | Uses Agave 4.3's corrected proof-account and signer mapping, including the `proofContextStateAccount` name for confidential supply-key rotation. Consumers must accommodate corrected JSON keys. |
| Bigtable v1 | The 4.3 storage-proto decoder preserves v1, empty/nonempty inline configuration, lifetime specifiers, and signed bytes. Previously ingested rows are not automatically repaired. |
| VAT rewards | Preserves negative lamports, balances, and `VATDebit`. Readers accept `VATDebit` and `validator-admission-ticket-debit`; JSON emits `VATDebit` and gRPC emits reward enum value 6. Inflation-reward queries continue to select only staking/voting rewards. |

The existing String reward-type columns and Int64 reward amounts accommodate VAT without a DDL migration. Old commission rows continue to omit unavailable `commissionBps`; no basis-point value is inferred from a percentage.

## Ingestion and streaming boundaries

JSON-RPC and Bigtable ingestion use Agave 4.3 types. Yellowstone gRPC ingestion, Fumarole block assembly, and the head cache preserve VAT reward value 6 with the stable Yellowstone 13 schema (upgraded from 13.0.0-rc4). Prost retains this i32 value when decoding and re-encoding. The `grpc-streaming` feature uses Agave's `solana-storage-proto = 4.3.0` generated output types, including `VATDebit = 6`, without changing existing field numbers or values. Only the Superbank service envelope is compiled locally.

Synthetic wire fixtures exercise these conversions. They do **not** certify a deployed Yellowstone/Fumarole producer: record its exact version and replay captured payloads before qualifying a production rollout. Never infer producer correctness or cluster feature activation from the Agave version alone.

The standalone Old Faithful workspace remains on its historical Agave 3 dependency graph. The Jetstreamer ClickHouse plugin uses the separate upstream Agave 4 workspace, but its block callback does not expose bank ID or footer data; postmigration backfills require an independently qualified source. `getHealth` retains its documented ClickHouse availability semantics rather than measuring cluster-tip distance.

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

The complexity gate requires `rust-code-analysis-cli 0.0.25`: new functions must have McCabe complexity <=10 and changed existing functions must not increase it. Run the basic k6 scenario and affected method comparisons described in [`tests/k6/README.md`](../tests/k6/README.md) against an isolated Superbank fixture and a version-verified Agave 4.3 reference. Record missing external coverage; passing unit tests does not establish live parity.

## Local acceptance

The checks below cover the Agave 4.3 contract. They need a disposable ClickHouse **26.1.2.11**. ClickHouse 25.6 cannot create the existing reverse-key disk-cache schema. Local throughput from these runs is a smoke test, not a capacity measure or a live Agave comparison.

| Area | Covered by |
| --- | --- |
| gIR configuration/error contract | Limits 32/100, boundaries, duplicate counting, disabled limit, exact HTTP/code/message/data assertions; configuration remains unchanged. |
| Encoding across supported cache paths | Head and real disk cache fixtures cover legacy, v0, populated v1 and empty-config v1; all five encodings, omitted/0/1/255 maxima and all block projections. Successful reads use an unavailable source endpoint. Missing-data rejection remains covered separately. |
| Bigtable v1 preservation | Production protobuf conversion preserves version, configuration, lifetime specifier and exact signed bytes. |
| Confidential-token parsing | Fixtures exercise all four affected parser families, mixed proof sources, multisig signers and renamed proof fields. |
| VAT wire/hydration | Upstream generated metadata bytes, all reward enum values, fixed wire tags, canonical JSON, signed lamports, commissions/basis points and partition counts. |
| Disk-cache preservation | Production Native fill and cache reopen preserve raw reward arrays and canonical transaction/block responses for both stored VAT spellings. |
| Archive preservation | Production Solparq local-bundle export and ingestor restore preserve raw columns and hydrated blocks, including historical absent basis points. |
| Inflation exclusion | Real query and RPC handler retain staking/voting rewards when the same address also has VAT; a VAT-only address returns null. |

Reproduce the integration checks against a disposable loopback server:

```sh
DISK_CACHE_TEST_URL=http://127.0.0.1:18196 cargo test -p superbank-rpc --all-features --locked disk_cache::key_tests::agave43 -- --ignored --test-threads=1
DISK_CACHE_TEST_URL=http://127.0.0.1:18196 cargo test -p superbank-rpc --all-features --locked key_routing_clickhouse_integration -- --ignored --test-threads=1
cargo build -p superbank -p superbank-solparq -p superbank-rpc --all-features --locked
DISK_CACHE_TEST_URL=http://127.0.0.1:18196 python3 scripts/test/agave43-archive-roundtrip.py
```

The archive script creates uniquely named databases and removes them in its cleanup path. Logs and bundles remain in a printed temporary artifact directory. It uses a deterministic local produced-slot reference to check archive completeness; this does not qualify a deployed producer.

The normal k6 runner includes the request-contract scenario. Its optional `AGAVE43_REFERENCE_RPC_URL` comparison refuses references outside reported `solana-core` 4.3.x. Harness checks verify rejection of wrong reference versions, wrong error codes, malformed JSON and invalid limit configuration. CI also checks the standalone `grpc-streaming` feature build.

## Deployment and historical audit

1. Deploy VAT-capable RPC readers before enabling writers that store VAT rows. Keep compatible readers running if rolling writers back: old readers cannot hydrate the new reward type.
2. Canary ingestion and serving, comparing response bodies, decode failures, ingestion lag, and existing latency metrics with the prior baseline.
3. Identify slot ranges actually ingested through Bigtable with storage-proto 4.2.1. Compare authoritative v1 transactions in those ranges with stored versions, configuration, lifetime specifiers, and reconstructed signed bytes. A stored v0 tag alone cannot distinguish a real v0 transaction from a v1 downgrade. Missing inline configuration cannot be recovered from the row.
4. Record affected slots/signatures and source provenance in a read-only audit report. Schedule any reingestion separately; do not automatically overwrite historical data or infer that a dependency upgrade repairs existing rows.

Live producer qualification, production canaries, and historical audits require the target endpoints and ingestion provenance. They are release gates, not claims established by the local source change.

## Alpenglow operational rollout

Treat certificate discovery, source qualification and storage rollout as separate gates. Record evidence for the cluster holding the ledger, rather than relying on the current state of a public network or the producer's binary version.

1. Configure a trusted same-cluster Agave endpoint for `getAgGenesisCert`. The RPC server uses `AG_GENESIS_CERT_RPC_URL`; the verifier uses `--alpenglow-rpc-url`, or an offline `--alpenglow-genesis-block <G>:<block-ID>`. An authoritative successful null is preactivation evidence; unsupported, missing, malformed and unavailable responses are errors. The certificate's G is the **last historical PoH slot**; Alpenglow entry rules apply strictly after G. Its consensus block ID is distinct from an entry blockhash or verifier `--anchor`.
2. Legacy Fumarole and Jetstreamer require a finite historical bound. Use the certificate slot, or before activation record finalized `getSlot` **before** a subsequent authoritative null from the same trusted endpoint. Retain both responses, then explicitly attest that finalized slot (or earlier) through `FUMAROLE_PREACTIVATION_THROUGH_SLOT` or `JETSTREAMER_PREACTIVATION_THROUGH_SLOT`. These are offline operator attestations, mutually exclusive with the genesis bound; missing evidence cannot select this mode. The bound never advances automatically. Independently qualify Jetstreamer block commissions using the same cluster's SIMD-0291 activation/source era; Alpenglow G does not determine whether a reward's original units were percent or basis points.
3. Apply the matching local, cluster or replicated DDL **before** upgraded readers or writers. Reapply `blocks_metadata.sql` even when `bank_id` already exists: it adds `bank_hash`, `block_producer_time_nanos` and `block_user_agent`, and its explicit `DEFAULT NULL` repair lets older writers, including RPC and Bigtable, omit every new column. Apply transaction reward/config columns and entries for the sources that need them.
4. Canary complete **finalized canonical** ingestion. gRPC joins block data, bank status and processed footers on one subscription. Fumarole assembles sealed `(slot, blockhash)` banks; it cannot supply postmigration footer qualification. RPC/Bigtable backfills supply canonical block data but no entry/footer stream. Validate exact transaction/entry completeness before writing. A scalar zero gRPC bank ID needs matching optional-ID status evidence; unresolved identity holds later data and every metadata flush for replay. Never move those proofs across reconnects. At a Fumarole cutoff, valid prior rows flush without the client's all-offset acknowledgment, so a restart may replay the valid prefix.
5. Monitor footer coverage separately from canonical progress. An identified gRPC block waits up to two seconds for its footer, then the row is written with `NULL` footer columns and `superbank_ingest_source_errors_total_total{stage="grpc_footer", kind="missing"}` counts it. The `missing` kind starts only after the first footer arrives. These signals do not disable canonical validation or bypass an unresolved identity hold. First-shred turbine telemetry cannot advance that window. A restart replays the latest durable slot without its footer, and the gRPC and Fumarole sources copy the stored footer fields into the replayed row. Other backfills over an existing range overwrite them with `NULL`. Record gaps without fabricating a hash or joining stored node-local IDs. Independently audit footer coverage before archiving, because a complete block archive alone does not prove it.
6. Qualify speculative serving separately. The head cache scopes bank counters and commitment tokens to a subscription/session, retains competing banks until a winner is proven, discards dead/skipped branches, and clears proofs on reconnect. Frozen content cannot publish below the configured minimum, even to concurrent `processed` readers. A signature retried on a winning slot must acquire that slot's projection and token after its abandoned branch is removed. Stored finalized data remains slot-keyed and is never treated as a session ID registry. The head subscriber uses ordinary `subscribe_with_request` plus its outer retry loop, not `subscribe_with_reconnect`/`DiscardBanks`. Interruption clears all cached slots, including finalized content, and rebuilds the protocol, block machine and proof chain. Old returned records retain their old bank token and cannot be promoted by reused IDs. Plan for ClickHouse fallback and a temporary loss of head-only data during recovery. Until ClickHouse ingestion catches up, `isBlockhashValid` at `processed`/`confirmed` can return `false` for a recent blockhash, the latest-slot methods regress to the finalized tip, and head-only paging and `getBlocks` without `endSlot` can error. Retaining finalized cache content would require generation-aware replacement/discard handling before the metadata and block-machine taps; that continuity is not implemented.
7. Configure the verifier boundary before postmigration work. Resume can add a trusted `None -> Some(G, ID)` boundary only when `next_start <= G + 1`, before postboundary slots were verified with historical rules; it persists that descriptor. Removal, changed boundaries, anchors or other job settings reject resume. A moving `--full` tip can only advance. Rollback leaves additive DDL and qualified readers in place, pauses sources that cannot safely represent the active era, and retains recorded evidence/checkpoints. Do not remove a trusted verifier boundary or deploy historical-only writers beyond their bound.

See [ingestor integrity and footer policy](../crates/superbank/README.md#live-stream-integrity), [Jetstreamer runner/commission qualification](../ingest/jetstreamer-clickhouse-plugin/README.md), and [Solparq footer inspection](../crates/superbank-solparq/README.md#read-archives). Local wire, ClickHouse and archive fixtures qualify implementation behavior; production producer captures, canaries, replay retention and target-cluster evidence remain operator release gates.
