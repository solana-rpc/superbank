# superbank-verify

Validates Solana **Proof-of-History** from the data superbank stores in
ClickHouse — genesis-to-tip or any slot/epoch range. It recomputes the SHA-256
PoH chain over the `entries` table (full mode) or checks the cheap structural
invariants Agave enforces during replay (structural mode), and verifies
blockhash linkage across blocks either way.

The verifier core is a port of Agave's `solana-entry`
(`next_hash`/`hash_transactions`) and is differentially tested against that
crate.

## Data requirements

| Table | Used for | Required |
|---|---|---|
| `blocks_metadata` | blockhash / parent linkage, entry & tx counts | always |
| `entries` | per-entry `num_hashes`, hash, tx index ranges | both modes |
| `transactions` (`tx_signatures`) | entry mixin (merkle root of signatures) | full mode only |

Entries are populated by the **Fumarole**, **Yellowstone gRPC**, and
**Jetstreamer** ingest paths. Ranges ingested via JSON-RPC or Bigtable have no
`entries` rows: those slots are reported as `unverifiable`, not failures.

## Usage

```bash
# Structural sweep over everything present in blocks_metadata
superbank-verify --full

# Full PoH recomputation for a slot range
superbank-verify --range 250000000:250001000 --mode full

# Epochs 700-701 (mainnet has no warmup epochs)
superbank-verify --range 700-701 --mode full

# Devnet data: genesis hash, warmup, hashes_per_tick and Alpenglow boundary for devnet
superbank-verify --solana-cluster devnet --range 1100:1101

# Multi-day genesis-to-tip run with resume + JSONL findings report
superbank-verify --full --mode full \
  --checkpoint-file verify.checkpoint.json --resume \
  --report-file findings.jsonl
```

Range grammar (same as the ingestor's `--bigtable-range`): `a:b` = slots
(inclusive), `a-b` = epochs, `a` = a single epoch.

### Clusters

`--solana-cluster` (`mainnet`, the default, `devnet` or `testnet`) sets the defaults below. An explicit flag, env var or YAML key overrides each one.

| Cluster | Epoch warmup | Genesis hash | Alpenglow genesis slot |
|---|---|---|---|
| mainnet | false | `5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d` | 459652999 |
| devnet | false | `EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG` | 504148999 |
| testnet | true | `4uhcVJyU9pJkvQyS88uRDiswHXSCkY3zQawwpjk2NsNY` | 444625255 |

Each cluster also has its own `hashes_per_tick` history (`src/eras.rs`). The Alpenglow slot and block ID come from each cluster's `getAgGenesisCert` (`src/cluster.rs`).

### Modes

- **structural** (default): no hash recomputation. Checks entry counts and
  index contiguity, tick counts (`(slot - parent_slot) * ticks_per_slot`,
  including ticks carried for skipped slots), the trailing-tick rule, a
  replica of Agave's `verify_tick_hash_count` (per-tick `num_hashes` windows
  against the era's `hashes_per_tick`), transaction-index tiling, the
  last-entry-hash == blockhash equality, and blockhash chain linkage.
- After the Alpenglow genesis certificate slot, structural mode requires one ending Alpentick and `num_hashes = 1` for every entry. Skipped slots do not add ticks. The boundary defaults to the `--solana-cluster` value. `--alpenglow-genesis-block <slot>:<base58-block-id>` overrides it, and `none` turns it off. With `--alpenglow-rpc-url`, the trusted `getAgGenesisCert` response must match the resolved boundary. The certified genesis slot itself still uses PoH rules. The certificate's consensus block ID is not treated as an RPC blockhash anchor.
- **full**: everything above **plus** recomputing every SHA-256 hash of every
  entry, including the transaction-signature merkle mixin, and comparing
  against the recorded entry hashes and blockhash. Roughly 800k hashes per
  slot in the 12,500 hashes-per-tick era (~4M in the current 62,500 era);
  budget multiple days for a genesis-to-tip run on a large machine.
  In Alpenglow slots this checks the low power entry hash chain; it does not prove elapsed time or verify Alpenglow consensus certificates.

### Duplicate-conflict audit

The verifier reads one arbitrary row per logical key (`LIMIT 1 BY`). The audit scans the raw ClickHouse tables for logical duplicates with differing values and fails the slot with `duplicate_conflict`. It is on by default. It adds aggregation scans of the blocks and entries tables for every window, plus transactions in full mode. Turn it off with `--audit-duplicate-conflicts=false` (`SUPERBANK_VERIFY_AUDIT_DUPLICATE_CONFLICTS=false`, YAML `audit-duplicate-conflicts: false`).

### hashes_per_tick eras

`--hashes-per-tick-schedule` defaults to the `--solana-cluster` history. On mainnet that is 12,500 at genesis, then the `update_hashes_per_tick2..6` feature activations at slots 253584001 / 255312004 / 255744008 / 257040000 / 257904000 up to 62,500. See `src/eras.rs` for provenance and the devnet and testnet tables. Override with `"<from_slot>:<value>,..."`. A value of `0` disables the tick-hash-count check for that era.

### Trust model

Full mode proves each block's PoH is internally consistent and chained from
its recorded `parent_blockhash`; chain linkage extends that to the whole
range. A fabricated-but-self-consistent segment after a coverage gap is only
detectable against an external reference: pass one or more
`--anchor <slot>:<base58-blockhash>` (e.g. from a trusted RPC's `getBlock`)
to pin recorded blockhashes, and `--expected-genesis-hash` (defaults to the
`--solana-cluster` genesis hash; pass `""` to disable) to pin slot 0. PoH commits only to transaction *signatures* — message
contents are outside its proof and are not checked by this tool.

### Findings and statuses

Every slot in the range ends up `ok`, `failed`, `unverifiable`, `skipped`
(chain skipped it), or `missing` (absent from our data with no skip claim).
Findings are logged and, with `--report-file`, written as JSONL with codes
like `entry_hash_mismatch`, `blockhash_mismatch`, `chain_break`,
`tick_count_mismatch`, `tick_hash_count_mismatch`, `tx_index_mismatch`,
`duplicate_conflict`, `num_hashes_out_of_range`, `missing_entries`,
`anchor_mismatch`, `anchor_not_checked` (an anchor whose slot never met a
block is reported as unverifiable, not silently ignored).

### Library use

The crate also builds a library. `superbank_verify::verify_slot_range` runs the
same pipeline over one inclusive slot range with the cluster parameters of a
`SlotRangeCluster` (mainnet, devnet or testnet) and no checkpoint, findings file or metrics server, and returns slot counts plus
the failed, unverifiable and missing slots. It always audits conflicting
duplicate rows. superbank-solparq uses it for its
pre-archive `--poh-verify-mode` check. It needs a multi-thread Tokio runtime.

### Exit codes

| Code | Meaning |
|---|---|
| 0 | every slot ok (or only skipped) |
| 1 | operational error (bad config, ClickHouse unreachable, ...) |
| 2 | at least one verification failure |
| 3 | no failures, but unverifiable/missing slots (suppress with `--allow-unverifiable`) |

### Resume

`--checkpoint-file` saves progress after every window (atomic rename);
`--resume` continues an interrupted run as long as the job parameters (range
start, mode, tables, era schedule, genesis pin, and anchors) are identical.
A trusted Alpenglow boundary may be added once while the saved `next_start` is at most `G + 1`, before any postboundary slot was verified with historical rules. The updated descriptor is persisted immediately; removing or changing it is rejected.
For `--full`, the upper bound is a live tip: resume accepts a later tip and
continues from the saved cursor. It rejects a regressed tip or a changed range
start. Checkpoints retain already checked anchors and the genesis-pin check, so
pins behind the cursor stay part of the resumed result. The findings report is
appended on resume, truncated otherwise.

### Resource bounds

The defaults are `--window-slots 64 --fetch-ahead 1`. The verifier accepts at
most 128 slots per window, two queued fetch-ahead windows, and 512 in-flight
window slots using `window-slots * (fetch-ahead + 2)`: one active verification
window, up to `fetch-ahead` queued windows, and one fetch blocked on channel
capacity. This is a bound on complete ClickHouse result sets retained by the
pipeline, not an assertion that every slot has a fixed byte size. The entries
and transaction rows can vary with ledger density; operators needing a larger
budget need an explicit resource-policy change rather than an unbounded flag.

### Metrics

Prometheus metrics on `:9902` (`/metrics`, `/health`): slots processed by
status, findings by code, entries/hashes verified, window durations, fetch
latencies. `/health` returns 503 when no window completed within
`--health-stale-secs`.

## Configuration

CLI flags > environment variables > YAML config (`--config` /
`SUPERBANK_VERIFY_CONFIG`) > defaults. See `superbank-verify.example.yaml` at
the repo root and `superbank-verify --help` for the full reference. ClickHouse
connection uses the same `CLICKHOUSE_URL` / `CLICKHOUSE_DATABASE` /
`CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` variables as the other binaries;
table names default to `blocks_metadata`, `entries` and `transactions` in the selected database. Explicit database-qualified table names still override the database.

Logs default to `info`, which shows progress and the final summary. Set `RUST_LOG` to override the log filter.

Checkpoints include the table names. To resume a checkpoint created with the database-qualified defaults, pass `--blocks-table default.blocks_metadata --entries-table default.entries --transactions-table default.transactions`.

## Test fixtures

`--export-fixture-slot <slot> --export-fixture-out <path>` dumps one slot's
block metadata, entries, and per-transaction signatures as JSON — useful for
capturing real mainnet blocks (era boundaries, skipped-slot successors) as
golden vectors.

## Notes

- ReplacingMergeTree duplicates are deduplicated with `LIMIT 1 BY`. The default duplicate-conflict audit fails slots whose duplicates differ.
- Set `--solana-cluster` to match the stored data. A wrong cluster applies the wrong Alpenglow boundary, genesis hash and `hashes_per_tick` history.
- This crate carries its own epoch math (`src/epoch.rs`). Only testnet has warmup epochs.
- Checkpoints from runs before `--solana-cluster` recorded no Alpenglow boundary and the duplicate audit off. Such a run resumes only before the boundary, and with `--audit-duplicate-conflicts=false`. Otherwise start a fresh job.

### Trusted migration evidence

The boundary defaults to the built-in `--solana-cluster` pair. `--alpenglow-genesis-block` / `SUPERBANK_VERIFY_ALPENGLOW_GENESIS_BLOCK` / `alpenglow-genesis-block` overrides it with a certificate's `<slot>:<base58-block-id>` pair, or `none` to apply PoH rules to every slot. `--alpenglow-rpc-url` / `SUPERBANK_VERIFY_ALPENGLOW_RPC_URL` / `alpenglow-rpc-url` asks a trusted endpoint for the same cluster, and its answer must match the resolved boundary. CLI overrides env, which overrides YAML. The certificate slot is the last historical PoH slot; new entry rules apply strictly after it. An authoritative successful null means no certificate; unsupported, unavailable and malformed responses are operational errors, never null. Discovery is bounded to ten seconds and 64 KiB, rejects redirects, and does not include upstream URLs/messages in errors. Wire validation establishes shape only; no BLS or stake verification is claimed.

The resolved slot and consensus block ID are part of checkpoint identity. Legacy checkpoints without this field resume with historical rules. A `None` to trusted `Some(G, ID)` handoff is allowed only with `next_start <= G + 1`; later progress requires starting a fresh verification job. Removing or changing either component rejects resume even after the boundary is behind the cursor. The comparison handles `G = UInt64::MAX` without wrapping. The consensus block ID is distinct from the entry blockhash; use `--anchor` separately.
