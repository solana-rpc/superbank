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

# Epochs 700-701 (mainnet warmup schedule is applied for epochs < 14)
superbank-verify --range 700-701 --mode full

# Multi-day genesis-to-tip run with resume + JSONL findings report
superbank-verify --full --mode full \
  --checkpoint-file verify.checkpoint.json --resume \
  --report-file findings.jsonl
```

Range grammar (same as the ingestor's `--bigtable-range`): `a:b` = slots
(inclusive), `a-b` = epochs, `a` = a single epoch.

### Modes

- **structural** (default): no hash recomputation. Checks entry counts and
  index contiguity, tick counts (`(slot - parent_slot) * ticks_per_slot`,
  including ticks carried for skipped slots), the trailing-tick rule, a
  replica of Agave's `verify_tick_hash_count` (per-tick `num_hashes` windows
  against the era's `hashes_per_tick`), transaction-index tiling, the
  last-entry-hash == blockhash equality, duplicate conflicts, and blockhash
  chain linkage.
- **full**: everything above **plus** recomputing every SHA-256 hash of every
  entry, including the transaction-signature merkle mixin, and comparing
  against the recorded entry hashes and blockhash. Roughly 800k hashes per
  slot in the 12,500 hashes-per-tick era (~4M in the current 62,500 era);
  budget multiple days for a genesis-to-tip run on a large machine.

### hashes_per_tick eras

`--hashes-per-tick-schedule` defaults to the built-in mainnet history
(12,500 at genesis, then the `update_hashes_per_tick2..6` feature activations
at slots 253584001 / 255312004 / 255744008 / 257040000 / 257904000 up to
62,500; see `src/eras.rs` for provenance). Override for other clusters with
`"<from_slot>:<value>,..."`; a value of `0` disables the tick-hash-count check
for that era.

### Trust model

Full mode proves each block's PoH is internally consistent and chained from
its recorded `parent_blockhash`; chain linkage extends that to the whole
range. A fabricated-but-self-consistent segment after a coverage gap is only
detectable against an external reference: pass one or more
`--anchor <slot>:<base58-blockhash>` (e.g. from a trusted RPC's `getBlock`)
to pin recorded blockhashes, and `--expected-genesis-hash` (defaults to
mainnet's `5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d`; pass `""` to
disable) to pin slot 0. PoH commits only to transaction *signatures* — message
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

### Exit codes

| Code | Meaning |
|---|---|
| 0 | every slot ok (or only skipped) |
| 1 | operational error (bad config, ClickHouse unreachable, ...) |
| 2 | at least one verification failure |
| 3 | no failures, but unverifiable/missing slots (suppress with `--allow-unverifiable`) |

### Resume

`--checkpoint-file` saves progress after every window (atomic rename);
`--resume` continues an interrupted run as long as the job parameters (range,
mode, tables, era schedule) are identical. The findings report is appended on
resume, truncated otherwise.

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
table names default to `default.blocks_metadata`, `default.entries`,
`default.transactions`.

## Test fixtures

`--export-fixture-slot <slot> --export-fixture-out <path>` dumps one slot's
block metadata, entries, and per-transaction signatures as JSON — useful for
capturing real mainnet blocks (era boundaries, skipped-slot successors) as
golden vectors.

## Notes

- ReplacingMergeTree duplicates are deduplicated with `LIMIT 1 BY`; rows whose
  duplicates *differ* are reported as `duplicate_conflict` since no
  deterministic winner exists.
- Not yet handled: the Alpenglow migration (post-PoH tick rules on Agave
  master) — revisit the era schedule when it activates on a target cluster.
- `getEpochSchedule` in superbank-rpc ignores mainnet's warmup epochs; this
  crate carries its own epoch math (`src/epoch.rs`) instead of sharing that
  code.
