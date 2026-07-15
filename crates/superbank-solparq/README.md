<div align="center">

<h1>
  <img width="660" alt="Superbank" src="../../docs/assets/solparq.png" />
  <br/>Superbank - Solparq
</h1>

Archive Solana ledger data to Parquet bundles.

</div>

# superbank-solparq

`superbank-solparq` archives Superbank/Solana ClickHouse tables to Parquet bundles.
It can run once or continuously in server mode.

Archive bundles use:

```text
type_epoch_from-slot_to-slot/
  manifest.json
  report.json
  SHA256SUMS.txt
  .done.<hostname>.txt
  transactions.parquet
  blocks_metadata.parquet
  entries.parquet
  gsfa.parquet
  gsfa_hot.parquet
  signatures.parquet
  token_owner_activity.parquet
```

Example:

```text
hourly_988_427236024-427245023/
```

`transactions` and `blocks_metadata` are required. Other Superbank tables are
included when they exist on the ClickHouse server and are recorded as skipped in
`manifest.json` when absent. This lets RPC/Bigtable deployments archive cleanly
without PoH `entries`, while Fumarole/gRPC/Jetstreamer deployments preserve
entries for later PoH-specific tooling.

`SHA256SUMS.txt` contains one SHA-256 checksum for each `.parquet` file in the
bundle. `report.json` is a machine-readable run report: it carries a
`report_version` and the same `producer` build-identity block as the manifest
(binary name, version, git SHA), the run timestamp in Unix and UTC forms, the
node `$HOSTNAME`, and the full run outcome — planned/created archive details,
validation results, and `run_metrics` (per-phase durations, per-table row
counts, mismatch-repair and gap-backfill outcomes, disk/table sizes). A
`.done.<hostname>.txt` marker is written after the archive data
and manifest exist; if a later run sees an archive with any `.done*` marker, it
treats that archive as already successful and still performs any safe
ClickHouse cleanup that is due. If the target archive bundle already exists
without a `.done*` marker, the incomplete bundle is removed before the archive
is recreated.

`manifest.json` carries a `format_version` (bump on schema changes) and a
`producer` block recording the build identity of the `superbank-solparq`
binary that wrote the archive:

```json
{
  "format_version": 2,
  "producer": {
    "name": "superbank-solparq",
    "version": "0.5.1-rc1-sp1",
    "git_sha": "abcdef1234567890"
  }
}
```

`git_sha` comes from `SUPERBANK_GIT_SHA` (or `GITHUB_SHA` in CI) at build time,
the same convention used by `superbank` and `superbank-rpc`; it is `"unknown"`
for local builds without that env var set. Readers ignore unrecognized
manifest fields, so older `superbank-solparq-read` builds can still read
newer bundles, and `producer`/`format_version` are `null`/absent when reading
bundles written before this field existed (`format_version` 1). `superbank-solparq-read summary`
prints both fields.

`report.json` shares the `producer` build-identity block and adds the run
outcome. Its top level is flattened, so run fields sit alongside the header:

```json
{
  "report_version": 1,
  "producer": {
    "name": "superbank-solparq",
    "version": "0.5.1-rc1-sp1",
    "git_sha": "abcdef1234567890"
  },
  "hostname": "archiver-01",
  "timestamp_unix": 1700000000,
  "timestamp_utc": "2023-11-14 22:13:20 UTC",
  "archive_created": true,
  "archive_name": "hourly_988_427236024-427245023.parquet",
  "archive_kind": { "type": "hourly" },
  "archive_slot_start": 427236024,
  "archive_slot_end": 427245023,
  "validation": { "missing_blocks": [], "transaction_mismatches": [] },
  "run_metrics": {
    "phase_durations": [{ "phase": "validate", "seconds": 1.2 }],
    "archived_table_rows": [{ "table": "transactions", "rows": 123456 }],
    "mismatch_repair": null,
    "gap_backfill": { "slots_targeted": 3, "missing_blocks_after": 0, "succeeded": true }
  }
}
```

(Fields that are `None`/empty for a given run — e.g. `mismatch_repair`,
`gap_backfill`, `db_bounds` — are `null` or omitted.)

## Build

From the repository root:

```bash
cargo build -p superbank-solparq --release
```

Check the packaged version with:

```bash
cargo run -p superbank-solparq -- --version
```

The companion archive reader is built separately:

```bash
cargo build -p superbank-solparq --bin superbank-solparq-read --release
```

## Release

`superbank-solparq` ships through the same release workflow as the main Superbank
binaries. Releases are triggered by annotated `vX.Y.Z` tags and use
`.goreleaser.yaml`.

Before tagging, update workspace member versions in `Cargo.toml` files,
including `crates/superbank-solparq/Cargo.toml` (which builds both the
`superbank-solparq` and `superbank-solparq-read` binaries), commit release
changes, and push the branch:

```bash
git add Cargo.toml crates/superbank-solparq/Cargo.toml .goreleaser.yaml .github/workflows/release.yml
git commit -m "Release v0.3.0"
git push origin <branch-name>
```

Then create and push the annotated release tag:

```bash
git tag -a v0.3.0 -m "Release v0.3.0"
git push origin v0.3.0
```

The tag must point at a commit that already contains release workflow and
GoReleaser changes; Git tags do not include uncommitted working-tree changes.

The workflow builds Linux binaries for:

- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`

and publishes GitHub Release assets similar to:

```text
superbank-solparq-v0.3.0-linux-amd64.tar.gz
superbank-solparq-v0.3.0-linux-arm64.tar.gz
superbank-solparq-read-v0.3.0-linux-amd64.tar.gz
superbank-solparq-read-v0.3.0-linux-arm64.tar.gz
SHA256SUMS.txt
```

To verify the release packaging locally without publishing:

```bash
goreleaser check --config .goreleaser.yaml
goreleaser release --snapshot --clean --config .goreleaser.yaml
```

## One-Shot Local Archive

```bash
cargo run -p superbank-solparq -- \
  --db-server 192.168.0.184 \
  --db-user admin \
  --db-password 'change-me' \
  --archive-range-type hourly \
  --archive-file-output-location ./archives
```

To archive an exact inclusive slot range in one-shot mode, pass
`--archive-slot-range START-END`:

```bash
cargo run -p superbank-solparq -- \
  --db-server 192.168.0.184 \
  --db-user admin \
  --db-password 'change-me' \
  --archive-range-type custom \
  --archive-slot-range 1000-3222 \
  --archive-file-output-location ./archives
```

The configured `--archive-range-type` still controls the archive kind/name and
destination grouping, but the requested slot range is archived exactly. The
range must already be fully available in ClickHouse. `--archive-slot-range` is
only supported for one-shot archives and is rejected with `--server-mode`.

By default this creates a local bundle directory. Each available table streams
with a table-specific stable order, for example:

```sql
SELECT *
FROM transactions
WHERE slot BETWEEN <start> AND <end>
ORDER BY slot, slot_idx, signature
FORMAT Parquet
```

Local bundles are written to a staging directory and moved into place after the
table Parquet files, `SHA256SUMS.txt`, and `manifest.json` are written.

## One-Shot S3 Archive

S3 mode asks ClickHouse to write the archive with `INSERT INTO FUNCTION s3(...)`.
This works with S3-compatible endpoints, including Backblaze-style custom
endpoints.

```bash
cargo run -p superbank-solparq -- \
  --db-server 192.168.0.184 \
  --db-user admin \
  --db-password 'change-me' \
  --archive-range-type hourly \
  --archive-location-type s3 \
  --archive-s3-endpoint https://s3.us-west.example \
  --archive-s3-bucket-name my-bucket \
  --archive-s3-bucket-path archives \
  --archive-s3-auth-key "$S3_ACCESS_KEY" \
  --archive-s3-auth-secret-key "$S3_SECRET_KEY"
```

The object paths are:

```text
s3://<bucket>/<bucket-path>/<archive-type>/<archive-id>/<table>.parquet
s3://<bucket>/<bucket-path>/<archive-type>/<archive-id>/SHA256SUMS.txt
s3://<bucket>/<bucket-path>/<archive-type>/<archive-id>/manifest.json
s3://<bucket>/<bucket-path>/<archive-type>/<archive-id>/.done.<hostname>.txt
```

`SHA256SUMS.txt` is only written for S3 archives when
`--archive-s3-write-checksums` (env `SOLPARQ_ARCHIVE_S3_WRITE_CHECKSUMS`) is set.
It is off by default: ClickHouse writes the Parquet objects straight to S3, so
checksumming them requires downloading every object back, which is
API-rate-limit intensive on large archives. Local archives always write
`SHA256SUMS.txt` (they hash files already on disk).

`manifest.json` is written after the table objects and checksum file. The final
success marker is `.done.<hostname>.txt`.

## Read Archives

Use `superbank-solparq-read` to inspect local or S3 archives without connecting to
ClickHouse.

```bash
cargo run -p superbank-solparq --bin superbank-solparq-read -- list \
  --archive-dir ./crates/superbank-solparq/archives

cargo run -p superbank-solparq --bin superbank-solparq-read -- summary \
  --archive ./crates/superbank-solparq/archives/hourly_989_427299625-427308624

cargo run -p superbank-solparq --bin superbank-solparq-read -- scan \
  --archive ./crates/superbank-solparq/archives/hourly_989_427299625-427308624 \
  --slot-range 427300000-427300500 \
  --columns slot,signature \
  --format jsonl
```

Use `--table` to read a non-transaction table from a bundle, for example
`--table blocks_metadata` or `--table entries`.

S3 reads use the same endpoint, bucket, path, and credentials model as
`superbank-solparq`. In S3 mode, `--archive` is the object key relative to
`--archive-s3-bucket-path`:

```bash
cargo run -p superbank-solparq --bin superbank-solparq-read -- summary \
  --archive-location-type s3 \
  --archive-s3-endpoint https://s3.eu-central-003.backblazeb2.com \
  --archive-s3-bucket-name solparq \
  --archive-s3-bucket-path archives/test \
  --archive-s3-auth-key "$S3_ACCESS_KEY" \
  --archive-s3-auth-secret-key "$S3_SECRET_KEY" \
  --archive hourly/hourly_989_427299625-427308624
```

## Server Mode

Server mode runs continuously and can process multiple archive range types.

```bash
cargo run -p superbank-solparq -- \
  --db-server 192.168.0.184 \
  --db-user admin \
  --db-password 'change-me' \
  --archive-range-type hourly \
  --archive-range-type epoch \
  --server-mode \
  --archive-file-output-location ./archives
```

Defaults:

- Ops and health dashboard: `http://0.0.0.0:30303/`
- Health JSON: `http://0.0.0.0:30303/health`
- Status JSON: `http://0.0.0.0:30303/status`
- Prometheus metrics: `http://0.0.0.0:31313/metrics`
- Archive check interval: `60` seconds

### Prometheus metrics

The metrics endpoint (default `SOLPARQ_METRICS_PORT`, `31313`) exposes
OpenMetrics-formatted series under the `solparq_` prefix, plus standard
`process_*` collectors. Per-archive series carry an `archive_kind` label
(`hourly`, `epoch`, or `custom`). These are the building blocks for an
operational Grafana dashboard.

Liveness and configuration:

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `solparq_build_info` | gauge | `name`, `version`, `git_sha` | Constant `1`; the running binary's build identity carried in the labels |
| `solparq_health` | gauge | — | `1` when the last loop had no error, else `0` |
| `solparq_started_at_unix` | gauge | — | Process start time (Unix seconds) |
| `solparq_check_interval_seconds` | gauge | — | Configured archive check interval |
| `solparq_last_run_at_unix` | gauge | `archive_kind` | Last archive check time |
| `solparq_last_success_at_unix` | gauge | `archive_kind` | Last successful archive time |
| `solparq_archive_in_flight` | gauge | `archive_kind` | `1` while an archive task is running |

Currency (how up to date the data is):

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `solparq_db_earliest_slot` | gauge | — | Earliest transaction slot in ClickHouse |
| `solparq_db_latest_slot` | gauge | — | Latest transaction slot in ClickHouse |
| `solparq_db_slots_available` | gauge | — | Distinct transaction slots visible in ClickHouse |
| `solparq_chain_tip_slot` | gauge | — | Latest Solana network slot (finalized) via `getSlot` |
| `solparq_chain_tip_lag_slots` | gauge | — | Network tip minus latest ClickHouse slot |
| `solparq_db_lag_slots` | gauge | `archive_kind` | Latest ClickHouse slot minus latest archived slot |
| `solparq_last_archived_start_slot` | gauge | `archive_kind` | Start slot of the most recent archive |
| `solparq_last_archived_end_slot` | gauge | `archive_kind` | End slot of the most recent archive |
| `solparq_last_archived_epoch` | gauge | `archive_kind` | Epoch of the most recent archive |
| `solparq_disk_free_bytes` | gauge | `disk`, `path` | Free space on a ClickHouse-managed disk, from `system.disks` |
| `solparq_disk_used_bytes` | gauge | `disk`, `path` | Used space on a ClickHouse-managed disk, from `system.disks` |
| `solparq_disk_total_bytes` | gauge | `disk`, `path` | Total space on a ClickHouse-managed disk, from `system.disks` |
| `solparq_db_table_bytes` | gauge | `table_kind`, `table` | Disk bytes used by a ClickHouse source table, from `system.tables` |
| `solparq_db_table_rows` | gauge | `table_kind`, `table` | Row count of a ClickHouse source table, from `system.tables` |

Throughput and outcomes:

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `solparq_archives_created_total` | counter | `archive_kind` | Archives created successfully |
| `solparq_archives_skipped_total` | counter | `archive_kind`, `reason` | Planning runs skipped, by skip reason (`not-enough-slots`, `no-data`, `user-declined`, `data-gap`, `validation-warning`, `dry-run`, `skipped`) |
| `solparq_archive_errors_total` | counter | `archive_kind` | Archive loop errors |
| `solparq_archives_cleaned_total` | counter | `archive_kind` | Old archive bundles pruned by retention |
| `solparq_clickhouse_range_deleted_total` | counter | `archive_kind` | Archived ClickHouse ranges deleted |
| `solparq_archive_rows_total` | counter | `archive_kind`, `table` | Rows archived per source table |
| `solparq_last_archive_rows` | gauge | `archive_kind` | Rows written in the most recent archive |
| `solparq_last_archive_bytes` | gauge | `archive_kind` | Bytes written in the most recent archive (local destinations only) |

Latency (histograms, seconds):

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `solparq_phase_duration_seconds` | histogram | `archive_kind`, `phase` | Per-phase archive latency. Phases: `validate`, `repair`, `backfill`, `write`, `dry_run_count`, `delete_range`, `cleanup`, `total` |

Data quality — validation issues are split by `category` so a dashboard can
separate actionable gaps from expected leader gaps and other problems:

- `missing_block` — Solana produced the block but it is missing from ClickHouse
  (**actual gap, needs backfill**).
- `not_produced` — the slot was never produced on-chain (**expected leader gap**,
  informational).
- `transaction_mismatch` — block transaction count does not match the archived
  transaction rows (**other data issue**).

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `solparq_validation_slots` | gauge | `archive_kind`, `category` | Slots flagged in the last validated range, by category |
| `solparq_validation_ranges` | gauge | `archive_kind`, `category` | Contiguous slot ranges flagged in the last validated range, by category |
| `solparq_validation_flagged_slots_total` | counter | `archive_kind`, `category` | Cumulative flagged slots across runs, by category (use `rate()`/`increase()` to alert on new gaps) |
| `solparq_validation_range_start_slot` | gauge | `archive_kind` | Start slot of the last validated range |
| `solparq_validation_range_end_slot` | gauge | `archive_kind` | End slot of the last validated range |
| `solparq_validation_db_block_slots` | gauge | `archive_kind` | Block slots present in ClickHouse for the last validated range |
| `solparq_validation_rpc_produced_slots` | gauge | `archive_kind` | Slots the RPC reports as produced for the last validated range |
| `solparq_validation_mismatch_slots` | gauge | `archive_kind`, `direction` | Transaction-mismatch slots in the last validated range, by `direction` (`undercount` = missing rows, `overcount` = duplicate rows) |
| `solparq_validation_rpc_errors_total` | counter | `archive_kind` | Validation runs where the Solana RPC cross-check failed |
| `solparq_mismatch_repairs_total` | counter | `archive_kind`, `outcome` | Overcount-mismatch repair attempts, by `outcome` (`repaired` = clean after dedup, `still_dirty` = mismatch remains) |
| `solparq_gap_backfills_total` | counter | `archive_kind`, `outcome` | Pre-archive RPC gap-backfill attempts (`--backfill-gaps`), by `outcome` (`filled` = no missing blocks remain, `partial` = some blocks still missing, `failed` = backfill subprocess errored) |
| `solparq_known_gaps` | gauge | `archive_kind`, `classification` | Currently tracked known data gaps, by classification (`Needs backfill`, `Legit not-produced`, `Transaction mismatch (undercount)`, `Transaction mismatch (overcount)`) |

> `solparq_last_archive_bytes` is only populated for local destinations.
> ClickHouse-driven S3 exports do not report bytes written, so the byte gauge is
> left unset for S3 archives.

> `solparq_disk_*_bytes` come from `system.disks` on whichever ClickHouse node
> serves the HTTP request, so on a cluster this is per-node disk usage, not a
> cluster-wide aggregate.

> `solparq_db_table_*` come from `system.tables` on whichever ClickHouse node
> serves the HTTP request. On a `Distributed`-backed setup this reflects only
> the local shard's data unless the table itself is a `Distributed` table.
> Optional tables that are not present are simply omitted, matching
> `check_tables`.

#### Repairing transaction mismatches

A transaction mismatch means a slot's `blocks_metadata.executed_transaction_count`
does not equal the number of logically-distinct `transactions` rows for that slot.
Validation counts rows dedup-aware (matching the ReplacingMergeTree key
`slot, slot_idx, signature`), so transient duplicates awaiting a background merge
are not reported. Remaining mismatches split two ways:

- **Overcount** (more rows than declared) — duplicate rows that have not merged
  yet. Fixable inside ClickHouse by forcing dedup.
- **Undercount** (fewer rows than declared) — rows that never landed. Requires
  **re-ingesting** the affected slots from a source of truth (run the ingestor's
  `getBlock` backfill over the slot range printed in the archive report / ops
  dashboard). solparq does not re-ingest.

With `--repair-mismatches` (`SOLPARQ_REPAIR_MISMATCHES`), solparq attempts to fix
**overcount** mismatches before archiving: it runs
`OPTIMIZE TABLE <transactions-local> PARTITION <epoch> FINAL DEDUPLICATE` on each
affected epoch partition, then re-validates and only archives if the range is now
clean. Undercount mismatches are left untouched.

In clustered/replicated deployments `OPTIMIZE` must target the shard-local table,
so set `--db-transactions-local-table-name` (e.g. `transactions_local`) and
`--clickhouse-cluster` so the statement runs `ON CLUSTER` across all shards. On a
single node the defaults (local table = transactions table, no cluster) are fine.

#### Bounding ClickHouse memory during export

Each Parquet export is a single `SELECT * ... ORDER BY ...` over a whole slot
range. For an epoch (432,000 slots) of the wide `transactions` table this can
exceed the ClickHouse server memory limit and fail with `MEMORY_LIMIT_EXCEEDED`.
To keep the export bounded, solparq appends a `SETTINGS` clause to the export
query, controlled by `--clickhouse-archive-settings`
(`SOLPARQ_CLICKHOUSE_ARCHIVE_SETTINGS`). The default,

```
max_bytes_before_external_sort=1073741824, max_threads=4, output_format_parquet_row_group_size=100000
```

spills the sort to disk, caps read parallelism, and shrinks the Parquet writer's
in-memory row group. Override it with your own ClickHouse settings (e.g. raise
`max_threads` on a large box), or pass an empty string to omit the clause
entirely.

#### Grafana dashboard

An importable dashboard covering all of the above lives at
[`grafana/solparq-ops-dashboard.json`](grafana/solparq-ops-dashboard.json). It is
organised into rows — Overview (including a **Version** stat sourced from
`solparq_build_info`), Currency, Throughput & outcomes, Latency, Data quality,
Resources (including ClickHouse disk and per-table sizes), and Transaction
mismatches — and includes `Node` (`nodename`) and `Archive kind` template
variables for filtering.

**Multiple nodes:** every query is filtered by the `Node` variable and grouped by
`nodename`, and each series is labelled with its node, so a Prometheus that
scrapes several solparq servers shows one series per node. Use the `Node` variable
to focus on a single server or leave it on `All` to compare them side by side.
This assumes your scrape config attaches a `nodename` label to each solparq
target.

To import: in Grafana, **Dashboards → New → Import**, upload the JSON (or paste
its contents), and select your Prometheus datasource when prompted for
`DS_PROMETHEUS`. The `solparq_process_*` panels (CPU, memory, file descriptors)
are populated on Linux; on macOS only uptime/start-time are exported.

Archive range types run independently in server mode. For example, a
`custom:500` archive check can start while a larger `hourly` or `epoch` archive
is still being written. `superbank-solparq` does not start a second task for the same
archive type while that type is already running; it logs that the task is
already active and checks again on the next interval.

Server mode skips an archive with validation warnings unless
`--force-archive` is set. This avoids a non-interactive process silently
archiving questionable data.

Server mode logs every archive check at the standard `info` level, including
which archive type is being checked, when a task is ready to run, when an
archive is created, and why a task is skipped. Use `-v` for more detail about
archive state and validation counts, or `-vv` for trace-level logs.

Before creating an archive, validation logs a gap summary even when
`--force-archive` is set. The summary includes:

- `missing_block_ranges`: produced Solana slots missing from `blocks_metadata`;
  these need backfill.
- `not_produced_slot_ranges`: slots not returned by Solana `getBlocks`; these
  are expected leader/production gaps.
- `transaction_mismatch_ranges`: slots where block transaction counts do not
  match transaction rows.

By default, each archive type continues from the highest existing archive of
that same type. If no archive exists for that type, `superbank-solparq` starts from the
oldest transaction slot available in ClickHouse. To ignore existing archives and
plan from the oldest ClickHouse slot at startup, use:

```bash
--no-continue-from-last-archive
```

To mirror logs to a file as well as the terminal:

```bash
--log-file ./solparq.log
```

The ops dashboard refreshes every 30 seconds. It shows the running binary
version and git commit (in the header and startup settings), human-readable UTC
timestamps for last run and last success, the number of transaction slots
available in ClickHouse, ClickHouse disk usage (used/free/total per disk, from
`system.disks`), per-table row counts and sizes (from `system.tables`),
startup settings, skip reasons, known data gaps, and a color-coded archive
timeline. The `/status` JSON endpoint also reports `version` and `git_sha`.

On shutdown, `superbank-solparq` handles `Ctrl+C` and `SIGTERM` gracefully. The first
shutdown signal stops new archive tasks from starting and waits for any archive
currently being written to finish, including parallel archive tasks, their
reports, cleanup, and optional ClickHouse delete steps. Press `Ctrl+C` again,
or send another `SIGTERM`, to abort immediately without waiting for active
archive tasks.

## Archive Ranges

- `hourly`: 9000 slots
- `epoch`: 432000 slots, aligned to epoch boundaries
- `custom`: defaults to 1000 slots
- `custom:<slots>`: explicit custom size, for example `custom:2500`

You can also set the custom default with:

```bash
--custom-slot-range 2500
```

Multiple `--archive-range-type` values are allowed only with `--server-mode`.
Only one custom archive range size can be configured at a time, because custom
archives use the shared `custom_*` archive namespace.

## Validation

Before writing an archive, `superbank-solparq` checks:

- ClickHouse has the required `transactions` and `blocks_metadata` tables.
- Produced Solana blocks in the range, discovered through `getBlocks`, exist in
  `blocks_metadata`.
- Each block's `executed_transaction_count` matches transaction rows in the
  `transactions` table.

If warnings are found in regular mode, `superbank-solparq` asks for confirmation before
creating the archive. Use `--force-archive` to archive despite warnings.

### When the RPC cross-check itself fails

The produced-slots check calls Solana `getBlocks`. Some RPC providers disable
`getBlocks` (it is expensive) and return an error such as
`{"code":405,"message":"Bad method"}`. That failure is recorded as
`rpc_check_error`, and by default it is treated as a validation warning — so in
`--server-mode` archiving is skipped (it requires `--force-archive`), even when
the block and transaction-count checks are clean.

Set `--allow-rpc-validation-failure` (`SOLPARQ_ALLOW_RPC_VALIDATION_FAILURE`) to
let archiving proceed when *only* the RPC cross-check failed. Unlike
`--force-archive`, this still blocks on verified data problems (missing blocks,
transaction-count mismatches) — it only stops a "couldn't verify" condition from
stalling the archiver. Note the trade-off: with `getBlocks` unavailable,
missing-block detection is disabled for that run, so it relies on the
transaction-count check and on ingestion completeness. The failure is still
logged and exposed via the `missing_block` / `rpc_errors` validation metrics.
Prefer pointing `--solana-rpc-url` at an endpoint that supports `getBlocks` when
you want full verification.

The default Solana RPC endpoint is:

```text
https://api.mainnet-beta.solana.com
```

Override it with:

```bash
--solana-rpc-url https://your.rpc.endpoint
```

### Backfilling gaps before archiving

By default a missing block just produces a warning (skip in server mode, prompt
otherwise). With `--backfill-gaps` (`SOLPARQ_BACKFILL_GAPS`), `superbank-solparq`
instead **repairs** the range before archiving: it takes the missing slots that
validation already found (`missing_blocks`, plus transaction-undercount slots
unless `--backfill-include-undercounts=false`), and invokes the `superbank` RPC
ingestor as a subprocess to fetch and insert exactly those slots
(`--source rpc --rpc-slot-list`). It then re-validates and proceeds with the
archive decision on the post-backfill state. This makes server mode
self-healing across transient ingest hiccups.

```bash
cargo run -p superbank-solparq -- \
  --db-server 192.168.0.184 \
  --db-user admin \
  --db-password 'change-me' \
  --archive-range-type hourly \
  --backfill-gaps \
  --backfill-superbank-bin /usr/local/bin/superbank \
  --solana-rpc-url https://your.rpc.endpoint
```

Details and limitations:

- The `superbank` binary must be available (on `PATH`, or set
  `--backfill-superbank-bin` / `SOLPARQ_BACKFILL_SUPERBANK_BIN`). ClickHouse and
  RPC config are passed to the subprocess via environment variables.
- The subprocess is launched with `METRICS_HOST=127.0.0.1` and `METRICS_PORT=0`
  (an OS-assigned ephemeral port) so its metrics/health server does not collide
  with a live ingestor already bound to `9901` on the same host.
- Backfill is **best-effort**: if the subprocess can't be spawned or exits
  non-zero, the failure is logged and the run falls through to the normal
  validation gate (skip / `--force-archive` / prompt). Re-validation always runs
  so the archive decision reflects reality.
- Re-ingesting is idempotent (`ReplacingMergeTree(slot)`), so backfilling slots
  that already exist is safe.
- **Entries are not restored**: RPC `getBlock` carries no PoH `entries`; backfill
  fills `blocks_metadata` and `transactions` only. Validation today checks only
  blocks and transaction counts, so this does not block the archive.
- `--dry-run` skips backfill (it only logs how many slots it would fetch).

## Dry Run

`--dry-run` (`SOLPARQ_DRY_RUN`) plans and validates an archive without making
any changes. It works in both one-shot and `--server-mode` runs:

```bash
cargo run -p superbank-solparq -- \
  --db-server 192.168.0.184 \
  --db-user admin \
  --db-password 'change-me' \
  --archive-range-type hourly \
  --dry-run
```

ClickHouse and Solana RPC reads (bounds, validation, row counts) still run
normally, but a dry run never:

- writes Parquet files, `manifest.json`, `SHA256SUMS.txt`, or a `.done*` marker
- runs the `--repair-mismatches` `OPTIMIZE ... DEDUPLICATE` step
- runs the `--backfill-gaps` RPC backfill subprocess
- deletes the archived ClickHouse range (`--delete-archived-data-range`)
- prunes old archives (`--archives-to-keep`)

The one-shot report (and, in `--server-mode`, the ops dashboard/metrics) shows
what the run would have archived: the planned archive name, slot range, and
per-table row counts, alongside the same validation output a real run would
see. If validation warnings would normally prompt for confirmation, a dry run
skips the prompt and reports the archive as skipped instead (matching what a
non-interactive real run would require: `--force-archive`).

## Cleanup

`--archives-to-keep` controls local and S3 archive retention. The default is
`5`. A value of `0` disables archive cleanup.

```bash
--archives-to-keep 10
```

To delete the archived ClickHouse data range after a successful archive:

```bash
--delete-archived-data-range
```

When multiple archive types are configured, ClickHouse data deletion is gated by
the completed archive high-watermark for every configured type. After any
archive succeeds, `superbank-solparq` checks the latest completed archive for each type
and deletes everything from the beginning of the table up to the highest slot
that all configured types have already covered. Sweeping from slot `0` (rather
than only the current archive's range) ensures no older slots are stranded when
one archive type was lagging while an earlier range was written. For example, if
`custom:500` and `hourly` are both configured, early `custom:500` archives will
defer deletion until an `hourly` archive covers the same slots. Once the
`hourly` archive exists, later `custom:500` completions can delete every slot up
to their safe high-watermark without waiting for another hourly cycle.

That deletes matching slots from:

- `transactions`
- `blocks_metadata`
- `entries`
- `gsfa`
- `gsfa_hot`
- `signatures`
- `token_owner_activity`

Optional tables are deleted only when they were available for the archive run.

## Options

Required:

- `--db-server`
- `--db-user`
- `--db-password`
- `--archive-range-type`

Common defaults:

- `--db-server-port 8123`
- `--db-database default`
- `--db-transactions-table-name transactions`
- `--db-blocks-table-name blocks_metadata`
- `--db-entries-table-name entries`
- `--db-gsfa-table-name gsfa`
- `--db-gsfa-hot-table-name gsfa_hot`
- `--db-signatures-table-name signatures`
- `--db-token-owner-activity-table-name token_owner_activity`
- `--archive-location-type local`
- `--archive-file-output-location ./`
- `--archives-to-keep 5`
- `--ops-port 30303`
- `--metrics-port 31313`
- `--archive-slot-range` unset
- `--no-continue-from-last-archive` unset
- `--log-file` unset
- `--repair-mismatches` unset (off)
- `--allow-rpc-validation-failure` unset (off)
- `--backfill-gaps` unset (off)
- `--backfill-superbank-bin superbank`
- `--backfill-include-undercounts` on (only applies when `--backfill-gaps` is set)
- `--db-transactions-local-table-name` unset (defaults to `--db-transactions-table-name`)
- `--clickhouse-cluster` unset
- `--clickhouse-archive-settings max_bytes_before_external_sort=1073741824, max_threads=4, output_format_parquet_row_group_size=100000`
- `--dry-run` unset (off)

Use `-v` for debug logs and `-vv` for trace logs. `RUST_LOG` is also honored.

Environment variables are available for the new startup behavior as
`SOLPARQ_NO_CONTINUE_FROM_LAST_ARCHIVE`, and for one-shot explicit ranges as
`SOLPARQ_ARCHIVE_SLOT_RANGE`.
