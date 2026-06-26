# superbank-solparq

`superbank-solparq` archives Superbank/Solana ClickHouse tables to Parquet bundles.
It can run once or continuously in server mode.

Archive bundles use:

```text
type_epoch_from-slot_to-slot/
  manifest.json
  report.txt
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
cargo build -p superbank-solparq-read --release
```

## Release

`superbank-solparq` is released independently from the main Superbank binaries. The
release workflow is triggered by tags named `vX.Y.Z-superbank-solparq` and uses
`.goreleaser.solparq.yaml`.

Before tagging, update the package version in `crates/superbank-solparq/Cargo.toml`,
commit all superbank-solparq release changes, and push the branch:

```bash
git add crates/superbank-solparq crates/superbank-solparq-read .goreleaser.solparq.yaml .github/workflows/release-solparq.yml
git commit -m "Release superbank-solparq v0.1.0"
git push origin <branch-name>
```

Then create and push the annotated release tag:

```bash
git tag -a v0.1.0-superbank-solparq -m "Release superbank-solparq v0.1.0"
git push origin v0.1.0-superbank-solparq
```

The tag must point at a commit that already contains
`.github/workflows/release-solparq.yml` and `.goreleaser.solparq.yaml`; Git tags
do not include uncommitted working-tree changes.

The workflow builds Linux binaries for:

- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`

and publishes GitHub Release assets similar to:

```text
superbank-solparq-v0.1.0-linux-amd64.tar.gz
superbank-solparq-v0.1.0-linux-arm64.tar.gz
superbank-solparq-read-v0.1.0-linux-amd64.tar.gz
superbank-solparq-read-v0.1.0-linux-arm64.tar.gz
superbank-solparq-v0.1.0-SHA256SUMS.txt
```

To verify the release packaging locally without publishing:

```bash
goreleaser check --config .goreleaser.solparq.yaml
goreleaser release --snapshot --clean --config .goreleaser.solparq.yaml
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
table Parquet files and `manifest.json` are written.

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
s3://<bucket>/<bucket-path>/<archive-type>/<archive-id>/manifest.json
```

`manifest.json` is written after the table objects so readers can treat its
presence as the data-completion marker.

## Read Archives

Use `superbank-solparq-read` to inspect local or S3 archives without connecting to
ClickHouse.

```bash
cargo run -p superbank-solparq-read -- list \
  --archive-dir ./crates/superbank-solparq/archives

cargo run -p superbank-solparq-read -- summary \
  --archive ./crates/superbank-solparq/archives/hourly_989_427299625-427308624

cargo run -p superbank-solparq-read -- scan \
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
cargo run -p superbank-solparq-read -- summary \
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

The ops dashboard refreshes every 30 seconds. It shows human-readable UTC
timestamps for last run and last success, the number of transaction slots
available in ClickHouse, startup settings, skip reasons, known data gaps, and a
color-coded archive timeline.

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

The default Solana RPC endpoint is:

```text
https://api.mainnet-beta.solana.com
```

Override it with:

```bash
--solana-rpc-url https://your.rpc.endpoint
```

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
and deletes only the part of the current archive range that all types have
already covered. For example, if `custom:500` and `hourly` are both configured,
early `custom:500` archives will defer deletion until an `hourly` archive covers
the same slots. Once the `hourly` archive exists, later `custom:500` completions
can delete their safe ranges without waiting for another hourly cycle.

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

Use `-v` for debug logs and `-vv` for trace logs. `RUST_LOG` is also honored.

Environment variables are available for the new startup behavior as
`SOLPARQ_NO_CONTINUE_FROM_LAST_ARCHIVE`, and for one-shot explicit ranges as
`SOLPARQ_ARCHIVE_SLOT_RANGE`.
