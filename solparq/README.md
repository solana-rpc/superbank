# solparq

`solparq` archives Superbank/Solana ClickHouse transaction data to Parquet.
It can run once or continuously in server mode.

Archive files use:

```text
type_epoch_from-slot_to-slot.parquet
```

Example:

```text
hourly_988_427236024-427245023.parquet
```

## Build

From the repository root:

```bash
cargo build --manifest-path solparq/Cargo.toml --release
```

## One-Shot Local Archive

```bash
cargo run --manifest-path solparq/Cargo.toml -- \
  --db-server 192.168.0.184 \
  --db-user admin \
  --db-password 'change-me' \
  --archive-range-type hourly \
  --archive-file-output-location ./archives
```

By default this creates local Parquet files. Local output streams:

```sql
SELECT *
FROM transactions
WHERE slot BETWEEN <start> AND <end>
ORDER BY slot, slot_idx, signature
FORMAT Parquet
```

The stream is written to a temporary file and then moved into place.

## One-Shot S3 Archive

S3 mode asks ClickHouse to write the archive with `INSERT INTO FUNCTION s3(...)`.
This works with S3-compatible endpoints, including Backblaze-style custom
endpoints.

```bash
cargo run --manifest-path solparq/Cargo.toml -- \
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

The object path is:

```text
s3://<bucket>/<bucket-path>/<archive-type>/<archive-name>
```

## Server Mode

Server mode runs continuously and can process multiple archive range types.

```bash
cargo run --manifest-path solparq/Cargo.toml -- \
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

Server mode skips an archive with validation warnings unless
`--force-archive` is set. This avoids a non-interactive process silently
archiving questionable data.

Server mode logs every archive check at the standard `info` level, including
which archive type is being checked, when a task is ready to run, when an
archive is created, and why a task is skipped. Use `-v` for more detail about
archive state and validation counts, or `-vv` for trace-level logs.

To mirror logs to a file as well as the terminal:

```bash
--log-file ./solparq.log
```

The ops dashboard refreshes every 30 seconds. It shows human-readable UTC
timestamps for last run and last success, the number of transaction slots
available in ClickHouse, startup settings, and a color-coded archive timeline.

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

## Validation

Before writing an archive, `solparq` checks:

- ClickHouse has the required `transactions` and `blocks_metadata` tables.
- Produced Solana blocks in the range, discovered through `getBlocks`, exist in
  `blocks_metadata`.
- Each block's `executed_transaction_count` matches transaction rows in the
  `transactions` table.

If warnings are found in regular mode, `solparq` asks for confirmation before
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

That deletes matching slots from:

- `transactions`
- `blocks_metadata`
- `gsfa`
- `signatures`

`gsfa` and `signatures` only need to exist when this delete flag is enabled.

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
- `--db-gsfa-table-name gsfa`
- `--db-signatures-table-name signatures`
- `--archive-location-type local`
- `--archive-file-output-location ./`
- `--archives-to-keep 5`
- `--ops-port 30303`
- `--metrics-port 31313`
- `--log-file` unset

Use `-v` for debug logs and `-vv` for trace logs. `RUST_LOG` is also honored.
