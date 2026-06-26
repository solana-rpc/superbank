# superbank-solparq-read

`superbank-solparq-read` inspects and reads Parquet archives created by `superbank-solparq`.
It is read-only and works with local archives or S3-compatible object stores.
It supports current DB archive bundles and older single-file transaction
Parquet archives.

## Build

From the repository root:

```bash
cargo build -p superbank-solparq-read --release
```

## Local Archives

List archives:

```bash
cargo run -p superbank-solparq-read -- list \
  --archive-dir ./crates/superbank-solparq/archives
```

Show archive counts and metadata:

```bash
cargo run -p superbank-solparq-read -- summary \
  --archive ./crates/superbank-solparq/archives/custom_989_427299625-427300124
```

Show the Parquet schema:

```bash
cargo run -p superbank-solparq-read -- schema \
  --archive ./crates/superbank-solparq/archives/custom_989_427299625-427300124 \
  --table transactions
```

Read transactions in an inclusive slot range:

```bash
cargo run -p superbank-solparq-read -- scan \
  --archive ./crates/superbank-solparq/archives/custom_989_427299625-427300124 \
  --slot-range 427299700-427299800 \
  --columns slot,signature \
  --format jsonl
```

Read block metadata from the same bundle:

```bash
cargo run -p superbank-solparq-read -- scan \
  --archive ./crates/superbank-solparq/archives/custom_989_427299625-427300124 \
  --table blocks_metadata \
  --slot-range 427299700-427299800 \
  --columns slot,block_time,executed_transaction_count \
  --format jsonl
```

Read the full archive explicitly:

```bash
cargo run -p superbank-solparq-read -- scan \
  --archive ./crates/superbank-solparq/archives/custom_989_427299625-427300124 \
  --all \
  --format csv
```

`scan` requires exactly one of `--slot-range START-END` or `--all` so large
archives are not dumped accidentally.

## S3 Archives

S3 mode uses the same endpoint, bucket, path, and credential model as
`superbank-solparq`.

List archives under a prefix:

```bash
cargo run -p superbank-solparq-read -- list \
  --archive-location-type s3 \
  --archive-s3-endpoint https://s3.eu-central-003.backblazeb2.com \
  --archive-s3-bucket-name solparq \
  --archive-s3-bucket-path archives/test \
  --archive-s3-auth-key "$S3_ACCESS_KEY" \
  --archive-s3-auth-secret-key "$S3_SECRET_KEY"
```

Read one archive. In S3 mode, `--archive` is relative to
`--archive-s3-bucket-path`:

```bash
cargo run -p superbank-solparq-read -- summary \
  --archive-location-type s3 \
  --archive-s3-endpoint https://s3.eu-central-003.backblazeb2.com \
  --archive-s3-bucket-name solparq \
  --archive-s3-bucket-path archives/test \
  --archive-s3-auth-key "$S3_ACCESS_KEY" \
  --archive-s3-auth-secret-key "$S3_SECRET_KEY" \
  --archive custom/custom_989_427299625-427300124
```

## Output

`summary` reports the archive format, transaction rows, actual min/max slot,
distinct observed slots, observed blocks, row groups, columns, file size, and
per-table row counts from `manifest.json` when reading a bundle.

`scan` supports:

- `--format jsonl`
- `--format json`
- `--format csv`
- `--table transactions|blocks_metadata|entries|gsfa|gsfa_hot|signatures|token_owner_activity`
- `--columns slot,signature,fee`
- `--limit 1000`

Some transaction payload fields are stored as Parquet UTF8 columns even when
they can contain raw non-UTF8 bytes. If an unprojected `scan` reports
`encountered non UTF-8 data`, rerun it with `--columns` to read only the fields
you need, for example:

```bash
cargo run -p superbank-solparq-read -- scan \
  --archive ./crates/superbank-solparq/archives/custom_989_427299625-427300124 \
  --slot-range 427299700-427299800 \
  --columns slot,signature,meta_fee
```

For legacy single-file transaction archives, pass the `.parquet` file path or
S3 object key directly. The `--table` option is ignored for single-file
archives.
