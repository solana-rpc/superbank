# solparq-read

`solparq-read` inspects and reads Parquet archives created by `solparq`.
It is read-only and works with local archives or S3-compatible object stores.

## Build

From the repository root:

```bash
cargo build -p solparq-read --release
```

## Local Archives

List archives:

```bash
cargo run -p solparq-read -- list \
  --archive-dir ./crates/solparq/archives
```

Show archive counts and metadata:

```bash
cargo run -p solparq-read -- summary \
  --archive ./crates/solparq/archives/custom_989_427299625-427300124.parquet
```

Show the Parquet schema:

```bash
cargo run -p solparq-read -- schema \
  --archive ./crates/solparq/archives/custom_989_427299625-427300124.parquet
```

Read transactions in an inclusive slot range:

```bash
cargo run -p solparq-read -- scan \
  --archive ./crates/solparq/archives/custom_989_427299625-427300124.parquet \
  --slot-range 427299700-427299800 \
  --columns slot,signature \
  --format jsonl
```

Read the full archive explicitly:

```bash
cargo run -p solparq-read -- scan \
  --archive ./crates/solparq/archives/custom_989_427299625-427300124.parquet \
  --all \
  --format csv
```

`scan` requires exactly one of `--slot-range START-END` or `--all` so large
archives are not dumped accidentally.

## S3 Archives

S3 mode uses the same endpoint, bucket, path, and credential model as
`solparq`.

List archives under a prefix:

```bash
cargo run -p solparq-read -- list \
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
cargo run -p solparq-read -- summary \
  --archive-location-type s3 \
  --archive-s3-endpoint https://s3.eu-central-003.backblazeb2.com \
  --archive-s3-bucket-name solparq \
  --archive-s3-bucket-path archives/test \
  --archive-s3-auth-key "$S3_ACCESS_KEY" \
  --archive-s3-auth-secret-key "$S3_SECRET_KEY" \
  --archive custom/custom_989_427299625-427300124.parquet
```

## Output

`summary` reports transaction rows, actual min/max slot, distinct observed
slots, observed blocks, row groups, columns, and file size. Because current
`solparq` archives contain transaction rows, observed blocks are counted as
distinct slots present in the archive.

`scan` supports:

- `--format jsonl`
- `--format json`
- `--format csv`
- `--columns slot,signature,fee`
- `--limit 1000`

Some transaction payload fields are stored as Parquet UTF8 columns even when
they can contain raw non-UTF8 bytes. If an unprojected `scan` reports
`encountered non UTF-8 data`, rerun it with `--columns` to read only the fields
you need, for example:

```bash
cargo run -p solparq-read -- scan \
  --archive ./crates/solparq/archives/custom_989_427299625-427300124.parquet \
  --slot-range 427299700-427299800 \
  --columns slot,signature,meta_fee
```
