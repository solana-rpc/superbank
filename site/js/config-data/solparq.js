// superbank-solparq (archiver) configuration. Checked against the writer Cli
// (clap) in crates/superbank-solparq/src/config.rs, including the cross-field
// checks in Config::from_cli, and the tracing setup in src/main.rs. The reader
// binary has its own module (solparq-read.js). tests/site/config.test.mjs
// enforces that every flag and env name in the writer code is listed here.
//
// Plain text only: `code` spans use backticks, no markup.

const CONFIG = 'crates/superbank-solparq/src/config.rs';
const MAIN = 'crates/superbank-solparq/src/main.rs';

// Clap field with an explicit env name; the flag is given explicitly.
const opt = (env, flag, rest) => ({ env, flag, ...rest });
const ONE_SHOT = 'when:SOLPARQ_SERVER_MODE=false';
const SERVER = 'when:SOLPARQ_SERVER_MODE=true';
const S3 = 'when:SOLPARQ_ARCHIVE_LOCATION_TYPE=s3';
const S3_REQUIRED = 'when `SOLPARQ_ARCHIVE_LOCATION_TYPE` is `s3`';
const TABLE_NOTE = 'Validated to ASCII letters, digits, `_` and `.`.';
const BACKFILL = 'when:SOLPARQ_BACKFILL_GAPS=true';

export default {
  id: 'solparq',
  label: 'superbank-solparq',
  summary: 'Archives the ClickHouse tables to Parquet, once or continuously, with validation, repair and gap backfill.',
  source: CONFIG,
  readme: 'crates/superbank-solparq/README.md',
  primary: 'flag',
  intro:
    'Each option is a flag or an env var (flag wins, then env, then the default). Without `--server-mode` the binary archives once and exits; with it, it keeps archiving and serves ops and metrics ports. Most boolean options are plain switches (`--force-archive`), but `--archive-dedup-export`, `--custom-aligned` and `--backfill-include-undercounts` take a value (`--archive-dedup-export true`).',
  groups: [
    {
      id: 'clickhouse',
      title: 'ClickHouse connection',
      items: [
        opt('SOLPARQ_DB_SERVER', '--db-server', {
          type: 'string',
          required: true,
          text: 'ClickHouse server as a bare host, `host:port` or an `http(s)://` URL. A bare host gets `http://` and the port from `SOLPARQ_DB_SERVER_PORT`.',
        }),
        opt('SOLPARQ_DB_SERVER_PORT', '--db-server-port', {
          type: 'u16',
          default: '8123',
          text: 'ClickHouse HTTP port. Ignored when the server value already has a port or a scheme.',
          relations: [{ type: 'see', to: 'SOLPARQ_DB_SERVER' }],
        }),
        opt('SOLPARQ_DB_DATABASE', '--db-database', { type: 'string', default: 'default', text: 'ClickHouse database holding the archived tables.' }),
        opt('SOLPARQ_DB_USER', '--db-user', { type: 'string', required: true, text: 'ClickHouse user.' }),
        opt('SOLPARQ_DB_PASSWORD', '--db-password', { type: 'string', required: true, secret: true, text: 'ClickHouse password.' }),
      ],
    },
    {
      id: 'tables',
      title: 'Table names',
      intro: 'Names of the ClickHouse tables to archive. Each flag also accepts a shorter alias without the `db-` prefix and `-name` suffix.',
      items: [
        opt('SOLPARQ_DB_TRANSACTIONS_TABLE_NAME', '--db-transactions-table-name', {
          type: 'string',
          default: 'transactions',
          text: `Transactions table. Alias \`--transactions-table\`. ${TABLE_NOTE}`,
        }),
        opt('SOLPARQ_DB_TRANSACTIONS_LOCAL_TABLE_NAME', '--db-transactions-local-table-name', {
          type: 'string',
          text: 'Shard-local transactions table that mismatch-repair `OPTIMIZE` statements target; defaults to the transactions table name. Set it to the `*_local` table on clustered deployments, since `OPTIMIZE` cannot run on a Distributed table.',
          relations: [{ type: 'see', to: 'SOLPARQ_REPAIR_MISMATCHES' }],
        }),
        opt('SOLPARQ_DB_BLOCKS_TABLE_NAME', '--db-blocks-table-name', {
          type: 'string',
          default: 'blocks_metadata',
          text: `Blocks metadata table. Alias \`--blocks-table\`. ${TABLE_NOTE}`,
        }),
        opt('SOLPARQ_DB_ENTRIES_TABLE_NAME', '--db-entries-table-name', {
          type: 'string',
          default: 'entries',
          text: `Entries table. Alias \`--entries-table\`. ${TABLE_NOTE}`,
        }),
        opt('SOLPARQ_DB_GSFA_TABLE_NAME', '--db-gsfa-table-name', {
          type: 'string',
          default: 'gsfa',
          text: `GSFA table. Alias \`--gsfa-table\`. ${TABLE_NOTE}`,
        }),
        opt('SOLPARQ_DB_GSFA_HOT_TABLE_NAME', '--db-gsfa-hot-table-name', {
          type: 'string',
          default: 'gsfa_hot',
          text: `Hot GSFA table. Alias \`--gsfa-hot-table\`. ${TABLE_NOTE}`,
        }),
        opt('SOLPARQ_DB_SIGNATURES_TABLE_NAME', '--db-signatures-table-name', {
          type: 'string',
          default: 'signatures',
          text: `Signatures table. Alias \`--signatures-table\`. ${TABLE_NOTE}`,
        }),
        opt('SOLPARQ_DB_TOKEN_OWNER_ACTIVITY_TABLE_NAME', '--db-token-owner-activity-table-name', {
          type: 'string',
          default: 'token_owner_activity',
          text: `Token owner activity table. Alias \`--token-owner-activity-table\`. ${TABLE_NOTE}`,
        }),
      ],
    },
    {
      id: 'clickhouse-tuning',
      title: 'ClickHouse export tuning',
      items: [
        opt('SOLPARQ_CLICKHOUSE_CLUSTER', '--clickhouse-cluster', {
          type: 'string',
          text: 'ClickHouse cluster name for `ON CLUSTER` mismatch-repair `OPTIMIZE` statements. Leave unset (or empty) on single-node deployments.',
          relations: [{ type: 'see', to: 'SOLPARQ_REPAIR_MISMATCHES' }],
        }),
        opt('SOLPARQ_CLICKHOUSE_ARCHIVE_SETTINGS', '--clickhouse-archive-settings', {
          type: 'string',
          default: 'max_bytes_before_external_sort=1073741824, max_threads=4, output_format_parquet_row_group_size=100000',
          text: 'Raw ClickHouse `SETTINGS` appended to the Parquet-export queries, local and S3. Bounds per-query memory on a full-epoch export; an empty string omits the `SETTINGS` clause.',
        }),
        opt('SOLPARQ_ARCHIVE_DEDUP_EXPORT', '--archive-dedup-export', {
          type: 'bool',
          default: 'false',
          text: 'Add `FINAL` to the Parquet-export queries so files hold ReplacingMergeTree-collapsed rows that match the manifest `row_count`, and two deployments archiving one slot range produce identical files. Costs an extra merge pass, heaviest on gsfa and signatures. Takes a value: `--archive-dedup-export true`.',
        }),
      ],
    },
    {
      id: 'selection',
      title: 'Archive ranges',
      items: [
        opt('SOLPARQ_ARCHIVE_RANGE_TYPE', '--archive-range-type', {
          type: 'list',
          required: true,
          text: 'Archive kinds to produce: `hourly`, `epoch`, `custom` or `custom:{n}`, comma-separated or by repeating the flag. More than one kind requires `--server-mode`; duplicates are rejected, and only one custom size may be configured.',
          relations: [{ type: 'see', to: 'SOLPARQ_SERVER_MODE' }],
        }),
        opt('SOLPARQ_HOURLY_SLOT_DURATION_MS', '--hourly-slot-duration-ms', {
          type: 'u64 (ms)',
          default: '400',
          text: 'Nominal slot duration used to size `hourly` archives: 400 gives 9000 slots, 200 gives 18000. Must be positive and divide 3600000 exactly. Alpenglow does not change it.',
          relations: [{ type: 'see', to: 'SOLPARQ_ARCHIVE_RANGE_TYPE' }],
        }),
        opt('SOLPARQ_CUSTOM_SLOT_RANGE', '--custom-slot-range', {
          type: 'u64 (slots)',
          default: '1000',
          text: 'Slot count of a bare `custom` kind. Must be greater than zero.',
          relations: [{ type: 'see', to: 'SOLPARQ_ARCHIVE_RANGE_TYPE' }],
        }),
        opt('SOLPARQ_CUSTOM_ALIGNED', '--custom-aligned', {
          type: 'bool',
          default: 'false',
          text: 'Snap `custom` archives onto fixed slot-count boundaries (`custom:1000` gives 1000, 2000, 3000, ...) and wait until ClickHouse holds the whole window. Only affects the `custom` kind. Takes a value: `--custom-aligned true`.',
          relations: [{ type: 'see', to: 'SOLPARQ_ARCHIVE_RANGE_TYPE' }],
        }),
        opt('SOLPARQ_ARCHIVE_SLOT_RANGE', '--archive-slot-range', {
          type: 'string (START-END)',
          text: 'Archive exactly this slot range. One-shot runs only.',
          requires: [ONE_SHOT],
          relations: [{ type: 'conflicts', to: 'SOLPARQ_SERVER_MODE' }],
        }),
        opt('SOLPARQ_NO_CONTINUE_FROM_LAST_ARCHIVE', '--no-continue-from-last-archive', {
          type: 'bool',
          default: 'false',
          text: 'Plan the next archive from the oldest slot in ClickHouse instead of continuing after the latest existing archive.',
        }),
        opt('SOLPARQ_FORCE_ARCHIVE', '--force-archive', {
          type: 'bool',
          default: 'false',
          text: 'Archive a range even when validation would block it (missing blocks, transaction count mismatches or a failed RPC cross-check).',
          relations: [{ type: 'see', to: 'SOLPARQ_ALLOW_RPC_VALIDATION_FAILURE' }],
        }),
        opt('SOLPARQ_ARCHIVES_TO_KEEP', '--archives-to-keep', {
          type: 'usize',
          default: '5',
          text: 'Archives of each kind to keep at the output location. After a new archive is written the oldest beyond this count are deleted; 0 keeps everything.',
        }),
      ],
    },
    {
      id: 'location',
      title: 'Output location',
      items: [
        opt('SOLPARQ_ARCHIVE_LOCATION_TYPE', '--archive-location-type', {
          type: 'local | s3',
          default: 'local',
          text: 'Where archives are written. `s3` makes ClickHouse write the Parquet objects straight to the bucket.',
        }),
        opt('SOLPARQ_ARCHIVE_FILE_OUTPUT_LOCATION', '--archive-file-output-location', {
          type: 'path',
          default: './',
          text: 'Directory archives are written to.',
          requires: ['when:SOLPARQ_ARCHIVE_LOCATION_TYPE=local'],
        }),
        opt('SOLPARQ_ARCHIVE_S3_BUCKET_NAME', '--archive-s3-bucket-name', {
          type: 'string',
          required: S3_REQUIRED,
          text: 'S3 bucket name.',
          requires: [S3],
        }),
        opt('SOLPARQ_ARCHIVE_S3_BUCKET_PATH', '--archive-s3-bucket-path', {
          type: 'string',
          default: '',
          text: 'Key prefix inside the bucket. Empty by default.',
          requires: [S3],
        }),
        opt('SOLPARQ_ARCHIVE_S3_AUTH_KEY', '--archive-s3-auth-key', {
          type: 'string',
          required: S3_REQUIRED,
          text: 'S3 access key.',
          requires: [S3],
        }),
        opt('SOLPARQ_ARCHIVE_S3_AUTH_SECRET_KEY', '--archive-s3-auth-secret-key', {
          type: 'string',
          required: S3_REQUIRED,
          secret: true,
          text: 'S3 secret access key.',
          requires: [S3],
        }),
        opt('SOLPARQ_ARCHIVE_S3_ENDPOINT', '--archive-s3-endpoint', {
          type: 'url',
          required: S3_REQUIRED,
          text: 'S3-compatible endpoint URL.',
          requires: [S3],
        }),
        opt('SOLPARQ_ARCHIVE_S3_REGION', '--archive-s3-region', {
          type: 'string',
          default: 'us-east-1',
          text: 'S3 region used for request signing.',
          requires: [S3],
        }),
        opt('SOLPARQ_ARCHIVE_S3_WRITE_CHECKSUMS', '--archive-s3-write-checksums', {
          type: 'bool',
          default: 'false',
          text: 'Write `SHA256SUMS.txt` for S3 archives. This forces a download of every object, since ClickHouse writes them directly, so it is API-rate-limit intensive on large archives.',
          requires: [S3],
        }),
      ],
    },
    {
      id: 'server',
      title: 'Server mode and ops',
      items: [
        opt('SOLPARQ_SERVER_MODE', '--server-mode', {
          type: 'bool',
          default: 'false',
          text: 'Run continuously instead of archiving once. Required to configure more than one archive range type.',
        }),
        opt('SOLPARQ_OPS_PORT', '--ops-port', { type: 'u16', default: '30303', text: 'Port of the ops endpoint.', requires: [SERVER] }),
        opt('SOLPARQ_METRICS_PORT', '--metrics-port', { type: 'u16', default: '31313', text: 'Port of the Prometheus metrics endpoint.', requires: [SERVER] }),
        opt('SOLPARQ_ARCHIVE_CHECK_INTERVAL_SECS', '--archive-check-interval-secs', {
          type: 'u64 (s)',
          default: '60',
          text: 'Seconds between checks for a new archive to produce. Must be greater than zero.',
          requires: [SERVER],
        }),
        opt('SOLPARQ_DELETE_ARCHIVED_DATA_RANGE', '--delete-archived-data-range', {
          type: 'bool',
          default: 'false',
          text: 'Delete the archived slot range from ClickHouse after a successful archive.',
          relations: [{ type: 'see', to: 'SOLPARQ_DELETE_ARCHIVED_DATA_FROM_SLOT_ZERO' }],
        }),
        opt('SOLPARQ_DELETE_ARCHIVED_DATA_FROM_SLOT_ZERO', '--delete-archived-data-from-slot-zero', {
          type: 'bool',
          default: 'false',
          text: 'Sweep the ClickHouse delete range from slot 0 instead of flooring it at the lowest archived start slot, which also purges the never-archived leading window. Only valid with `--server-mode`.',
          requires: [SERVER],
          relations: [{ type: 'requires', to: 'SOLPARQ_SERVER_MODE' }],
        }),
      ],
    },
    {
      id: 'validation',
      title: 'Validation, repair and backfill',
      items: [
        opt('SOLPARQ_SOLANA_RPC_URL', '--solana-rpc-url', {
          type: 'url',
          default: 'https://api.mainnet-beta.solana.com',
          text: 'Solana RPC used for the produced-slots cross-check, and by gap backfill.',
        }),
        opt('SOLPARQ_ALLOW_RPC_VALIDATION_FAILURE', '--allow-rpc-validation-failure', {
          type: 'bool',
          default: 'false',
          text: 'Keep archiving when the produced-slots cross-check cannot run (for example a provider without `getBlocks`). Verified problems such as missing blocks or transaction mismatches still block, and missing-block detection is unavailable for that run.',
          relations: [{ type: 'see', to: 'SOLPARQ_FORCE_ARCHIVE' }],
        }),
        opt('SOLPARQ_REPAIR_MISMATCHES', '--repair-mismatches', {
          type: 'bool',
          default: 'false',
          text: 'Before archiving, try to repair overcount transaction mismatches with `OPTIMIZE ... FINAL DEDUPLICATE` on the affected epoch partitions, then validate again. Undercounts still need re-ingestion.',
        }),
        opt('SOLPARQ_BACKFILL_GAPS', '--backfill-gaps', {
          type: 'bool',
          default: 'false',
          text: 'Backfill blocks missing from the archive slot range from Solana RPC before archiving, by running the `superbank` ingestor as a subprocess. Uses `--solana-rpc-url` and the ClickHouse connection.',
          relations: [{ type: 'see', to: 'SOLPARQ_SOLANA_RPC_URL' }],
        }),
        opt('SOLPARQ_BACKFILL_SUPERBANK_BIN', '--backfill-superbank-bin', {
          type: 'path',
          default: 'superbank',
          text: 'The `superbank` binary used for gap backfill; a bare name is resolved from `PATH`.',
          requires: [BACKFILL],
          relations: [{ type: 'requires', to: 'SOLPARQ_BACKFILL_GAPS' }],
        }),
        opt('SOLPARQ_BACKFILL_INCLUDE_UNDERCOUNTS', '--backfill-include-undercounts', {
          type: 'bool',
          default: 'true',
          text: 'Also backfill slots flagged as transaction undercounts (block present, fewer archived rows than declared). Takes a value: `--backfill-include-undercounts false`.',
          requires: [BACKFILL],
          relations: [{ type: 'requires', to: 'SOLPARQ_BACKFILL_GAPS' }],
        }),
      ],
    },
    {
      id: 'logging',
      title: 'Logging and dry run',
      items: [
        opt('SOLPARQ_LOG_FILE', '--log-file', {
          type: 'path',
          text: 'Also append logs to this file, creating missing parent directories. Logs still go to stderr.',
        }),
        { flag: '--verbose', type: 'count', text: 'Raise the default log level: none is `info`, `-v` is `debug`, `-vv` or more is `trace`. Short form `-v`, repeatable.' },
        {
          env: 'RUST_LOG',
          source: MAIN,
          type: 'tracing filter',
          text: 'Log filter in `tracing_subscriber` `EnvFilter` syntax. When set and valid it overrides the level chosen by `-v`.',
          relations: [{ type: 'see', to: 'verbose' }],
        },
        opt('SOLPARQ_DRY_RUN', '--dry-run', {
          type: 'bool',
          default: 'false',
          text: 'Plan and validate without writing files, deleting ClickHouse data or creating done markers. Works one-shot and with `--server-mode`; validation still runs read-only queries.',
        }),
      ],
    },
  ],
};
