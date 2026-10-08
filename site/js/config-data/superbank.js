// superbank (ingestor) configuration. Checked against CliArgs (clap) and
// FileConfig (serde) in crates/superbank/src/cli.rs and the validation in
// validate_args / validate_start_slot_ownership; per-source applicability
// follows the reads of each field in crates/superbank/src/ingest/. tests/site/
// config.test.mjs enforces env, flag and YAML key drift in both directions.
//
// Plain text only: `code` spans use backticks, no markup.

// Clap field with an explicit env name; the YAML key is the flag without `--`.
const opt = (flag, env, rest) => ({ flag, env, yaml: flag.slice(2), ...rest });

const GRPC = 'source:grpc';
const FUMAROLE = 'source:fumarole';
const RPC = 'source:rpc';
const BIGTABLE = 'source:bigtable';
const SOLPARQ = 'source:solparq';
const FROM_SLOT_SPEC = 'u64 | *';
const S3 = 'when:SOLPARQ_ARCHIVE_LOCATION=s3';

export default {
  id: 'superbank',
  label: 'superbank',
  summary: 'Ingestor that writes Solana blocks, transactions and PoH entries into ClickHouse from gRPC, Fumarole, JSON-RPC, Bigtable or solparq archives.',
  source: 'crates/superbank/src/cli.rs',
  readme: 'crates/superbank/README.md',
  primary: 'flag',
  intro:
    'Each option can be a flag, an env var or a YAML key. Precedence: flag, then env var, then the YAML file (`--config` / `SUPERBANK_CONFIG`), then the default. YAML keys equal the flag names without `--`; snake_case aliases are accepted, and an unknown key is an error (`deny_unknown_fields`). Options that belong to one `--source` are rejected or ignored for the others as noted.',
  groups: [
    {
      id: 'core',
      title: 'Core',
      items: [
        opt('--source', 'SUPERBANK_SOURCE', {
          type: 'grpc | fumarole | rpc | bigtable | solparq',
          required: true,
          text: 'Ingest source. Startup fails when none is set by flag, env or YAML.',
        }),
        {
          flag: '--config',
          env: 'SUPERBANK_CONFIG',
          type: 'path',
          text: 'YAML config file. It has no YAML key of its own.',
        },
        {
          yaml: 'rpc-parameter-filters',
          type: 'list of [method, ...params]',
          text: 'Request filters for superbank-rpc. The ingestor accepts and ignores this key so both binaries can share one file.',
        },
        {
          flag: '--from-slot',
          yaml: 'from-slot',
          type: FROM_SLOT_SPEC,
          status: 'deprecated',
          text: 'Hidden legacy shared start slot. Always rejected at startup, whether set by flag or YAML; use the per-source from-slot option instead.',
          relations: [
            { type: 'see', to: 'DRAGONSMOUTH_FROM_SLOT' },
            { type: 'see', to: 'FUMAROLE_FROM_SLOT' },
            { type: 'see', to: 'RPC_FROM_SLOT' },
          ],
        },
      ],
    },
    {
      id: 'grpc',
      title: 'Yellowstone gRPC',
      intro: 'Used when `--source grpc`. `--commitment` and two of the `--grpc-*` options also apply to Fumarole, as noted.',
      items: [
        opt('--endpoint', 'DRAGONSMOUTH_ENDPOINT', {
          type: 'url',
          requires: [GRPC],
          required: 'when `--source grpc`',
          text: 'Yellowstone gRPC (Dragons Mouth) endpoint.',
        }),
        opt('--x-token', 'DRAGONSMOUTH_X_TOKEN', {
          type: 'string',
          secret: true,
          requires: [GRPC],
          text: 'x-token for Dragons Mouth authentication.',
        }),
        opt('--commitment', 'DRAGONSMOUTH_COMMITMENT', {
          type: 'processed | confirmed | finalized',
          default: 'finalized',
          requires: [GRPC, FUMAROLE, RPC, BIGTABLE],
          text: 'Commitment level of the stream or RPC queries. RPC block discovery does not support `processed` and uses `confirmed` instead.',
        }),
        opt('--dragonsmouth-from-slot', 'DRAGONSMOUTH_FROM_SLOT', {
          type: FROM_SLOT_SPEC,
          requires: [GRPC],
          text: "Starting slot for replay. An integer, or `'*'` for the latest slot already in `blocks_metadata` (quote it in YAML). Rejected by every other source.",
        }),
        opt('--grpc-max-decoding-bytes', 'GRPC_MAX_DECODING_BYTES', {
          type: 'usize (bytes)',
          default: '67108864',
          requires: [GRPC, FUMAROLE],
          text: 'Largest gRPC message the client decodes. Must be greater than 0.',
        }),
        opt('--grpc-http2-adaptive-window', 'GRPC_HTTP2_ADAPTIVE_WINDOW', {
          type: 'bool',
          default: 'false',
          requires: [GRPC],
          text: 'Enable HTTP/2 adaptive window sizing on the gRPC channel.',
        }),
        opt('--grpc-idle-timeout-secs', 'GRPC_IDLE_TIMEOUT_SECS', {
          type: 'u64 (s)',
          default: '30',
          requires: [GRPC, FUMAROLE],
          text: 'Exit if no gRPC messages arrive for this many seconds. Must be greater than 0.',
        }),
        opt('--grpc-health-watch-enabled', 'GRPC_HEALTH_WATCH_ENABLED', {
          type: 'bool',
          default: 'true',
          requires: [GRPC],
          text: 'Watch the gRPC health service and exit if the stream health degrades.',
        }),
        opt('--grpc-slot-notifications', 'GRPC_SLOT_NOTIFICATIONS', {
          type: 'bool',
          default: 'true',
          requires: [GRPC],
          text: 'Subscribe to slot notifications on the stream to populate the chain-tip lag metric.',
        }),
      ],
    },
    {
      id: 'fumarole',
      title: 'Fumarole',
      intro: 'Used when `--source fumarole`. `--commitment`, `--grpc-max-decoding-bytes` and `--grpc-idle-timeout-secs` (listed under Yellowstone gRPC) also apply.',
      requires: [FUMAROLE],
      items: [
        opt('--fumarole-endpoint', 'FUMAROLE_ENDPOINT', {
          type: 'url',
          required: true,
          text: 'Fumarole endpoint.',
        }),
        opt('--fumarole-x-token', 'FUMAROLE_X_TOKEN', { type: 'string', secret: true, text: 'x-token for Fumarole authentication.' }),
        opt('--fumarole-consumer-group', 'FUMAROLE_CONSUMER_GROUP', {
          type: 'string',
          required: true,
          text: 'Name of the Fumarole persistent consumer group.',
        }),
        opt('--fumarole-create-consumer-group', 'FUMAROLE_CREATE_CONSUMER_GROUP', {
          type: 'bool',
          default: 'false',
          text: 'Create the consumer group before subscribing.',
        }),
        opt('--fumarole-from-slot', 'FUMAROLE_FROM_SLOT', {
          type: FROM_SLOT_SPEC,
          requires: ['when:FUMAROLE_CREATE_CONSUMER_GROUP=true'],
          text: "Starting slot for consumer group initialization; only used when the group is created. An integer, or `'*'` for the latest slot already in `blocks_metadata` (quote it in YAML). Rejected by every other source.",
        }),
        opt('--fumarole-data-plane-tcp-connections', 'FUMAROLE_DATA_PLANE_TCP_CONNECTIONS', {
          type: 'u8',
          default: '4',
          text: 'Number of data-plane TCP connections. Must be between 1 and 20.',
        }),
        opt('--fumarole-concurrent-download-limit-per-tcp', 'FUMAROLE_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP', {
          type: 'usize',
          default: '1',
          status: 'deprecated',
          text: 'Accepted for compatibility. Values other than 1 are ignored because the Fumarole client fixes this concurrency at 1; 0 is rejected.',
        }),
        opt('--fumarole-data-channel-capacity', 'FUMAROLE_DATA_CHANNEL_CAPACITY', {
          type: 'usize',
          default: '4096',
          text: 'Capacity of the stream output channel. Must be greater than 0.',
        }),
        opt('--fumarole-memory-soft-limit-bytes', 'FUMAROLE_MEMORY_SOFT_LIMIT_BYTES', {
          type: 'u64 (bytes)',
          default: '25769803776',
          text: 'Memory pressure soft limit of the backpressure guard (24 GiB by default). 0 disables it.',
        }),
        opt('--fumarole-commit-interval-secs', 'FUMAROLE_COMMIT_INTERVAL_SECS', {
          type: 'u64 (s)',
          default: '10',
          text: 'Interval between offset commits. Must be greater than 0.',
        }),
        opt('--fumarole-no-commit', 'FUMAROLE_NO_COMMIT', { type: 'bool', default: 'false', text: 'Disable Fumarole offset commits.' }),
      ],
    },
    {
      id: 'rpc',
      title: 'JSON-RPC backfill',
      intro: 'Used when `--source rpc`. `--commitment` also applies, and `--rpc-url` and `--rpc-max-supported-tx-version` are shared with Bigtable as noted.',
      items: [
        opt('--rpc-url', 'RPC_URL', {
          type: 'url',
          requires: [RPC, BIGTABLE],
          required: 'when `--source rpc`, or when `--bigtable-range` uses epochs',
          text: 'Solana JSON-RPC URL. Bigtable uses it for the epoch schedule.',
        }),
        opt('--rpc-from-slot', 'RPC_FROM_SLOT', {
          type: FROM_SLOT_SPEC,
          requires: [RPC],
          required: 'unless `--rpc-slot-list` is set',
          text: "First slot of the range. An integer (`0` for the earliest available), or `'*'` for the latest slot already in `blocks_metadata` (quote it in YAML). Rejected by every other source.",
        }),
        opt('--rpc-to-slot', 'RPC_TO_SLOT', {
          type: 'u64',
          requires: [RPC],
          required: 'unless `--rpc-slot-count` or `--rpc-slot-list` is set',
          text: 'Last slot of the range, inclusive.',
          relations: [{ type: 'conflicts', to: 'RPC_SLOT_COUNT' }],
        }),
        opt('--rpc-slot-count', 'RPC_SLOT_COUNT', {
          type: 'u64',
          requires: [RPC],
          required: 'unless `--rpc-to-slot` or `--rpc-slot-list` is set',
          text: 'Number of slots to fetch from the start slot. Must be greater than 0.',
        }),
        opt('--rpc-slot-list', 'RPC_SLOT_LIST', {
          type: 'path',
          requires: [RPC],
          text: 'File of whitespace-separated slots. Fetches exactly those slots with `getBlock` and skips `getBlocks` discovery.',
          relations: [
            { type: 'conflicts', to: 'RPC_FROM_SLOT' },
            { type: 'conflicts', to: 'RPC_TO_SLOT' },
            { type: 'conflicts', to: 'RPC_SLOT_COUNT' },
            { type: 'conflicts', to: 'RPC_SKIP_INGESTED_SLOTS' },
          ],
        }),
        opt('--rpc-skip-ingested-slots', 'RPC_SKIP_INGESTED_SLOTS', {
          type: 'bool',
          default: 'false',
          requires: [RPC],
          text: 'During discovery, skip slots already in `blocks_metadata`, so a re-run over the same range backfills only the gaps.',
        }),
        opt('--rpc-timeout-secs', 'RPC_TIMEOUT_SECS', { type: 'u64 (s)', default: '30', requires: [RPC], text: 'Timeout of one RPC request.' }),
        opt('--rpc-retry-backoff-ms', 'RPC_RETRY_BACKOFF_MS', { type: 'u64 (ms)', default: '500', requires: [RPC], text: 'Backoff between RPC request retries.' }),
        opt('--rpc-max-inflight', 'RPC_MAX_INFLIGHT', {
          type: 'usize',
          default: '64',
          requires: [RPC],
          text: 'Maximum in-flight `getBlock` requests. Must be greater than 0.',
        }),
        opt('--rpc-max-supported-tx-version', 'RPC_MAX_SUPPORTED_TX_VERSION', {
          type: 'u8',
          default: '1',
          requires: [RPC, BIGTABLE],
          text: 'Highest transaction version requested from `getBlock` and accepted when decoding Bigtable blocks. The CLI help says 0, but the code default is 1.',
        }),
        opt('--rpc-flush-every-slots', 'RPC_FLUSH_EVERY_SLOTS', {
          type: 'u64',
          default: '500',
          requires: [RPC],
          text: 'Flush inserts every N slots. Must be greater than 0.',
        }),
        opt('--rpc-progress-every-slots', 'RPC_PROGRESS_EVERY_SLOTS', {
          type: 'u64',
          default: '100',
          requires: [RPC],
          text: 'Log progress every N slots. Must be greater than 0.',
        }),
        opt('--rpc-discovery-chunk-slots', 'RPC_DISCOVERY_CHUNK_SLOTS', {
          type: 'u64',
          default: '10000',
          requires: [RPC],
          text: 'Slot range size of each discovery request. Must be greater than 0.',
        }),
      ],
    },
    {
      id: 'bigtable',
      title: 'Bigtable',
      intro: 'Used when `--source bigtable`. Exactly one of `--bigtable-range` or `--bigtable-slot-file` is required. `--commitment`, `--rpc-url` and `--rpc-max-supported-tx-version` (listed above) also apply.',
      requires: [BIGTABLE],
      items: [
        opt('--bigtable-range', 'BIGTABLE_RANGE', {
          type: 'string',
          required: 'unless `--bigtable-slot-file` is set',
          text: 'Range spec: slots `123:456`, epochs `1-10`, or a single epoch `5`.',
          relations: [
            { type: 'conflicts', to: 'BIGTABLE_SLOT_FILE' },
            { type: 'see', to: 'RPC_URL' },
          ],
        }),
        opt('--bigtable-slot-file', 'BIGTABLE_SLOT_FILE', {
          type: 'path',
          required: 'unless `--bigtable-range` is set',
          text: 'File of whitespace-separated slot numbers.',
        }),
        opt('--bigtable-instance', 'BIGTABLE_INSTANCE', { type: 'string', default: 'solana-ledger', text: 'Bigtable instance name.' }),
        opt('--bigtable-app-profile', 'BIGTABLE_APP_PROFILE', { type: 'string', default: 'default', text: 'Bigtable app profile id.' }),
        opt('--bigtable-timeout-secs', 'BIGTABLE_TIMEOUT_SECS', {
          type: 'u64 (s)',
          text: 'Request timeout. Unset leaves the client default; 0 is rejected.',
        }),
        opt('--bigtable-max-message-bytes', 'BIGTABLE_MAX_MESSAGE_BYTES', {
          type: 'usize (bytes)',
          default: '67108864',
          text: 'Largest gRPC message accepted. Must be greater than 0.',
        }),
        opt('--bigtable-credential-path', 'BIGTABLE_CREDENTIAL_PATH', {
          type: 'path',
          text: 'Path of the Bigtable credential JSON file.',
          relations: [{ type: 'conflicts', to: 'BIGTABLE_CREDENTIAL_JSON' }],
        }),
        opt('--bigtable-credential-json', 'BIGTABLE_CREDENTIAL_JSON', {
          type: 'string',
          secret: true,
          text: 'Bigtable credential JSON as a string.',
        }),
        opt('--bigtable-discovery-limit', 'BIGTABLE_DISCOVERY_LIMIT', {
          type: 'usize',
          default: '10000',
          text: 'Maximum slots fetched per discovery call. Must be greater than 0.',
        }),
        opt('--bigtable-fetch-batch-size', 'BIGTABLE_FETCH_BATCH_SIZE', {
          type: 'usize',
          default: '500',
          text: 'Maximum slots per multi-row fetch. Must be greater than 0.',
        }),
        opt('--bigtable-fetch-concurrency', 'BIGTABLE_FETCH_CONCURRENCY', {
          type: 'usize',
          default: '4',
          text: 'Maximum in-flight fetch batches. Must be greater than 0.',
        }),
        opt('--bigtable-insert-concurrency', 'BIGTABLE_INSERT_CONCURRENCY', {
          type: 'usize',
          default: '1',
          text: 'Maximum in-flight insert batches. Must be greater than 0.',
        }),
        opt('--bigtable-decode-concurrency', 'BIGTABLE_DECODE_CONCURRENCY', {
          type: 'usize',
          default: 'CPU count',
          text: 'Maximum in-flight decode tasks. The default is the available CPU parallelism (4 if it cannot be read), not the 8 in `superbank.example.yaml`. Must be greater than 0.',
        }),
        opt('--bigtable-progress-every-slots', 'BIGTABLE_PROGRESS_EVERY_SLOTS', {
          type: 'u64',
          default: '10000',
          text: 'Log progress every N slots. Must be greater than 0.',
        }),
      ],
    },
    {
      id: 'solparq',
      title: 'solparq restore',
      intro:
        'Used when `--source solparq`: restores a solparq Parquet archive into ClickHouse. The `SOLPARQ_ARCHIVE_S3_*` env names are shared with superbank-solparq on purpose, but `SOLPARQ_ARCHIVE_LOCATION` and `SOLPARQ_ARCHIVE_PATH` differ from the writer\'s names. Rows go to the configured `--clickhouse-database` under each archived table\'s own name.',
      requires: [SOLPARQ],
      items: [
        opt('--solparq-archive-location', 'SOLPARQ_ARCHIVE_LOCATION', {
          type: 'local | s3',
          required: true,
          text: 'Where the archive to restore lives.',
        }),
        opt('--solparq-archive-path', 'SOLPARQ_ARCHIVE_PATH', {
          type: 'path',
          requires: ['when:SOLPARQ_ARCHIVE_LOCATION=local'],
          required: true,
          text: 'A solparq bundle directory, or a directory containing bundles.',
        }),
        opt('--solparq-archive-s3-endpoint', 'SOLPARQ_ARCHIVE_S3_ENDPOINT', {
          type: 'url',
          requires: [S3],
          required: true,
          text: 'S3 endpoint of the archive.',
        }),
        opt('--solparq-archive-s3-bucket-name', 'SOLPARQ_ARCHIVE_S3_BUCKET_NAME', {
          type: 'string',
          requires: [S3],
          required: true,
          text: 'S3 bucket name of the archive.',
        }),
        opt('--solparq-archive-s3-bucket-path', 'SOLPARQ_ARCHIVE_S3_BUCKET_PATH', {
          type: 'string',
          requires: [S3],
          text: 'Path or prefix inside the bucket. Empty uses the bucket root.',
        }),
        opt('--solparq-archive-s3-auth-key', 'SOLPARQ_ARCHIVE_S3_AUTH_KEY', {
          type: 'string',
          requires: [S3],
          required: true,
          text: 'S3 access key.',
        }),
        opt('--solparq-archive-s3-auth-secret-key', 'SOLPARQ_ARCHIVE_S3_AUTH_SECRET_KEY', {
          type: 'string',
          secret: true,
          requires: [S3],
          required: true,
          text: 'S3 secret key.',
        }),
        opt('--solparq-archive-s3-region', 'SOLPARQ_ARCHIVE_S3_REGION', {
          type: 'string',
          default: 'us-east-1',
          requires: [S3],
          text: 'S3 region.',
        }),
        opt('--solparq-from-slot', 'SOLPARQ_FROM_SLOT', {
          type: 'u64',
          text: 'Restore only bundles and rows at or after this slot, inclusive. Must be less than or equal to the to-slot.',
          relations: [{ type: 'see', to: 'SOLPARQ_TO_SLOT' }],
        }),
        opt('--solparq-to-slot', 'SOLPARQ_TO_SLOT', { type: 'u64', text: 'Restore only bundles and rows at or before this slot, inclusive.' }),
        opt('--solparq-tables', 'SOLPARQ_TABLES', {
          type: 'list',
          text: 'Restrict the restore to these archived table kinds, for example `transactions` and `blocks_metadata`. Comma-separated string on the CLI and in the env var, but a YAML list in the config file. Default is every table in the bundle.',
        }),
        opt('--solparq-clickhouse-settings', 'SOLPARQ_CLICKHOUSE_SETTINGS', {
          type: 'string',
          default: '',
          text: 'Raw ClickHouse `SETTINGS` clause body appended to restore statements, to bound insert memory. Empty omits the clause.',
        }),
      ],
    },
    {
      id: 'clickhouse',
      title: 'ClickHouse connection',
      items: [
        opt('--clickhouse-url', 'CLICKHOUSE_URL', { type: 'url', default: 'http://localhost:8123', text: 'ClickHouse HTTP URL.' }),
        opt('--clickhouse-database', 'CLICKHOUSE_DATABASE', { type: 'string', default: 'default', text: 'ClickHouse database name.' }),
        opt('--clickhouse-user', 'CLICKHOUSE_USER', { type: 'string', default: 'default', text: 'ClickHouse user.' }),
        opt('--clickhouse-password', 'CLICKHOUSE_PASSWORD', { type: 'string', default: '', secret: true, text: 'ClickHouse password. Empty by default.' }),
        opt('--clickhouse-async-insert', 'CLICKHOUSE_ASYNC_INSERT', { type: 'bool', default: 'false', text: 'Enable ClickHouse async inserts.' }),
      ],
    },
    {
      id: 'tables',
      title: 'Target tables',
      intro: 'Table names for the live and backfill sources. The solparq source writes each archived table to `--clickhouse-database` under its own name instead.',
      items: [
        opt('--transactions-table', 'CLICKHOUSE_TRANSACTIONS_TABLE', {
          type: 'string',
          default: 'default.transactions',
          requires: [GRPC, FUMAROLE, RPC, BIGTABLE],
          text: 'Target transactions table. The default is the distributed table.',
        }),
        opt('--blocks-table', 'CLICKHOUSE_BLOCKS_TABLE', {
          type: 'string',
          default: 'default.blocks_metadata',
          requires: [GRPC, FUMAROLE, RPC, BIGTABLE],
          text: 'Target blocks metadata table. The default is the distributed table. Also queried for the latest slot when a from-slot is `*`.',
        }),
        opt('--entries-table', 'CLICKHOUSE_ENTRIES_TABLE', {
          type: 'string',
          default: 'default.entries',
          requires: [GRPC, FUMAROLE],
          text: 'PoH entries table. Only the gRPC and Fumarole live sources write entries; they subscribe to entries only when this is set.',
        }),
      ],
    },
    {
      id: 'flush',
      title: 'Flushing and insert retries',
      intro: 'Buffering thresholds and retry policy for ClickHouse inserts. Which options apply depends on the source, as noted on each.',
      items: [
        opt('--transactions-flush-rows', 'TRANSACTIONS_FLUSH_ROWS', {
          type: 'usize',
          default: '25000',
          requires: [GRPC, RPC, BIGTABLE],
          text: 'Flush when this many transaction rows are buffered. The gRPC source applies it to entry rows too.',
        }),
        opt('--blocks-flush-rows', 'BLOCKS_FLUSH_ROWS', {
          type: 'usize',
          default: '2000',
          requires: [GRPC, RPC, BIGTABLE],
          text: 'Flush when this many block rows are buffered.',
        }),
        opt('--flush-interval-secs', 'FLUSH_INTERVAL_SECS', {
          type: 'u64 (s)',
          default: '5',
          requires: [GRPC, FUMAROLE, RPC, BIGTABLE],
          text: 'Periodic flush interval.',
        }),
        opt('--flush-every-block', 'FLUSH_EVERY_BLOCK', {
          type: 'bool',
          default: 'false',
          requires: [GRPC, BIGTABLE],
          text: 'Flush after every block, which disables batching. The RPC source flushes by slot count instead.',
          relations: [
            { type: 'see', to: 'TRANSACTIONS_FLUSH_ROWS' },
            { type: 'see', to: 'BLOCKS_FLUSH_ROWS' },
            { type: 'see', to: 'FLUSH_INTERVAL_SECS' },
            { type: 'see', to: 'RPC_FLUSH_EVERY_SLOTS' },
          ],
        }),
        opt('--insert-max-retries', 'CLICKHOUSE_INSERT_MAX_RETRIES', {
          type: 'u32',
          default: '5',
          requires: [GRPC, RPC, BIGTABLE],
          text: 'Maximum insert retry attempts before giving up.',
        }),
        opt('--insert-retry-base-ms', 'CLICKHOUSE_INSERT_RETRY_BASE_MS', {
          type: 'u64 (ms)',
          default: '1000',
          requires: [GRPC, RPC, BIGTABLE],
          text: 'Initial backoff between insert retries.',
          relations: [{ type: 'capped-by', to: 'CLICKHOUSE_INSERT_RETRY_MAX_MS' }],
        }),
        opt('--insert-retry-max-ms', 'CLICKHOUSE_INSERT_RETRY_MAX_MS', {
          type: 'u64 (ms)',
          default: '30000',
          requires: [GRPC, RPC, BIGTABLE],
          text: 'Maximum backoff cap for insert retries.',
        }),
      ],
    },
    {
      id: 'metrics',
      title: 'Metrics and health',
      items: [
        opt('--metrics-host', 'METRICS_HOST', { type: 'string', default: '0.0.0.0', text: 'Address the Prometheus metrics and `/health` server binds.' }),
        opt('--metrics-port', 'METRICS_PORT', { type: 'u16', default: '9901', text: 'Port the Prometheus metrics and `/health` server binds.' }),
        opt('--health-stale-secs', 'HEALTH_STALE_SECS', {
          type: 'u64 (s)',
          default: '120',
          text: '`/health` returns 503 when the last successful ClickHouse flush is older than this. 0 disables the check.',
        }),
        opt('--metrics-cluster-label', 'METRICS_CLUSTER_LABEL', { type: 'string', text: 'Static `cluster` label added to all Prometheus metrics. Unset adds none.' }),
      ],
    },
  ],
};
