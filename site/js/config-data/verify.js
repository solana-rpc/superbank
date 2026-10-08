// superbank-verify configuration. Checked against CliArgs and FileConfig in
// crates/superbank-verify/src/cli.rs, including validate_args, and the log
// filter in src/main.rs. FileConfig has no rename_all, so each YAML key is the
// kebab-case name given by its explicit serde rename (snake_case is accepted
// as an alias). tests/site/config.test.mjs enforces env/flag/YAML parity.
//
// Plain text only: `code` spans use backticks, no markup.

const CLI = 'crates/superbank-verify/src/cli.rs';
const MAIN = 'crates/superbank-verify/src/main.rs';

const kebab = (name) => name.toLowerCase().replaceAll('_', '-');
// SUPERBANK_VERIFY_{NAME}: flag --name, YAML key name (all kebab case).
const opt = (name, rest) => ({ env: `SUPERBANK_VERIFY_${name}`, flag: `--${kebab(name)}`, yaml: kebab(name), ...rest });
// Shared env name, flag and YAML key spelled out.
const shared = (env, flag, rest) => ({ env, flag, yaml: flag.slice(2), ...rest });
// Read outside clap: env only, no flag or YAML key.
const envOnly = (env, source, rest) => ({ env, source, ...rest });

export default {
  id: 'verify',
  label: 'superbank-verify',
  summary: 'Proof-of-History validator that re-checks ledger data stored in ClickHouse: entry hash chains, tick counts, blockhashes and anchors.',
  source: CLI,
  readme: 'crates/superbank-verify/README.md',
  primary: 'flag',
  intro:
    'Precedence is flag, then env var, then the YAML file (`--config`), then the default. Unknown YAML keys are rejected. Boolean flags take `--flag` or `--flag=false` (the value needs `=`). `--range` and `--full` form one selector: setting either by flag or env ignores the file\'s `range` and `full` pair entirely.',
  groups: [
    {
      id: 'run',
      title: 'Config and range selection',
      items: [
        {
          env: 'SUPERBANK_VERIFY_CONFIG',
          flag: '--config',
          type: 'path',
          text: 'YAML config file. Keys use the flag names without the leading dashes.',
        },
        opt('RANGE', {
          type: 'string',
          required: 'unless `SUPERBANK_VERIFY_FULL=true` or a fixture export is requested',
          text: 'Range to verify: `{start}:{end}` slots, `{a}-{b}` epochs, or `{e}` for one epoch. Exactly one of this and `SUPERBANK_VERIFY_FULL` is required, except when exporting a fixture.',
          relations: [{ type: 'conflicts', to: 'SUPERBANK_VERIFY_FULL' }],
        }),
        opt('FULL', {
          type: 'bool',
          default: 'false',
          text: 'Verify everything present in `blocks_metadata`, genesis to tip. Mutually exclusive with `SUPERBANK_VERIFY_RANGE`.',
        }),
      ],
    },
    {
      id: 'verification',
      title: 'Verification',
      items: [
        opt('MODE', {
          type: 'structural | full',
          default: 'structural',
          text: '`structural` checks chain and counts without recomputing hashes; `full` recomputes every hash.',
        }),
        opt('SLOTS_PER_EPOCH', { type: 'u64', default: '432000', text: 'Slots per normal epoch, used to resolve epoch ranges.' }),
        opt('EPOCH_WARMUP', {
          type: 'bool',
          default: 'true',
          text: 'Whether the cluster uses warmup epochs (true for mainnet-beta).',
        }),
        opt('TICKS_PER_SLOT', { type: 'u64', default: '64', text: 'Ticks per slot; 64 for all of mainnet history. Must be at least 1.' }),
        opt('EXPECTED_GENESIS_HASH', {
          type: 'string (base58)',
          default: '5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d',
          text: "Expected genesis blockhash, checked against slot 0's `parent_blockhash`. An empty string disables the check; any other value must decode to 32 bytes.",
        }),
        opt('HASHES_PER_TICK_SCHEDULE', {
          type: 'string',
          text: 'The `hashes_per_tick` eras as `{from_slot}:{value},...`; 0 disables the tick-hash-count check for that era. Unset uses the built-in mainnet history.',
        }),
        opt('ALPENGLOW_RPC_URL', {
          type: 'url',
          text: 'Trusted Agave 4.3+ RPC on the same cluster as the stored data, asked for `getAgGenesisCert`. The certificate slot is the last PoH slot; Alpenglow entry rules apply after it. Without a boundary every slot is checked with PoH rules.',
          relations: [{ type: 'see', to: 'SUPERBANK_VERIFY_ALPENGLOW_GENESIS_BLOCK' }],
        }),
        opt('ALPENGLOW_GENESIS_BLOCK', {
          type: 'string ({slot}:{base58-block-id})',
          text: "Offline Alpenglow boundary from the genesis certificate. With `--alpenglow-rpc-url` also set, the fetched certificate must match it. The boundary is part of checkpoint identity, so changing or removing it rejects `--resume`.",
        }),
        opt('ANCHOR', {
          flag: '--anchor',
          yaml: 'anchor',
          type: 'list',
          text: 'External trust anchor `{slot}:{base58-blockhash}`, checked against the recorded blockhash. Repeat the flag or comma-separate the env value; a slot may appear only once. A flag or env value replaces the YAML list (YAML also accepts the key `anchors`).',
        }),
        opt('AUDIT_DUPLICATE_CONFLICTS', {
          type: 'bool',
          default: 'false',
          text: 'Forensic audit that scans raw ClickHouse rows for conflicting duplicates and reports them as `duplicate_conflict`. Adds extra scans for every window.',
        }),
        opt('VERIFY_THREADS', {
          type: 'usize',
          default: '0',
          text: 'Worker threads for hash recomputation; 0 uses all available cores.',
        }),
        opt('MAX_FAILURES', { type: 'u64', default: '0', text: 'Abort after this many failed slots; 0 is unlimited.' }),
        opt('ALLOW_UNVERIFIABLE', {
          type: 'bool',
          default: 'false',
          text: 'Exit 0 even when unverifiable or missing slots remain.',
        }),
      ],
    },
    {
      id: 'window',
      title: 'Windowing',
      intro:
        'The verifier bounds slots, not bytes. Besides the per-option limits, `window-slots * (fetch-ahead + 2)` must be at most 512 (one active window, queued windows, and one fetch blocked on channel capacity).',
      items: [
        opt('WINDOW_SLOTS', {
          type: 'u64',
          default: '64',
          text: 'Slots fetched and verified per window. Between 1 and 128.',
          relations: [{ type: 'see', to: 'SUPERBANK_VERIFY_FETCH_AHEAD' }],
        }),
        opt('FETCH_AHEAD', {
          type: 'usize',
          default: '1',
          text: 'Complete windows queued ahead of verification. Between 1 and 2.',
        }),
      ],
    },
    {
      id: 'resume',
      title: 'Checkpoints and reports',
      items: [
        opt('CHECKPOINT_FILE', { type: 'path', text: 'Checkpoint file that makes runs resumable.' }),
        opt('RESUME', {
          type: 'bool',
          default: 'false',
          text: 'Resume from the checkpoint file instead of starting over. Startup fails if no checkpoint file is configured.',
          relations: [{ type: 'requires', to: 'SUPERBANK_VERIFY_CHECKPOINT_FILE' }],
        }),
        opt('REPORT_FILE', {
          type: 'path',
          text: 'Write findings as JSONL to this file. Appended on resume, truncated otherwise.',
        }),
        opt('PROGRESS_EVERY_SLOTS', { type: 'u64', default: '10000', text: 'Emit a progress log line roughly every this many slots.' }),
      ],
    },
    {
      id: 'clickhouse',
      title: 'ClickHouse',
      items: [
        shared('CLICKHOUSE_URL', '--clickhouse-url', { type: 'url', default: 'http://localhost:8123', text: 'ClickHouse HTTP URL.' }),
        shared('CLICKHOUSE_DATABASE', '--clickhouse-database', { type: 'string', default: 'default', text: 'ClickHouse database.' }),
        shared('CLICKHOUSE_USER', '--clickhouse-user', { type: 'string', default: 'default', text: 'ClickHouse user.' }),
        shared('CLICKHOUSE_PASSWORD', '--clickhouse-password', { type: 'string', default: '', secret: true, text: 'ClickHouse password.' }),
        opt('BLOCKS_TABLE', { type: 'string', default: 'default.blocks_metadata', text: 'Blocks metadata table, database-qualified.' }),
        opt('ENTRIES_TABLE', { type: 'string', default: 'default.entries', text: 'Entries table, database-qualified.' }),
        opt('TRANSACTIONS_TABLE', { type: 'string', default: 'default.transactions', text: 'Transactions table, database-qualified.' }),
      ],
    },
    {
      id: 'metrics',
      title: 'Metrics and health',
      items: [
        shared('METRICS_HOST', '--metrics-host', { type: 'string', default: '0.0.0.0', text: 'Address the metrics server binds.' }),
        shared('METRICS_PORT', '--metrics-port', { type: 'u16', default: '9902', text: 'Port the metrics server binds; serves `/metrics` and `/health`.' }),
        opt('HEALTH_STALE_SECS', {
          type: 'u64 (s)',
          default: '300',
          text: '`/health` returns 503 when no window completed for this many seconds; 0 disables.',
        }),
        opt('METRICS_CLUSTER_LABEL', { type: 'string', text: 'Optional cluster label added to all metrics.' }),
      ],
    },
    {
      id: 'fixture',
      title: 'Fixture export',
      intro: 'Exports one slot as a JSON test fixture and exits. Both options are needed together, and neither has a YAML key.',
      items: [
        {
          env: 'SUPERBANK_VERIFY_EXPORT_FIXTURE_SLOT',
          flag: '--export-fixture-slot',
          type: 'u64',
          text: "Export one slot's block, entries and signatures as a JSON test fixture and exit. With this set, neither `--range` nor `--full` is needed.",
          relations: [{ type: 'requires', to: 'SUPERBANK_VERIFY_EXPORT_FIXTURE_OUT' }],
        },
        {
          env: 'SUPERBANK_VERIFY_EXPORT_FIXTURE_OUT',
          flag: '--export-fixture-out',
          type: 'path',
          text: 'Output path for the exported fixture.',
          relations: [{ type: 'requires', to: 'SUPERBANK_VERIFY_EXPORT_FIXTURE_SLOT' }],
        },
      ],
    },
    {
      id: 'logging',
      title: 'Logging',
      items: [
        envOnly('RUST_LOG', MAIN, {
          type: 'tracing filter',
          text: 'Log level filter (`tracing_subscriber` `EnvFilter` syntax) and the only control of the log level. Unset logs errors only, so set it (for example `info`) to see progress lines.',
        }),
      ],
    },
  ],
};
