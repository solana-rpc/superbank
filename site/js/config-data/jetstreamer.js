// jetstreamer-clickhouse configuration. Checked against ClickhouseIngestConfig
// and apply_env_overrides in ingest/jetstreamer-clickhouse-plugin/src/lib.rs
// and the runner in src/bin/jetstreamer-clickhouse.rs. Every option is an env
// var; the binary has no flags and reads no YAML. tests/site/config.test.mjs
// enforces that every env name read in the crate is listed here.
//
// Plain text only: `code` spans use backticks, no markup.

const LIB = 'ingest/jetstreamer-clickhouse-plugin/src/lib.rs';
const BIN = 'ingest/jetstreamer-clickhouse-plugin/src/bin/jetstreamer-clickhouse.rs';

const envOnly = (env, source, rest) => ({ env, source, ...rest });
const ch = (name, rest) => envOnly(`JETSTREAMER_CLICKHOUSE_${name}`, LIB, rest);

export default {
  id: 'jetstreamer',
  label: 'jetstreamer-clickhouse',
  summary: 'Standalone plugin binary that replays Old Faithful epochs through Jetstreamer into ClickHouse. A separate Cargo workspace, not built with the root one.',
  source: LIB,
  readme: 'ingest/jetstreamer-clickhouse-plugin/README.md',
  primary: 'env',
  intro:
    'Takes one positional argument, `{epoch}` or `{start}:{end}` (an inclusive slot range). Env vars override the built-in config, and an invalid value is logged and ignored. Booleans accept 1, true, yes, on and 0, false, no, off. The `ClickhouseIngestConfig` fields `single_node`, `database`, the table names and `validate_schema` can only be set from Rust code, not through this binary, so it writes to `default.transactions`, `default.blocks_metadata` and `default.entries` in clustered mode.',
  groups: [
    {
      id: 'runner',
      title: 'Runner',
      items: [
        envOnly('JETSTREAMER_THREADS', BIN, {
          type: 'usize',
          default: 'auto',
          text: 'Firehose worker threads. Unset or unparsable uses the thread count Jetstreamer computes for the machine; it also sizes the plugin.',
        }),
        envOnly('JETSTREAMER_INGEST_CLICKHOUSE_DSN', LIB, {
          type: 'url',
          secret: true,
          text: "DSN of the ingest ClickHouse cluster. When set, the plugin writes there instead of using the runner's ClickHouse client; a user and password in the DSN are split out and sent as credentials.",
        }),
      ],
    },
    {
      id: 'batching',
      title: 'Flushing and batching',
      items: [
        ch('FLUSH_MAX_ROWS', { type: 'u64', default: '100000', text: 'Max rows per ClickHouse insert batch.' }),
        ch('FLUSH_MAX_BYTES', { type: 'u64 (bytes)', default: '67108864', text: 'Max bytes per ClickHouse insert batch.' }),
        ch('FLUSH_INTERVAL_MS', { type: 'u64 (ms)', default: '10000', text: 'Periodic flush interval.' }),
        ch('MAX_INFLIGHT_BATCHES', {
          type: 'usize',
          default: '8',
          text: 'Max concurrent insert workers per thread. Also sizes the inbound queue.',
        }),
        ch('PENDING_TX_CAPACITY', {
          type: 'usize',
          default: '4096',
          text: 'Reserved capacity for per-slot transaction and entry buffers.',
        }),
      ],
    },
    {
      id: 'inserts',
      title: 'Inserts and retries',
      items: [
        ch('ASYNC_INSERT', {
          type: 'bool',
          default: 'true',
          text: 'Use server-side async inserts. Unlike the ingestor, this defaults to on.',
          relations: [
            { type: 'see', to: 'JETSTREAMER_CLICKHOUSE_WAIT_FOR_ASYNC_INSERT' },
            { type: 'see', to: 'superbank:CLICKHOUSE_ASYNC_INSERT' },
          ],
        }),
        ch('WAIT_FOR_ASYNC_INSERT', { type: 'bool', default: 'false', text: 'Wait for an async insert to complete before returning.' }),
        ch('RETRY_MAX', { type: 'usize', default: '5', text: 'Max retries for a failed insert.' }),
        ch('RETRY_BACKOFF_MS', { type: 'u64 (ms)', default: '50', text: 'Base backoff between retries.' }),
        ch('INSERT_SEND_TIMEOUT_MS', {
          type: 'u64 (ms)',
          default: '10000',
          text: 'Timeout for sending insert data chunks to ClickHouse; 0 disables.',
        }),
        ch('INSERT_END_TIMEOUT_MS', {
          type: 'u64 (ms)',
          default: '60000',
          text: 'Timeout for finalizing an insert request; 0 disables.',
        }),
      ],
    },
  ],
};
