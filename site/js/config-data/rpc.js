// superbank-rpc configuration. Checked against RpcConfig (clap) in
// crates/superbank-rpc/src/config.rs, the validation in src/server.rs and the
// direct std::env reads listed with their own `source`. Feature gates follow
// the #[cfg(feature)] on each field; tests/site/config.test.mjs enforces them
// and that every env name read in the crate is listed here.
//
// Plain text only: `code` spans use backticks, no markup.

const CONFIG = 'crates/superbank-rpc/src/config.rs';
const CLIENT = 'crates/superbank-rpc/src/clickhouse/client.rs';
const UTIL = 'crates/superbank-rpc/src/clickhouse/util.rs';
const CACHE = 'crates/superbank-rpc/src/clickhouse/cache.rs';
const HANDLERS = 'crates/superbank-rpc/src/handlers/mod.rs';
const MAIN = 'crates/superbank-rpc/src/main.rs';

// Clap field whose flag is the env name in kebab case.
const opt = (env, rest) => ({ env, flag: `--${env.toLowerCase().replaceAll('_', '-')}`, ...rest });
// Read with std::env::var outside clap: env only, no flag.
const envOnly = (env, source, rest) => ({ env, source, ...rest });
const SHARD_DIRECT = 'when:CLICKHOUSE_SCOPE=shard-direct';

export default {
  id: 'rpc',
  label: 'superbank-rpc',
  summary: 'JSON-RPC server over ClickHouse, with optional head cache, disk cache and gRPC streaming.',
  source: CONFIG,
  readme: 'crates/superbank-rpc/README.md',
  primary: 'env',
  intro:
    'Each option is a flag or an env var (flag wins). Feature-gated options only exist in builds with that Cargo feature (`cargo build -p superbank-rpc --features …`); passing one to a build without it is a startup error. A few tuning knobs are env-only. The YAML file only carries request filters.',
  groups: [
    {
      id: 'config-file',
      title: 'Config file',
      items: [
        opt('SUPERBANK_CONFIG', {
          flag: '--config',
          type: 'path',
          text: 'Shared YAML file, the same one the ingestor reads. superbank-rpc only uses its `rpc-parameter-filters` key.',
        }),
        {
          yaml: 'rpc-parameter-filters',
          type: 'list of [method, ...params]',
          text: 'Exact request filters. Each entry is a method followed by its complete params array; a matching call returns HTTP 405 with a JSON-RPC `Method not allowed` error and never reaches a handler. The ingestor accepts and ignores this key so both can share one file.',
          relations: [{ type: 'requires', to: 'SUPERBANK_CONFIG' }],
        },
      ],
    },
    {
      id: 'server',
      title: 'Server and request limits',
      items: [
        opt('RPC_HOST', { flag: '--host', type: 'string', default: '0.0.0.0', text: 'Address the JSON-RPC HTTP server binds.' }),
        opt('RPC_PORT', { flag: '--port', type: 'u16', default: '8899', text: 'Port the JSON-RPC HTTP server binds.' }),
        opt('RPC_MAX_BODY_BYTES', { type: 'usize (bytes)', default: '1048576', text: 'Largest accepted JSON-RPC request body.' }),
        opt('RPC_REQUEST_TIMEOUT_MS', {
          type: 'u64 (ms)',
          default: '10000',
          text: 'End-to-end timeout for one JSON-RPC request.',
          relations: [{ type: 'see', to: 'CLICKHOUSE_QUERY_TIMEOUT_MS' }],
        }),
        opt('RPC_CONCURRENCY_LIMIT', { type: 'usize', default: '512', text: 'Maximum in-flight JSON-RPC requests.' }),
        opt('RPC_MAX_BATCH_SIZE', { type: 'usize', default: '64', text: 'Maximum calls accepted in one batch request.' }),
        opt('RPC_BATCH_CONCURRENCY_LIMIT', { type: 'usize', default: '8', text: 'Maximum calls of one batch executed concurrently.' }),
        opt('RPC_RESPONSE_GZIP_ENABLED', { type: 'bool', default: 'false', text: 'Gzip responses when the client advertises gzip support.' }),
        opt('SUPERBANK_RPC_EMIT_HTTP_ERRORS', {
          flag: '--emit-http-errors',
          type: 'bool',
          default: 'false',
          text: 'Return HTTP 503 for server-side JSON-RPC failures. Response bodies are unchanged.',
        }),
        opt('GENESIS_PATH', {
          type: 'path',
          text: "The target cluster's `genesis.bin`, read at startup for epoch math. Unset uses the no-warmup epoch schedule with a warning; a configured file that cannot be read or decoded fails startup.",
        }),
        opt('MAX_SIGNATURES_LIMIT', {
          type: 'u64',
          default: '1000',
          text: 'Maximum signatures per address page. Also the head cache per-address index cap.',
          relations: [{ type: 'see', to: 'HEAD_CACHE_ENABLED' }],
        }),
        opt('HYDRATION_CPU_CONCURRENCY', { type: 'usize', default: '8', text: 'Concurrent CPU-heavy response hydration jobs (bounds `spawn_blocking` use).' }),
      ],
    },
    {
      id: 'logging',
      title: 'Logging and metrics',
      items: [
        envOnly('RUST_LOG', MAIN, {
          type: 'tracing filter',
          default: 'info,clickhouse_rs=warn',
          text: 'Log level filter (`tracing_subscriber` `EnvFilter` syntax).',
        }),
        envOnly('LOG_FORMAT', MAIN, { type: 'plain | json', default: 'plain', text: 'Log line format.' }),
        envOnly('SLOW_RPC_MS', HANDLERS, {
          type: 'u64 (ms)',
          default: '250',
          text: 'Requests slower than this are logged as slow. Zero or unparsable falls back to the default.',
        }),
        envOnly('SLOW_RPC_LOG_REQUEST_MAX_BYTES', HANDLERS, {
          type: 'usize (bytes)',
          default: '4096',
          text: 'How much of a slow request body the slow-request log line includes.',
          relations: [{ type: 'see', to: 'SLOW_RPC_MS' }],
        }),
        opt('METRICS_HOST', { type: 'string', default: '0.0.0.0', text: 'Address the Prometheus metrics server binds.' }),
        opt('METRICS_PORT', { type: 'u16', default: '9900', text: 'Port the Prometheus metrics server binds.' }),
        opt('METRICS_CAPTURE_HEADERS', {
          flag: '--metrics-capture-header',
          type: 'list',
          text: 'Request headers to add as labels on route and request metrics. Accepts `X-Endpoint`, `X-RPC-Node`, `X-Subscription-ID` and `X-Account-ID` (case-insensitive); repeat the flag or comma-separate the env value. Mind label cardinality.',
        }),
      ],
    },
    {
      id: 'get-block',
      title: 'getBlock',
      items: [
        opt('GET_BLOCK_RESPONSE_CACHE_MAX_BYTES', {
          type: 'u64 (bytes)',
          default: '0',
          text: 'Memory budget for serialized finalized `getBlock` results. Zero disables the cache.',
        }),
        opt('GET_BLOCK_RESPONSE_CACHE_SHARE_CONFIRMED', {
          type: 'bool',
          default: 'true',
          text: 'Let confirmed `getBlock` requests read and fill the finalized response cache when their data is provably finalized.',
          relations: [{ type: 'requires', to: 'GET_BLOCK_RESPONSE_CACHE_MAX_BYTES' }],
        }),
        opt('GET_BLOCK_RESPONSE_CACHE_UNSUPPORTED_VERSION', {
          type: 'bool',
          default: 'true',
          text: 'Remember the deterministic `-32015` (unsupported transaction version) answer for finalized blocks. `false` recomputes it on every request.',
          relations: [{ type: 'requires', to: 'GET_BLOCK_RESPONSE_CACHE_MAX_BYTES' }],
        }),
        opt('GET_BLOCK_HYDRATION_PARALLELISM', {
          type: 'usize',
          default: '4',
          text: 'Blocking threads one full or accounts `getBlock` build may use. Extra threads come from the hydration pool only when free; 1 disables chunking.',
          relations: [{ type: 'see', to: 'HYDRATION_CPU_CONCURRENCY' }],
        }),
      ],
    },
    {
      id: 'get-transaction',
      title: 'getTransaction',
      items: [
        opt('GET_TRANSACTION_PRIMARY_CACHE_MAX_BYTES', {
          type: 'u64 (bytes)',
          default: '0',
          text: 'Approximate memory budget for `getTransaction` records served by the primary ClickHouse. Zero disables the cache.',
        }),
        opt('GET_TRANSACTION_PRIMARY_CACHE_TTL_SECS', {
          type: 'u64 (seconds, 1–86400)',
          default: '600',
          text: 'How long a primary-served record stays cached.',
          relations: [{ type: 'requires', to: 'GET_TRANSACTION_PRIMARY_CACHE_MAX_BYTES' }],
        }),
        envOnly('SIGNATURE_SLOT_CACHE_SIZE', CACHE, {
          type: 'usize',
          default: '50000',
          text: 'Entries in the in-process signature-to-slot position cache used by `getTransaction`.',
        }),
        envOnly('SIGNATURE_SLOT_CACHE_TTL_FOUND_SECS', CACHE, {
          type: 'u64 (seconds)',
          default: '21600',
          text: 'How long a found signature position stays cached (6 hours by default).',
          relations: [{ type: 'see', to: 'SIGNATURE_SLOT_CACHE_SIZE' }],
        }),
        envOnly('SIGNATURE_SLOT_CACHE_TTL_MISSING_SECS', CACHE, {
          type: 'u64 (seconds)',
          default: '1',
          text: 'How long a signature that was not found stays cached as missing.',
          relations: [{ type: 'see', to: 'SIGNATURE_SLOT_CACHE_SIZE' }],
        }),
      ],
    },
    {
      id: 'get-signature-statuses',
      title: 'getSignatureStatuses',
      items: [
        opt('GET_SIGNATURE_STATUSES_MAX_CONCURRENCY', {
          type: 'usize',
          default: '4',
          text: 'Concurrent primary `getSignatureStatuses` workflows, including cancellation cleanup. Must be above zero.',
        }),
        opt('GET_SIGNATURE_STATUSES_MAX_THREADS', {
          type: 'usize',
          default: '2',
          text: 'ClickHouse thread cap for primary status queries and primary-index filtering. Must be above zero.',
        }),
        opt('SIGNATURE_STATUS_HISTORY_CACHE_ENTRIES', {
          type: 'u64',
          default: '0',
          text: 'Signatures whose empty primary `searchTransactionHistory` answer is remembered so a repeat lookup can skip the primary. Zero disables it. Only takes effect with both caches running and `HEAD_CACHE_RETAIN_SLOTS` above the disk cache lag (startup warns below 256).',
          // Not cfg-gated in code, but the README and server.rs make it a no-op
          // without both caches, so it is labelled with both features.
          requires: ['feature:grpc-head-cache', 'feature:disk-cache'],
          relations: [
            { type: 'see', to: 'HEAD_CACHE_RETAIN_SLOTS' },
            { type: 'see', to: 'DISK_CACHE_ENABLED' },
          ],
        }),
        opt('SIGNATURE_STATUS_HISTORY_CACHE_MAX_BYTES', {
          type: 'u64 (bytes)',
          default: '67108864',
          text: 'Memory bound for the absence cache, budgeted at about 384 bytes per entry.',
          requires: ['feature:grpc-head-cache', 'feature:disk-cache'],
          relations: [{ type: 'see', to: 'SIGNATURE_STATUS_HISTORY_CACHE_ENTRIES' }],
        }),
        opt('SIGNATURE_STATUS_HISTORY_CACHE_TTL_SECS', {
          type: 'u64 (seconds, 1–300)',
          default: '300',
          text: 'How long an empty primary answer may skip the primary.',
          requires: ['feature:grpc-head-cache', 'feature:disk-cache'],
          relations: [{ type: 'see', to: 'SIGNATURE_STATUS_HISTORY_CACHE_ENTRIES' }],
        }),
      ],
    },
    {
      id: 'get-inflation-reward',
      title: 'getInflationReward',
      items: [
        opt('GET_INFLATION_REWARD_MAX_ADDRESSES', {
          type: 'usize (0–100)',
          default: '100',
          text: 'Maximum addresses per request. Zero disables the limit; values above 100 are rejected.',
        }),
        opt('GET_INFLATION_REWARD_MAX_CONCURRENCY', { type: 'usize', default: '20', text: 'Concurrent requests admitted. Zero disables admission control.' }),
        opt('GET_INFLATION_REWARD_QUERY_TIMEOUT_MS', {
          type: 'u64 (ms)',
          default: '5000',
          text: 'End-to-end ClickHouse budget for one lookup. Must be above zero and below `RPC_REQUEST_TIMEOUT_MS`, or startup fails.',
          relations: [{ type: 'capped-by', to: 'RPC_REQUEST_TIMEOUT_MS' }],
        }),
        opt('GET_INFLATION_REWARD_MAX_THREADS', { type: 'usize', default: '2', text: 'Per-query ClickHouse thread cap. Must be above zero.' }),
        opt('GET_INFLATION_REWARD_MAX_MEMORY_BYTES', { type: 'u64 (bytes)', default: '536870912', text: 'Per-query ClickHouse memory cap. Must be above zero.' }),
        opt('GET_INFLATION_REWARD_MAX_BYTES_TO_READ', { type: 'u64 (bytes)', default: '536870912', text: 'Per-query ClickHouse read cap. Must be above zero.' }),
        opt('GET_INFLATION_REWARD_EPOCH_CACHE_MAX_BYTES', {
          type: 'u64 (bytes)',
          default: '16777216',
          text: 'Budget for the in-process cache of validated epoch boundary and partition-slot metadata. Zero disables it.',
        }),
      ],
    },
    {
      id: 'ag-genesis-cert',
      title: 'getAgGenesisCert',
      intro: 'Source for the Alpenglow genesis certificate (Agave 4.3+). The answer comes from a trusted RPC on the same cluster as the ClickHouse data; superbank-rpc checks its shape, not its signature.',
      items: [
        opt('AG_GENESIS_CERT_RPC_URL', {
          type: 'url',
          text: 'Trusted same-cluster HTTP(S) RPC that supports `getAgGenesisCert`. Unset, empty or blank keeps startup working, but `getAgGenesisCert` then returns an unavailable-source error. Never point it back at this instance.',
        }),
        opt('AG_GENESIS_CERT_RPC_TIMEOUT_MS', {
          type: 'u64 (ms)',
          default: '2000',
          text: 'Total budget for one certificate fetch, including admission, connection and response body. With a source configured it must be below `RPC_REQUEST_TIMEOUT_MS`, or startup fails.',
          relations: [
            { type: 'capped-by', to: 'RPC_REQUEST_TIMEOUT_MS' },
            { type: 'see', to: 'AG_GENESIS_CERT_RPC_URL' },
          ],
        }),
        opt('AG_GENESIS_CERT_REFRESH_INTERVAL_SECS', {
          type: 'u64 (seconds, 1–300)',
          default: '5',
          text: 'How long an authoritative `null` (not migrated yet) is reused before refreshing. A certificate stays cached until restart; failures are cached for 1 second.',
          relations: [{ type: 'see', to: 'AG_GENESIS_CERT_RPC_URL' }],
        }),
      ],
    },
    {
      id: 'clickhouse',
      title: 'ClickHouse connection',
      items: [
        opt('CLICKHOUSE_URL', { type: 'url', default: 'http://localhost:8123', text: 'HTTP endpoint of the source ClickHouse cluster.' }),
        opt('CLICKHOUSE_DATABASE', { type: 'string', default: 'default', text: 'Database for the connection.' }),
        opt('CLICKHOUSE_USER', { type: 'string', default: 'default', text: 'ClickHouse user.' }),
        opt('CLICKHOUSE_PASSWORD', { type: 'string', default: '', secret: true, text: 'ClickHouse password.' }),
        opt('CLICKHOUSE_QUERY_TIMEOUT_MS', {
          type: 'u64 (ms)',
          default: '8000',
          text: 'Timeout for ClickHouse queries. Keep it below `RPC_REQUEST_TIMEOUT_MS` (startup warns otherwise).',
          relations: [{ type: 'capped-by', to: 'RPC_REQUEST_TIMEOUT_MS' }],
        }),
        opt('CLICKHOUSE_HTTP_MAX_CONCURRENCY', {
          type: 'usize',
          default: '512',
          text: 'Server-wide cap on concurrent direct (scalar and lookup) ClickHouse HTTP queries, independent of shard fan-out and batching. Keep it at or below the ClickHouse per-user budget.',
        }),
        opt('CLICKHOUSE_HTTP_CONNECT_TIMEOUT_MS', {
          type: 'u64 (ms)',
          default: '2000',
          text: 'TCP connect timeout for ClickHouse HTTP connections, so a new connection fails fast under backpressure.',
        }),
        opt('CLICKHOUSE_CLUSTER', {
          type: 'string',
          default: '{cluster}',
          text: 'Cluster used to cancel primary status queries and, in shard-direct scope, for discovery. Macros such as `{cluster}` resolve on the server; empty selects local-only cancellation on a standalone node.',
        }),
        opt('CLICKHOUSE_STARTUP_VERIFICATION_TIMEOUT_MS', {
          type: 'u64 (ms)',
          default: '10000',
          text: 'Per-query timeout for cancellation macro resolution, discovery and startup probes.',
        }),
        opt('CLICKHOUSE_RUNTIME_VERIFICATION_TIMEOUT_MS', {
          type: 'u64 (ms)',
          default: '10000',
          text: 'Per-batch timeout when verifying that abandoned queries were terminated.',
        }),
        opt('CLICKHOUSE_STARTUP_TABLE_CHECK', { type: 'exists | count', default: 'exists', text: 'How startup validates access to each table.' }),
        opt('CLICKHOUSE_IN_CLAUSE_CHUNK', { type: 'usize', default: '512', text: 'Chunk size for large `IN (...)` filters, which caps the SQL string size.' }),
        envOnly('CLICKHOUSE_KILL_QUERY_MAX_CONCURRENCY', UTIL, {
          type: 'usize (≥ 1)',
          default: '16',
          text: 'Concurrent best-effort `KILL QUERY` cleanups after a read times out, errors or is dropped, so a burst of timeouts does not amplify an overload.',
        }),
        envOnly('CLICKHOUSE_QUERY_ID_PREFIX', UTIL, {
          type: 'string | auto | off',
          default: 'superbank',
          text: 'Prefix of the query IDs superbank-rpc sends. `auto` uses a PID-based prefix; `off`, `0`, `false`, `no` or empty drop the configured prefix. Required query IDs still carry a random process namespace and counter.',
        }),
        envOnly('CLICKHOUSE_DISABLE_QUERY_SETTINGS', CLIENT, {
          type: 'bool',
          default: 'false',
          text: 'Truthy disables optional per-query `SETTINGS` overrides, such as the `getInflationReward` caps and owner-shard routing. Required HTTP disconnect settings, disk cache settings and primary status limits still apply.',
          relations: [
            { type: 'see', to: 'GET_INFLATION_REWARD_MAX_THREADS' },
            { type: 'see', to: 'CLICKHOUSE_SIGNATURES_OWNER_SHARD_ROUTING' },
          ],
        }),
      ],
    },
    {
      id: 'clickhouse-tables',
      title: 'ClickHouse tables',
      intro: 'Read directly from the environment. Defaults are the Distributed tables from `ddl/`.',
      items: [
        envOnly('CLICKHOUSE_TRANSACTION_TABLE', CLIENT, { type: 'string', default: 'default.transactions', text: 'Transactions table.' }),
        envOnly('CLICKHOUSE_SIGNATURE_TABLE', CLIENT, {
          type: 'string',
          status: 'deprecated',
          text: 'Legacy name for the transactions table, used only when `CLICKHOUSE_TRANSACTION_TABLE` is unset.',
          relations: [{ type: 'alias-of', to: 'CLICKHOUSE_TRANSACTION_TABLE' }],
        }),
        envOnly('CLICKHOUSE_BLOCKS_METADATA_TABLE', CLIENT, { type: 'string', default: 'default.blocks_metadata', text: 'Block metadata table.' }),
        envOnly('CLICKHOUSE_GSFA_TABLE', CLIENT, { type: 'string', default: 'default.gsfa', text: 'Signatures-for-address index table.' }),
        envOnly('CLICKHOUSE_SIGNATURE_STATUSES_TABLE', CLIENT, { type: 'string', default: 'default.signatures', text: 'Signature status index table.' }),
        envOnly('CLICKHOUSE_TOKEN_OWNER_ACTIVITY_TABLE', CLIENT, {
          type: 'string',
          default: 'default.token_owner_activity',
          text: 'Token owner activity table used by `getTransactionsForAddress` token-account filters.',
        }),
      ],
    },
    {
      id: 'clickhouse-query-cache',
      title: 'ClickHouse query caches',
      items: [
        opt('CLICKHOUSE_QUERY_CACHE_ENABLED', { type: 'bool', default: 'false', text: 'Use the ClickHouse query cache for historical read queries.' }),
        opt('CLICKHOUSE_QUERY_CACHE_TTL_SECONDS', {
          type: 'u64 (seconds)',
          default: '1',
          text: 'Query cache TTL for historical reads.',
          requires: ['when:CLICKHOUSE_QUERY_CACHE_ENABLED=true'],
        }),
        opt('CLICKHOUSE_GET_TRANSACTION_QUERY_CACHE_TTL_SECONDS', {
          type: 'u64 (seconds)',
          default: '300',
          text: 'Query cache TTL for historical `getTransaction` point lookups only.',
          requires: ['when:CLICKHOUSE_QUERY_CACHE_ENABLED=true'],
        }),
        opt('CLICKHOUSE_GET_TRANSACTION_QUERY_CACHE_MIN_QUERY_RUNS', {
          type: 'u64',
          default: '2',
          text: 'Identical `getTransaction` executions before ClickHouse caches the result.',
          requires: ['when:CLICKHOUSE_QUERY_CACHE_ENABLED=true'],
        }),
        opt('CLICKHOUSE_QUERY_CACHE_SHARE_BETWEEN_USERS', {
          type: 'bool',
          default: 'false',
          text: 'Share query cache entries between ClickHouse users.',
          requires: ['when:CLICKHOUSE_QUERY_CACHE_ENABLED=true'],
        }),
        opt('CLICKHOUSE_QUERY_CONDITION_CACHE_ENABLED', {
          type: 'bool',
          default: 'false',
          text: 'Use the ClickHouse query condition cache for selected historical address-filtered reads. Independent of the query cache.',
        }),
      ],
    },
    {
      id: 'clickhouse-query-paths',
      title: 'ClickHouse query paths',
      intro: 'Switches between equivalent query plans. Each one changes round trips or ClickHouse work, not results.',
      items: [
        opt('CLICKHOUSE_GET_TRANSACTION_SINGLE_ROUND_TRIP', {
          type: 'bool',
          default: 'false',
          text: 'Resolve an uncached `getTransaction` position and read its payload in one primary query instead of two. Distributed scope only; shard-direct keeps the two-query path.',
          requires: ['when:CLICKHOUSE_SCOPE=distributed'],
        }),
        opt('CLICKHOUSE_LATEST_SLOT_HINT', {
          type: 'bool',
          default: 'true',
          text: "Bound the latest-finalized-slot query to slots at or above the previous answer minus a margin. `false` sends the unbounded query every time.",
        }),
        opt('CLICKHOUSE_TRANSACTIONS_FOR_ADDRESS_POSITION_TOKENS', {
          type: 'bool',
          default: 'false',
          text: 'Return `slot:idx` pagination tokens for ClickHouse-sourced `getTransactionsForAddress` rows, so the next page needs no primary signature lookup. Signature tokens are still accepted.',
        }),
        opt('CLICKHOUSE_TRANSACTIONS_FOR_ADDRESS_CURSOR_CACHE', {
          type: 'bool',
          default: 'false',
          text: "Remember the position of each ClickHouse-sourced `getTransactionsForAddress` page's last row in process, so a follow-up page on this node skips the primary signature lookup.",
          relations: [{ type: 'see', to: 'CLICKHOUSE_TRANSACTIONS_FOR_ADDRESS_POSITION_TOKENS' }],
        }),
        opt('CLICKHOUSE_TRANSACTIONS_FOR_ADDRESS_UNION_PUSHDOWN', {
          type: 'bool',
          default: 'true',
          text: 'Push the token-accounts filter, `ORDER BY` and `LIMIT` of `getTransactionsForAddress` into each `UNION` branch. `false` filters outside the union.',
        }),
        opt('CLICKHOUSE_GSFA_INLINE_CURSOR', {
          type: 'bool',
          default: 'false',
          text: 'Resolve a `getSignaturesForAddress` `before`/`until` cursor the head and local tiers missed inside the primary page query, instead of a separate lookup first. Distributed scope only.',
          requires: ['when:CLICKHOUSE_SCOPE=distributed'],
          relations: [{ type: 'see', to: 'CLICKHOUSE_GSFA_FALLBACK_TRANSACTIONS' }],
        }),
        opt('CLICKHOUSE_SIGNATURES_OWNER_SHARD_ROUTING', {
          type: 'bool',
          default: 'false',
          text: 'Send primary signature lookups (signature to slot, and status history) to the owning shard via `cluster(...)` with `optimize_skip_unused_shards=1`, instead of the `default.signatures` view that queries every shard. Verified at startup.',
          relations: [
            { type: 'requires', to: 'CLICKHOUSE_CLUSTER' },
            { type: 'requires', to: 'CLICKHOUSE_SIGNATURES_LOCAL_TABLE' },
          ],
        }),
        envOnly('CLICKHOUSE_GSFA_FALLBACK_TRANSACTIONS', UTIL, {
          type: 'off | empty | force',
          default: 'off',
          text: 'Fall back to scanning the transactions table for `getSignaturesForAddress`. `empty` (or `1`, `true`, `yes`, `on`) only when the index page is empty; `force` (or `always`, `full`, `incomplete`) also when it is incomplete.',
        }),
      ],
    },
    {
      id: 'shard-direct',
      title: 'Shard-direct scope',
      intro: '`CLICKHOUSE_SCOPE=shard-direct` queries each shard directly instead of through Distributed tables. Distributed scope ignores the options marked for shard-direct and warns when they are set.',
      items: [
        opt('CLICKHOUSE_SCOPE', { type: 'distributed | shard-direct', default: 'distributed', text: 'Routing scope for every query.' }),
        opt('CLICKHOUSE_TRANSPORT', { requires: [SHARD_DIRECT],
          type: 'http | tcp',
          default: 'http',
          text: 'Transport for shard-direct queries. `tcp` uses the native protocol and is rejected unless the scope is `shard-direct`.',
        }),
        opt('CLICKHOUSE_SHARD_FANOUT_CONCURRENCY', { requires: [SHARD_DIRECT], type: 'usize', default: '8', text: 'Concurrent per-shard queries in one fan-out.' }),
        opt('CLICKHOUSE_TOPOLOGY_CONFIG', { requires: [SHARD_DIRECT],
          type: 'path',
          text: 'Authoritative YAML topology (`nodes` with `shard-id`, `hostname`, `ip-address`, `tcp-port`, `shard-weight`). Skips `system.clusters` discovery.',
        }),
        opt('CLICKHOUSE_SHARD_HTTP_PORT', { requires: [SHARD_DIRECT], type: 'u16', text: 'Shard HTTP port. Defaults to the port in `CLICKHOUSE_URL`.', relations: [{ type: 'see', to: 'CLICKHOUSE_URL' }] }),
        opt('CLICKHOUSE_TCP_ACCESS_CHECK_TIMEOUT_MS', { requires: [SHARD_DIRECT],
          type: 'u64 (ms)',
          default: '2000',
          text: 'Timeout of the startup check that each shard accepts native TCP connections.',
        }),
        opt('CLICKHOUSE_REPLICA_HEALTH_CHECK_INTERVAL_MS', { type: 'u64 (ms)', default: '10000', text: 'Interval between background health checks of unavailable shard replicas.' }),
        opt('CLICKHOUSE_TCP_POOL_MIN', { requires: [SHARD_DIRECT],
          type: 'usize',
          default: '10',
          text: 'Connections kept per shard in each native (TCP) connection pool.',
        }),
        opt('CLICKHOUSE_TCP_POOL_MAX', { requires: [SHARD_DIRECT],
          type: 'usize',
          default: '20',
          text: 'Maximum connections per shard in each native pool; total native connections are this times the shard count. Keep it at or above the minimum.',
          relations: [{ type: 'see', to: 'CLICKHOUSE_TCP_POOL_MIN' }],
        }),
        opt('CLICKHOUSE_TRANSACTIONS_LOCAL_TABLE', { requires: [SHARD_DIRECT],
          type: 'string',
          text: 'Shard-local transactions table. Defaults to the transactions table plus `_local`.',
          relations: [{ type: 'see', to: 'CLICKHOUSE_TRANSACTION_TABLE' }],
        }),
        opt('CLICKHOUSE_BLOCKS_METADATA_LOCAL_TABLE', { requires: [SHARD_DIRECT],
          type: 'string',
          text: 'Shard-local block metadata table. Defaults to the block metadata table plus `_local`.',
          relations: [{ type: 'see', to: 'CLICKHOUSE_BLOCKS_METADATA_TABLE' }],
        }),
        opt('CLICKHOUSE_GSFA_LOCAL_TABLE', { requires: [SHARD_DIRECT], type: 'string', text: 'Shard-local gsfa table for address queries.', relations: [{ type: 'see', to: 'CLICKHOUSE_GSFA_TABLE' }] }),
        opt('CLICKHOUSE_SIGNATURES_LOCAL_TABLE', {
          type: 'string',
          text: 'Shard-local signatures table. Defaults to the signature statuses table plus `_local`. Used in shard-direct scope, and by owner-shard routing in either scope.',
          relations: [{ type: 'see', to: 'CLICKHOUSE_SIGNATURE_STATUSES_TABLE' }],
        }),
        opt('CLICKHOUSE_TOKEN_OWNER_ACTIVITY_LOCAL_TABLE', { requires: [SHARD_DIRECT],
          type: 'string',
          text: 'Shard-local token owner activity table. Defaults to the base table plus `_local`.',
          relations: [{ type: 'see', to: 'CLICKHOUSE_TOKEN_OWNER_ACTIVITY_TABLE' }],
        }),
      ],
    },
    {
      id: 'gsfa-hot',
      title: 'GSFA hot addresses',
      intro: 'Very active addresses can be read from a separate `gsfa_hot` table. The hot table is used only when at least one listed address is a valid pubkey.',
      items: [
        opt('CLICKHOUSE_GSFA_HOT_ADDRESSES', {
          flag: '--clickhouse-hot-address',
          type: 'list of pubkeys',
          text: 'Addresses routed to the hot table. Repeat the flag or comma-separate the env value.',
        }),
        opt('CLICKHOUSE_GSFA_HOT_TABLE', {
          type: 'string',
          default: 'default.gsfa_hot',
          text: 'Distributed hot table read for those addresses.',
          relations: [{ type: 'requires', to: 'CLICKHOUSE_GSFA_HOT_ADDRESSES' }],
        }),
        opt('CLICKHOUSE_GSFA_HOT_LOCAL_TABLE', {
          type: 'string',
          default: 'default.gsfa_hot_local',
          text: 'Shard-local hot table for shard-direct fan-out.',
          requires: [SHARD_DIRECT],
          relations: [{ type: 'requires', to: 'CLICKHOUSE_GSFA_HOT_ADDRESSES' }],
        }),
      ],
    },
    {
      id: 'head-cache',
      title: 'Head cache',
      intro: 'In-memory cache of the newest slots, fed by a Yellowstone DragonsMouth gRPC stream and tried before the disk cache and ClickHouse. Build with `--features grpc-head-cache` (pulls in AGPL-3.0 `yellowstone-block-machine`) and set `HEAD_CACHE_ENABLED=true`.',
      requires: ['feature:grpc-head-cache', 'when:HEAD_CACHE_ENABLED=true'],
      items: [
        opt('HEAD_CACHE_ENABLED', { type: 'bool', default: 'false', text: 'Run the head cache. Without `DRAGONSMOUTH_ENDPOINT` it logs a warning and stays off.' }),
        opt('DRAGONSMOUTH_ENDPOINT', {
          type: 'url',
          required: 'when `HEAD_CACHE_ENABLED=true`',
          text: 'Yellowstone gRPC (DragonsMouth) endpoint that feeds the cache.',
        }),
        opt('DRAGONSMOUTH_X_TOKEN', { type: 'string', secret: true, text: 'Optional `x-token` header for the DragonsMouth endpoint.' }),
        opt('HEAD_CACHE_RETAIN_SLOTS', {
          type: 'u64',
          default: '32',
          text: "Newest slots kept in memory. The default of 32 is a development size; production keeps several hundred or more, sized to the memory superbank-rpc has. With the disk cache, the window must reach down to the disk cache's tip for the signature-status history cache and the empty-address watermark to answer (startup warns below 256 when the history cache is on).",
          relations: [
            { type: 'see', to: 'SIGNATURE_STATUS_HISTORY_CACHE_ENTRIES' },
            { type: 'see', to: 'DISK_CACHE_GSFA_EMPTY_WATERMARK_TTL_SECS' },
          ],
        }),
        opt('HEAD_CACHE_MIN_COMMITMENT', {
          type: 'processed | confirmed | finalized',
          default: 'processed',
          text: 'Lowest commitment the cache serves. An invalid value falls back to `processed` with a warning.',
        }),
        opt('GRPC_MAX_DECODING_BYTES', { type: 'usize (bytes)', default: '67108864', text: 'Largest gRPC message decoded from the stream.' }),
        opt('GET_BLOCKS_CLAMP_TO_HEAD_TIP', {
          type: 'bool',
          default: 'true',
          text: 'Clamp an explicit `getBlocks`/`getBlocksWithLimit` end to the trusted head tip. `false` keeps the requested end and asks the primary for slots above the tip.',
        }),
      ],
    },
    {
      id: 'disk-cache',
      title: 'Disk cache',
      intro: 'A local ClickHouse holding recent finalized slots, filled from the source cluster and read before it. Build with `--features disk-cache` and set `DISK_CACHE_ENABLED=true`; with it false every other `DISK_CACHE_*` value is ignored.',
      requires: ['feature:disk-cache', 'when:DISK_CACHE_ENABLED=true'],
      items: [
        opt('DISK_CACHE_ENABLED', { type: 'bool', default: 'false', text: 'Run the local forward cache of recent finalized slots.' }),
        opt('DISK_CACHE_REQUIRED', {
          type: 'bool',
          default: 'false',
          text: 'Make cache initialization and health mandatory for startup and `/health`. Individual reads still fall back to the source on cache failures.',
        }),
        opt('DISK_CACHE_CLICKHOUSE_URL', {
          type: 'url',
          default: 'http://127.0.0.1:8123',
          text: 'HTTP endpoint of the local ClickHouse. Must be http or https on a loopback host.',
        }),
        opt('DISK_CACHE_CLICKHOUSE_DATABASE', {
          type: 'string',
          default: 'superbank_disk_cache',
          text: 'Dedicated database the disposable cache owns. Must be a plain identifier; `default`, `system` and `information_schema` are rejected.',
        }),
        opt('DISK_CACHE_CLICKHOUSE_USER', { type: 'string', default: 'default', text: 'Local ClickHouse user.' }),
        opt('DISK_CACHE_CLICKHOUSE_PASSWORD', { type: 'string', default: '', secret: true, text: 'Local ClickHouse password.' }),
        opt('DISK_CACHE_RETAIN_SLOTS', {
          type: 'u64',
          required: 'when `DISK_CACHE_ENABLED=true`',
          text: 'Finalized slots to retain.',
        }),
        opt('DISK_CACHE_MAX_BYTES', {
          type: 'u64 (bytes)',
          default: '0',
          text: 'MergeTree byte budget; 0 is unlimited. Over budget, the oldest complete slot partitions are dropped until usage is under the low-water mark.',
        }),
        opt('DISK_CACHE_PARTITION_SLOTS', {
          type: 'u64',
          text: 'Slot width of local partitions. Unset derives a width that keeps at most 128 active partitions.',
          relations: [{ type: 'see', to: 'DISK_CACHE_RETAIN_SLOTS' }],
        }),
        opt('DISK_CACHE_KEY_INDEX_MAX_MEMORY_BYTES', {
          type: 'u64 (bytes, ≥ 64 MiB)',
          default: '4294967296',
          text: 'Memory budget of the partition routing index, including build buffers.',
        }),
        opt('DISK_CACHE_SCHEMA_CHECK_INTERVAL_SECS', { type: 'u64 (seconds)', default: '300', text: 'How often the source schema fingerprint is rechecked.' }),
        opt('DISK_CACHE_COMPACT_TRANSACTIONS_PARTS', {
          type: 'bool',
          default: 'false',
          text: 'Write new local `transactions` parts in Compact format. They take about 13.5% more disk, so raise `DISK_CACHE_MAX_BYTES` to match. `false` restores the server default layout for new parts.',
          relations: [{ type: 'see', to: 'DISK_CACHE_MAX_BYTES' }],
        }),
      ],
    },
    {
      id: 'disk-cache-reads',
      title: 'Disk cache: reads',
      requires: ['feature:disk-cache', 'when:DISK_CACHE_ENABLED=true'],
      items: [
        opt('DISK_CACHE_QUERY_TIMEOUT_MS', { type: 'u64 (ms)', default: '2000', text: 'Timeout for one local cache read and index work.' }),
        opt('DISK_CACHE_GET_TX_TIMEOUT_MS', {
          type: 'u64 (ms)',
          default: '1000',
          text: 'Budget for one local `getTransaction` attempt; the primary query starts when it expires.',
          relations: [{ type: 'capped-by', to: 'DISK_CACHE_QUERY_TIMEOUT_MS' }],
        }),
        opt('DISK_CACHE_GET_TX_UNKNOWN_TIMEOUT_MS', {
          type: 'u64 (ms)',
          default: '150',
          text: 'Replaces the `getTransaction` budget while the signature index has more than 4 partitions of unknown membership, for example after a restart. Set it equal to the regular budget to disable.',
          relations: [{ type: 'capped-by', to: 'DISK_CACHE_GET_TX_TIMEOUT_MS' }],
        }),
        opt('DISK_CACHE_ADDRESS_QUERY_TIMEOUT_MS', {
          type: 'u64 (ms)',
          default: '100',
          text: 'Shared budget for all cache stages of one `getSignaturesForAddress` or `getTransactionsForAddress` request, including cursor lookup and hydration. On expiry the request falls back to the primary.',
          relations: [{ type: 'see', to: 'DISK_CACHE_QUERY_TIMEOUT_MS' }],
        }),
        opt('DISK_CACHE_QUERY_CONCURRENCY', { type: 'u64 (1–64)', default: '8', text: 'Concurrent local interactive queries.' }),
        opt('DISK_CACHE_BACKGROUND_QUERY_CONCURRENCY', {
          type: 'u64 (1–64)',
          default: 'min(query concurrency, 8)',
          text: 'Concurrent local background reads (coverage reloads, fill validation, signature-membership scans).',
          relations: [{ type: 'see', to: 'DISK_CACHE_QUERY_CONCURRENCY' }],
        }),
        opt('DISK_CACHE_QUERY_MAX_THREADS', { type: 'u64 (1–16)', default: '2', text: 'Execution threads per local interactive query.' }),
        opt('DISK_CACHE_FUSED_GET_TX', {
          type: 'bool',
          default: 'true',
          text: 'Resolve a local `getTransaction` position and payload in one query, falling back to the two-step lookup. `false` uses only the two-step lookup.',
        }),
        opt('DISK_CACHE_GET_TX_SPAN_CHECK', {
          type: 'bool',
          default: 'true',
          text: 'After an empty fused read over several candidate partitions, ask the whole span once before probing each partition.',
          relations: [{ type: 'see', to: 'DISK_CACHE_FUSED_GET_TX' }],
        }),
        opt('DISK_CACHE_EVICTION_SAFE_HITS', {
          type: 'bool',
          default: 'true',
          text: 'Serve a local `getTransaction` hit that raced an eviction while its slot is still covered. `false` discards every read that raced an eviction.',
        }),
        opt('DISK_CACHE_STATUS_SPAN_QUERY', {
          type: 'bool',
          default: 'true',
          text: 'Look up local signature statuses with one query over the candidate slot span. `false` queries each candidate partition in turn.',
        }),
        opt('GSFA_RACE_PRIMARY', {
          type: 'bool',
          default: 'true',
          text: "Race the local `getSignaturesForAddress` page against the primary's full page. `false` waits for the local page and asks the primary only for the remainder.",
        }),
        opt('DISK_CACHE_GSFA_EMPTY_WATERMARK_TTL_SECS', {
          type: 'u64 (seconds)',
          default: '0',
          text: 'How long `getSignaturesForAddress` remembers that an address had no primary rows at or below a finalized slot, so a repeat request can be answered from the head and local tiers. Zero disables it. Also needs the head cache.',
          requires: ['feature:grpc-head-cache'],
          relations: [{ type: 'see', to: 'HEAD_CACHE_ENABLED' }],
        }),
        opt('DISK_CACHE_GSFA_EMPTY_WATERMARK_MAX_ENTRIES', {
          type: 'u64 (1–10000000)',
          default: '100000',
          text: 'Addresses kept in that watermark cache, about 128 bytes each.',
          relations: [{ type: 'see', to: 'DISK_CACHE_GSFA_EMPTY_WATERMARK_TTL_SECS' }],
        }),
      ],
    },
    {
      id: 'disk-cache-memory',
      title: 'Disk cache: Memory tables',
      requires: ['feature:disk-cache', 'when:DISK_CACHE_ENABLED=true'],
      items: [
        opt('DISK_CACHE_MEMORY_TABLES', {
          type: 'list',
          text: 'Query-facing tables kept in a ClickHouse Memory table. Only `blocks_metadata` is accepted.',
          relations: [
            { type: 'requires', to: 'DISK_CACHE_MEMORY_RETAIN_SLOTS' },
            { type: 'requires', to: 'DISK_CACHE_MEMORY_MAX_BYTES' },
            { type: 'conflicts', to: 'DISK_CACHE_BLOCK_INDEX_ENABLED' },
          ],
        }),
        opt('DISK_CACHE_MEMORY_RETAIN_SLOTS', {
          type: 'u64',
          required: 'when `DISK_CACHE_MEMORY_TABLES` is set',
          text: 'Row cap of the Memory table, in slots.',
          relations: [{ type: 'capped-by', to: 'DISK_CACHE_RETAIN_SLOTS' }],
        }),
        opt('DISK_CACHE_MEMORY_MAX_BYTES', {
          type: 'u64 (bytes)',
          required: 'when `DISK_CACHE_MEMORY_TABLES` is set',
          text: 'Byte cap of the Memory table.',
        }),
      ],
    },
    {
      id: 'disk-cache-block-index',
      title: 'Disk cache: block index',
      intro: 'A durable full-history block-time index with an in-process read cache.',
      requires: ['feature:disk-cache', 'when:DISK_CACHE_ENABLED=true'],
      items: [
        opt('DISK_CACHE_BLOCK_INDEX_ENABLED', {
          type: 'bool',
          default: 'false',
          text: 'Build and serve the block-time index.',
          relations: [{ type: 'requires', to: 'DISK_CACHE_BLOCK_INDEX_MAX_MEMORY_BYTES' }],
        }),
        // config.rs has no #[cfg(feature = "disk-cache")] on this field, so it
        // parses in every build; it does nothing without the disk cache.
        opt('DISK_CACHE_BLOCK_INDEX_MAX_MEMORY_BYTES', {
          type: 'u64 (bytes)',
          required: 'when `DISK_CACHE_BLOCK_INDEX_ENABLED=true`',
          text: "Bytes the index's in-process segment cache may hold. At least one segment (about 8.1 MB).",
          requires: ['when:DISK_CACHE_BLOCK_INDEX_ENABLED=true'],
        }),
        opt('DISK_CACHE_BLOCK_INDEX_SLOTS_PER_QUERY', {
          type: 'u64',
          default: '250000',
          text: 'Slots copied per historical metadata query.',
          requires: ['when:DISK_CACHE_BLOCK_INDEX_ENABLED=true'],
        }),
        opt('DISK_CACHE_BLOCK_INDEX_MAX_SLOTS_PER_SEC', {
          type: 'u64',
          default: '25000',
          text: 'Rate limit of the read-only source scan that builds the index.',
          requires: ['when:DISK_CACHE_BLOCK_INDEX_ENABLED=true'],
        }),
        opt('DISK_CACHE_BLOCK_INDEX_QUERY_TIMEOUT_MS', {
          type: 'u64 (ms)',
          default: '300000',
          text: 'Timeout for one historical metadata range query.',
          requires: ['when:DISK_CACHE_BLOCK_INDEX_ENABLED=true'],
        }),
      ],
    },
    {
      id: 'disk-cache-fill',
      title: 'Disk cache: backfill and repair',
      requires: ['feature:disk-cache', 'when:DISK_CACHE_ENABLED=true'],
      items: [
        opt('DISK_CACHE_BACKFILL_ENABLED', {
          type: 'bool',
          default: 'true',
          text: 'Run the source-to-local forward and repair task. Turn off for debugging only.',
        }),
        opt('DISK_CACHE_BACKFILL_SLOTS_PER_QUERY', { type: 'u64', default: '8', text: 'Slots fetched per backfill range query.' }),
        opt('DISK_CACHE_BACKFILL_CONCURRENCY', { type: 'u64 (1–64)', default: '4', text: 'Independent source-to-local ranges forwarded at once.' }),
        opt('DISK_CACHE_BACKFILL_MAX_SLOTS_PER_SEC', {
          type: 'u64',
          default: '50',
          text: 'Backfill rate limit. The default fills a 10-epoch window in roughly a day.',
        }),
        opt('DISK_CACHE_BACKFILL_QUERY_TIMEOUT_MS', {
          type: 'u64 (ms)',
          default: '30000',
          text: 'Timeout for backfill range queries, which need longer than interactive reads.',
          relations: [{ type: 'see', to: 'CLICKHOUSE_QUERY_TIMEOUT_MS' }],
        }),
        opt('DISK_CACHE_REPAIR_INTERVAL_MS', { type: 'u64 (ms)', default: '5000', text: 'Idle wait between repair and backfill planning rounds.' }),
        opt('DISK_CACHE_REPAIR_MIN_LAG_SLOTS', {
          type: 'u64',
          default: '75',
          text: 'Never backfill slots this close to the finalized tip, so ingestion has had time to land them.',
        }),
      ],
    },
    {
      id: 'disk-cache-removed',
      title: 'Disk cache: removed settings',
      intro: 'Leftovers of the RocksDB disk cache. Setting any of them is a startup error, even with the cache disabled.',
      requires: ['feature:disk-cache'],
      items: [
        opt('DISK_CACHE_PATH', { flag: undefined, type: 'string', status: 'deprecated', text: 'Removed; the cache now lives in the local ClickHouse.' }),
        opt('DISK_CACHE_BLOCK_CACHE_BYTES', { flag: undefined, type: 'usize', status: 'deprecated', text: 'Removed RocksDB block cache size.' }),
        opt('DISK_CACHE_WRITE_QUEUE_SLOTS', { flag: undefined, type: 'usize', status: 'deprecated', text: 'Removed RocksDB write queue depth.' }),
        opt('DISK_CACHE_READ_CONCURRENCY', {
          flag: undefined,
          type: 'usize',
          status: 'deprecated',
          text: 'Removed. See `DISK_CACHE_QUERY_CONCURRENCY`.',
          relations: [{ type: 'see', to: 'DISK_CACHE_QUERY_CONCURRENCY' }],
        }),
      ],
    },
    {
      id: 'grpc-streaming',
      title: 'gRPC streaming',
      intro: 'A separate gRPC listener that streams bounded historical slot ranges from ClickHouse. Build with `--features grpc-streaming` and set `SUPERBANK_GRPC_ENABLED=true`.',
      requires: ['feature:grpc-streaming', 'when:SUPERBANK_GRPC_ENABLED=true'],
      items: [
        opt('SUPERBANK_GRPC_ENABLED', { type: 'bool', default: 'false', text: 'Run the Superbank gRPC streaming API.' }),
        opt('SUPERBANK_GRPC_HOST', { type: 'string', default: '0.0.0.0', text: 'Address the gRPC listener binds.' }),
        opt('SUPERBANK_GRPC_PORT', { type: 'u16', default: '10000', text: 'Port the gRPC listener binds.' }),
        opt('SUPERBANK_GRPC_MAX_SLOT_RANGE', { type: 'u64', default: '100', text: 'Largest inclusive slot range one stream may request.' }),
        opt('SUPERBANK_GRPC_CHUNK_SLOTS', { type: 'u64', default: '8', text: 'Slots fetched per ClickHouse chunk query.' }),
        opt('SUPERBANK_GRPC_QUERY_TIMEOUT_MS', { type: 'u64 (ms)', default: '30000', text: 'Timeout for each ClickHouse chunk query.' }),
        opt('SUPERBANK_GRPC_MAX_SEND_BYTES', { type: 'usize (bytes)', default: '104857600', text: 'Largest encoded gRPC message sent.' }),
        opt('SUPERBANK_GRPC_MAX_CONCURRENT_STREAMS', { type: 'u32', default: '20', text: 'Concurrent HTTP/2 streams per gRPC connection.' }),
      ],
    },
    {
      id: 'pyroscope',
      title: 'Pyroscope profiling',
      intro: 'Continuous CPU profiling pushed to a Pyroscope server. Build with `--features pyroscope` and set `PYROSCOPE_ENABLED=true` with a `PYROSCOPE_URL`.',
      requires: ['feature:pyroscope', 'when:PYROSCOPE_ENABLED=true'],
      items: [
        opt('PYROSCOPE_ENABLED', { flag: '--pyroscope', type: 'bool', default: 'false', text: 'Start the profiler. Without `PYROSCOPE_URL` it logs a warning and stays off.' }),
        opt('PYROSCOPE_URL', { type: 'url', required: 'when `PYROSCOPE_ENABLED=true`', text: 'Pyroscope server URL, for example `http://localhost:4040`.' }),
        opt('PYROSCOPE_APP_NAME', { type: 'string', default: 'superbank-rpc', text: 'Application name shown in Pyroscope.' }),
        opt('PYROSCOPE_SAMPLE_RATE', { type: 'u32 (Hz)', default: '100', text: 'CPU sampling rate.' }),
        opt('PYROSCOPE_REPORT_THREAD_NAME', { type: 'bool', default: 'true', text: 'Include thread names in profiles.' }),
        opt('PYROSCOPE_REPORT_THREAD_ID', { type: 'bool', default: 'false', text: 'Include thread IDs in profiles.' }),
        opt('PYROSCOPE_TAGS', { type: 'list of key=value', text: 'Tags attached to every profile. Repeat the flag or comma-separate the env value.' }),
        opt('PYROSCOPE_REPORT_ENCODING', { type: 'pprof | folded', default: 'pprof', text: 'Report encoding.' }),
        opt('PYROSCOPE_COMPRESSION', { type: 'gzip | off', default: 'gzip', text: 'Request body compression.' }),
        opt('PYROSCOPE_AUTH_TOKEN', { type: 'string', secret: true, text: 'Bearer token for ingestion. Preferred over basic auth when both are set.' }),
        opt('PYROSCOPE_BASIC_AUTH_USER', {
          type: 'string',
          text: 'Basic auth user for ingestion.',
          relations: [
            { type: 'requires', to: 'PYROSCOPE_BASIC_AUTH_PASS' },
            { type: 'see', to: 'PYROSCOPE_AUTH_TOKEN' },
          ],
        }),
        opt('PYROSCOPE_BASIC_AUTH_PASS', { type: 'string', secret: true, text: 'Basic auth password for ingestion.' }),
        opt('PYROSCOPE_TENANT_ID', { type: 'string', text: 'Tenant for multi-tenant Pyroscope, sent as `X-Scope-OrgID`.' }),
        opt('PYROSCOPE_HTTP_HEADERS', {
          flag: '--pyroscope-http-header',
          type: 'list of Header=Value',
          text: 'Extra HTTP headers on ingestion requests. Repeat the flag or comma-separate the env value.',
        }),
      ],
    },
  ],
};
