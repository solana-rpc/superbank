// Copy for the node info panel. Config keys and defaults were checked against
// the code they name (cli.rs, config.rs, DDL, clap attributes), not against the
// READMEs, because the READMEs have drifted before. Each node lists the files a
// reader should open to re-check it.
//
// Plain text only: `code` spans use backticks and callers render with
// textContent. Refs are repo-relative paths with no line numbers (they drift).

import { normalizeState } from './state.js';
import { ENTRY_SOURCES, buildTopology } from './topology.js';

export const REPO_BLOB = 'https://github.com/solana-rpc/superbank/blob/main/';

// ClickHouse DDL directory per `state.ch`, and the files in each.
const DDL_DIR = { single: 'ddl/local', cluster: 'ddl/cluster', replicated: 'ddl/replicated' };
const DDL_FILES = [
  'transactions',
  'blocks_metadata',
  'entries',
  'gsfa',
  'gsfa_nohot',
  'gsfa_hot',
  'signatures',
  'token_owner_activity',
];
const ddl = (ch, name) => `${DDL_DIR[ch]}/${name}.sql`;

const REFS = {
  architecture: 'docs/architecture.md',
  selfHosting: 'docs/self-hosting.md',
  ddlReadme: 'ddl/README.md',
  ingestReadme: 'crates/superbank/README.md',
  ingestExample: 'superbank.example.yaml',
  ingestCli: 'crates/superbank/src/cli.rs',
  ingestWriter: 'crates/superbank/src/clickhouse.rs',
  ingestGrpc: 'crates/superbank/src/ingest/grpc.rs',
  ingestFumarole: 'crates/superbank/src/ingest/fumarole.rs',
  ingestRpc: 'crates/superbank/src/ingest/rpc.rs',
  ingestBigtable: 'crates/superbank/src/ingest/bigtable.rs',
  ingestSolparq: 'crates/superbank/src/ingest/solparq.rs',
  jetstreamerReadme: 'ingest/jetstreamer-clickhouse-plugin/README.md',
  jetstreamerLib: 'ingest/jetstreamer-clickhouse-plugin/src/lib.rs',
  jetstreamerBatch: 'ingest/run_batch.sh',
  rpcReadme: 'crates/superbank-rpc/README.md',
  rpcConfig: 'crates/superbank-rpc/src/config.rs',
  rpcHeadCache: 'crates/superbank-rpc/src/head_cache',
  rpcDiskCache: 'crates/superbank-rpc/src/disk_cache',
  rpcGrpc: 'crates/superbank-rpc/src/grpc',
  rpcProto: 'crates/superbank-rpc/proto/superbank.proto',
  k6: 'tests/k6',
  solparqReadme: 'crates/superbank-solparq/README.md',
  solparqConfig: 'crates/superbank-solparq/src/config.rs',
  solparqStorage: 'crates/superbank-solparq/src/storage.rs',
  verifyReadme: 'crates/superbank-verify/README.md',
  verifyExample: 'superbank-verify.example.yaml',
  verifyCli: 'crates/superbank-verify/src/cli.rs',
  verifySmoke: 'scripts/dev/run-verify-smoke.sh',
  solparqValidation: 'crates/superbank-solparq/src/clickhouse.rs',
  solparqBackfill: 'crates/superbank-solparq/src/backfill.rs',
};

export function allRefs() {
  const refs = new Set(Object.values(REFS));
  for (const dir of Object.values(DDL_DIR)) refs.add(dir);
  for (const ch of Object.keys(DDL_DIR)) for (const name of DDL_FILES) refs.add(ddl(ch, name));
  return [...refs];
}

const cfg = (key, value, note) => (note ? { key, value, note } : { key, value });
const compact = (items) => items.filter(Boolean);

// Offer "use as ingest source" only when it would change the selected source.
const sourceAction = (state, source) =>
  state.source === source ? {} : { action: { label: 'Use as ingest source', patch: { source } } };

const activeLine = (active, activeText) => (active ? activeText : 'It is not the active ingest source with the current selection.');

// --- Upstream ---------------------------------------------------------------

function solana(state) {
  const inUse = buildTopology(state)
    .nodes.filter((n) => n.zone === 'upstream' && n.kind === 'endpoint')
    .map((n) => n.label);
  return {
    title: 'Solana network',
    subtitle: 'validators · origin of every block',
    body: [
      'Validators produce the ledger, and Superbank never joins gossip or runs a validator: it reads blocks through an endpoint that someone else operates.',
      'Live endpoints (Yellowstone gRPC, Fumarole) stream blocks as they are produced, while history endpoints (Solana JSON-RPC, Bigtable, Old Faithful) are pulled in bounded jobs.',
      inUse.length > 0
        ? `Endpoints in use with the current selection: ${inUse.join(', ')}.`
        : 'No endpoint is in use with the current selection: the data comes from Parquet bundles instead.',
    ],
    config: [],
    refs: [REFS.architecture, REFS.ingestReadme],
  };
}

function srcDragonsmouth(state) {
  return {
    title: 'Yellowstone gRPC',
    subtitle: 'DragonsMouth · Geyser block stream',
    body: [
      'DragonsMouth streams full blocks over gRPC, and `superbank --source grpc` opens one `blocks` subscription with transactions and PoH entries, at `finalized` commitment by default.',
      activeLine(
        state.source === 'grpc',
        'This is the active ingest source, so a stream error, unhealthy status or idle timeout ends the process rather than reconnecting.',
      ),
      state.head
        ? 'The head cache is enabled, so `superbank-rpc` also opens its own separate subscription to a DragonsMouth endpoint for the newest slots.'
        : 'If the optional head cache is enabled, `superbank-rpc` opens its own separate subscription; it does not share the ingestor connection.',
    ],
    config: [
      cfg('endpoint', 'required', '--endpoint / DRAGONSMOUTH_ENDPOINT'),
      cfg('x-token', 'optional', 'DRAGONSMOUTH_X_TOKEN'),
      cfg('commitment', 'finalized', 'processed | confirmed | finalized'),
      cfg('dragonsmouth-from-slot', 'unset', '"*" resumes from max(slot) in blocks_metadata; 0 starts at the earliest available slot'),
      cfg('grpc-idle-timeout-secs', '30', 'exit if no message arrives for this long'),
      cfg('grpc-health-watch-enabled', 'true', 'exit if the gRPC health status stops being SERVING'),
      cfg('grpc-max-decoding-bytes', '67108864'),
    ],
    refs: [REFS.ingestGrpc, REFS.ingestExample, REFS.rpcHeadCache],
    ...sourceAction(state, 'grpc'),
  };
}

function srcFumarole(state) {
  return {
    title: 'Yellowstone Fumarole',
    subtitle: 'persistent stream · consumer groups',
    body: [
      'Fumarole is Yellowstone’s persistent stream: the server keeps a cursor per consumer group, and `superbank --source fumarole` reassembles each block from transaction, block-meta and entry updates.',
      'Offsets are committed only after the rows are flushed to ClickHouse, so a restart resumes where the group left off.',
      activeLine(state.source === 'fumarole', 'This is the active ingest source.'),
    ],
    config: [
      cfg('fumarole-endpoint', 'required', 'FUMAROLE_ENDPOINT'),
      cfg('fumarole-x-token', 'optional', 'FUMAROLE_X_TOKEN'),
      cfg('fumarole-consumer-group', 'required', 'FUMAROLE_CONSUMER_GROUP'),
      cfg('fumarole-create-consumer-group', 'false', 'create the group before subscribing'),
      cfg('fumarole-from-slot', 'unset', 'only used when the group is created; an existing group keeps its stored offset'),
      cfg('commitment', 'finalized', 'processed | confirmed | finalized'),
      cfg('fumarole-data-plane-tcp-connections', '4', 'maximum 20'),
    ],
    refs: [REFS.ingestFumarole, REFS.ingestExample, REFS.ingestReadme],
    ...sourceAction(state, 'fumarole'),
  };
}

function srcJsonrpc(state) {
  return {
    title: 'Solana JSON-RPC',
    subtitle: 'getBlocks · getBlock',
    body: [
      '`superbank --source rpc` uses a plain Solana JSON-RPC endpoint for bounded backfills: `getBlocks` discovers the slots, then `getBlock` fetches each one.',
      activeLine(state.source === 'rpc', 'This is the active ingest source, and the job ends when the requested range is done.'),
    ],
    config: [
      cfg('rpc-url', 'required', 'RPC_URL'),
      cfg('rpc-from-slot', 'required', 'unless rpc-slot-list is set; "*" resumes from max(slot) in blocks_metadata, 0 starts at the earliest available slot'),
      cfg('rpc-to-slot', 'unset', 'inclusive end; give exactly one of rpc-to-slot or rpc-slot-count'),
      cfg('rpc-slot-list', 'unset', 'fetch exactly these slots; excludes the range options and rpc-skip-ingested-slots'),
      cfg('rpc-timeout-secs', '30'),
      cfg('rpc-retry-backoff-ms', '500'),
    ],
    refs: [REFS.ingestRpc, REFS.ingestExample],
    ...sourceAction(state, 'rpc'),
  };
}

// The archiver's own validation endpoint; only drawn while archiving is on.
function solparqRpc() {
  return {
    title: 'Solana RPC (archive validation)',
    subtitle: 'superbank-solparq · --solana-rpc-url',
    body: [
      'Before it archives a range, `superbank-solparq` calls `getBlocks` here to confirm which slots Solana actually produced, then checks ClickHouse for missing blocks and transaction-count mismatches.',
      'It is configured on the archiver alone (`--solana-rpc-url`), separate from whatever endpoint feeds `superbank`, so it can be a different provider.',
      'If this endpoint cannot answer (for example `getBlocks` is disabled), archiving stalls unless `--allow-rpc-validation-failure` is set; verified data problems still block. `--backfill-gaps` also uses it to refetch missing slots through a `superbank --source rpc` subprocess.',
    ],
    config: [
      cfg('--solana-rpc-url', 'https://api.mainnet-beta.solana.com', 'SOLPARQ_SOLANA_RPC_URL'),
      cfg('--allow-rpc-validation-failure', 'false', 'archive even when this check cannot run'),
      cfg('--force-archive', 'false', 'archive despite missing blocks or mismatches'),
      cfg('--backfill-gaps', 'false', 'refetch missing slots from this endpoint first'),
    ],
    refs: [REFS.solparqReadme, REFS.solparqConfig, REFS.solparqValidation, REFS.solparqBackfill],
  };
}

function srcBigtable(state) {
  return {
    title: 'Solana Bigtable',
    subtitle: 'LedgerStorage · read-only',
    body: [
      '`superbank --source bigtable` reads Solana’s Bigtable ledger storage read-only, as a one-shot backfill over a slot range, an epoch range or a slot file.',
      'It writes no `entries`, and epoch ranges need `rpc-url` to resolve epochs to slots.',
      activeLine(state.source === 'bigtable', 'This is the active ingest source.'),
    ],
    config: [
      cfg('bigtable-range', 'unset', 'exactly one of this or bigtable-slot-file; slots 123:456, epochs 1-10, or one epoch 5'),
      cfg('bigtable-slot-file', 'unset', 'whitespace-separated slot list; BIGTABLE_SLOT_FILE'),
      cfg('bigtable-instance', 'solana-ledger'),
      cfg('bigtable-app-profile', 'default'),
      cfg('bigtable-credential-path', 'unset', 'mutually exclusive with bigtable-credential-json'),
      cfg('bigtable-credential-json', 'unset', 'stringified credentials'),
    ],
    refs: [REFS.ingestBigtable, REFS.ingestExample, REFS.ingestReadme],
    ...sourceAction(state, 'bigtable'),
  };
}

function srcOldFaithful(state) {
  return {
    title: 'Old Faithful',
    subtitle: 'epoch CAR archives',
    body: [
      'Old Faithful is a public archive of Solana ledger history, stored as one CAR file per epoch.',
      'The Jetstreamer ClickHouse plugin replays those epochs, PoH entries included; it is a separate binary, not a `--source` of `superbank`.',
      activeLine(state.source === 'jetstreamer', 'This is the active ingest source.'),
    ],
    config: [
      cfg('JETSTREAMER_COMPACT_INDEX_BASE_URL', 'https://files.old-faithful.net', 'default set by ingest/run_batch.sh'),
      cfg('JETSTREAMER_HTTP_BASE_URL', 'http://localhost:8080', 'default set by ingest/run_batch.sh; JETSTREAMER_ARCHIVE_BASE also feeds it'),
    ],
    refs: [REFS.jetstreamerBatch, REFS.jetstreamerReadme],
    ...sourceAction(state, 'jetstreamer'),
  };
}

// --- Ingest -----------------------------------------------------------------

const sourceCfg = (value) =>
  cfg('source', value, '--source / SUPERBANK_SOURCE; precedence is flags, then env, then YAML, then defaults');

// Shared by the sources that go through the buffered writer (grpc, fumarole).
const writerSentence =
  'Rows are buffered and flushed in order, `transactions` then `blocks_metadata` then `entries`, on the first of `flush-interval-secs`, `transactions-flush-rows` or `blocks-flush-rows`.';

function ingestGrpc() {
  return {
    title: 'superbank',
    subtitle: 'crates/superbank · --source grpc',
    body: [
      'Subscribes to one DragonsMouth `blocks` filter (transactions and entries included) and maps each block to rows for `transactions`, `blocks_metadata` and `entries`.',
      'A live daemon with no reconnect loop: a stream end, error, unhealthy status or idle timeout flushes once and exits non-zero, so run it under a supervisor.',
      writerSentence,
    ],
    config: [
      sourceCfg('grpc'),
      cfg('dragonsmouth-from-slot', 'unset', '"*" resumes from max(slot) in blocks_metadata'),
      cfg('flush-interval-secs', '5'),
      cfg('transactions-flush-rows', '25000'),
      cfg('blocks-flush-rows', '2000'),
      cfg('insert-max-retries', '5', 'backoff from insert-retry-base-ms 1000 up to insert-retry-max-ms 30000'),
      cfg('clickhouse-async-insert', 'false'),
      cfg('health-stale-secs', '120', '/health returns 503 if the last flush is older; metrics on :9901'),
    ],
    refs: [REFS.ingestGrpc, REFS.ingestWriter, REFS.ingestCli, REFS.ingestReadme],
  };
}

function ingestFumarole() {
  return {
    title: 'superbank',
    subtitle: 'crates/superbank · --source fumarole',
    body: [
      'Joins a Fumarole consumer group and reassembles each block from transaction, block-meta and entry updates before buffering its rows.',
      'Offsets are committed only after a successful flush, `fumarole-memory-soft-limit-bytes` makes it stop polling and flush early, and a stream error or idle timeout flushes once and exits non-zero.',
      writerSentence,
      'Insert retries (`insert-max-retries`) cover only the gRPC, RPC and Bigtable sources, so a failed Fumarole flush ends the run.',
    ],
    config: [
      sourceCfg('fumarole'),
      cfg('fumarole-commit-interval-secs', '10'),
      cfg('fumarole-no-commit', 'false', 'diagnostics only'),
      cfg('fumarole-memory-soft-limit-bytes', '25769803776', '24 GiB; 0 disables the guard'),
      cfg('fumarole-data-channel-capacity', '4096'),
      cfg('flush-interval-secs', '5'),
      cfg('transactions-flush-rows', '25000'),
      cfg('health-stale-secs', '120', '/health returns 503 if the last flush is older; metrics on :9901'),
    ],
    refs: [REFS.ingestFumarole, REFS.ingestWriter, REFS.ingestCli, REFS.ingestReadme],
  };
}

function ingestRpc() {
  return {
    title: 'superbank',
    subtitle: 'crates/superbank · --source rpc',
    body: [
      'A one-shot, bounded backfill: `getBlocks` discovers the slots in the range, then up to `rpc-max-inflight` workers fetch `getBlock` and map each block to rows.',
      'No cursor is kept and re-runs are safe because the base tables are `ReplacingMergeTree`; `rpc-skip-ingested-slots` fetches only the gaps, and retryable RPC errors are retried.',
      'It writes no `entries` because `getBlock` does not expose them, and slots that Solana skipped simply produce no row.',
    ],
    config: [
      sourceCfg('rpc'),
      cfg('rpc-max-inflight', '64', 'concurrent getBlock requests'),
      cfg('rpc-flush-every-slots', '500'),
      cfg('rpc-skip-ingested-slots', 'false', 'discovery skips slots already in blocks_metadata'),
      cfg('rpc-max-supported-tx-version', '1', 'blocks with a newer transaction version are rejected'),
      cfg('rpc-discovery-chunk-slots', '10000'),
      cfg('insert-max-retries', '5', 'backoff from insert-retry-base-ms 1000 up to insert-retry-max-ms 30000'),
    ],
    refs: [REFS.ingestRpc, REFS.ingestCli, REFS.ingestExample, REFS.ingestReadme],
  };
}

function ingestBigtable() {
  return {
    title: 'superbank',
    subtitle: 'crates/superbank · --source bigtable',
    body: [
      'A one-shot, read-only backfill: slots are discovered, fetched in batches of `bigtable-fetch-batch-size`, decoded in parallel and inserted while later batches are still being fetched.',
      'It writes no `entries`, so `superbank-verify` cannot check these slots.',
      'Epoch ranges resolve through the RPC epoch schedule and need `rpc-url`, and failed inserts retry with backoff (`insert-max-retries`).',
    ],
    config: [
      sourceCfg('bigtable'),
      cfg('bigtable-fetch-concurrency', '4', 'in-flight fetch batches'),
      cfg('bigtable-fetch-batch-size', '500', 'slots per multi-row fetch'),
      cfg('bigtable-decode-concurrency', 'CPU threads', 'defaults to the available parallelism'),
      cfg('bigtable-insert-concurrency', '1'),
      cfg('bigtable-discovery-limit', '10000'),
      cfg('rpc-max-supported-tx-version', '1', 'shared with the rpc source'),
      cfg('insert-max-retries', '5'),
    ],
    refs: [REFS.ingestBigtable, REFS.ingestCli, REFS.ingestExample, REFS.ingestReadme],
  };
}

function ingestSolparq(state) {
  const s3 = state.archive === 's3';
  return {
    title: 'superbank',
    subtitle: 'crates/superbank · --source solparq',
    body: [
      'A one-shot restore that skips the row pipeline: ClickHouse does the loading from bundles written by `superbank-solparq`, and a bundle whose manifest `format_version` is newer than superbank understands is refused.',
      s3
        ? 'S3: ClickHouse pulls each object itself with `INSERT … SELECT FROM s3()`, naming the columns explicitly so they match by name.'
        : 'Local: each bundle’s Parquet file is streamed to ClickHouse as the body of `INSERT … FORMAT Parquet`, with no row decoding in superbank.',
      'Every archived table is restored, derived index tables included, into the configured `clickhouse-database` under its bare table name, and `entries` returns only if the bundle archived it.',
    ],
    config: [
      sourceCfg('solparq'),
      cfg('solparq-archive-location', s3 ? 's3' : 'local', 'required; local | s3'),
      s3
        ? cfg('solparq-archive-s3-endpoint', 'required', 'with solparq-archive-s3-bucket-name, -auth-key and -auth-secret-key')
        : cfg('solparq-archive-path', 'required', 'a bundle directory, or a directory of bundles'),
      s3 ? cfg('solparq-archive-s3-region', 'us-east-1') : cfg('solparq-from-slot', 'unset', 'inclusive lower bound'),
      s3 ? cfg('solparq-archive-s3-bucket-path', 'unset', 'optional prefix inside the bucket') : cfg('solparq-to-slot', 'unset', 'inclusive upper bound'),
      cfg('solparq-tables', 'every table in the bundle', 'comma-separated table kinds to restore'),
      cfg('solparq-clickhouse-settings', 'empty', 'raw SETTINGS clause appended to restore statements'),
    ],
    refs: [REFS.ingestSolparq, REFS.ingestCli, REFS.ingestExample, REFS.solparqReadme],
  };
}

function ingestJetstreamer() {
  return {
    title: 'jetstreamer-clickhouse',
    subtitle: 'ingest/jetstreamer-clickhouse-plugin · separate binary',
    body: [
      'Not a `superbank` source: the Jetstreamer ClickHouse plugin takes an epoch number or a `start:end` slot range, replays Old Faithful data and inserts blocks, transactions and PoH entries.',
      'Each table has its own async inserter, so nothing orders them (`blocks_metadata` can land before its transactions), and after its retries run out it keeps backing off and retrying.',
      'It lives outside the root Cargo workspace, needs the `ingest/jetstreamer` submodule, and is not built by root CI.',
    ],
    config: [
      cfg('JETSTREAMER_THREADS', 'auto', 'derived from the machine; ingest/run_batch.sh sets 48'),
      cfg('JETSTREAMER_CLICKHOUSE_FLUSH_MAX_ROWS', '100000'),
      cfg('JETSTREAMER_CLICKHOUSE_FLUSH_INTERVAL_MS', '10000'),
      cfg('JETSTREAMER_CLICKHOUSE_MAX_INFLIGHT_BATCHES', '8', 'concurrent insert workers per thread'),
      cfg('JETSTREAMER_CLICKHOUSE_ASYNC_INSERT', 'true', 'the superbank ingestor defaults to false'),
      cfg('JETSTREAMER_CLICKHOUSE_RETRY_MAX', '5'),
      cfg('JETSTREAMER_INGEST_CLICKHOUSE_DSN', 'unset', 'overrides the ClickHouse client supplied by the runner'),
    ],
    refs: [REFS.jetstreamerReadme, REFS.jetstreamerLib, REFS.jetstreamerBatch],
  };
}

const INGEST = {
  grpc: ingestGrpc,
  fumarole: ingestFumarole,
  rpc: ingestRpc,
  bigtable: ingestBigtable,
  solparq: ingestSolparq,
  jetstreamer: ingestJetstreamer,
};

// --- ClickHouse -------------------------------------------------------------

// Shard keys from ddl/cluster/*.sql (the same map topology.js labels tables with).
const SHARD_KEY = {
  transactions: 'intDiv(slot, 432000)',
  blocks_metadata: 'intDiv(slot, 432000)',
  entries: 'intDiv(slot, 432000)',
  gsfa: 'cityHash64(address)',
  signatures: 'cityHash64(signature)',
  gsfa_hot: 'cityHash64(signature)',
  token_owner_activity: 'cityHash64(owner)',
};

const engineFor = (state) =>
  state.ch === 'replicated' ? 'ReplicatedReplacingMergeTree(…, slot)' : 'ReplacingMergeTree(slot)';

function layoutSentence(state, table) {
  if (state.ch === 'single') return null;
  const engine = state.ch === 'replicated' ? 'ReplicatedReplacingMergeTree' : 'ReplacingMergeTree';
  return `In this layout rows live in shard-local \`${table}_local\` ${engine} tables behind a \`Distributed\` table sharded by \`${SHARD_KEY[table]}\` (the drawing shows three shards only as an illustration).`;
}

function tableConfig(state, table, partition, order, extra) {
  return compact([
    cfg('ENGINE', engineFor(state)),
    cfg('PARTITION BY', partition),
    cfg('ORDER BY', order),
    state.ch === 'single' ? null : cfg('Distributed shard key', SHARD_KEY[table]),
    ...extra,
  ]);
}

const tableSubtitle = (state, what) => (state.ch === 'single' ? what : `${what} · Distributed`);

function tableTransactions(state) {
  const write =
    state.source === 'jetstreamer'
      ? 'Inserted by the plugin’s own writer with no ordering against the other tables, it serves'
      : state.source === 'solparq'
        ? 'Restored from the bundle’s `transactions.parquet`, it serves'
        : 'Written first in every flush, it serves';
  return {
    title: 'transactions',
    subtitle: tableSubtitle(state, 'ClickHouse base table'),
    body: compact([
      'The source of truth: one row per transaction with its message, status meta, balances and logs, and the base of every RPC index.',
      `${write} \`getTransaction\`, the transaction payloads in \`getBlock\`, and full hydration in \`getTransactionsForAddress\`.`,
      layoutSentence(state, 'transactions'),
    ]),
    config: tableConfig(state, 'transactions', 'intDiv(slot, 432000)', '(slot, slot_idx, signature)', [
      cfg('transactions-table', 'default.transactions', 'ingestor; env CLICKHOUSE_TRANSACTIONS_TABLE'),
      cfg('CLICKHOUSE_TRANSACTION_TABLE', 'default.transactions', 'superbank-rpc'),
    ]),
    refs: [ddl(state.ch, 'transactions'), REFS.architecture, REFS.ddlReadme],
  };
}

function tableBlocksMetadata(state) {
  const write =
    state.source === 'jetstreamer'
      ? 'The plugin writes it independently, so a row can appear before its transactions; it serves'
      : state.source === 'solparq'
        ? 'Restored from the bundle’s `blocks_metadata.parquet`, it serves'
        : 'Written second, after the slot’s transactions, so a row normally means they have landed; it serves';
  return {
    title: 'blocks_metadata',
    subtitle: tableSubtitle(state, 'ClickHouse base table'),
    body: compact([
      'One row per produced slot: blockhash, parent, block time, heights, a rewards summary and `executed_transaction_count`.',
      `${write} \`getBlock\`, \`getBlockTime\`, \`getBlocks\` and \`getFirstAvailableBlock\`.`,
      'It is also the cursor and cross-check table: `"*"` start slots resume from its highest slot, solparq compares its transaction counts with `transactions`, and `superbank-verify` reads it.',
      layoutSentence(state, 'blocks_metadata'),
    ]),
    config: tableConfig(state, 'blocks_metadata', 'intDiv(slot, 432000)', '(slot)', [
      cfg('blocks-table', 'default.blocks_metadata', 'ingestor; env CLICKHOUSE_BLOCKS_TABLE'),
      cfg('blocks-flush-rows', '2000'),
      cfg('CLICKHOUSE_BLOCKS_METADATA_TABLE', 'default.blocks_metadata', 'superbank-rpc'),
    ]),
    refs: [ddl(state.ch, 'blocks_metadata'), REFS.architecture, REFS.ddlReadme],
  };
}

function tableEntries(state) {
  const source =
    state.source === 'solparq'
      ? 'It is restored only if the bundle archived it.'
      : ENTRY_SOURCES.includes(state.source)
        ? 'The selected source writes it, so these slots can be verified.'
        : 'The selected source writes no entries, so the table stays empty for these slots.';
  return {
    title: 'entries',
    subtitle: tableSubtitle(state, 'ClickHouse base table'),
    body: compact([
      'Proof-of-History entries, one row per entry (`num_hashes`, hash, transaction index range): no RPC handler needs them, only `superbank-verify` does.',
      source,
      'Written by the gRPC and Fumarole sources and the Jetstreamer plugin, so apply `entries.sql` first, since the default `entries-table` is `default.entries`.',
      layoutSentence(state, 'entries'),
    ]),
    config: tableConfig(state, 'entries', 'intDiv(slot, 432000)', '(slot, entry_index)', [
      cfg('entries-table', 'default.entries', 'ingestor; env CLICKHOUSE_ENTRIES_TABLE'),
    ]),
    refs: [ddl(state.ch, 'entries'), REFS.verifyReadme, REFS.ddlReadme],
  };
}

function tableGsfa(state) {
  return {
    title: 'gsfa',
    subtitle: tableSubtitle(state, 'ClickHouse materialized view'),
    body: compact([
      'The address index: one row per (address, transaction), built by a materialized view over `transactions` that skips the System and Vote programs and the Clock and SlotHashes sysvars.',
      'Bucketed by `cityHash64(address) % 32` and sorted newest first, it serves `getSignaturesForAddress` and the signature side of `getTransactionsForAddress`.',
      'Apply `gsfa_nohot.sql` instead of `gsfa.sql` if hot addresses should live only in `gsfa_hot`.',
      layoutSentence(state, 'gsfa'),
    ]),
    config: tableConfig(state, 'gsfa', 'addr_bucket', '(addr_bucket, address, slot DESC, slot_idx DESC, signature)', [
      cfg('CLICKHOUSE_GSFA_TABLE', 'default.gsfa', 'superbank-rpc'),
    ]),
    refs: [ddl(state.ch, 'gsfa'), ddl(state.ch, 'gsfa_nohot'), REFS.architecture],
  };
}

function tableSignatures(state) {
  return {
    title: 'signatures',
    subtitle: tableSubtitle(state, 'ClickHouse materialized view'),
    body: compact([
      'The signature index: (slot, slot_idx, err) for every signature of every transaction, not only the first, with a bloom filter on `signature` for point lookups.',
      'It serves `getSignatureStatuses` and the position lookups behind `getTransaction` and address pagination.',
      layoutSentence(state, 'signatures'),
    ]),
    config: tableConfig(state, 'signatures', 'sig_bucket', '(sig_bucket, signature, slot DESC, slot_idx)', [
      cfg('CLICKHOUSE_SIGNATURE_STATUSES_TABLE', 'default.signatures', 'superbank-rpc'),
    ]),
    refs: [ddl(state.ch, 'signatures'), REFS.architecture],
  };
}

function tableGsfaHot(state) {
  return {
    title: 'gsfa_hot',
    subtitle: tableSubtitle(state, 'ClickHouse materialized view · optional'),
    body: compact([
      'An optional second address index for a few extremely busy addresses (the shipped DDL lists the USDC mint), partitioned by 14-day slot windows to ease part and merge pressure.',
      'Reads reach it only for addresses in `CLICKHOUSE_GSFA_HOT_ADDRESSES`, and an address with no rows falls back to `gsfa`, so keep that setting and the DDL address list in sync.',
      'The RPC README pairs it with `gsfa_nohot.sql` in place of `gsfa.sql`, so hot addresses are stored once.',
      layoutSentence(state, 'gsfa_hot'),
    ]),
    config: tableConfig(state, 'gsfa_hot', '(addr_bucket, intDiv(slot, 3024000))', '(addr_bucket, address, slot DESC, slot_idx DESC, signature)', [
      cfg('CLICKHOUSE_GSFA_HOT_ADDRESSES', 'empty', 'superbank-rpc; comma-separated'),
      cfg('CLICKHOUSE_GSFA_HOT_TABLE', 'default.gsfa_hot', 'superbank-rpc'),
    ]),
    refs: [ddl(state.ch, 'gsfa_hot'), ddl(state.ch, 'gsfa_nohot'), REFS.rpcReadme],
  };
}

function tableTokenOwnerActivity(state) {
  return {
    title: 'token_owner_activity',
    subtitle: tableSubtitle(state, 'ClickHouse materialized view · optional'),
    body: compact([
      'An optional index from a token account’s owner to transactions, built from token balance changes, with the token account and a `balance_changed` flag on each row.',
      'It enables the `tokenAccounts` filters in `getTransactionsForAddress`; without the table they are rejected.',
      layoutSentence(state, 'token_owner_activity'),
    ]),
    config: tableConfig(state, 'token_owner_activity', 'owner_bucket', '(owner_bucket, owner, slot DESC, slot_idx DESC, signature, token_account)', [
      cfg('CLICKHOUSE_TOKEN_OWNER_ACTIVITY_TABLE', 'default.token_owner_activity', 'superbank-rpc'),
    ]),
    refs: [ddl(state.ch, 'token_owner_activity'), REFS.architecture],
  };
}

function clickhouse(state) {
  const lead = {
    single: [
      'One ClickHouse node using `ddl/local`: base tables and materialized views live directly in `default`, the layout meant for local development.',
      'Base tables are `ReplacingMergeTree(slot)`, so re-ingesting a range deduplicates when parts merge.',
    ],
    cluster: [
      'A ClickHouse cluster using `ddl/cluster`: each table is a shard-local `*_local` `ReplacingMergeTree` plus a `Distributed` table on the `{cluster}` macro, and both the ingestor and `superbank-rpc` use the Distributed tables.',
      'Base tables shard by epoch (`intDiv(slot, 432000)`) and the indexes by a hash of address, signature or owner; the drawing shows three shards only as an illustration.',
    ],
    replicated: [
      'A replicated cluster using `ddl/replicated`: the cluster layout with every shard’s tables created as `ReplicatedReplacingMergeTree`, so each shard has replicas.',
      'It needs Keeper (or ZooKeeper), the `{cluster}`, `{shard}` and `{replica}` macros and `internal_replication=1`; the drawing shows three shards with two replicas as an illustration.',
    ],
  }[state.ch];
  const config = [
    cfg('CLICKHOUSE_URL', 'http://localhost:8123', 'every binary; HTTP interface'),
    cfg('CLICKHOUSE_DATABASE', 'default'),
    cfg('CLICKHOUSE_USER', 'default'),
    cfg('CLICKHOUSE_ASYNC_INSERT', 'false', 'ingestor'),
    cfg('DDL directory', DDL_DIR[state.ch], 'apply one set consistently; transactions.sql before the materialized views'),
  ];
  if (state.ch !== 'single') {
    config.push(
      cfg('CLICKHOUSE_CLUSTER', '{cluster}', 'superbank-rpc; empty selects local-only cancellation on standalone nodes'),
      cfg('CLICKHOUSE_SCOPE', 'distributed', 'superbank-rpc; shard-direct queries shard-local tables instead'),
    );
  }
  return {
    title: 'ClickHouse',
    subtitle: { single: 'single node · ddl/local', cluster: 'Distributed · ddl/cluster', replicated: 'Distributed + replicas · ddl/replicated' }[state.ch],
    body: [
      ...lead,
      'The DDL is the contract between ingestion and serving: the ingestor writes the base tables, materialized views build the indexes, and the RPC reads them.',
    ],
    config,
    refs: [REFS.ddlReadme, DDL_DIR[state.ch], REFS.architecture, REFS.selfHosting],
  };
}

function keeper() {
  return {
    title: 'ClickHouse Keeper',
    subtitle: 'replicated mode · replication coordination',
    body: [
      'Keeper (or ZooKeeper) holds the replication log for the `ReplicatedReplacingMergeTree` tables, so each shard’s replicas converge on the same parts.',
      'Only ClickHouse talks to it, and this repository ships no Keeper deployment: the replicated DDL fails unless one is already configured and reachable.',
    ],
    config: [
      cfg('ZooKeeper path', '/clickhouse/tables/{cluster}/{database}/{table}/{shard}', 'from the replicated DDL'),
      cfg('replica name', '{replica}'),
      cfg('internal_replication', '1', 'required on the cluster definition'),
    ],
    refs: [DDL_DIR.replicated, REFS.selfHosting, REFS.architecture],
  };
}

// --- Serve ------------------------------------------------------------------

function rpc(state) {
  const features = [];
  if (state.head) features.push('grpc-head-cache');
  if (state.disk) features.push('disk-cache');
  if (state.stream) features.push('grpc-streaming');
  const tiers = [state.head && 'the head cache', state.disk && 'the disk cache', 'source ClickHouse'].filter(Boolean);
  return {
    title: 'superbank-rpc',
    subtitle: state.stream ? 'crates/superbank-rpc · JSON-RPC :8899 + gRPC :10000' : 'crates/superbank-rpc · JSON-RPC :8899',
    body: [
      'A Solana-compatible JSON-RPC server (`getTransaction`, `getBlock`, `getSignaturesForAddress`, a custom `getTransactionsForAddress` and more) that reads the ClickHouse tables the ingestor fills.',
      `Each read tries ${tiers.join(', then ')}, falling through on a miss, and there is no upstream Solana RPC fallback and no Parquet read path.`,
      features.length > 0
        ? `It needs the Cargo features ${features.map((f) => `\`${f}\``).join(', ')} on top of the default ClickHouse-only build, each also switched on at runtime.`
        : 'The default build reads ClickHouse only; the head cache, disk cache and gRPC streaming are optional Cargo features, all off here.',
    ],
    config: [
      cfg('RPC_PORT', '8899', 'RPC_HOST defaults to 0.0.0.0'),
      cfg('METRICS_PORT', '9900'),
      cfg('CLICKHOUSE_URL', 'http://localhost:8123'),
      cfg('RPC_REQUEST_TIMEOUT_MS', '10000'),
      cfg('RPC_CONCURRENCY_LIMIT', '512', 'in-flight HTTP envelopes'),
      cfg('RPC_MAX_BATCH_SIZE', '64'),
      cfg('Cargo features', features.length > 0 ? features.join(', ') : 'none (default build)'),
    ],
    refs: [REFS.rpcReadme, REFS.rpcConfig, REFS.architecture, REFS.k6],
  };
}

function headCache(state) {
  return {
    title: 'Head cache',
    subtitle: 'superbank-rpc · Cargo feature grpc-head-cache',
    body: compact([
      'An in-memory window of the newest `HEAD_CACHE_RETAIN_SLOTS` slots (32 by default, a development size; production keeps several hundred or more, sized to the memory superbank-rpc has), fed by its own DragonsMouth subscription, independent of the ingestor, which reconnects with backoff when the stream drops.',
      'It makes `processed` commitment possible on a subset of methods (`getBlock` never accepts it), and without it `processed` requests are rejected.',
      'Handlers merge its data with ClickHouse results while the cache itself never queries ClickHouse, and the feature pulls in an AGPL-3.0 dependency, which is why it is not a default feature.',
      state.disk ? 'It is independent of the disk cache, which is filled from source ClickHouse and not from this cache.' : null,
    ]),
    config: [
      cfg('HEAD_CACHE_ENABLED', 'false', 'runtime switch on top of the Cargo feature'),
      cfg('DRAGONSMOUTH_ENDPOINT', 'required', 'when enabled'),
      cfg('DRAGONSMOUTH_X_TOKEN', 'optional'),
      cfg('HEAD_CACHE_RETAIN_SLOTS', '32', 'development default; production keeps several hundred or more'),
      cfg('HEAD_CACHE_MIN_COMMITMENT', 'processed', 'processed | confirmed | finalized; a floor on how fresh head reads may be'),
      cfg('GRPC_MAX_DECODING_BYTES', '67108864'),
    ],
    refs: [REFS.rpcHeadCache, REFS.rpcReadme, REFS.rpcConfig],
  };
}

function diskCache(state) {
  return {
    title: 'Disk cache',
    subtitle: 'superbank-rpc · Cargo feature disk-cache',
    body: compact([
      'A separate ClickHouse on localhost that keeps recent finalized slots: the forwarder streams `SELECT … FORMAT Native` from source ClickHouse into a local `INSERT … FORMAT Native`, newest ranges first, at least 75 slots behind the finalized tip.',
      'Local materialized views rebuild the indexes, a range counts as covered only after its transaction counts validate, and misses, holes and errors fall through to source ClickHouse.',
      'It serves `getBlock`, `getBlocks`, `getBlockTime`, `getTransaction`, `getSignatureStatuses`, `getSignaturesForAddress` and `getTransactionsForAddress`, but it is a near cache and never a source of truth.',
      state.head ? 'The head cache is also on; the two are independent features and this cache is not fed from the head cache.' : null,
    ]),
    config: [
      cfg('DISK_CACHE_ENABLED', 'false', 'runtime switch on top of the Cargo feature'),
      cfg('DISK_CACHE_RETAIN_SLOTS', 'required', 'finalized slots to keep'),
      cfg('DISK_CACHE_CLICKHOUSE_URL', 'http://127.0.0.1:8123', 'host must be localhost or a loopback IP'),
      cfg('DISK_CACHE_CLICKHOUSE_DATABASE', 'superbank_disk_cache', 'owned exclusively by the cache'),
      cfg('DISK_CACHE_REQUIRED', 'false', 'true makes startup and /health depend on the cache'),
      cfg('DISK_CACHE_REPAIR_MIN_LAG_SLOTS', '75'),
      cfg('DISK_CACHE_MAX_BYTES', '0', '0 means unlimited'),
      cfg('GSFA_RACE_PRIMARY', 'true', 'race the local address page against the source page'),
    ],
    refs: [REFS.rpcDiskCache, REFS.rpcReadme, REFS.rpcConfig],
  };
}

function jsonrpcClients(state) {
  return {
    title: 'JSON-RPC clients',
    subtitle: 'wallets, explorers, indexers · HTTP :8899',
    body: [
      'Anything that speaks Solana JSON-RPC over HTTP, with batches of up to `RPC_MAX_BATCH_SIZE` calls.',
      state.head
        ? '`processed` commitment is accepted on a subset of methods because the head cache is on.'
        : '`processed` commitment is rejected with `-32602`, so use `confirmed` or `finalized`.',
      'The `X-Superbank-Sources` response header reports which tiers a request touched.',
    ],
    config: [
      cfg('RPC_PORT', '8899'),
      cfg('RPC_MAX_BATCH_SIZE', '64'),
      cfg('RPC_MAX_BODY_BYTES', '1048576'),
      cfg('RPC_REQUEST_TIMEOUT_MS', '10000'),
    ],
    refs: [REFS.rpcReadme, REFS.k6],
  };
}

function grpcClients() {
  return {
    title: 'gRPC clients',
    subtitle: 'Superbank gRPC · StreamBlocks / StreamTransactions',
    body: [
      'Clients of the optional Superbank gRPC API: `StreamBlocks` returns one message per block and `StreamTransactions` one per transaction, over a bounded, inclusive slot range read from ClickHouse.',
      'It is historical only, separate from Yellowstone ingestion and the head cache, and its unary methods and bidirectional `Get` return `UNIMPLEMENTED`.',
    ],
    config: [
      cfg('SUPERBANK_GRPC_ENABLED', 'false', 'also needs the grpc-streaming Cargo feature'),
      cfg('SUPERBANK_GRPC_PORT', '10000'),
      cfg('SUPERBANK_GRPC_MAX_SLOT_RANGE', '100', 'inclusive slots per stream request'),
      cfg('SUPERBANK_GRPC_CHUNK_SLOTS', '8'),
      cfg('SUPERBANK_GRPC_QUERY_TIMEOUT_MS', '30000'),
      cfg('SUPERBANK_GRPC_MAX_CONCURRENT_STREAMS', '20'),
    ],
    refs: [REFS.rpcProto, REFS.rpcGrpc, REFS.rpcReadme],
  };
}

// --- Archive & verify -------------------------------------------------------

function parquetStore(state) {
  const s3 = state.archive === 's3';
  const archiving = state.archive !== 'off';
  const restoring = state.source === 'solparq';
  const write = s3
    ? 'solparq sends ClickHouse an `INSERT INTO FUNCTION s3(…)` query per table and ClickHouse uploads the Parquet itself; solparq then writes `manifest.json`, `report.json` and the `.done` marker.'
    : 'solparq writes each bundle into a staging directory and renames it into place once the checksums and manifest exist.';
  const read = s3
    ? 'A restore has ClickHouse read the objects back with `INSERT … SELECT FROM s3()`.'
    : 'A restore streams each Parquet file back into ClickHouse as `INSERT … FORMAT Parquet`.';
  const role = archiving && restoring ? `${write} ${read}` : archiving ? write : `${read} The bundles come from an earlier solparq run.`;
  const config = [];
  if (archiving) {
    config.push(cfg('--archive-location-type', s3 ? 's3' : 'local', 'superbank-solparq; default local'));
    if (s3) {
      config.push(
        cfg('--archive-s3-endpoint', 'required', 'with --archive-s3-bucket-name, --archive-s3-auth-key and --archive-s3-auth-secret-key'),
        cfg('--archive-s3-region', 'us-east-1'),
        cfg('--archive-s3-write-checksums', 'false', 'SHA256SUMS.txt for S3 needs every object read back, so it is opt-in'),
      );
    } else {
      config.push(cfg('--archive-file-output-location', './'));
    }
    config.push(cfg('--archives-to-keep', '5', 'newest bundles kept per archive kind; 0 disables pruning'));
  }
  if (restoring) {
    config.push(cfg('solparq-archive-location', s3 ? 's3' : 'local', 'ingestor, required'));
    config.push(s3 ? cfg('solparq-archive-s3-bucket-name', 'required') : cfg('solparq-archive-path', 'required'));
  }
  return {
    title: 'Parquet bundles',
    subtitle: s3 ? 'S3-compatible bucket · solparq archive' : 'local disk · solparq archive',
    body: [
      `${s3 ? 'An S3-compatible bucket' : 'A local directory'} of solparq bundles named \`{kind}_{epoch}_{start}-{end}\`, each with one Parquet file per table, \`manifest.json\`, \`report.json\` and \`SHA256SUMS.txt\`${s3 ? ' (opt-in for S3)' : ''}, plus a \`.done\` marker once it is complete.`,
      role,
      'The readers are `superbank --source solparq` (restore) and the offline `superbank-solparq-read` inspector, which never connects to ClickHouse, while `superbank-rpc` has no Parquet read path.',
    ],
    config,
    refs: [REFS.solparqReadme, REFS.solparqStorage, REFS.ingestSolparq],
    ...(restoring ? {} : { action: { label: 'Restore from Parquet', patch: { source: 'solparq' } } }),
  };
}

function solparq(state) {
  const s3 = state.archive === 's3';
  return {
    title: 'superbank-solparq',
    subtitle: 'crates/superbank-solparq · archiver',
    body: [
      'Archives ClickHouse tables into Parquet bundles: hourly (9,000 slots), epoch (432,000, aligned) or custom (`--custom-slot-range`, default 1,000), with `transactions` and `blocks_metadata` required and the other tables included when present.',
      'Before archiving it checks the range against Solana RPC `getBlocks` and each slot’s `executed_transaction_count` in `blocks_metadata`, and a range with missing blocks or mismatches is skipped unless you pass `--force-archive`.',
      s3
        ? 'In S3 mode solparq sends the `INSERT INTO FUNCTION s3(…)` query and ClickHouse uploads each table\'s Parquet itself, so the bytes never pass through solparq; solparq writes only the bundle\'s small files (`manifest.json`, `report.json`, `.done`, and `SHA256SUMS.txt` with `--archive-s3-write-checksums`).'
        : 'Local mode has ClickHouse stream `SELECT … FORMAT Parquet` over HTTP to solparq, which writes the files to disk.',
      'It runs once or as a loop with `--server-mode` (ops page :30303, metrics :31313, a check every 60 seconds), and ships as a release binary with no compose or Kubernetes manifest in this repository.',
    ],
    config: [
      cfg('--db-server', 'required', 'SOLPARQ_DB_SERVER; port --db-server-port defaults to 8123'),
      cfg('--archive-range-type', 'required', 'hourly | epoch | custom; repeatable'),
      cfg('--archive-location-type', s3 ? 's3' : 'local', 'default local'),
      cfg('--server-mode', 'false'),
      cfg('--archives-to-keep', '5', 'newest bundles kept per archive kind; 0 disables pruning'),
      cfg('--solana-rpc-url', 'https://api.mainnet-beta.solana.com', 'used for getBlocks validation'),
      cfg('--backfill-gaps', 'false', 'spawns superbank --source rpc to fill missing slots'),
      cfg('--delete-archived-data-range', 'false', 'deletes archived ranges from ClickHouse'),
    ],
    refs: [REFS.solparqReadme, REFS.solparqConfig, REFS.solparqStorage],
  };
}

const VERIFY_BY_SOURCE = {
  verifiable: 'This source writes `entries`, so its slots can be verified end to end.',
  unverifiable: 'This source writes no `entries`, so its slots are reported `unverifiable` (exit code 3), not failed.',
  depends: 'Restored slots verify only if the bundles archived `entries`; otherwise they are `unverifiable`.',
};

function verify(state) {
  const kind = state.source === 'solparq' ? 'depends' : ENTRY_SOURCES.includes(state.source) ? 'verifiable' : 'unverifiable';
  return {
    title: 'superbank-verify',
    subtitle: 'crates/superbank-verify · Proof-of-History validator',
    body: [
      'Re-checks Proof of History from ClickHouse (entry and tick counts, tick-hash windows, transaction index tiling, last entry hash against the blockhash, and the blockhash chain across slots) and never reads Parquet.',
      VERIFY_BY_SOURCE[kind],
      'Structural mode is the default, `--mode full` recomputes every SHA-256 hash and also reads `transactions`, and `--full` only means every slot in `blocks_metadata`. Exit codes are 0 ok, 1 operational error, 2 verification failure and 3 unverifiable or missing.',
    ],
    config: [
      cfg('mode', 'structural', 'structural | full'),
      cfg('range', 'unset', 'slots 250000000:250001000, epochs 700-701, or one epoch; exclusive with full'),
      cfg('full', 'false', 'verify everything in blocks_metadata'),
      cfg('window-slots', '64', 'maximum 128'),
      cfg('allow-unverifiable', 'false', 'exit 0 even when unverifiable slots remain'),
      cfg('checkpoint-file', 'unset', 'with resume, continue an interrupted run'),
      cfg('metrics-port', '9902', '/metrics and /health'),
      cfg('health-stale-secs', '300'),
    ],
    refs: [REFS.verifyReadme, REFS.verifyExample, REFS.verifyCli, REFS.verifySmoke],
  };
}

// --- Entry point ------------------------------------------------------------

// Nodes whose copy does not depend on the selector state.
const STATIC = {
  keeper,
  'grpc-clients': grpcClients,
  'solparq-rpc': solparqRpc,
};

const BY_STATE = {
  solana,
  'src-dragonsmouth': srcDragonsmouth,
  'src-fumarole': srcFumarole,
  'src-jsonrpc': srcJsonrpc,
  'src-bigtable': srcBigtable,
  'src-oldfaithful': srcOldFaithful,
  't-transactions': tableTransactions,
  't-blocks_metadata': tableBlocksMetadata,
  't-entries': tableEntries,
  't-gsfa': tableGsfa,
  't-signatures': tableSignatures,
  't-gsfa_hot': tableGsfaHot,
  't-token_owner_activity': tableTokenOwnerActivity,
  ch: clickhouse,
  rpc,
  'head-cache': headCache,
  'disk-cache': diskCache,
  'jsonrpc-clients': jsonrpcClients,
  'parquet-store': parquetStore,
  solparq,
  verify,
};

export function contentFor(id, input) {
  if (typeof id !== 'string') return null;
  if (Object.hasOwn(STATIC, id)) return STATIC[id]();
  const isIngest = id === 'ingest';
  if (!isIngest && !Object.hasOwn(BY_STATE, id)) return null;
  const state = normalizeState(input);
  return isIngest ? INGEST[state.source](state) : BY_STATE[id](state);
}
