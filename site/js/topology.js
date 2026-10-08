// Pure model of Superbank's runtime topology for one selector state.
// No DOM or Three.js imports: scene.js renders it and tests/site/ checks it
// under node:test. Each rule cites the code that justifies it so the model can
// be re-checked when that code moves.

import { normalizeState } from './state.js';

// Logical layout. `u` runs left -> right along the write path; `v` runs from
// the back of the scene (negative) to the front (positive). scene.js maps
// (u, v) onto the isometric ground plane.
const POS = {
  solana: [-15, 0],
  ingest: [-6.5, 0],
  't-transactions': [-1.5, -2],
  't-blocks_metadata': [-1.5, 0],
  't-entries': [-1.5, 2],
  't-gsfa': [2, -3],
  't-signatures': [2, -1],
  't-gsfa_hot': [2, 1],
  't-token_owner_activity': [2, 3],
  ch: [5.5, 0],
  keeper: [5.5, -3.2],
  'disk-cache': [9.5, 3],
  'head-cache': [11.5, -3],
  rpc: [12, 0],
  'jsonrpc-clients': [16, -1.2],
  'grpc-clients': [16, 2],
  'parquet-store': [-1.5, 6.5],
  solparq: [4, 6.5],
  'solparq-rpc': [7.8, 6.5],
  verify: [2, -6.1],
};

const ZONES = {
  // Upstream and Archive are refitted to the nodes drawn in them (zoneRect).
  upstream: { label: 'Upstream', rect: [-17, -2, -9.4, 2] },
  ingest: { label: 'Ingest', rect: [-8.4, -1.8, -4.6, 1.8] },
  clickhouse: { label: 'ClickHouse', rect: [-3.2, -4.4, 7.2, 4.4] },
  serve: { label: 'Serve', rect: [8.2, -4.4, 17.6, 4] },
  archive: { label: 'Archive', rect: [-3.2, 5.2, 6, 7.8] },
  verify: { label: 'Verify', rect: [0.4, -7.2, 3.6, -5] },
};

export const SOURCE_ENDPOINT = Object.freeze({
  grpc: 'src-dragonsmouth',
  fumarole: 'src-fumarole',
  rpc: 'src-jsonrpc',
  bigtable: 'src-bigtable',
  jetstreamer: 'src-oldfaithful',
  solparq: 'parquet-store',
});

// Sources whose rows include PoH entries. rpc/bigtable write entry_count 0
// (crates/superbank/src/ingest/rpc.rs, bigtable.rs); grpc/fumarole request the
// entry filter; the Jetstreamer plugin writes entries via on_entry.
// solparq restores entries only if the bundle archived them.
export const ENTRY_SOURCES = Object.freeze(['grpc', 'fumarole', 'jetstreamer']);

export const BASE_TABLES = Object.freeze(['transactions', 'blocks_metadata', 'entries']);
export const DERIVED_TABLES = Object.freeze(['gsfa', 'signatures', 'gsfa_hot', 'token_owner_activity']);
export const OPTIONAL_TABLES = Object.freeze(['gsfa_hot', 'token_owner_activity']);

// Shard keys from ddl/cluster/*.sql, abbreviated for scene sublabels; the
// exact expressions are in the summary and the info panel.
const SHARD_KEY = {
  transactions: 'epoch',
  blocks_metadata: 'epoch',
  entries: 'epoch',
  gsfa: 'address',
  signatures: 'signature',
  gsfa_hot: 'signature',
  token_owner_activity: 'owner',
};

// Short on purpose: these render as scene label sublabels.
const INGEST_LIFECYCLE = {
  grpc: 'live · no reconnect',
  fumarole: 'live · durable cursor',
  rpc: 'one-shot backfill',
  bigtable: 'one-shot backfill',
  solparq: 'one-shot restore',
  jetstreamer: 'per-epoch job',
};

// Upstream column, top to bottom. Only endpoints in use are drawn, stacked
// around v = 0 so the active source sits level with the ingest node.
const ENDPOINT_U = -11;
const ENDPOINT_SPACING = 2.2;
const ENDPOINTS = [
  ['src-dragonsmouth', 'grpc', 'Yellowstone gRPC', 'DragonsMouth'],
  ['src-fumarole', 'fumarole', 'Yellowstone Fumarole', 'consumer group'],
  ['src-jsonrpc', 'jsonrpc', 'Solana JSON-RPC', 'getBlocks / getBlock'],
  ['src-bigtable', 'bigtable', 'Solana Bigtable', 'LedgerStorage'],
  ['src-oldfaithful', 'oldfaithful', 'Old Faithful', 'epoch CAR archives'],
];

function makeNode(id, kind, label, { pos = POS[id], ...extra } = {}) {
  return {
    id,
    kind,
    label,
    sublabel: '',
    variant: null,
    // Copied so a renderer mutating node.pos can't corrupt later builds.
    pos: [...pos],
    zone: null,
    status: null,
    buffer: null,
    shards: 1,
    replicas: 1,
    optional: false,
    ...extra,
  };
}

function makeEdge(from, to, style, extra = {}) {
  return {
    id: `${from}->${to}`,
    from,
    to,
    style,
    channel: style === 'control' ? null : 'data',
    particle: null,
    emit: null,
    speed: 4,
    label: '',
    conditional: false,
    ...extra,
  };
}

export function buildTopology(input) {
  const state = normalizeState(input);
  const nodes = [];
  const edges = [];
  const addNode = (...args) => {
    const node = makeNode(...args);
    nodes.push(node);
    return node;
  };
  const addEdge = (...args) => {
    const edge = makeEdge(...args);
    edges.push(edge);
    return edge;
  };

  const archiveOn = state.archive !== 'off';
  const restoring = state.source === 'solparq';
  const jetstreamer = state.source === 'jetstreamer';
  const writesEntries = ENTRY_SOURCES.includes(state.source);
  const sharded = state.ch !== 'single';
  const shards = sharded ? 3 : 1;
  const replicas = state.ch === 'replicated' ? 2 : 1;
  // solparq source reuses the archive location when archiving is on; with
  // archiving off we show a local bundle directory (solparq-archive-location
  // defaults to local in crates/superbank/src/cli.rs).
  const storeLocation = state.archive === 's3' ? 's3' : 'local';

  // --- Upstream -----------------------------------------------------------
  // Only components involved in the selected configuration are drawn.
  const usedEndpoints = new Set();
  if (state.source !== 'solparq') usedEndpoints.add(SOURCE_ENDPOINT[state.source]);
  // Head cache opens its own DragonsMouth subscription inside superbank-rpc
  // (crates/superbank-rpc/src/head_cache/dragonsmouth.rs).
  if (state.head) usedEndpoints.add('src-dragonsmouth');

  if (usedEndpoints.size > 0) addNode('solana', 'network', 'Solana', { sublabel: 'validators', zone: 'upstream' });
  const endpoints = ENDPOINTS.filter(([id]) => usedEndpoints.has(id));
  endpoints.forEach(([id, variant, label, sublabel], i) => {
    const v = (i - (endpoints.length - 1) / 2) * ENDPOINT_SPACING;
    addNode(id, 'endpoint', label, { variant, sublabel, zone: 'upstream', pos: [ENDPOINT_U, v] });
  });
  // Solana feeds every endpoint in use. Live endpoints carry a block stream;
  // historical ones (JSON-RPC history, Bigtable, Old Faithful) are pulled in
  // bursts by the ingest job instead.
  const liveFeeds = new Set();
  if (state.source === 'grpc' || state.head) liveFeeds.add('src-dragonsmouth');
  if (state.source === 'fumarole') liveFeeds.add('src-fumarole');
  for (const id of usedEndpoints) {
    const live = liveFeeds.has(id);
    addEdge('solana', id, 'stream', {
      particle: live ? 'block' : null,
      emit: live ? { type: 'stream', rate: 2.5 } : null,
      label: live ? 'live blocks' : id === 'src-oldfaithful' ? 'ledger archived per epoch' : 'ledger history',
    });
  }

  // --- Ingest -------------------------------------------------------------
  const ingest = addNode('ingest', 'process', jetstreamer ? 'jetstreamer-clickhouse' : 'superbank', {
    variant: jetstreamer ? 'jetstreamer' : 'superbank',
    sublabel: INGEST_LIFECYCLE[state.source],
    zone: 'ingest',
  });
  if (!restoring) {
    // superbank flushes transactions -> blocks_metadata -> entries
    // (flush_buffers in crates/superbank/src/clickhouse.rs). The Jetstreamer
    // plugin runs independent inserters per table, so there is no ordering
    // (ingest/jetstreamer-clickhouse-plugin/src/lib.rs).
    ingest.buffer = { size: 6, maxWait: 2.5, ordered: !jetstreamer };
  }

  const endpoint = SOURCE_ENDPOINT[state.source];
  if (state.source === 'grpc' || state.source === 'fumarole') {
    addEdge(endpoint, 'ingest', 'stream', {
      particle: 'block',
      emit: { type: 'stream', rate: 2.5 },
      label: state.source === 'grpc' ? 'full blocks + entries' : 'tx, block-meta and entry updates',
    });
  } else if (state.source === 'rpc' || state.source === 'bigtable' || jetstreamer) {
    const labels = { rpc: 'getBlocks + getBlock', bigtable: 'parallel fetch + decode', jetstreamer: 'epoch replay' };
    addEdge(endpoint, 'ingest', 'batch', {
      particle: 'block',
      emit: { type: 'burst', count: 8, every: 2.2, offset: 0 },
      speed: 5,
      label: labels[state.source],
    });
  } else if (storeLocation === 'local') {
    // Local restore streams `INSERT ... FORMAT Parquet` through superbank
    // without decoding rows (crates/superbank/src/ingest/solparq.rs).
    addEdge('parquet-store', 'ingest', 'batch', {
      particle: 'parquet',
      emit: { type: 'burst', count: 6, every: 2.4, offset: 0 },
      label: 'Parquet bytes, no row decoding',
    });
  } else {
    // S3 restore: ClickHouse pulls the objects itself via s3().
    addEdge('ingest', 't-transactions', 'control', { label: 'INSERT … SELECT FROM s3()' });
  }

  // --- ClickHouse ---------------------------------------------------------
  for (const table of [...BASE_TABLES, ...DERIVED_TABLES]) {
    // Nothing writes entries for rpc/bigtable, so the table is left out.
    if (table === 'entries' && !(writesEntries || restoring)) continue;
    const derived = DERIVED_TABLES.includes(table);
    const optional = OPTIONAL_TABLES.includes(table);
    addNode(`t-${table}`, 'table', table, {
      variant: derived ? 'view' : 'base',
      sublabel: sharded ? `sharded by ${SHARD_KEY[table]}` : optional ? 'optional index' : derived ? 'MV index' : 'base table',
      zone: 'clickhouse',
      shards,
      replicas,
      optional,
    });
  }
  const chSublabel = {
    single: ':8123 · single node',
    cluster: 'Distributed · 3 shards',
    replicated: 'Distributed · 3 shards × 2 replicas',
  }[state.ch];
  addNode('ch', 'cluster', 'ClickHouse', { variant: state.ch, sublabel: chSublabel, zone: 'clickhouse', shards, replicas });
  if (state.ch === 'replicated') {
    addNode('keeper', 'coordinator', 'ClickHouse Keeper', { sublabel: 'replication log', zone: 'clickhouse' });
    addEdge('keeper', 'ch', 'control', { label: 'replica coordination' });
  }

  const tablesWritten = ['transactions', 'blocks_metadata'];
  if (writesEntries || restoring) tablesWritten.push('entries');
  const writeFrom = restoring && storeLocation === 's3' ? 'parquet-store' : 'ingest';
  tablesWritten.forEach((table, order) => {
    const conditional = restoring && table === 'entries';
    if (writeFrom === 'ingest') {
      addEdge('ingest', `t-${table}`, 'batch', {
        particle: 'rows',
        emit: restoring ? { type: 'relay' } : { type: 'flush', order: jetstreamer ? null : order },
        label: restoring ? 'INSERT … FORMAT Parquet' : jetstreamer ? 'independent async insert' : `flush step ${order + 1}`,
        conditional,
      });
    } else {
      addEdge('parquet-store', `t-${table}`, 'batch', {
        particle: 'parquet',
        emit: { type: 'burst', count: 3, every: 2.4, offset: order * 0.3 },
        label: 'ClickHouse reads s3()',
        conditional,
      });
    }
  });

  // Materialized views on transactions build the RPC indexes (ddl/*/gsfa.sql,
  // signatures.sql, gsfa_hot.sql, token_owner_activity.sql).
  for (const table of DERIVED_TABLES) {
    addEdge('t-transactions', `t-${table}`, 'stream', {
      particle: 'index',
      emit: { type: 'relay' },
      speed: 3,
      label: 'materialized view',
      conditional: OPTIONAL_TABLES.includes(table),
    });
  }

  // --- Serve --------------------------------------------------------------
  addNode('rpc', 'process', 'superbank-rpc', {
    variant: 'rpc',
    sublabel: state.stream ? ':8899 JSON-RPC · :10000 gRPC' : ':8899 JSON-RPC',
    zone: 'serve',
  });
  addNode('jsonrpc-clients', 'clients', 'JSON-RPC clients', { variant: 'jsonrpc', zone: 'serve' });
  addEdge('ch', 'rpc', 'read', { channel: 'serve', label: 'source of truth' });
  addEdge('rpc', 'jsonrpc-clients', 'read', { channel: 'serve', label: 'JSON-RPC responses' });

  const tiers = [];
  if (state.head) {
    addNode('head-cache', 'memory', 'Head cache', { sublabel: 'in-memory · newest 32 slots', zone: 'serve' });
    addEdge('src-dragonsmouth', 'head-cache', 'stream', {
      particle: 'block',
      emit: { type: 'stream', rate: 3 },
      speed: 7,
      label: 'own subscription · processed',
    });
    addEdge('head-cache', 'rpc', 'read', { channel: 'serve', label: 'tip reads' });
    tiers.push({ node: 'head-cache', edge: 'head-cache->rpc', serves: ['tip'] });
  }
  if (state.disk) {
    // Filled from source ClickHouse, never from the head cache
    // (crates/superbank-rpc/src/disk_cache/filler.rs).
    addNode('disk-cache', 'localdb', 'Disk cache', { sublabel: 'loopback ClickHouse', zone: 'serve' });
    addEdge('ch', 'disk-cache', 'batch', {
      particle: 'rows',
      emit: { type: 'burst', count: 4, every: 3.2, offset: 0.5 },
      speed: 2.5,
      label: 'Native-format forward · ≥75 slots behind',
    });
    addEdge('disk-cache', 'rpc', 'read', { channel: 'serve', label: 'recent finalized reads' });
    tiers.push({ node: 'disk-cache', edge: 'disk-cache->rpc', serves: ['recent'] });
  }
  tiers.push({ node: 'ch', edge: 'ch->rpc', serves: ['tip', 'recent', 'historical'] });

  if (state.stream) {
    addNode('grpc-clients', 'clients', 'gRPC clients', { variant: 'grpc', sublabel: 'StreamBlocks / StreamTransactions', zone: 'serve' });
    addEdge('rpc', 'grpc-clients', 'stream', {
      channel: 'serve',
      particle: 'rows',
      emit: { type: 'stream', rate: 1.2 },
      label: 'historical slot-range streams',
    });
  }

  // --- Archive ------------------------------------------------------------
  if (archiveOn || restoring) {
    addNode('parquet-store', 'store', 'Parquet bundles', {
      variant: storeLocation,
      sublabel: storeLocation === 's3' ? 'S3-compatible bucket' : 'local disk',
      zone: 'archive',
    });
  }
  if (archiveOn) {
    addNode('solparq', 'process', 'superbank-solparq', { variant: 'solparq', sublabel: 'archiver · :30303 ops', zone: 'archive' });
    // solparq validates each range against Solana RPC getBlocks through its own
    // --solana-rpc-url (crates/superbank-solparq/src/config.rs, clickhouse.rs).
    // It is unrelated to any ingest endpoint, so it is drawn in the archive lane.
    addNode('solparq-rpc', 'endpoint', 'Solana RPC', { variant: 'jsonrpc', sublabel: 'getBlocks validation', zone: 'archive' });
    addEdge('solparq', 'solparq-rpc', 'control', { label: 'getBlocks validation' });
    if (state.archive === 'local') {
      addEdge('ch', 'solparq', 'batch', {
        particle: 'parquet',
        emit: { type: 'burst', count: 5, every: 5, offset: 1 },
        speed: 3,
        label: 'SELECT … FORMAT Parquet',
      });
      addEdge('solparq', 'parquet-store', 'batch', { particle: 'parquet', emit: { type: 'relay' }, speed: 3, label: 'bundle directory' });
    } else {
      // ClickHouse uploads the objects; solparq never touches the bytes
      // (INSERT INTO FUNCTION s3 in crates/superbank-solparq/src/clickhouse.rs).
      addEdge('solparq', 'ch', 'control', { label: 'orchestrates export' });
      addEdge('ch', 'parquet-store', 'batch', {
        particle: 'parquet',
        emit: { type: 'burst', count: 5, every: 5, offset: 1 },
        speed: 3,
        label: 'INSERT INTO FUNCTION s3(…)',
      });
    }
  }

  // --- Verify -------------------------------------------------------------
  if (state.verify) {
    // superbank-verify reads ClickHouse only; slots without entries are
    // reported unverifiable (exit 3), not failed (crates/superbank-verify).
    const status = writesEntries ? 'ok' : restoring ? 'info' : 'warn';
    const sublabel = {
      ok: 'PoH verifiable',
      warn: 'unverifiable · no entries',
      info: 'verifiable if bundle has entries',
    }[status];
    addNode('verify', 'process', 'superbank-verify', { variant: 'verify', sublabel, status, zone: 'verify' });
    addEdge('ch', 'verify', 'batch', {
      particle: 'rows',
      emit: { type: 'burst', count: 3, every: 3.5, offset: 1.5 },
      speed: 3,
      label: 'blocks_metadata + entries',
    });
  }

  const zoneIds = new Set(nodes.map((n) => n.zone).filter(Boolean));
  const zones = Object.entries(ZONES)
    .filter(([id]) => zoneIds.has(id))
    .map(([id, zone]) => ({ id, ...zone, rect: zoneRect(id, nodes) }));

  return {
    state,
    nodes,
    edges,
    zones,
    animate: { data: state.flow !== 'requests', serve: state.flow !== 'blocks' },
    readPath: {
      client: 'jsonrpc-clients',
      entryEdge: 'rpc->jsonrpc-clients',
      tiers,
      classes: [
        { id: 'tip', label: 'tip (newest slots)', weight: 0.35 },
        { id: 'recent', label: 'recent finalized', weight: 0.35 },
        { id: 'historical', label: 'historical', weight: 0.3 },
      ],
    },
    summary: describe(state),
  };
}

// Slabs whose membership varies hug the nodes drawn on them: Upstream along
// its endpoint column (v), Archive along store -> archiver (u). Margins keep
// each node's footprint on the slab.
function zoneRect(id, nodes) {
  const [u0, v0, u1, v1] = ZONES[id].rect;
  const pos = nodes.filter((n) => n.zone === id).map((n) => n.pos);
  if (id === 'upstream') {
    const vs = pos.map((p) => p[1]);
    return [u0, Math.min(...vs) - 2, u1, Math.max(...vs) + 2];
  }
  if (id === 'archive') {
    const us = pos.map((p) => p[0]);
    return [Math.min(...us) - 1.7, v0, Math.max(...us) + 2, v1];
  }
  return [u0, v0, u1, v1];
}

// Ordered, plain-text description of the data path for the given state. Used
// for the accessible summary list and the no-WebGL fallback. Inline code is
// marked with backticks; callers must render it with textContent, never HTML.
function describe(state) {
  const sections = [];
  const write = [];
  const sourceSteps = {
    grpc: [
      'Solana → Yellowstone gRPC (DragonsMouth) → `superbank --source grpc`: full blocks with transactions and entries, at `finalized` commitment by default.',
      'Live daemon without a reconnect loop: a stream error, unhealthy status or idle timeout triggers one last flush and a non-zero exit, so run it under a supervisor. With `dragonsmouth-from-slot: "*"` it resumes from `max(slot)` in `blocks_metadata`.',
    ],
    fumarole: [
      'Solana → Yellowstone Fumarole → `superbank --source fumarole`: joins a consumer group and reassembles each block from transaction, block-meta and entry updates.',
      'The cursor lives server-side in the consumer group. Offsets are committed only after a successful flush, so a restart resumes where the group left off.',
    ],
    rpc: [
      'Solana JSON-RPC → `superbank --source rpc`: a bounded one-shot backfill. `getBlocks` discovers slots and up to `rpc-max-inflight` workers fetch `getBlock`.',
      'No cursor: re-runs are safe because base tables are `ReplacingMergeTree`. This source writes no `entries`.',
    ],
    bigtable: [
      'Solana Bigtable → `superbank --source bigtable`: a bounded one-shot backfill over a slot or epoch range with parallel fetch and decode.',
      'This source writes no `entries`.',
    ],
    solparq: [
      'Parquet bundles → ClickHouse via `superbank --source solparq`: a one-shot restore that skips the row pipeline. ClickHouse does the loading (`INSERT … SELECT FROM s3()` for S3, `INSERT … FORMAT Parquet` streamed from local disk).',
      'Every archived table is restored, including derived index tables. `entries` comes back only if the bundle archived it.',
    ],
    jetstreamer: [
      'Old Faithful → `jetstreamer-clickhouse`: a separate Jetstreamer plugin binary (not a `superbank` source) that replays historical epochs into the same three base tables.',
      'It uses async inserts, retries failed inserts forever, and commits each table independently, so `blocks_metadata` can land before its transactions.',
    ],
  };
  write.push(...sourceSteps[state.source]);
  if (state.source !== 'solparq' && state.source !== 'jetstreamer') {
    const order = ENTRY_SOURCES.includes(state.source)
      ? '`transactions` → `blocks_metadata` → `entries`'
      : '`transactions` → `blocks_metadata`';
    write.push(`\`superbank\` buffers rows and flushes each batch in order: ${order}. A slot in \`blocks_metadata\` therefore implies its transactions are stored.`);
  }
  write.push('Inside ClickHouse, materialized views on `transactions` build the RPC indexes: `gsfa` (address → signatures) and `signatures` (signature → slot), plus the optional `gsfa_hot` and `token_owner_activity`.');
  write.push(
    {
      single: 'Single-node ClickHouse (`ddl/local`).',
      cluster: 'Clustered ClickHouse (`ddl/cluster`): base tables are sharded by epoch (`intDiv(slot, 432000)`), `gsfa` by `cityHash64(address)` and `signatures` by `cityHash64(signature)`, all behind `Distributed` tables.',
      replicated: 'Replicated ClickHouse (`ddl/replicated`): the cluster layout, with each shard a `ReplicatedReplacingMergeTree` replica set coordinated by ClickHouse Keeper.',
    }[state.ch],
  );
  sections.push({ id: 'write', title: 'Write path', steps: write });

  const read = ['JSON-RPC clients → `superbank-rpc` on `:8899`. Each request tries the enabled tiers in order and falls through on a miss.'];
  if (state.head) {
    read.push('Head cache: an in-memory window of the newest 32 slots, fed by its own DragonsMouth subscription (separate from any ingest connection). It enables `processed` commitment.');
  } else {
    read.push('No head cache: `processed` commitment is rejected and the newest data is whatever the ingestor has flushed.');
  }
  if (state.disk) {
    read.push('Disk cache: a loopback ClickHouse with recent finalized slots, copied from the source cluster in Native format at least 75 slots behind the finalized tip. It is a near cache, never a source of truth.');
  }
  read.push('Source ClickHouse is always the last tier. There is no upstream Solana RPC fallback and no Parquet read path.');
  if (state.stream) {
    read.push('gRPC clients → `superbank-rpc` on `:10000`: `StreamBlocks` and `StreamTransactions` over bounded historical slot ranges, read from ClickHouse.');
  }
  sections.push({ id: 'read', title: 'Read path', steps: read });

  if (state.archive !== 'off' || state.source === 'solparq') {
    const archive = [];
    if (state.archive === 'local') {
      archive.push('`superbank-solparq` streams `SELECT … FORMAT Parquet` from ClickHouse and writes bundle directories to local disk, named `{kind}_{epoch}_{start}-{end}` (hourly = 9,000 slots, epoch = 432,000).');
    } else if (state.archive === 's3') {
      archive.push('`superbank-solparq` has ClickHouse write Parquet straight to S3 (`INSERT INTO FUNCTION s3(…)`); the bytes never pass through solparq. Bundles are named `{kind}_{epoch}_{start}-{end}`.');
    }
    if (state.archive !== 'off') {
      archive.push('Before archiving a range, solparq checks it against Solana RPC `getBlocks` and against the transaction counts in `blocks_metadata`.');
    }
    if (state.source === 'solparq') {
      archive.push(state.archive === 'off' ? 'Archiving is off here; the restore reads bundles produced by an earlier solparq run.' : 'Archive → restore loop: bundles written by solparq are what `superbank --source solparq` restores.');
    }
    sections.push({ id: 'archive', title: 'Archive & restore', steps: archive });
  }

  if (state.verify) {
    const verify = ['`superbank-verify` re-checks Proof of History from ClickHouse (`blocks_metadata`, `entries`; `transactions` too with `--mode full`, which recomputes every PoH hash). It never reads Parquet.'];
    if (ENTRY_SOURCES.includes(state.source)) verify.push('This source writes `entries`, so its slots are verifiable.');
    else if (state.source === 'solparq') verify.push('Restored slots are verifiable only if the bundles archived `entries`.');
    else verify.push('This source writes no `entries`, so verify reports its slots as `unverifiable` (exit code 3), not failed.');
    sections.push({ id: 'verify', title: 'Verify', steps: verify });
  }
  return sections;
}
