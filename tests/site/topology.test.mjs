// Exhaustive checks of buildTopology() over all 2592 selector states.
// Part 1 asserts structural invariants the scene renderer relies on.
// Part 2 pins architecture facts taken from the Rust code; if one of these
// fails the model has drifted from the code, so fix topology.js, not the test.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { isDeepStrictEqual } from 'node:util';
import { DEFAULT_STATE, allStates } from '../../site/js/state.js';
import { READ_ORDERS, buildTopology, walkthroughSteps } from '../../site/js/topology.js';

function index(topo) {
  const nodes = new Map(topo.nodes.map((n) => [n.id, n]));
  const edges = new Map(topo.edges.map((e) => [e.id, e]));
  return {
    topo,
    state: topo.state,
    node: (id) => nodes.get(id),
    edge: (id) => edges.get(id),
    into: (id) => topo.edges.filter((e) => e.to === id),
    out: (id) => topo.edges.filter((e) => e.from === id),
    touching: (id) => topo.edges.filter((e) => e.from === id || e.to === id),
  };
}

const MODELS = [...allStates()].map((state) => index(buildTopology(state)));

// Failures name the exact state, so a broken rule can be reproduced from the report.
function fail(m, msg) {
  assert.fail(`${msg}\n  state: ${JSON.stringify(m.topo.state)}`);
}
function ok(m, cond, msg) {
  if (!cond) fail(m, msg);
}
function eq(m, actual, expected, msg) {
  if (!isDeepStrictEqual(actual, expected)) {
    fail(m, `${msg}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`);
  }
}
function forEachModel(fn) {
  for (const m of MODELS) fn(m);
}

const isNum = (x) => typeof x === 'number' && Number.isFinite(x);
const isText = (x) => typeof x === 'string' && x.trim().length > 0;

// ---------------------------------------------------------------------------
// Part 1: structural invariants
// ---------------------------------------------------------------------------

test('enumerates every selector state', () => {
  assert.equal(MODELS.length, 2592);
});

test('node ids and edge ids are unique; edge id is from->to', () => {
  forEachModel((m) => {
    const nodeIds = m.topo.nodes.map((n) => n.id);
    const edgeIds = m.topo.edges.map((e) => e.id);
    eq(m, new Set(nodeIds).size, nodeIds.length, 'duplicate node id');
    eq(m, new Set(edgeIds).size, edgeIds.length, 'duplicate edge id');
    for (const e of m.topo.edges) eq(m, e.id, `${e.from}->${e.to}`, 'edge id must be from->to');
  });
});

test('every edge connects two existing, distinct nodes', () => {
  forEachModel((m) => {
    for (const e of m.topo.edges) {
      ok(m, m.node(e.from), `edge ${e.id}: unknown from`);
      ok(m, m.node(e.to), `edge ${e.id}: unknown to`);
      ok(m, e.from !== e.to, `edge ${e.id}: self loop`);
    }
  });
});

test('every drawn component is involved: each node has at least one edge', () => {
  // Components that take no part in the selected configuration are removed.
  forEachModel((m) => {
    for (const n of m.topo.nodes) ok(m, m.touching(n.id).length > 0, `node ${n.id} is drawn but not connected`);
  });
});

test('zones are well formed and every node sits inside its zone', () => {
  forEachModel((m) => {
    const zones = new Map();
    for (const z of m.topo.zones) {
      ok(m, !zones.has(z.id), `duplicate zone ${z.id}`);
      ok(m, isText(z.label), `zone ${z.id}: label`);
      ok(m, Array.isArray(z.rect) && z.rect.length === 4 && z.rect.every(isNum), `zone ${z.id}: rect`);
      ok(m, z.rect[0] < z.rect[2] && z.rect[1] < z.rect[3], `zone ${z.id}: rect must be [u0,v0,u1,v1] with u0<u1, v0<v1`);
      zones.set(z.id, z);
    }
    const used = new Set();
    for (const n of m.topo.nodes) {
      ok(m, Array.isArray(n.pos) && n.pos.length === 2 && n.pos.every(isNum), `node ${n.id}: pos must be two finite numbers`);
      ok(m, zones.has(n.zone), `node ${n.id}: zone ${n.zone} missing from zones`);
      const [u0, v0, u1, v1] = zones.get(n.zone).rect;
      // A node poking out of its platform means the layout table and zone table disagree.
      ok(m, n.pos[0] >= u0 && n.pos[0] <= u1 && n.pos[1] >= v0 && n.pos[1] <= v1, `node ${n.id}: pos ${n.pos} outside zone ${n.zone}`);
      used.add(n.zone);
    }
    for (const id of zones.keys()) ok(m, used.has(id), `zone ${id} has no nodes (empty platform)`);
  });
});

const VARIANTS = {
  network: [null],
  endpoint: ['grpc', 'fumarole', 'jsonrpc', 'bigtable', 'oldfaithful'],
  process: ['superbank', 'jetstreamer', 'rpc', 'solparq', 'verify'],
  table: ['base', 'view'],
  cluster: ['single', 'cluster', 'replicated'],
  coordinator: [null],
  memory: [null],
  localdb: [null],
  clients: ['jsonrpc', 'grpc'],
  store: ['local', 's3'],
};

test('node fields follow the contract', () => {
  forEachModel((m) => {
    for (const n of m.topo.nodes) {
      const at = `node ${n.id}`;
      ok(m, Object.hasOwn(VARIANTS, n.kind), `${at}: kind ${n.kind}`);
      ok(m, VARIANTS[n.kind].includes(n.variant), `${at}: variant ${n.variant} invalid for kind ${n.kind}`);
      ok(m, isText(n.label), `${at}: label`);
      ok(m, typeof n.sublabel === 'string', `${at}: sublabel must be a string`);
      // Unused components are removed, never drawn dimmed.
      ok(m, !Object.hasOwn(n, 'dimmed'), `${at}: no dimmed flag`);
      ok(m, typeof n.optional === 'boolean', `${at}: optional`);
      ok(m, [null, 'ok', 'warn', 'info'].includes(n.status), `${at}: status`);
      ok(m, n.shards === 1 || n.shards === 3, `${at}: shards`);
      ok(m, n.replicas === 1 || n.replicas === 2, `${at}: replicas`);
      if (n.buffer !== null) {
        ok(m, Number.isInteger(n.buffer.size) && n.buffer.size > 0, `${at}: buffer.size`);
        ok(m, isNum(n.buffer.maxWait) && n.buffer.maxWait > 0, `${at}: buffer.maxWait`);
        ok(m, typeof n.buffer.ordered === 'boolean', `${at}: buffer.ordered`);
      }
    }
  });
});

test('only verify carries a status and only ingest carries a buffer', () => {
  forEachModel((m) => {
    for (const n of m.topo.nodes) {
      if (n.id !== 'verify') eq(m, n.status, null, `node ${n.id}: status is verify-only`);
      if (n.id !== 'ingest') eq(m, n.buffer, null, `node ${n.id}: buffer is ingest-only`);
    }
  });
});

const EMIT_TYPES = ['stream', 'burst', 'flush', 'relay'];

test('edge fields follow the contract', () => {
  forEachModel((m) => {
    for (const e of m.topo.edges) {
      const at = `edge ${e.id}`;
      ok(m, ['stream', 'batch', 'control', 'read'].includes(e.style), `${at}: style ${e.style}`);
      ok(m, ['data', 'serve', null].includes(e.channel), `${at}: channel ${e.channel}`);
      ok(m, [null, 'block', 'rows', 'index', 'parquet', 'query', 'meta'].includes(e.particle), `${at}: particle ${e.particle}`);
      ok(m, isNum(e.speed) && e.speed > 0, `${at}: speed`);
      ok(m, typeof e.label === 'string', `${at}: label must be a string`);
      ok(m, typeof e.conditional === 'boolean', `${at}: conditional`);
      ok(m, e.emit === null || EMIT_TYPES.includes(e.emit.type), `${at}: emit.type`);
      // An emission with no particle kind (or vice versa) would be invisible or meaningless.
      eq(m, e.emit === null, e.particle === null, `${at}: emit and particle must be set together`);
      if (e.emit?.type === 'stream') ok(m, isNum(e.emit.rate) && e.emit.rate > 0, `${at}: stream rate`);
      if (e.emit?.type === 'burst') {
        ok(m, Number.isInteger(e.emit.count) && e.emit.count > 0, `${at}: burst count`);
        ok(m, isNum(e.emit.every) && e.emit.every > 0, `${at}: burst every`);
        ok(m, isNum(e.emit.offset) && e.emit.offset >= 0, `${at}: burst offset`);
      }
      if (e.emit?.type === 'flush') {
        ok(m, e.emit.order === null || (Number.isInteger(e.emit.order) && e.emit.order >= 0), `${at}: flush order`);
      }
      if (e.emit?.type === 'relay' && e.emit.count !== undefined) {
        ok(m, Number.isInteger(e.emit.count) && e.emit.count > 0, `${at}: relay count`);
      }
    }
  });
});

test('control edges carry no data: at most a query pulse on the data channel', () => {
  forEachModel((m) => {
    for (const e of m.topo.edges.filter((x) => x.style === 'control')) {
      if (e.particle === null) eq(m, [e.emit, e.channel], [null, null], `control edge ${e.id} must not animate`);
      else eq(m, [e.particle, e.channel], ['query', 'data'], `control edge ${e.id} may only carry query pulses`);
    }
    // Query pulses only ever travel on control edges.
    for (const e of m.topo.edges.filter((x) => x.particle === 'query')) eq(m, e.style, 'control', `query edge ${e.id}`);
  });
});

test('read edges never emit on their own (request journeys drive them)', () => {
  forEachModel((m) => {
    for (const e of m.topo.edges.filter((x) => x.style === 'read')) {
      eq(m, [e.emit, e.particle, e.channel], [null, null, 'serve'], `read edge ${e.id}`);
    }
  });
});

test('flush edges only leave a node with a buffer, and every buffer has flush edges', () => {
  forEachModel((m) => {
    for (const e of m.topo.edges.filter((x) => x.emit?.type === 'flush')) {
      ok(m, m.node(e.from).buffer !== null, `flush edge ${e.id} leaves a node without a buffer`);
    }
    for (const n of m.topo.nodes.filter((x) => x.buffer !== null)) {
      ok(m, m.out(n.id).some((e) => e.emit?.type === 'flush'), `buffered node ${n.id} has no flush edge`);
      ok(m, m.into(n.id).some((e) => e.emit !== null), `buffered node ${n.id} receives no particles`);
    }
  });
});

test('relay edges have a trigger, no cycles, and chains of at most 4 hops', () => {
  forEachModel((m) => {
    const relays = m.topo.edges.filter((e) => e.emit?.type === 'relay');
    const depth = (edge, seen) => {
      ok(m, !seen.has(edge.id), `relay cycle through ${edge.id}`);
      const next = new Set(seen).add(edge.id);
      const feeders = relays.filter((r) => r.to === edge.from);
      return 1 + Math.max(0, ...feeders.map((r) => depth(r, next)));
    };
    for (const e of relays) {
      ok(m, m.into(e.from).some((x) => x.emit !== null), `relay ${e.id}: nothing ever arrives at ${e.from}`);
      ok(m, depth(e, new Set()) <= 4, `relay chain ending at ${e.id} exceeds 4 hops`);
    }
  });
});

test('readPath references existing nodes and edges and ends at ClickHouse', () => {
  forEachModel((m) => {
    const rp = m.topo.readPath;
    ok(m, m.node(rp.client)?.kind === 'clients', 'readPath.client must be a clients node');
    const entry = m.edge(rp.entryEdge);
    ok(m, entry, `entryEdge ${rp.entryEdge} missing`);
    eq(m, [entry.from, entry.to, entry.channel], ['rpc', rp.client, 'serve'], 'entryEdge must run rpc -> client on the serve channel');

    ok(m, rp.classes.length > 0, 'no request classes');
    const classIds = rp.classes.map((c) => c.id);
    eq(m, new Set(classIds).size, classIds.length, 'duplicate class id');
    for (const c of rp.classes) ok(m, isText(c.label) && isNum(c.weight) && c.weight > 0, `class ${c.id}`);
    ok(m, Math.abs(rp.classes.reduce((sum, c) => sum + c.weight, 0) - 1) < 1e-9, 'class weights must sum to 1');

    ok(m, rp.tiers.length > 0, 'no tiers');
    eq(m, new Set(rp.tiers.map((t) => t.node)).size, rp.tiers.length, 'duplicate tier node');
    for (const t of rp.tiers) {
      ok(m, m.node(t.node), `tier node ${t.node} missing`);
      const edge = m.edge(t.edge);
      ok(m, edge, `tier edge ${t.edge} missing`);
      eq(m, [edge.from, edge.to, edge.channel], [t.node, 'rpc', 'serve'], `tier edge ${t.edge} must run tier -> rpc on the serve channel`);
      ok(m, t.serves.length > 0 && t.serves.every((id) => classIds.includes(id)), `tier ${t.node}: serves unknown class`);
    }
    // ClickHouse is the source of truth and the only tier guaranteed to answer.
    const last = rp.tiers.at(-1);
    eq(m, last.node, 'ch', 'last tier must be ch');
    eq(m, [...last.serves].sort(), [...classIds].sort(), 'ch must serve every class');
  });
});

test('summary is non-empty sections of non-empty, tag-free strings', () => {
  forEachModel((m) => {
    const { summary } = m.topo;
    ok(m, Array.isArray(summary) && summary.length > 0, 'summary empty');
    eq(m, new Set(summary.map((s) => s.id)).size, summary.length, 'duplicate summary section id');
    for (const section of summary) {
      ok(m, isText(section.id) && isText(section.title), `section ${section.id}: id/title`);
      ok(m, Array.isArray(section.steps) && section.steps.length > 0, `section ${section.id}: no steps`);
      for (const text of [section.title, ...section.steps]) {
        ok(m, isText(text), `section ${section.id}: empty string`);
        // Callers render with textContent, but keep markup out of the data anyway.
        ok(m, !text.includes('<'), `section ${section.id}: contains '<': ${text}`);
        ok(m, text.split('`').length % 2 === 1, `section ${section.id}: unbalanced backticks: ${text}`);
      }
    }
  });
});

test('summary sections track the selected state', () => {
  forEachModel((m) => {
    const { archive, source, verify } = m.state;
    const ids = m.topo.summary.map((s) => s.id);
    ok(m, ids.includes('write') && ids.includes('read'), 'write and read sections are always present');
    eq(m, ids.includes('archive'), archive !== 'off' || source === 'solparq', 'archive section iff archiving or restoring');
    eq(m, ids.includes('verify'), verify, 'verify section iff verify');
  });
});

test('buildTopology is deterministic and falls back to defaults for junk input', () => {
  for (const m of MODELS.filter((_, i) => i % 97 === 0)) {
    eq(m, buildTopology(m.state), m.topo, 'same state must build an identical topology');
  }
  for (const junk of [undefined, null, {}, 'source=rpc', { source: '<script>', head: 'yes' }]) {
    assert.deepEqual(buildTopology(junk).state, DEFAULT_STATE);
  }
});

const WALKS = new Map(MODELS.map((m) => [m, Object.fromEntries(READ_ORDERS.map((order) => [order, walkthroughSteps(m.topo, { readOrder: order })]))]));
const walkIds = (steps) => steps.map((s) => s.id);

test('walkthrough visits every drawn node exactly once, and nothing else', () => {
  forEachModel((m) => {
    const drawn = m.topo.nodes.map((n) => n.id).sort();
    for (const [order, steps] of Object.entries(WALKS.get(m))) {
      eq(m, walkIds(steps).sort(), drawn, `${order} walkthrough must cover the drawn nodes once each`);
    }
  });
});

test('walkthrough sections are contiguous and follow the summary order', () => {
  forEachModel((m) => {
    const summaryIds = m.topo.summary.map((s) => s.id);
    for (const [order, steps] of Object.entries(WALKS.get(m))) {
      const runs = steps.map((s) => s.section).filter((section, i, all) => section !== all[i - 1]);
      eq(m, new Set(runs).size, runs.length, `${order}: a section is split`);
      for (const section of runs) ok(m, summaryIds.includes(section), `${order}: section ${section} is not a summary section`);
      eq(m, runs, summaryIds.filter((id) => runs.includes(id)), `${order}: sections out of summary order`);
    }
  });
});

test('walkthrough starts at the source of the data', () => {
  forEachModel((m) => {
    const first = m.state.source === 'solparq' ? 'parquet-store' : 'solana';
    for (const [order, steps] of Object.entries(WALKS.get(m))) eq(m, steps[0].id, first, `${order}: first step`);
  });
});

test('walkthrough in data order follows the arrows', () => {
  forEachModel((m) => {
    const at = new Map(walkIds(WALKS.get(m).data).map((id, i) => [id, i]));
    for (const e of m.topo.edges) {
      if (e.style === 'control') continue;
      // Archive -> restore loop: bundles are written back into the restore source.
      if (m.state.source === 'solparq' && e.to === 'parquet-store') continue;
      ok(m, at.get(e.from) < at.get(e.to), `edge ${e.id} runs backwards in the data-order walkthrough`);
    }
  });
});

test('walkthrough in request order follows a request through the tiers', () => {
  forEachModel((m) => {
    const { data, request } = WALKS.get(m);
    const ids = walkIds(request);
    const journey = [m.topo.readPath.client, 'rpc', ...m.topo.readPath.tiers.map((t) => t.node).filter((id) => id !== 'ch')];
    const positions = journey.map((id) => ids.indexOf(id));
    eq(m, positions, [...positions].sort((a, b) => a - b), 'request journey out of order');
    // Only the read section differs between the two orders.
    const notRead = (steps) => steps.filter((s) => s.section !== 'read');
    eq(m, notRead(request), notRead(data), 'write, archive and verify steps must not depend on the read order');
  });
});

test('walkthroughSteps is deterministic and falls back to data order', () => {
  for (const m of MODELS.filter((_, i) => i % 97 === 0)) {
    const { data } = WALKS.get(m);
    eq(m, walkthroughSteps(m.topo), data, 'default read order must be data');
    for (const junk of ['', 'requests', '<script>', null, 42]) {
      eq(m, walkthroughSteps(m.topo, { readOrder: junk }), data, `readOrder ${String(junk)} must fall back to data`);
    }
  }
});

// ---------------------------------------------------------------------------
// Part 2: architecture rules (facts from the code; see comments in topology.js)
// ---------------------------------------------------------------------------

test('rule: entries edge exists only for sources that write PoH entries', () => {
  // grpc/fumarole request the entry filter, the Jetstreamer plugin writes on_entry;
  // rpc/bigtable write entry_count 0; solparq restores entries only if archived.
  forEachModel((m) => {
    const { source } = m.state;
    const direct = m.edge('ingest->t-entries');
    const carriesEntries = ['grpc', 'fumarole', 'jetstreamer'].includes(source);
    eq(m, Boolean(direct && !direct.conditional && direct.channel === 'data'), carriesEntries, 'unconditional data edge ingest->t-entries iff source carries entries');
    const into = m.into('t-entries');
    if (source === 'solparq') {
      eq(m, into.length, 1, 'solparq: exactly one entries edge');
      ok(m, into[0].conditional, 'solparq: entries edge must be conditional ("if archived")');
    } else if (carriesEntries) {
      eq(m, into.map((e) => e.id), ['ingest->t-entries'], 'entries source: only the ingest edge feeds t-entries');
    } else {
      ok(m, !m.node('t-entries'), 'rpc/bigtable: nothing writes entries, so the table is not drawn');
    }
  });
});

test('rule: every drawn base table is written, MVs hang off transactions, optional tables are flagged', () => {
  // gsfa/signatures/gsfa_hot/token_owner_activity are materialized views on transactions (ddl/*).
  const derived = ['t-gsfa', 't-signatures', 't-gsfa_hot', 't-token_owner_activity'];
  forEachModel((m) => {
    const base = ['t-transactions', 't-blocks_metadata'];
    if (m.node('t-entries')) base.push('t-entries');
    eq(m, m.topo.nodes.filter((n) => n.kind === 'table').map((n) => n.id).sort(), [...base, ...derived].sort(), 'table node set');
    for (const id of base) ok(m, m.into(id).length > 0, `${id}: a drawn base table must be written`);
    for (const id of base) eq(m, m.node(id).variant, 'base', `${id} is a base table`);
    for (const id of derived) {
      eq(m, m.node(id).variant, 'view', `${id} is a materialized view`);
      eq(m, m.into(id).map((e) => e.id), [`t-transactions->${id}`], `${id} is fed only by an MV on transactions`);
    }
    for (const n of m.topo.nodes.filter((x) => x.kind === 'table')) {
      eq(m, n.optional, ['t-gsfa_hot', 't-token_owner_activity'].includes(n.id), `${n.id}: optional flag`);
    }
  });
});

test('rule: Yellowstone DragonsMouth is in use iff source=grpc or the head cache is on', () => {
  // The head cache opens its own DragonsMouth subscription (head_cache/dragonsmouth.rs),
  // so grpc + head means two independent subscriptions.
  forEachModel((m) => {
    const { source, head } = m.state;
    const dm = m.node('src-dragonsmouth');
    eq(m, Boolean(dm), source === 'grpc' || head, 'src-dragonsmouth drawn iff in use');
    eq(m, Boolean(m.edge('src-dragonsmouth->ingest')), source === 'grpc', 'ingest subscription iff source=grpc');
    eq(m, Boolean(m.edge('src-dragonsmouth->head-cache')), head, 'head-cache subscription iff head');
    if (source === 'grpc' && head) {
      ok(m, m.edge('src-dragonsmouth->ingest') && m.edge('src-dragonsmouth->head-cache'), 'grpc + head: two separate subscriptions');
    }
  });
});

test('rule: each source lights exactly its own upstream endpoint', () => {
  const endpointOf = { grpc: 'src-dragonsmouth', fumarole: 'src-fumarole', rpc: 'src-jsonrpc', bigtable: 'src-bigtable', jetstreamer: 'src-oldfaithful' };
  forEachModel((m) => {
    const { source, head, archive } = m.state;
    if (source !== 'solparq') ok(m, m.edge(`${endpointOf[source]}->ingest`), `${source}: ${endpointOf[source]}->ingest`);
    else ok(m, !m.topo.edges.some((e) => e.from.startsWith('src-') && e.to === 'ingest'), 'solparq: no upstream endpoint feeds ingest');
    // Only endpoints in use are drawn.
    const lit = {
      'src-dragonsmouth': source === 'grpc' || head,
      'src-fumarole': source === 'fumarole',
      'src-jsonrpc': source === 'rpc',
      'src-bigtable': source === 'bigtable',
      'src-oldfaithful': source === 'jetstreamer',
    };
    for (const [id, expected] of Object.entries(lit)) {
      eq(m, Boolean(m.node(id)), expected, `${id}: drawn iff in use`);
      eq(m, Boolean(m.edge(`solana->${id}`)), expected, `${id}: solana feed edge iff in use`);
    }
    eq(m, Boolean(m.node('solana')), Object.values(lit).some(Boolean), 'solana drawn iff an endpoint is in use');
  });
});

test('rule: the head cache never touches ClickHouse', () => {
  // In-memory slots fed by its own DragonsMouth subscription; never written to or read from ClickHouse.
  forEachModel((m) => {
    eq(m, Boolean(m.node('head-cache')), m.state.head, 'head-cache exists iff head');
    for (const e of m.touching('head-cache')) {
      const other = e.from === 'head-cache' ? e.to : e.from;
      ok(m, other !== 'ch' && !other.startsWith('t-'), `head-cache must not connect to ClickHouse: ${e.id}`);
    }
    if (m.state.head) {
      eq(m, m.into('head-cache').map((e) => e.id), ['src-dragonsmouth->head-cache'], 'head-cache is fed only by its own subscription');
      eq(m, m.out('head-cache').map((e) => e.id), ['head-cache->rpc'], 'head-cache only serves rpc');
    }
  });
});

test('rule: the disk cache is filled only from source ClickHouse', () => {
  // disk_cache/filler.rs forwards from ClickHouse in Native format; it is never fed from the head cache.
  forEachModel((m) => {
    eq(m, Boolean(m.node('disk-cache')), m.state.disk, 'disk-cache exists iff disk');
    if (m.state.disk) {
      eq(m, m.into('disk-cache').map((e) => e.id), ['ch->disk-cache'], 'disk-cache incoming edges');
      eq(m, m.out('disk-cache').map((e) => e.id), ['disk-cache->rpc'], 'disk-cache only serves rpc');
    }
    ok(m, !m.edge('head-cache->disk-cache'), 'head-cache must not feed disk-cache');
  });
});

test('rule: no Parquet read path and no upstream fallback from RPC', () => {
  forEachModel((m) => {
    ok(m, !m.edge('parquet-store->rpc'), 'no Parquet -> RPC read path');
    for (const e of m.out('rpc')) {
      ok(m, !e.to.startsWith('src-') && e.to !== 'solana', `rpc must not fall back upstream: ${e.id}`);
      // rpc only ever answers clients.
      eq(m, m.node(e.to).kind, 'clients', `rpc outgoing edge ${e.id} must go to a clients node`);
    }
    ok(m, m.out('rpc').length > 0, 'rpc must answer JSON-RPC clients');
    ok(m, m.edge('ch->rpc'), 'rpc reads ClickHouse');
  });
});

test('rule: archive mode decides who moves the Parquet bytes', () => {
  forEachModel((m) => {
    const { archive, source } = m.state;
    eq(m, Boolean(m.node('solparq')), archive !== 'off', 'solparq node iff archiving');
    // A bundle location is drawn for archiving and for a solparq restore source.
    eq(m, Boolean(m.node('parquet-store')), archive !== 'off' || source === 'solparq', 'parquet-store node');
    if (m.node('parquet-store')) eq(m, m.node('parquet-store').variant, archive === 's3' ? 's3' : 'local', 'parquet-store variant');

    if (archive === 'local') {
      // local: solparq streams SELECT ... FORMAT Parquet from ClickHouse and writes the directory.
      ok(m, m.edge('ch->solparq') && m.edge('solparq->parquet-store'), 'local: ch->solparq->parquet-store');
      ok(m, !m.edge('ch->parquet-store'), 'local: ClickHouse does not write the store directly');
      ok(m, !m.edge('solparq->ch'), 'local: no solparq->ch control edge');
    } else if (archive === 's3') {
      // s3: solparq sends INSERT INTO FUNCTION s3 and ClickHouse uploads the Parquet itself
      // (superbank-solparq/clickhouse.rs); solparq writes only manifest/report/.done (storage.rs).
      const query = m.edge('solparq->ch');
      eq(m, [query?.style, query?.particle], ['control', 'query'], 's3: solparq->ch carries the export query');
      const upload = m.edge('ch->parquet-store');
      eq(m, [upload?.particle, upload?.emit?.type], ['parquet', 'relay'], 's3: the query arrival triggers the ClickHouse upload');
      ok(m, !m.edge('ch->solparq'), 's3: Parquet bytes never pass through solparq');
      eq(m, m.edge('solparq->parquet-store')?.particle, 'meta', 's3: solparq writes only bundle metadata to the bucket');
    } else {
      eq(m, m.touching('solparq'), [], 'archive off: nothing touches solparq');
      ok(m, !m.edge('ch->parquet-store'), 'archive off: nothing archives to the store');
    }
    if (archive !== 'off') {
      // solparq validates ranges against its own Solana RPC (--solana-rpc-url), drawn
      // in the archive lane rather than reusing an upstream ingest endpoint.
      eq(m, m.edge('solparq->solparq-rpc')?.style, 'control', 'archive on: control edge to solparq-rpc');
      eq(m, m.node('solparq-rpc')?.zone, 'archive', 'archive on: solparq-rpc sits in the archive lane');
      ok(m, !m.touching('solparq').some((e) => e.from.startsWith('src-') || e.to.startsWith('src-')), 'solparq never links to an upstream endpoint');
    } else {
      ok(m, !m.node('solparq-rpc'), 'archive off: no solparq-rpc');
    }
  });
});

test('rule: source=solparq restores from the store, never through the row pipeline', () => {
  // Local restore streams INSERT ... FORMAT Parquet via superbank; S3 restore is INSERT ... SELECT FROM s3()
  // run by ClickHouse itself. Both use the archive location, local when archiving is off.
  forEachModel((m) => {
    const { source, archive } = m.state;
    const viaIngest = source === 'solparq' && archive !== 's3';
    const viaS3 = source === 'solparq' && archive === 's3';
    eq(m, Boolean(m.edge('parquet-store->ingest')), viaIngest, 'parquet-store->ingest iff local restore');
    eq(m, Boolean(m.edge('parquet-store->t-transactions')), viaS3, 'parquet-store->t-transactions iff s3 restore');
    eq(m, m.edge('ingest->t-transactions')?.style === 'control', viaS3, 'control ingest->t-transactions iff s3 restore');
    if (viaS3) eq(m, m.edge('parquet-store->t-transactions').channel, 'data', 's3 restore edge carries data');
    if (source === 'solparq') {
      ok(m, m.node('parquet-store'), 'solparq source needs the parquet-store node');
      eq(m, m.node('ingest').buffer, null, 'restore skips the row buffer');
      ok(m, !m.topo.edges.some((e) => e.emit?.type === 'flush'), 'restore has no flush edges');
    } else {
      ok(m, m.node('ingest').buffer !== null, 'row sources buffer before flushing');
    }
  });
});

test('rule: ingest process identity and flush ordering', () => {
  // superbank flushes transactions -> blocks_metadata -> entries (flush_buffers in clickhouse.rs);
  // the Jetstreamer plugin runs independent inserters per table.
  forEachModel((m) => {
    const { source } = m.state;
    const ingest = m.node('ingest');
    const flushes = m.out('ingest').filter((e) => e.emit?.type === 'flush');
    if (source === 'jetstreamer') {
      eq(m, ingest.label, 'jetstreamer-clickhouse', 'jetstreamer ingest label');
      eq(m, ingest.variant, 'jetstreamer', 'jetstreamer ingest variant');
      eq(m, flushes.map((e) => e.to).sort(), ['t-blocks_metadata', 't-entries', 't-transactions'], 'jetstreamer writes the three base tables');
      for (const e of flushes) eq(m, e.emit.order, null, `${e.id}: no ordering across tables`);
      eq(m, ingest.buffer.ordered, false, 'jetstreamer buffer is unordered');
    } else {
      eq(m, ingest.label, 'superbank', 'superbank ingest label');
      eq(m, ingest.variant, 'superbank', 'superbank ingest variant');
    }
    if (['grpc', 'fumarole', 'rpc', 'bigtable'].includes(source)) {
      const expected = ['grpc', 'fumarole'].includes(source)
        ? [['t-transactions', 0], ['t-blocks_metadata', 1], ['t-entries', 2]]
        : [['t-transactions', 0], ['t-blocks_metadata', 1]];
      eq(m, flushes.map((e) => [e.to, e.emit.order]).sort((a, b) => a[1] - b[1]), expected, 'superbank flush order');
      eq(m, ingest.buffer.ordered, true, 'superbank buffer is ordered');
    }
  });
});

test('rule: verify node and status', () => {
  // superbank-verify reads ClickHouse only; slots without entries are unverifiable (exit 3), not failed.
  const expected = { grpc: 'ok', fumarole: 'ok', jetstreamer: 'ok', rpc: 'warn', bigtable: 'warn', solparq: 'info' };
  forEachModel((m) => {
    eq(m, Boolean(m.node('verify')), m.state.verify, 'verify node iff verify');
    if (m.state.verify) {
      eq(m, m.node('verify').status, expected[m.state.source], 'verify status for source');
      eq(m, m.into('verify').map((e) => e.id), ['ch->verify'], 'verify reads ClickHouse only');
      eq(m, m.out('verify'), [], 'verify is a sink');
    }
  });
});

test('rule: ClickHouse layout decides shards, replicas and Keeper', () => {
  forEachModel((m) => {
    const { ch } = m.state;
    const shape = { single: [1, 1], cluster: [3, 1], replicated: [3, 2] }[ch];
    eq(m, m.node('ch').variant, ch, 'ch variant');
    eq(m, [m.node('ch').shards, m.node('ch').replicas], shape, 'ch [shards, replicas]');
    for (const t of m.topo.nodes.filter((n) => n.kind === 'table')) {
      eq(m, [t.shards, t.replicas], shape, `${t.id} [shards, replicas]`);
    }
    // Only ReplicatedMergeTree needs Keeper.
    eq(m, Boolean(m.node('keeper')), ch === 'replicated', 'keeper node iff replicated');
    eq(m, m.edge('keeper->ch')?.style === 'control', ch === 'replicated', 'control keeper->ch iff replicated');
  });
});

test('rule: gRPC streaming clients and animation channels follow the selectors', () => {
  forEachModel((m) => {
    const { stream, flow } = m.state;
    eq(m, Boolean(m.node('grpc-clients')), stream, 'grpc-clients exists iff stream');
    eq(m, Boolean(m.edge('rpc->grpc-clients')), stream, 'rpc->grpc-clients iff stream');
    eq(m, m.topo.animate.data, flow !== 'requests', 'animate.data');
    eq(m, m.topo.animate.serve, flow !== 'blocks', 'animate.serve');
  });
});

test('rule: read tiers follow the enabled caches in head -> disk -> ClickHouse order', () => {
  forEachModel((m) => {
    const expected = [...(m.state.head ? ['head-cache'] : []), ...(m.state.disk ? ['disk-cache'] : []), 'ch'];
    eq(m, m.topo.readPath.tiers.map((t) => t.node), expected, 'tier order');
  });
});

test('rule: there is no accounts table', () => {
  forEachModel((m) => {
    for (const n of m.topo.nodes) {
      ok(m, !/account/i.test(n.id) && !/account/i.test(n.label), `node ${n.id} (${n.label}) mentions an accounts table`);
    }
  });
});
