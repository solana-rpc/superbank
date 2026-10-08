// Checks of contentFor() over all 2592 selector states: every node the topology
// can emit has well-formed copy, actions make sense for the state they are
// shown in, and every referenced repo path exists. Run from the repo root:
//   node --test tests/site/content.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { ENUMS, FLAGS, allStates, normalizeState } from '../../site/js/state.js';
import { SOURCE_ENDPOINT, buildTopology } from '../../site/js/topology.js';
import { REPO_BLOB, allRefs, contentFor } from '../../site/js/content.js';

const REPO_ROOT = fileURLToPath(new URL('../../', import.meta.url));

// Every node id buildTopology can emit. If topology.js grows a node, this list
// and content.js must both learn about it.
const EXPECTED_IDS = [
  'solana',
  'src-dragonsmouth',
  'src-fumarole',
  'src-jsonrpc',
  'src-bigtable',
  'src-oldfaithful',
  'ingest',
  't-transactions',
  't-blocks_metadata',
  't-entries',
  't-gsfa',
  't-signatures',
  't-gsfa_hot',
  't-token_owner_activity',
  'ch',
  'keeper',
  'rpc',
  'head-cache',
  'disk-cache',
  'jsonrpc-clients',
  'grpc-clients',
  'parquet-store',
  'solparq',
  'verify',
];

// Endpoint node -> the `source` value that makes it the ingest source.
const ENDPOINT_SOURCE = {
  'src-dragonsmouth': 'grpc',
  'src-fumarole': 'fumarole',
  'src-jsonrpc': 'rpc',
  'src-bigtable': 'bigtable',
  'src-oldfaithful': 'jetstreamer',
};

const nonEmptyString = (value) => typeof value === 'string' && value.trim().length > 0;

function stringsOf(content) {
  const strings = [content.title, content.subtitle, ...content.body];
  for (const entry of content.config) strings.push(entry.key, entry.value, ...(entry.note === undefined ? [] : [entry.note]));
  if (content.action) strings.push(content.action.label);
  return strings;
}

// One pass over every state; the individual tests below assert on what it saw.
const seenIds = new Set();
const seenRefs = new Set();
const failures = [];
const bodies = new Map(); // `${id}|${variant}` -> Set of first body strings

function check(state, id, cond, msg) {
  if (!cond) failures.push(`${id}: ${msg}\n    state: ${JSON.stringify(state)}`);
}

for (const state of allStates()) {
  const current = normalizeState(state);
  for (const node of buildTopology(state).nodes) {
    const id = node.id;
    seenIds.add(id);
    const c = contentFor(id, state);
    if (c === null || typeof c !== 'object') {
      failures.push(`${id}: contentFor returned ${c}\n    state: ${JSON.stringify(state)}`);
      continue;
    }
    check(state, id, nonEmptyString(c.title), 'title must be a non-empty string');
    check(state, id, nonEmptyString(c.subtitle), 'subtitle must be a non-empty string');
    check(state, id, Array.isArray(c.body) && c.body.length >= 1 && c.body.length <= 4, 'body needs 1-4 strings (the panel is meant to stay short)');
    check(state, id, Array.isArray(c.body) && c.body.every(nonEmptyString), 'body entries must be non-empty strings');
    check(state, id, Array.isArray(c.config), 'config must be an array');
    check(
      state,
      id,
      Array.isArray(c.config) &&
        c.config.every((e) => nonEmptyString(e.key) && nonEmptyString(e.value) && (e.note === undefined || nonEmptyString(e.note))),
      'config entries need string key and value (note optional)',
    );
    check(state, id, Array.isArray(c.refs) && c.refs.length >= 1 && c.refs.length <= 4, 'refs must list 1-4 paths');
    check(state, id, Array.isArray(c.refs) && c.refs.every(nonEmptyString), 'refs must be non-empty strings');
    check(state, id, Array.isArray(c.config) && c.config.length <= 8, 'config must have at most 8 entries');
    if (!Array.isArray(c.body) || !Array.isArray(c.config) || !Array.isArray(c.refs)) continue;

    // Content is rendered with textContent, but it must also never contain markup.
    for (const text of stringsOf(c)) check(state, id, !text.includes('<'), `string contains "<": ${text}`);
    // Backticks must pair up, or the renderer would leave a stray code span open.
    for (const text of c.body) check(state, id, (text.match(/`/g) ?? []).length % 2 === 0, `unbalanced backticks: ${text}`);
    for (const ref of c.refs) {
      seenRefs.add(ref);
      check(state, id, !/[#:]\d+$|#L\d+/.test(ref), `ref must not carry a line number: ${ref}`);
    }

    // Actions: only well-formed patches that actually change the state.
    if (c.action !== undefined) {
      const { label, patch } = c.action;
      check(state, id, nonEmptyString(label), 'action needs a label');
      check(state, id, patch && typeof patch === 'object' && Object.keys(patch).length > 0, 'action needs a patch');
      for (const [key, value] of Object.entries(patch ?? {})) {
        if (key in ENUMS) check(state, id, ENUMS[key].includes(value), `patch ${key}=${value} is not a valid enum value`);
        else if (FLAGS.includes(key)) check(state, id, typeof value === 'boolean', `patch ${key} must be boolean`);
        else check(state, id, false, `patch key ${key} is not a selector key`);
      }
      const next = normalizeState({ ...state, ...patch });
      check(state, id, JSON.stringify(next) !== JSON.stringify(current), 'action patch would not change the state');
    }

    // Action rules from the contract.
    if (id in ENDPOINT_SOURCE) {
      const wanted = ENDPOINT_SOURCE[id];
      if (state.source === wanted) check(state, id, c.action === undefined, 'active source must not offer an action');
      else check(state, id, c.action?.patch?.source === wanted && Object.keys(c.action.patch).length === 1, `must offer source=${wanted}`);
    } else if (id === 'parquet-store') {
      if (state.source === 'solparq') check(state, id, c.action === undefined, 'already restoring: no action');
      else check(state, id, c.action?.patch?.source === 'solparq', 'must offer restore from Parquet');
    } else {
      check(state, id, c.action === undefined, 'only endpoints and the Parquet store offer actions');
    }

    // Track which variants of state-dependent nodes produced distinct copy.
    const variantKey = { ingest: state.source, ch: state.ch, 'parquet-store': `${state.archive}|${state.source === 'solparq'}` }[id];
    if (variantKey !== undefined) {
      const key = `${id}|${variantKey}`;
      if (!bodies.has(key)) bodies.set(key, new Set());
      bodies.get(key).add(c.body.join('\n'));
    }
    if (id === 'verify') {
      const kind = state.source === 'solparq' ? 'depends' : ['grpc', 'fumarole', 'jetstreamer'].includes(state.source) ? 'verifiable' : 'unverifiable';
      const key = `verify|${kind}`;
      if (!bodies.has(key)) bodies.set(key, new Set());
      bodies.get(key).add(c.body.join('\n'));
    }
  }
}

test('REPO_BLOB points at the repository blob root', () => {
  assert.equal(REPO_BLOB, 'https://github.com/solana-rpc/superbank/blob/main/');
});

test('every node in every state has well-formed content', () => {
  assert.deepEqual(failures.slice(0, 10), [], `${failures.length} problem(s); first 10 shown`);
});

test('content covers every node id the topology can emit, and nothing else is emitted', () => {
  assert.deepEqual([...seenIds].sort(), [...EXPECTED_IDS].sort());
});

test('endpoint ids line up with the topology source mapping', () => {
  for (const [source, id] of Object.entries(SOURCE_ENDPOINT)) {
    if (source === 'solparq') continue; // restore reads the Parquet store, not an endpoint node
    assert.equal(ENDPOINT_SOURCE[id], source, `${id} should map to source ${source}`);
  }
});

test('unknown ids return null in every state', () => {
  for (const state of allStates()) {
    assert.equal(contentFor('nope', state), null);
  }
  for (const id of ['', 'INGEST', '__proto__', 'constructor', 'toString', 'hasOwnProperty', undefined, null, 42]) {
    assert.equal(contentFor(id, {}), null, `id ${String(id)}`);
  }
});

test('contentFor tolerates missing or invalid state by falling back to defaults', () => {
  for (const id of EXPECTED_IDS) {
    assert.deepEqual(contentFor(id, undefined), contentFor(id, {}), id);
    assert.deepEqual(contentFor(id, { source: 'bogus', ch: 7 }), contentFor(id, {}), id);
  }
});

test('state-dependent nodes change their copy with the state that matters', () => {
  for (const source of ENUMS.source) {
    assert.ok(bodies.has(`ingest|${source}`), `ingest/${source} variant exists`);
  }
  const ingestBodies = ENUMS.source.map((source) => [...bodies.get(`ingest|${source}`)].join('\n'));
  assert.equal(new Set(ingestBodies).size, ENUMS.source.length, 'each ingest source has its own body');

  const chBodies = ENUMS.ch.map((ch) => [...bodies.get(`ch|${ch}`)].join('\n'));
  assert.equal(new Set(chBodies).size, ENUMS.ch.length, 'each ClickHouse layout has its own body');

  const verifyBodies = ['verifiable', 'unverifiable', 'depends'].map((kind) => [...bodies.get(`verify|${kind}`)].join('\n'));
  assert.equal(new Set(verifyBodies).size, 3, 'verify differs between verifiable, unverifiable and depends');

  assert.ok(bodies.has('parquet-store|local|false') && bodies.has('parquet-store|s3|false'), 'local and s3 stores differ');
  const store = [...bodies.keys()].filter((k) => k.startsWith('parquet-store|'));
  const distinct = new Set(store.map((k) => [...bodies.get(k)].join('\n')));
  assert.equal(distinct.size, store.length, 'each store variant has its own body');
});

test('every ref exists on disk and is listed by allRefs()', () => {
  const refs = allRefs();
  assert.ok(Array.isArray(refs) && refs.length > 0);
  assert.equal(new Set(refs).size, refs.length, 'allRefs() must not repeat paths');
  const missing = refs.filter((ref) => !existsSync(join(REPO_ROOT, ref)));
  assert.deepEqual(missing, [], 'allRefs() entries missing from the repository');
  const unlisted = [...seenRefs].filter((ref) => !refs.includes(ref));
  assert.deepEqual(unlisted, [], 'refs returned by contentFor() but absent from allRefs()');
  for (const ref of refs) {
    assert.ok(!ref.startsWith('/') && !ref.includes('..') && !ref.includes('://'), `ref must be repo-relative: ${ref}`);
  }
});
