import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DEFAULT_STATE, ENUMS, FLAGS, allStates, normalizeState, parseHash, serializeHash } from '../../site/js/state.js';

test('defaults match the out-of-box configuration', () => {
  assert.deepEqual(DEFAULT_STATE, {
    source: 'grpc',
    ch: 'single',
    head: false,
    disk: false,
    stream: false,
    archive: 'local',
    verify: false,
    flow: 'blocks',
  });
  // Every default must itself be an accepted value, or the selector bar could not show it.
  for (const [key, values] of Object.entries(ENUMS)) assert.ok(values.includes(DEFAULT_STATE[key]), key);
  for (const key of FLAGS) assert.equal(typeof DEFAULT_STATE[key], 'boolean', key);
});

test('every state key is covered by exactly one of ENUMS or FLAGS', () => {
  const keys = [...Object.keys(ENUMS), ...FLAGS].sort();
  assert.deepEqual(keys, Object.keys(DEFAULT_STATE).sort());
});

test('parseHash accepts every valid enum value, with or without a leading #', () => {
  for (const [key, values] of Object.entries(ENUMS)) {
    for (const value of values) {
      assert.equal(parseHash(`#${key}=${value}`)[key], value, `#${key}=${value}`);
      assert.equal(parseHash(`${key}=${value}`)[key], value, `${key}=${value}`);
    }
  }
});

test('parseHash reads 1/0 flags', () => {
  for (const key of FLAGS) {
    assert.equal(parseHash(`#${key}=1`)[key], true, `${key}=1`);
    assert.equal(parseHash(`#${key}=0`)[key], false, `${key}=0`);
  }
  // Duplicate keys: URLSearchParams.get returns the first occurrence, so a crafted link is still deterministic.
  assert.equal(parseHash('#head=1&head=0').head, true);
});

test('parseHash combines several parameters', () => {
  const state = parseHash('#source=rpc&head=1&archive=s3&ch=replicated&flow=both&verify=1');
  assert.deepEqual(state, {
    ...DEFAULT_STATE,
    source: 'rpc',
    head: true,
    archive: 's3',
    ch: 'replicated',
    flow: 'both',
    verify: true,
  });
});

test('parseHash ignores junk and falls back to defaults', () => {
  const junk = [
    '#source=<script>alert(1)</script>',
    '#source=%3Cscript%3E',
    '#head=yes',
    '#head=true',
    '#head=2',
    '#head=',
    '#source=GRPC',
    '#source=grpc%20',
    '#unknown=1',
    '#__proto__=polluted&constructor=1',
    '#source',
    '#=&&&=',
    '#%',
    '',
    '#',
    null,
    undefined,
    42,
    {},
  ];
  for (const input of junk) {
    assert.deepEqual(parseHash(input), DEFAULT_STATE, `input: ${JSON.stringify(input)}`);
  }
});

test('parseHash keeps valid params next to junk ones', () => {
  const state = parseHash('#source=nope&ch=cluster&head=maybe&disk=1&bogus=1');
  assert.deepEqual(state, { ...DEFAULT_STATE, ch: 'cluster', disk: true });
});

test('parseHash returns a fresh object and never mutates DEFAULT_STATE', () => {
  const state = parseHash('#source=rpc');
  assert.notEqual(state, DEFAULT_STATE);
  state.source = 'bigtable';
  assert.equal(DEFAULT_STATE.source, 'grpc');
  assert.equal(Object.isFrozen(DEFAULT_STATE), true);
});

test('serializeHash omits defaults', () => {
  assert.equal(serializeHash(DEFAULT_STATE), '');
  assert.equal(serializeHash({}), '');
  assert.equal(serializeHash(null), '');
  assert.equal(serializeHash({ source: 'grpc', archive: 'local' }), '');
  assert.equal(serializeHash({ source: 'rpc' }), 'source=rpc');
  assert.equal(serializeHash({ archive: 'off' }), 'archive=off');
  assert.equal(serializeHash({ head: true }), 'head=1');
});

test('serializeHash never emits unknown keys or invalid values', () => {
  assert.equal(serializeHash({ source: '<script>', bogus: 1, head: 'yes' }), '');
});

test('hash round-trips for every state', () => {
  for (const state of allStates()) {
    const hash = serializeHash(state);
    assert.deepEqual(parseHash(hash), state, `hash: "${hash}"`);
    assert.deepEqual(parseHash(`#${hash}`), state, `hash: "#${hash}"`);
  }
});

test('normalizeState fills defaults and drops invalid values and unknown keys', () => {
  assert.deepEqual(normalizeState(undefined), DEFAULT_STATE);
  assert.deepEqual(normalizeState(null), DEFAULT_STATE);
  assert.deepEqual(normalizeState('source=rpc'), DEFAULT_STATE);
  assert.deepEqual(normalizeState({}), DEFAULT_STATE);
  assert.deepEqual(normalizeState({ source: 'nope', ch: 7, archive: null, flow: {} }), DEFAULT_STATE);
  assert.deepEqual(normalizeState({ source: 'rpc', extra: 'x' }), { ...DEFAULT_STATE, source: 'rpc' });
  assert.equal('extra' in normalizeState({ extra: 'x' }), false);
});

test('normalizeState only accepts real booleans for flags', () => {
  for (const key of FLAGS) {
    for (const bad of ['1', 'true', 1, 0, null, undefined, [], {}]) {
      assert.equal(normalizeState({ [key]: bad })[key], DEFAULT_STATE[key], `${key}=${JSON.stringify(bad)}`);
    }
    assert.equal(normalizeState({ [key]: true })[key], true);
    assert.equal(normalizeState({ [key]: false })[key], false);
  }
});

test('normalizeState returns a fresh object and is idempotent', () => {
  const once = normalizeState({ source: 'solparq', head: true });
  assert.notEqual(once, DEFAULT_STATE);
  assert.deepEqual(normalizeState(once), once);
});

test('allStates yields 2592 unique, valid states', () => {
  const seen = new Set();
  let count = 0;
  for (const state of allStates()) {
    count++;
    seen.add(JSON.stringify(Object.entries(state).sort(([a], [b]) => a.localeCompare(b))));
    assert.deepEqual(normalizeState(state), state, 'every generated state is already normalized');
  }
  assert.equal(count, 2592);
  assert.equal(seen.size, 2592);
  assert.equal(count, ENUMS.source.length * ENUMS.ch.length * ENUMS.archive.length * ENUMS.flow.length * 2 ** FLAGS.length);
});
