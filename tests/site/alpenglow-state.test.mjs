import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DEFAULT_STATE, ENUMS, allStates, normalizeState, parseHash, serializeHash } from '../../site/js/alpenglow-state.js';

test('defaults are accepted values and cover every key', () => {
  assert.deepEqual(Object.keys(DEFAULT_STATE).sort(), Object.keys(ENUMS).sort());
  for (const [key, values] of Object.entries(ENUMS)) assert.ok(values.includes(DEFAULT_STATE[key]), key);
  // The page opens on Alpenglow with the head-cache default minimum.
  assert.equal(DEFAULT_STATE.era, 'alpenglow');
  assert.equal(DEFAULT_STATE.min, 'processed');
});

test('parseHash accepts every enum value, with or without a leading #', () => {
  for (const [key, values] of Object.entries(ENUMS)) {
    for (const value of values) {
      assert.equal(parseHash(`#${key}=${value}`)[key], value, `#${key}=${value}`);
      assert.equal(parseHash(`${key}=${value}`)[key], value, `${key}=${value}`);
    }
  }
});

test('parseHash ignores junk and falls back to defaults', () => {
  const junk = [
    '',
    '#',
    '#era=<script>alert(1)</script>',
    '#era=%3Cscript%3E',
    '#min=PROCESSED',
    '#scenario=__proto__',
    '#constructor=1&__proto__=x',
    '#method=getBlock%00',
    '#read=',
    '#%%%',
    null,
    undefined,
  ];
  for (const hash of junk) assert.deepEqual(parseHash(hash), DEFAULT_STATE, String(hash));
});

test('serializeHash writes only non-defaults and round-trips every state', () => {
  assert.equal(serializeHash(DEFAULT_STATE), '');
  let count = 0;
  for (const state of allStates()) {
    count++;
    const hash = serializeHash(state);
    assert.deepEqual(parseHash(hash), state, hash);
    // Idempotent: serializing the parsed state gives the same string.
    assert.equal(serializeHash(parseHash(hash)), hash);
  }
  assert.equal(count, 2 * 3 * 4 * 2 * 3);
});

test('normalizeState drops unknown keys and invalid values', () => {
  assert.deepEqual(normalizeState({ era: 'tower', bogus: 1, min: 'nope' }), { ...DEFAULT_STATE, era: 'tower' });
  assert.deepEqual(normalizeState(null), DEFAULT_STATE);
  assert.deepEqual(normalizeState('era=tower'), DEFAULT_STATE);
});
