import { test } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { allStates } from '../../site/js/alpenglow-state.js';
import { SOURCES, buildLifecycle } from '../../site/js/alpenglow-model.js';
import { LANE_IDS, STEP_IDS, allRefs, allSources, laneContent, stepContent } from '../../site/js/alpenglow-content.js';

const REPO_ROOT = join(dirname(fileURLToPath(import.meta.url)), '..', '..');
const STATES = [...allStates()];

function checkText(text, where) {
  assert.equal(typeof text, 'string', where);
  assert.ok(text.trim(), `empty text: ${where}`);
  assert.equal(text.split('`').length % 2, 1, `unbalanced backticks: ${where}`);
  assert.ok(!/[<>]/.test(text), `angle bracket: ${where}`);
}

function checkEntry(entry, where) {
  assert.ok(entry, `missing content: ${where}`);
  for (const [i, p] of (entry.body ?? []).entries()) checkText(p, `${where} body[${i}]`);
  assert.ok(entry.body?.length, `no body: ${where}`);
  for (const row of entry.config ?? []) {
    checkText(row.key, where);
    checkText(row.value, where);
    if (row.note) checkText(row.note, where);
  }
  for (const ref of entry.refs ?? []) assert.ok(allRefs().includes(ref), `${where}: ref ${ref} not in allRefs()`);
  for (const id of entry.sources ?? []) assert.ok(SOURCES[id], `${where}: unknown source ${id}`);
}

test('every lane has well-formed content in every state', () => {
  for (const state of STATES)
    for (const id of LANE_IDS) {
      const entry = laneContent(id, state);
      checkEntry(entry, `lane ${id} ${JSON.stringify(state)}`);
      checkText(entry.title, `lane ${id}`);
      checkText(entry.subtitle, `lane ${id}`);
    }
});

test('every step the model emits has well-formed content', () => {
  for (const id of STEP_IDS) for (const state of STATES) checkEntry(stepContent(id, state), `step ${id} ${JSON.stringify(state)}`);
  for (const state of STATES)
    for (const step of buildLifecycle(state).steps) assert.ok(STEP_IDS.includes(step.id), step.id);
});

test('unknown ids return null', () => {
  assert.equal(laneContent('nope', {}), null);
  assert.equal(stepContent('nope', {}), null);
  assert.equal(stepContent('__proto__', {}), null);
});

test('every repo ref exists on disk', () => {
  for (const ref of allRefs()) assert.ok(existsSync(join(REPO_ROOT, ref)), `missing ${ref}`);
});

test('every source is referenced by some content', () => {
  const used = new Set();
  for (const state of STATES) {
    for (const id of LANE_IDS) for (const s of laneContent(id, state).sources) used.add(s);
    for (const id of STEP_IDS) for (const s of stepContent(id, state).sources) used.add(s);
  }
  for (const id of allSources()) assert.ok(used.has(id), `source ${id} is never cited`);
});

test('the era changes the upstream, stream and head copy', () => {
  for (const id of ['upstream', 'stream', 'head', 'ingest']) {
    const a = laneContent(id, { era: 'alpenglow' });
    const t = laneContent(id, { era: 'tower' });
    assert.notDeepEqual(a.body, t.body, id);
  }
  assert.match(laneContent('upstream', { era: 'alpenglow' }).body.join(' '), /together/);
  assert.match(laneContent('stream', { era: 'tower' }).body.join(' '), /bank_id = slot/);
});

test('the probe copy follows the selected method and commitment', () => {
  const sub = laneContent('probe', { method: 'getBlock', read: 'finalized' }).subtitle;
  assert.match(sub, /getBlock/);
  assert.match(sub, /finalized/);
});

test('the head config row shows the selected minimum', () => {
  const row = laneContent('head', { min: 'finalized' }).config.find((r) => r.key === 'HEAD_CACHE_MIN_COMMITMENT');
  assert.match(row.note, /finalized/);
});
