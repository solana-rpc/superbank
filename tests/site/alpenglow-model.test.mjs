import { test } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { allStates } from '../../site/js/alpenglow-state.js';
import {
  CONSTANTS,
  LANES,
  PRODUCTION_RETAIN_EXAMPLE,
  RANK,
  SOURCES,
  STEP_IDS,
  UPSTREAM,
  answerRead,
  buildLifecycle,
  meets,
} from '../../site/js/alpenglow-model.js';

const REPO_ROOT = join(dirname(fileURLToPath(import.meta.url)), '..', '..');
const READS = ['processed', 'confirmed', 'finalized'];
const METHODS = ['getTransaction', 'getBlock'];
const STATUSES = new Set(['staged', 'frozen', 'held', 'visible', 'discarded', 'cleared', 'evicted']);

// Model state keys only; method/read do not change the frames.
const CASES = [...allStates()].filter((s) => s.method === 'getTransaction' && s.read === 'processed');
const BUILT = CASES.map((state) => ({ state, life: buildLifecycle(state) }));
const name = (s) => `era=${s.era} min=${s.min} retain=${s.retain} scenario=${s.scenario}`;

function eachFrame(fn) {
  for (const { state, life } of BUILT) life.steps.forEach((frame, i) => fn(frame, state, i, life.steps));
}

const visibleIn = (frame, slot) => frame.banks.find((b) => b.slot === slot && b.status === 'visible');

// --- Contract ---------------------------------------------------------------

test('every state builds unique, known steps', () => {
  for (const { state, life } of BUILT) {
    const ids = life.steps.map((s) => s.id);
    assert.ok(ids.length >= 5, name(state));
    assert.equal(new Set(ids).size, ids.length, `duplicate step in ${name(state)}: ${ids}`);
    for (const id of ids) assert.ok(STEP_IDS.includes(id), `unknown step ${id} in ${name(state)}`);
    assert.deepEqual(
      life.summary.map((s) => s.id),
      ids,
      name(state),
    );
  }
});

test('frames are well formed', () => {
  eachFrame((f, state) => {
    const where = `${name(state)} step=${f.id}`;
    assert.ok(Number.isInteger(f.tip) && Number.isInteger(f.fin), where);
    assert.ok(f.fin <= f.tip, `finalized tip ahead of the tip: ${where}`);
    assert.ok(f.featured === 0 || (state.scenario === 'retry' && f.featured === 1), where);
    assert.ok(typeof f.title === 'string' && f.title, where);
    assert.ok(typeof f.summary === 'string' && f.summary, where);
    assert.equal(f.summary.split('`').length % 2, 1, `unbalanced backticks: ${where}`);
    assert.ok(!/[<>]/.test(f.summary + f.title + (f.upstream ?? '') + (f.note ?? '')), `angle bracket in copy: ${where}`);
    assert.ok(f.headWindow.hi - f.headWindow.lo <= f.retain - 1, where);
    for (const b of f.banks) {
      assert.ok(STATUSES.has(b.status), `${where} bank ${b.id} status ${b.status}`);
      assert.ok(b.token === null || b.token in RANK, where);
    }
  });
});

test('the tip never moves backwards within a lifecycle', () => {
  for (const { state, life } of BUILT) {
    life.steps.forEach((f, i) => {
      if (i) assert.ok(f.tip >= life.steps[i - 1].tip && f.fin >= life.steps[i - 1].fin, `${name(state)} step=${f.id}`);
    });
  }
});

test('lanes are fixed', () => {
  assert.deepEqual(
    LANES.map((l) => l.id),
    ['upstream', 'stream', 'head', 'ingest', 'disk', 'probe'],
  );
});

// --- Rules pinned to the Rust code --------------------------------------------

test('rule: getBlock never accepts processed', () => {
  eachFrame((f, state) => {
    const answer = answerRead(f, 'getBlock', 'processed');
    assert.equal(answer.tier, null, `${name(state)} step=${f.id}`);
    assert.equal(answer.error, '-32602');
  });
});

test('rule: disk and ClickHouse only ever answer with finalized data', () => {
  eachFrame((f, state) => {
    for (const method of METHODS)
      for (const read of READS) {
        const a = answerRead(f, method, read);
        if (a.tier === 'disk' || a.tier === 'clickhouse') assert.equal(a.token, 'finalized', `${name(state)} step=${f.id}`);
        if (a.tier === 'head') assert.ok(meets(a.token, read), `head answered below ${read}: ${name(state)} step=${f.id}`);
      }
  });
});

test('rule: the head cache never shows a bank below HEAD_CACHE_MIN_COMMITMENT', () => {
  eachFrame((f, state) => {
    for (const b of f.banks)
      if (b.status === 'visible') assert.ok(meets(b.token, state.min), `${name(state)} step=${f.id} bank ${b.id}=${b.token}`);
  });
});

test('rule: the head cache shows a slot only inside its retained window', () => {
  eachFrame((f, state) => {
    for (const b of f.banks)
      if (b.status === 'visible')
        assert.ok(b.slot >= f.headWindow.lo && b.slot <= f.headWindow.hi, `${name(state)} step=${f.id} slot ${b.slot}`);
  });
});

test('rule: Alpenglow reports confirmed and finalized together', () => {
  eachFrame((f, state) => {
    if (state.era !== 'alpenglow') return;
    assert.ok(!f.banks.some((b) => b.token === 'confirmed'), `${name(state)} step=${f.id}`);
  });
  // So a confirmed minimum publishes at the same step as a finalized one.
  for (const scenario of ['clean', 'fork', 'retry', 'reconnect']) {
    const at = (min) =>
      buildLifecycle({ era: 'alpenglow', scenario, min })
        .steps.findIndex((f) => f.banks.some((b) => b.status === 'visible'));
    assert.equal(at('confirmed'), at('finalized'), scenario);
  }
});

test('rule: Tower confirms before it finalizes', () => {
  for (const retain of ['default', 'large']) {
    const steps = buildLifecycle({ era: 'tower', scenario: 'clean', min: 'processed', retain }).steps.map((s) => s.id);
    const root = steps.indexOf(retain === 'default' ? 'edge' : 'finalized');
    assert.ok(root > 0 && steps.indexOf('confirmed') < root, steps.join(','));
  }
});

test('rule: the Tower root races eviction only with a window no deeper than the root', () => {
  for (const { state, life } of BUILT) {
    const hasEdge = life.steps.some((s) => s.id === 'edge');
    const expected = state.era === 'tower' && state.retain === 'default' && state.min !== 'finalized' && state.scenario !== 'reconnect' && state.scenario !== 'fork';
    assert.equal(hasEdge, expected, name(state));
  }
});

test('rule: the default window evicts before the disk copy; a production window copies first', () => {
  for (const { state, life } of BUILT) {
    const ids = life.steps.map((s) => s.id);
    const evict = ids.indexOf('evicted');
    const disk = ids.indexOf('disk');
    if (evict < 0 || disk < 0) continue;
    if (state.retain === 'default') assert.ok(evict < disk, name(state));
    else assert.ok(disk < evict, name(state));
  }
});

test('rule: with a production window, ClickHouse never has to answer for a clean slot', () => {
  for (const { state, life } of BUILT) {
    if (state.retain !== 'large' || state.scenario !== 'clean') continue;
    for (const f of life.steps)
      for (const method of METHODS)
        for (const read of ['confirmed', 'finalized'])
          assert.notEqual(answerRead(f, method, read).tier, 'clickhouse', `${name(state)} step=${f.id} ${method}@${read}`);
  }
});

test('the production example is above the head/disk overlap threshold', () => {
  assert.ok(PRODUCTION_RETAIN_EXAMPLE >= CONSTANTS.statusHistoryMinRetainSlots.value);
  assert.ok(PRODUCTION_RETAIN_EXAMPLE > CONSTANTS.diskMinLagSlots.value + UPSTREAM.towerRootDepthSlots.value);
  assert.ok(CONSTANTS.headRetainSlots.value < CONSTANTS.statusHistoryMinRetainSlots.value);
});

test('rule: the ingestor writes only finalized, canonical banks', () => {
  eachFrame((f, state) => {
    for (const row of Object.values(f.stored)) {
      const bank = f.banks.find((b) => b.id === row.bank);
      assert.equal(bank.token, 'finalized', `${name(state)} step=${f.id}`);
      assert.notEqual(bank.status, 'discarded', `${name(state)} step=${f.id}`);
    }
  });
});

test('rule: the disk cache copies only ingested slots at least 75 behind the finalized tip', () => {
  eachFrame((f, state) => {
    for (const slot of Object.keys(f.disk)) {
      assert.ok(f.stored[slot], `disk before ClickHouse: ${name(state)} step=${f.id}`);
      assert.ok(f.fin >= Number(slot) + CONSTANTS.diskMinLagSlots.value, `${name(state)} step=${f.id}`);
      // ...and, with the 32-slot default, never while the head cache still shows the slot.
      if (state.retain === 'default') assert.ok(!visibleIn(f, Number(slot)), `${name(state)} step=${f.id}`);
    }
  });
});

test('rule: at the Tower root edge, ClickHouse never has the slot while the head still shows it', () => {
  eachFrame((f, state) => {
    if (state.era !== 'tower' || state.min === 'finalized' || state.retain !== 'default') return;
    for (const slot of Object.keys(f.stored)) assert.ok(!visibleIn(f, Number(slot)), `${name(state)} step=${f.id}`);
  });
});

test('rule: Tower streams carry no bank IDs and no footers', () => {
  eachFrame((f, state) => {
    if (state.era !== 'tower') return;
    for (const b of f.banks) {
      assert.equal(b.footer, false, `${name(state)} step=${f.id}`);
      assert.match(b.name, /^bank_id = N(\+\d+)?$/);
    }
    for (const row of Object.values(f.stored)) assert.equal(row.footer, 'null');
    for (const read of ['confirmed', 'finalized']) {
      const a = answerRead(f, 'getBlock', read);
      if (a.tier) assert.equal(a.footer, false, `${name(state)} step=${f.id}`);
    }
  });
});

test('rule: Alpenglow getBlock returns the footer from every tier', () => {
  eachFrame((f, state) => {
    if (state.era !== 'alpenglow') return;
    const a = answerRead(f, 'getBlock', 'finalized');
    if (a.tier) assert.equal(a.footer, true, `${name(state)} step=${f.id}`);
  });
});

test('rule: Alpenglow fork shows the first frozen bank, then only the finalized one', () => {
  const { steps } = buildLifecycle({ era: 'alpenglow', scenario: 'fork', min: 'processed' });
  const fin = steps.findIndex((s) => s.id === 'finalized');
  for (const f of steps.slice(steps.findIndex((s) => s.id === 'published'), fin)) {
    assert.equal(visibleIn(f, 0)?.name, 'bank_id 7', f.id);
    assert.equal(answerRead(f, 'getTransaction', 'processed').bank, 'bank_id 7', f.id);
  }
  for (const f of steps.slice(fin)) {
    assert.ok(!f.banks.some((b) => b.name === 'bank_id 7' && b.status !== 'discarded'), f.id);
    const a = answerRead(f, 'getTransaction', 'processed');
    if (a.tier) assert.equal(a.bank, 'bank_id 9', f.id);
  }
});

test('rule: a retried transaction moves only when its new bank is committed', () => {
  for (const era of ['alpenglow', 'tower']) {
    const { steps } = buildLifecycle({ era, scenario: 'retry', min: 'processed' });
    const retried = steps.find((s) => s.id === 'retried');
    assert.equal(retried.tx.slot, 0, `${era}: processed N+1 must not take the signature`);
    const moved = steps.find((s) => s.tx?.slot === 1);
    assert.ok(meets(moved.banks.find((b) => b.id === moved.tx.bank).token, 'confirmed'), era);
    // The final answer for the transaction is slot N+1.
    assert.equal(answerRead(steps.at(-1), 'getTransaction', 'finalized').slot, 1, era);
  }
});

test('rule: a reconnect clears the head cache and reads fall through', () => {
  eachFrame((f, state, i, steps) => {
    if (state.scenario !== 'reconnect') return;
    const drop = steps.findIndex((s) => s.id === 'dropped');
    if (i < drop) return;
    assert.ok(f.headCleared, `${name(state)} step=${f.id}`);
    assert.ok(!f.banks.some((b) => b.status === 'visible'), `${name(state)} step=${f.id}`);
    for (const read of READS) assert.notEqual(answerRead(f, 'getTransaction', read).tier, 'head');
  });
});

test('rule: Tower fork never reaches ClickHouse', () => {
  for (const min of READS) {
    const last = buildLifecycle({ era: 'tower', scenario: 'fork', min }).steps.at(-1);
    assert.equal(last.id, 'forked');
    assert.deepEqual(last.stored, {});
    assert.equal(answerRead(last, 'getTransaction', 'finalized').tier, null);
  }
});

// --- Drift: numbers on the page are re-read from the Rust source --------------

const DRIFT = {
  headRetainSlots: /env = "HEAD_CACHE_RETAIN_SLOTS", default_value_t = (\d+)\)/,
  statusHistoryMinRetainSlots: /const STATUS_HISTORY_MIN_HEAD_RETAIN_SLOTS: u64 = (\d+);/,
  diskMinLagSlots: /env = "DISK_CACHE_REPAIR_MIN_LAG_SLOTS", default_value_t = (\d+)\)/,
  pendingCommitmentSlots: /const PENDING_COMMITMENT_SLOTS: u64 = (\d+);/,
  tipMaxAgeSecs: /const TIP_MAX_AGE: Duration = Duration::from_secs\((\d+)\);/,
  backoffStartMs: /let mut backoff = Duration::from_millis\((\d+)\);/,
  backoffMaxSecs: /let max_backoff = Duration::from_secs\((\d+)\);/,
  footerWaitSecs: /const FOOTER_WAIT: Duration = Duration::from_secs\((\d+)\);/,
  flushIntervalSecs: /env = "FLUSH_INTERVAL_SECS", default_value_t = (\d+)\)/,
};

test('drift: every CONSTANTS value matches its Rust source', () => {
  assert.deepEqual(Object.keys(DRIFT).sort(), Object.keys(CONSTANTS).sort(), 'every constant needs a drift pattern');
  for (const [key, { value, ref, name: label }] of Object.entries(CONSTANTS)) {
    const path = join(REPO_ROOT, ref);
    assert.ok(existsSync(path), `${key}: ${ref} does not exist`);
    const match = readFileSync(path, 'utf8').match(DRIFT[key]);
    assert.ok(match, `${key}: ${label} no longer found in ${ref}; update site/js/alpenglow-model.js`);
    assert.equal(Number(match[1]), value, `${key}: ${ref} now says ${match[1]}, the page says ${value}`);
  }
});

test('drift: head retention still keeps `retain` slots counted back from the newest published slot', () => {
  const src = readFileSync(join(REPO_ROOT, 'crates/superbank-rpc/src/head_cache/mod.rs'), 'utf8');
  assert.match(src, /latest\.saturating_sub\(self\.retain_slots\.saturating_sub\(1\)\)/);
});

test('drift: durable writers still require finalized', () => {
  const src = readFileSync(join(REPO_ROOT, 'crates/superbank/src/commitment.rs'), 'utf8');
  assert.match(src, /fn parse_durable_commitment/);
});

// --- Upstream citations -----------------------------------------------------

test('every upstream number cites a pinned source', () => {
  for (const [key, entry] of Object.entries(UPSTREAM)) {
    assert.ok(entry.sources.length, key);
    for (const id of entry.sources) assert.ok(SOURCES[id], `${key}: unknown source ${id}`);
  }
  for (const [id, { label, url }] of Object.entries(SOURCES)) {
    assert.ok(label, id);
    const parsed = new URL(url);
    assert.equal(parsed.protocol, 'https:', id);
    // Pinned: an Agave tag, a 40-hex commit, or a versioned docs.rs path.
    assert.match(url, /\/blob\/v\d+\.\d+\.\d+\/|\/blob\/[0-9a-f]{40}\/|docs\.rs\/(crate\/)?[\w-]+\/\d+\.\d+\.\d+/, id);
  }
});
