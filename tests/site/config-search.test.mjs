// Checks of the config reference search, filters and URL params
// (site/js/config-search.js) on small fixtures. Run from the repo root:
//   node --test tests/site/config-search.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  MAX_QUERY,
  applyFilters,
  compact,
  entryTier,
  highlightRanges,
  parseParams,
  search,
  serializeParams,
  tokenize,
} from '../../site/js/config-search.js';

const entry = (names, { prose = '', component = 'rpc', features = [], sources = [], sourceScoped = component === 'superbank' } = {}) => ({
  names,
  prose,
  component,
  features,
  sources,
  sourceScoped,
});

const RETAIN = entry(['DISK_CACHE_RETAIN_SLOTS', '--disk-cache-retain-slots'], { prose: 'Finalized slots to retain.', features: ['disk-cache'] });
const QUERY_TIMEOUT = entry(['DISK_CACHE_QUERY_TIMEOUT_MS', '--disk-cache-query-timeout-ms'], { prose: 'Timeout for one local cache read.', features: ['disk-cache'] });
const CH_URL = entry(['CLICKHOUSE_URL', '--clickhouse-url'], { prose: 'HTTP endpoint of the source ClickHouse cluster.' });
const DISK_CH_URL = entry(['DISK_CACHE_CLICKHOUSE_URL'], { prose: 'HTTP endpoint of the local ClickHouse.', features: ['disk-cache'] });
const HEAD = entry(['HEAD_CACHE_ENABLED'], { prose: 'Run the head cache.', features: ['grpc-head-cache'] });
const INGEST_URL = entry(['--clickhouse-url', 'clickhouse-url', 'CLICKHOUSE_URL'], { component: 'superbank' });
const RPC_FROM = entry(['--rpc-from-slot', 'RPC_FROM_SLOT'], { component: 'superbank', sources: ['rpc'] });
const ALL = [RETAIN, QUERY_TIMEOUT, CH_URL, DISK_CH_URL, HEAD, INGEST_URL, RPC_FROM];

const names = (result) => [...result.keys()].map((e) => e.names[0]);

test('compact and tokenize normalize case and separators', () => {
  assert.equal(compact('--Disk-Cache_Retain slots'), 'diskcacheretainslots');
  assert.deepEqual(tokenize('  DISK_CACHE  retain\t--slots '), ['diskcache', 'retain', 'slots']);
  assert.deepEqual(tokenize('-- __ ..'), [], 'separator-only tokens are dropped');
  assert.deepEqual(tokenize(null), []);
  assert.equal(tokenize('x'.repeat(MAX_QUERY + 50))[0].length, MAX_QUERY, 'queries are capped');
});

test('every spelling of a name finds it', () => {
  for (const q of ['disk cache retain', 'DISK_CACHE_RETAIN', '--disk-cache-retain', 'diskcacheretain', 'Disk-Cache-Retain-Slots']) {
    assert.ok(search(ALL, q).has(RETAIN), q);
  }
});

test('the whole query equal to a name is tier 3', () => {
  assert.equal(entryTier(CH_URL, tokenize('clickhouse url')), 3);
  assert.equal(entryTier(CH_URL, tokenize('--clickhouse-url')), 3);
  assert.equal(entryTier(DISK_CH_URL, tokenize('clickhouse url')), 2, 'a longer name containing it is tier 2');
});

test('tokens are ANDed', () => {
  assert.deepEqual(names(search(ALL, 'disk timeout')), ['DISK_CACHE_QUERY_TIMEOUT_MS']);
  assert.deepEqual(names(search(ALL, 'disk nothingmatches')), []);
});

test('prose matches by word prefix only', () => {
  assert.ok(search(ALL, 'finaliz').has(RETAIN), 'prefix of "Finalized"');
  assert.ok(!search(ALL, 'inalized').has(RETAIN), 'not a mid-word substring of prose');
});

test('fuzzy matching only widens results when nothing matches precisely', () => {
  // `dcqt` = d(isk) c(ache) q(uery) t(imeout): word-prefix chunks.
  assert.deepEqual(names(search(ALL, 'dcqt')), ['DISK_CACHE_QUERY_TIMEOUT_MS']);
  assert.equal(entryTier(QUERY_TIMEOUT, tokenize('dcqt')), 1);
  // A dropped letter still finds the name via a tight subsequence.
  assert.ok(search(ALL, 'clikhouse').has(CH_URL));
  // `cache` matches several names exactly, so tier-1 hits are not shown with them.
  const cache = search(ALL, 'cache');
  assert.ok([...cache.values()].every((tier) => tier >= 2));
  assert.equal(entryTier(CH_URL, tokenize('cu')), 1, 'c(lickhouse) u(rl)');
  // Scattered letters are not a match: neither word-prefix chunks nor a
  // subsequence within twice the token length.
  assert.equal(entryTier(CH_URL, tokenize('ckl')), 0);
  assert.equal(entryTier(CH_URL, tokenize('xq')), 0);
});

test('empty and whitespace queries mean no search', () => {
  assert.equal(search(ALL, ''), null);
  assert.equal(search(ALL, '   '), null);
  assert.equal(search(ALL, '__'), null);
});

test('hostile queries stay fast', () => {
  const long = entry(['AAAA_AAAA_AAAA_AAAA_AAAA_AAAA_AAAA_AAAA_AAAA_AAAA']);
  const started = performance.now();
  search([long], 'a'.repeat(500));
  search([long], Array.from({ length: 200 }, () => 'aa').join(' '));
  assert.ok(performance.now() - started < 250, 'chunk matching is memoized');
});

test('highlight ranges point at the original characters', () => {
  const name = 'DISK_CACHE_QUERY_TIMEOUT_MS';
  const slice = (ranges) => ranges.map(([a, b]) => name.slice(a, b));
  assert.deepEqual(slice(highlightRanges(name, 'disk cache')), ['DISK_CACHE'], 'separator gaps are bridged');
  assert.deepEqual(slice(highlightRanges(name, 'timeout')), ['TIMEOUT']);
  assert.deepEqual(slice(highlightRanges(name, 'dcqt')), ['D', 'C', 'Q', 'T']);
  assert.deepEqual(highlightRanges('--clickhouse-url', 'clickhouse url'), [[2, 16]], 'exact match marks the whole name');
  assert.deepEqual(highlightRanges(name, ''), []);
  assert.deepEqual(highlightRanges(name, 'zzz'), []);
  for (const [a, b] of highlightRanges(name, 'qu ti ms')) assert.ok(a >= 0 && b <= name.length && a < b);
});

test('filters are OR within a category and AND across categories', () => {
  assert.deepEqual(applyFilters(ALL, {}), ALL);
  assert.deepEqual(applyFilters(ALL, { f: ['disk-cache'] }), [RETAIN, QUERY_TIMEOUT, DISK_CH_URL]);
  assert.deepEqual(applyFilters(ALL, { f: ['disk-cache', 'grpc-head-cache'] }), [RETAIN, QUERY_TIMEOUT, DISK_CH_URL, HEAD]);
  assert.deepEqual(applyFilters(ALL, { c: ['superbank'] }), [INGEST_URL, RPC_FROM]);
  assert.deepEqual(applyFilters(ALL, { c: ['superbank'], f: ['disk-cache'] }), []);
});

test('the source filter keeps ingestor items that apply to that source', () => {
  // Unrestricted ingestor items apply to every source; other components never match.
  assert.deepEqual(applyFilters(ALL, { s: ['rpc'] }), [INGEST_URL, RPC_FROM]);
  assert.deepEqual(applyFilters(ALL, { s: ['grpc'] }), [INGEST_URL]);
});

const ALLOWED = { components: ['superbank', 'rpc', 'solparq'], features: ['grpc-head-cache', 'disk-cache'], sources: ['grpc', 'rpc'] };

test('parseParams keeps only allowed values, in canonical order', () => {
  assert.deepEqual(parseParams('?q=disk&c=solparq&c=rpc&f=disk-cache&s=rpc', ALLOWED), {
    q: 'disk',
    c: ['rpc', 'solparq'],
    f: ['disk-cache'],
    s: ['rpc'],
  });
  assert.deepEqual(parseParams('c=rpc,superbank,rpc', ALLOWED).c, ['superbank', 'rpc'], 'comma lists and duplicates');
  assert.deepEqual(parseParams('c=%3Cscript%3E&f=__proto__&s=constructor&x=1', ALLOWED), { q: '', c: [], f: [], s: [] });
  assert.deepEqual(parseParams('', ALLOWED), { q: '', c: [], f: [], s: [] });
  assert.equal(parseParams(`q=${'a'.repeat(MAX_QUERY * 3)}`, ALLOWED).q.length, MAX_QUERY);
});

test('serializeParams omits empty values and round-trips', () => {
  assert.equal(serializeParams({}), '');
  assert.equal(serializeParams({ q: '   ' }), '');
  const state = { q: 'disk cache', c: ['superbank', 'rpc'], f: ['disk-cache'], s: ['grpc'] };
  assert.deepEqual(parseParams(serializeParams(state), ALLOWED), state);
});
