// SPDX-License-Identifier: AGPL-3.0-only
// Full-retention regression gate. See tests/k6/README.md for fixture requirements.
import http from 'k6/http';
import exec from 'k6/execution';
import { check, fail } from 'k6';
import { SharedArray } from 'k6/data';
import { Trend } from 'k6/metrics';
import { deepEqual } from '../../lib/compare.js';

const smoke = __ENV.KEY_ROUTING_SMOKE === '1';
const seconds = smoke ? 10 : 1800;
const rate = smoke ? 1 : 70;
const requests = new SharedArray('key routing workload', () => JSON.parse(open(__ENV.KEY_REQUEST_FILE)));
const signatureMs = new Trend('key_signature_ms');
const addressMs = new Trend('key_address_ms');
const allowed = new Set(['getTransaction', 'getSignatureStatuses', 'getSignaturesForAddress', 'getTransactionsForAddress']);

export const options = {
  scenarios: { keys: { executor: 'constant-arrival-rate', rate, timeUnit: '1s', duration: `${seconds}s`, preAllocatedVUs: 100, maxVUs: 200 } },
  thresholds: { checks: ['rate==1'], http_req_failed: ['rate==0'], dropped_iterations: ['count==0'],
    key_signature_ms: ['p(99)<250'], key_address_ms: ['p(99)<500'] },
};
function reject(message) {
  check(null, { 'benchmark preconditions and resource gates': () => false });
  fail(message);
}
function normalized(method, result) {
  if (method === 'getTransactionsForAddress') return result?.data ?? null;
  if (method === 'getSignatureStatuses') return result?.value ?? null;
  return result;
}
function preflight() {
  if (requests.length < rate * (seconds + 1)) reject('Workload must contain one entry per iteration; repeating a small key pool hides fanout.');
  const bands = new Set();
  const methods = new Set();
  const keys = new Set();
  let signatureHits = 0;
  let addressHits = 0;
  for (const request of requests) {
    if (!allowed.has(request.method) || !Object.prototype.hasOwnProperty.call(request, 'expected')) reject('Each entry requires an allowed method and expected result.');
    methods.add(request.method); bands.add(request.ageBand);
    if (request.expectedDiskHit === true) {
      if (request.method === 'getSignatureStatuses' && (request.params[1]?.searchTransactionHistory === true || !Array.isArray(request.expected) || request.expected.some(v => v === null))) reject('Status hit fixtures must disable history fallback and expect non-null cached values.');
      if (request.method === 'getTransaction' || request.method === 'getSignatureStatuses') signatureHits++;
      else addressHits++;
    }
    if (request.method === 'getTransaction') {
      if (keys.has(request.params[0])) reject('getTransaction keys must not repeat.');
      keys.add(request.params[0]);
    }
  }
  if (signatureHits === 0 || addressHits === 0) reject('Both signature and address hit latency need samples.');
  if (!smoke && (signatureHits < 10000 || addressHits < 10000)) reject('Full-size runs require at least 10,000 hits in each latency class.');
  if (!smoke && (methods.size !== 4 || !['recent', 'middle', 'oldest'].every(b => bands.has(b)))) reject('Include all four methods and all three retention age bands.');
}
function sizeGate() {
  if (smoke) return;
  const database = __ENV.KEY_CLICKHOUSE_DATABASE || 'superbank_disk_cache';
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(database)) reject('Invalid cache database identifier.');
  if (!__ENV.KEY_CLICKHOUSE_URL || !__ENV.METRICS_URL) reject('Full-size runs require KEY_CLICKHOUSE_URL and METRICS_URL.');
  const response = http.post(__ENV.KEY_CLICKHOUSE_URL,
    `SELECT sum(rows) AS rows, uniqExact(partition_id) AS partitions, count() AS parts FROM system.parts WHERE active AND database='${database}' AND table='signatures' FORMAT JSONEachRow`);
  if (response.status !== 200) reject('Could not inspect cache size.');
  const size = response.json();
  if (Number(size.rows) < 900000000 || Number(size.partitions) < 80 || Number(size.parts) < 600) reject('Dataset is too small to establish the full-size performance gate.');
}
function indexGate() {
  if (!__ENV.METRICS_URL) return;
  const response = http.get(__ENV.METRICS_URL);
  const bytes = response.body.match(/^superbank_disk_cache_key_index_bytes\s+(\S+)/m);
  if (response.status !== 200 || !bytes || Number(bytes[1]) > 4294967296) reject('Index memory budget exceeded or metrics unavailable.');
}
export function setup() { preflight(); sizeGate(); indexGate(); }
export default function () {
  const request = requests[exec.scenario.iterationInTest];
  if (!request) reject('Workload exhausted.');
  const response = http.post(__ENV.RPC_URL, JSON.stringify({ jsonrpc: '2.0', id: exec.scenario.iterationInTest, method: request.method, params: request.params }), { headers: { 'Content-Type': 'application/json' } });
  let body;
  try { body = response.json(); } catch (_) { body = null; }
  const source = String(response.headers['X-Superbank-Sources'] || '').toLowerCase();
  check(response, {
    'successful response': r => r.status === 200 && body !== null && !body.error,
    'exact data parity': () => body !== null && deepEqual(normalized(request.method, body.result), request.expected),
    'expected disk hit': () => request.expectedDiskHit !== true || (source.includes('disk-cache') && !source.includes('head-cache') && (request.method === 'getSignatureStatuses' || !source.includes('clickhouse'))),
  });
  // End-to-end hit latency is a conservative upper bound on the local attempt.
  // Miss/fallback attempt latency is evaluated separately from exported histograms.
  if (request.expectedDiskHit === true) {
    const metric = request.method === 'getTransaction' || request.method === 'getSignatureStatuses' ? signatureMs : addressMs;
    metric.add(response.timings.duration);
  }
  if (exec.scenario.iterationInTest % 700 === 0) indexGate();
}
export function teardown() { indexGate(); }
