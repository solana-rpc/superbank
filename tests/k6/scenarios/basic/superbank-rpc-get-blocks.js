// SPDX-License-Identifier: AGPL-3.0-only
// Bounded correctness smoke test; supply identical explicit bounds for reference parity.
import http from 'k6/http';
import { check } from 'k6';
import { Trend } from 'k6/metrics';

const rpcUrl = __ENV.RPC_URL || 'http://localhost:8899';
const start = Number(__ENV.START_SLOT);
const end = Number(__ENV.END_SLOT);
if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || end < start) {
  throw new Error('START_SLOT and END_SLOT must be nonnegative safe integers with start <= end');
}
const expected = __ENV.EXPECTED_SLOTS
  ? __ENV.EXPECTED_SLOTS.split(',').map(Number) : null;
const latency = new Trend('get_blocks_latency', true);
export const options = {
  vus: 1,
  iterations: Number(__ENV.ITERATIONS || 100),
  thresholds: { checks: ['rate==1'], http_req_failed: ['rate==0'] },
};
function call(url, params) {
  const response = http.post(url, JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'getBlocks', params }), {
    headers: { 'Content-Type': 'application/json' }, timeout: '15s',
  });
  let body;
  try { body = response.json(); } catch (_) { body = {}; }
  return { response, body };
}
export default function () {
  const commitment = __ITER % 2 ? 'confirmed' : 'finalized';
  const omitted = __ENV.TEST_OMITTED_END === 'true' && __ITER % 3 === 0;
  const params = omitted ? [start, { commitment }] : [start, end, { commitment }];
  const { response, body } = call(rpcUrl, params);
  const sources = response.headers['X-Superbank-Sources'] || '';
  const path = sources.includes('clickhouse') ? 'primary_touched' : 'local';
  latency.add(response.timings.duration, { path, commitment, omitted });
  check(response, { 'getBlocks HTTP success': r => r.status === 200 });
  check(body, {
    'getBlocks returns a slot list': b => !b.error && Array.isArray(b.result),
    'slots are sorted, unique and within bounds': b => Array.isArray(b.result) && b.result.every(
      (slot, index, slots) => Number.isSafeInteger(slot) && slot >= start && (omitted || slot <= end)
        && (index === 0 || slots[index - 1] < slot)),
  });
  if (expected && !omitted) {
    check(body, { 'slot list matches fixture': b => JSON.stringify(b.result) === JSON.stringify(expected) });
  }
  if (__ENV.REFERENCE_RPC_URL && !omitted) {
    const reference = call(__ENV.REFERENCE_RPC_URL, params).body;
    check(body, { 'identical reference slot list': b => !reference.error && Array.isArray(reference.result)
      && JSON.stringify(b.result) === JSON.stringify(reference.result) });
  }
}
