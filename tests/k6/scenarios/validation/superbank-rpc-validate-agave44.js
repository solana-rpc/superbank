// SPDX-License-Identifier: AGPL-3.0-only
// Requires a populated target context; optionally checks a pinned Agave reference.
import http from 'k6/http';
import { check, fail } from 'k6';

export const options = { vus: 1, iterations: 1, thresholds: { checks: ['rate==1'], http_req_failed: ['rate==0'] } };
const target = __ENV.RPC_URL || 'http://localhost:8899';
const reference = __ENV.REFERENCE_RPC_URL;
const signature = '99eUso3aSbE9tqGSTXzo3TLfKb9RkMTURrHKQ1K7Zh3BbeqPevr5E1iCbpTjqHuTFLtfxTTD5ekfVuZFzQyEQf8';
const future = Number.MAX_SAFE_INTEGER;

function rpc(endpoint, method, params) {
  const response = http.post(endpoint, JSON.stringify({ jsonrpc: '2.0', id: 44, method, params }), {
    headers: { 'Content-Type': 'application/json' }, timeout: '15s',
  });
  if (!check(response, { 'HTTP 200': (r) => r.status === 200 })) fail('RPC HTTP failure');
  let body;
  try { body = response.json(); } catch (_) {
    check(response, { 'JSON body': () => false });
    fail('Invalid JSON');
  }
  if (!check(body, { 'JSON-RPC envelope': (b) => b?.jsonrpc === '2.0' && b.id === 44 })) fail('Invalid envelope');
  return body;
}

export function setup() {
  if (!reference) return;
  const body = rpc(reference, 'getVersion', []);
  if (!check(body, { 'Agave 4.4.0 reference': (b) => b.result?.['solana-core'] === '4.4.0' })) {
    fail('Reference must report solana-core 4.4.0');
  }
}

function minimumError(endpoint, method, params) {
  const body = rpc(endpoint, method, params);
  check(body, {
    'minimum context error': (b) => b.error?.code === -32016,
    'minimum context message': (b) => b.error?.message === 'Minimum context slot has not been reached',
    'context below requested minimum': (b) => Number.isSafeInteger(b.error?.data?.contextSlot) && b.error.data.contextSlot < future,
    'no result on error': (b) => !('result' in b),
  });
}

export default function () {
  for (const endpoint of reference ? [target, reference] : [target]) {
    for (const commitment of ['confirmed', 'finalized']) {
      minimumError(endpoint, 'getTransaction', [signature, { commitment, minContextSlot: future }]);
    }
    for (const commitment of ['processed', 'confirmed', 'finalized']) {
      for (const searchTransactionHistory of [false, true]) {
        minimumError(endpoint, 'getSignatureStatuses', [[signature], { commitment, searchTransactionHistory, minContextSlot: future }]);
      }
      const body = rpc(endpoint, 'getSignatureStatuses', [[signature], { commitment, minContextSlot: 0 }]);
      check(body, {
        'satisfied minimum succeeds': (b) => !b.error && Number.isSafeInteger(b.result?.context?.slot),
        'one status per signature': (b) => b.result?.value?.length === 1,
      });
    }
    minimumError(endpoint, 'getSignatureStatuses', [[signature], { minContextSlot: future }]);
  }
}
