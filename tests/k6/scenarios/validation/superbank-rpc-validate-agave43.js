// SPDX-License-Identifier: AGPL-3.0-only
// Request-level Agave 4.3 errors; no chain data or reference endpoint required.
// Set INFLATION_REWARD_MAX_ADDRESSES to the server's configured nonzero limit.
import http from 'k6/http';
import { check, fail } from 'k6';

export const options = { vus: 1, iterations: 1, thresholds: { checks: ['rate==1'], http_req_failed: ['rate==0'] } };
const url = __ENV.RPC_URL || 'http://localhost:8899';
const limit = Number(__ENV.INFLATION_REWARD_MAX_ADDRESSES || 100);
const reference = __ENV.AGAVE43_REFERENCE_RPC_URL;
if (!Number.isSafeInteger(limit) || limit < 0 || limit > 100000) {
  throw new Error('INFLATION_REWARD_MAX_ADDRESSES must be an integer from 0 to 100000');
}
const encodingError = 'base58 encoding is not supported with maxSupportedTransactionVersion >= 1';

function rpc(endpoint, method, params) {
  const response = http.post(endpoint, JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }), {
    headers: { 'Content-Type': 'application/json' }, timeout: '15s',
  });
  if (!check(response, { 'HTTP 200': (r) => r.status === 200 })) fail('RPC HTTP failure');
  let body;
  try { body = response.json(); } catch (_) { fail('RPC returned invalid JSON'); }
  if (!check(body, { 'JSON-RPC object': (b) => b !== null && typeof b === 'object' && !Array.isArray(b) })) {
    fail('RPC returned an invalid envelope');
  }
  return body;
}

export function setup() {
  if (!reference) return;
  const body = rpc(reference, 'getVersion', []);
  const version = body.result?.['solana-core'];
  if (body.error || !/^4\.3\.\d+(?:[-+].*)?$/.test(version || '')) {
    fail('Reference must report solana-core 4.3.x; verify the endpoint before running parity checks');
  }
  console.log(`Agave reference reports solana-core ${version}`);
}

function assertError(endpoint, method, params, message) {
  const body = rpc(endpoint, method, params);
  check(body, {
    'invalid params': (b) => b.error?.code === -32602,
    'exact Agave message': (b) => b.error?.message === message,
    'no error data': (b) => b.error && !('data' in b.error),
    'id preserved': (b) => b.id === 1,
    'JSON-RPC 2.0': (b) => b.jsonrpc === '2.0',
    'no result on error': (b) => !('result' in b),
  });
}

function assertStandardError(method, params, message) {
  assertError(url, method, params, message);
  if (reference) assertError(reference, method, params, message);
}

export default function () {
  for (const encoding of ['base58', 'binary']) {
    const config = { encoding, maxSupportedTransactionVersion: 1 };
    assertStandardError('getTransaction', ['99eUso3aSbE9tqGSTXzo3TLfKb9RkMTURrHKQ1K7Zh3BbeqPevr5E1iCbpTjqHuTFLtfxTTD5ekfVuZFzQyEQf8', config], encodingError);
    for (const transactionDetails of ['full', 'accounts', 'signatures', 'none']) {
      assertStandardError('getBlock', [42, { ...config, transactionDetails }], encodingError);
    }
  }
  assertError(url, 'getTransactionsForAddress', ['1'.repeat(32), {
    encoding: 'base58', maxSupportedTransactionVersion: 1,
  }], encodingError);
  if (reference) {
    assertError(reference, 'getInflationReward', [Array(33).fill('invalid')], 'Too many inputs provided; max 32');
  }
  if (limit > 0) {
    assertError(url, 'getInflationReward', [Array(limit + 1).fill('invalid')], `Too many inputs provided; max ${limit}`);
  }
}
