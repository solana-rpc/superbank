// SPDX-License-Identifier: AGPL-3.0-only
// Compare complete getTransaction JSON-RPC envelopes against an authoritative
// reference. SIGNATURE_FILE must contain known-present signatures, including
// each transaction version being validated. This scenario generates real load.
import http from 'k6/http';
import { check, fail } from 'k6';
import { config } from '../../lib/config.js';
import { initSignaturePool, generateRandomSignature } from '../../lib/signatures.js';
import { deepEqual } from '../../lib/compare.js';

const signatures = initSignaturePool();
export const options = {
  vus: 1,
  iterations: 1,
  thresholds: { checks: ['rate==1'], http_req_failed: ['rate==0'] },
};

function rpc(url, signature, options, id) {
  const response = http.post(url, JSON.stringify({
    jsonrpc: '2.0', id, method: 'getTransaction', params: [signature, options],
  }), { headers: { 'Content-Type': 'application/json' }, timeout: '10s' });
  if (response.status !== 200) fail(`HTTP ${response.status}`);
  return response.json();
}

function compare(signature, options, id) {
  const reference = rpc(config.referenceRpcUrl, signature, options, id);
  const target = rpc(config.rpcUrl, signature, options, id);
  if (!check(target, { 'complete JSON-RPC envelope parity': value => deepEqual(value, reference) })) {
    fail(`getTransaction parity failed for options ${JSON.stringify(options)}`);
  }
  return reference;
}

export default function () {
  if (!config.referenceRpcUrl || !config.signatureFile) {
    fail('REFERENCE_RPC_URL and a known-present SIGNATURE_FILE are required');
  }
  const absent = generateRandomSignature();
  const missing = compare(absent, { maxSupportedTransactionVersion: 1 }, 'absent');
  check(missing, { 'absent signature is null': value => value.result === null && !value.error });
  let index = 0;
  for (const signature of signatures) {
    const known = rpc(config.referenceRpcUrl, signature, { maxSupportedTransactionVersion: 1 }, 'fixture');
    if (!check(known, { 'fixture transaction exists': value => value.result?.transaction != null })) {
      fail('The signature corpus must contain known-present transactions');
    }
    for (const encoding of ['json', 'jsonParsed', 'base58', 'base64']) {
      for (const commitment of ['confirmed', 'finalized']) {
        for (const version of [undefined, 0, 1]) {
          const id = [null, 'parity', index][index % 3];
          index += 1;
          compare(signature, { encoding, commitment, maxSupportedTransactionVersion: version }, id);
        }
      }
    }
    compare(signature, { slot: known.result.slot, maxSupportedTransactionVersion: 1 }, 'pinned');
    const mismatch = compare(signature, { slot: known.result.slot + 1, maxSupportedTransactionVersion: 1 }, 'mismatch');
    check(mismatch, { 'slot mismatch is null': value => value.result === null && !value.error });
  }
}
