// SPDX-License-Identifier: AGPL-3.0-only
// Replay a fixed, known-absent corpus. Client timeouts do not prove server cancellation.
import http from 'k6/http';
import exec from 'k6/execution';
import { SharedArray } from 'k6/data';
import { Counter, Rate, Trend } from 'k6/metrics';
import { resolveOpenPath } from '../../lib/path.js';

function positiveInteger(name, fallback) {
  const value = Number(__ENV[name] || fallback);
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}

const batchSize = positiveInteger('MISS_BATCH_SIZE', 256);
const rate = positiveInteger('MISS_RPS', 2);
const timeoutMs = positiveInteger('MISS_TIMEOUT_MS', 1000);
const duration = __ENV.MISS_DURATION || '30m';
const runLabel = __ENV.MISS_RUN_LABEL || 'unspecified';
const rpcUrl = __ENV.RPC_URL || 'http://localhost:8899';
if (batchSize > 256) throw new Error('MISS_BATCH_SIZE must be at most 256');
if (!__ENV.MISS_SIGNATURE_FILE) throw new Error('MISS_SIGNATURE_FILE is required');

const signatures = new SharedArray('known absent signatures', () => {
  const lines = open(resolveOpenPath(__ENV.MISS_SIGNATURE_FILE)).trim().split(/\s+/);
  if (lines.length < batchSize || new Set(lines).size !== lines.length) {
    throw new Error('The corpus must contain at least one batch of distinct signatures');
  }
  if (lines.some((signature) => !/^[1-9A-HJ-NP-Za-km-z]{64,88}$/.test(signature))) {
    throw new Error('The corpus must contain base58 signatures, one per line');
  }
  return lines;
});

const requests = new Counter('status_miss_requests');
const completedMisses = new Rate('status_miss_completed');
const clientTimeouts = new Counter('status_miss_client_timeouts');
const unexpectedResponses = new Counter('status_miss_unexpected_responses');
const clientElapsed = new Trend('status_miss_client_elapsed_ms', true);

export const options = {
  scenarios: {
    status_misses: {
      executor: 'constant-arrival-rate',
      rate,
      timeUnit: '1s',
      duration,
      preAllocatedVUs: positiveInteger('MISS_VUS', Math.ceil(rate * timeoutMs / 1000) + 2),
      gracefulStop: `${timeoutMs + 1000}ms`,
    },
  },
  tags: { workload: 'signature_statuses_misses', run: runLabel },
  thresholds: {
    dropped_iterations: ['count==0'],
    status_miss_requests: ['count>0'],
    status_miss_unexpected_responses: ['count==0'],
  },
};

function isCompleteMiss(response, requestId) {
  if (response.status !== 200) return false;
  try {
    const body = response.json();
    return body.jsonrpc === '2.0' && body.id === requestId && !body.error
      && Array.isArray(body.result?.value) && body.result.value.length === batchSize
      && body.result.value.every((status) => status === null);
  } catch (_) {
    return false;
  }
}

export default function () {
  const iteration = exec.scenario.iterationInTest;
  const offset = iteration * batchSize;
  if (offset + batchSize > signatures.length) {
    exec.test.abort('Miss corpus exhausted; supply enough distinct signatures for the entire run');
    return;
  }
  const batch = Array.from({ length: batchSize }, (_, index) => signatures[offset + index]);
  const requestId = `${runLabel}:${iteration}`;
  const started = Date.now();
  const response = http.post(rpcUrl, JSON.stringify({
    jsonrpc: '2.0', id: requestId, method: 'getSignatureStatuses',
    params: [batch, { searchTransactionHistory: true }],
  }), {
    timeout: `${timeoutMs}ms`,
    redirects: 0,
    headers: { 'Content-Type': 'application/json' },
    tags: { name: 'getSignatureStatuses known misses' },
  });
  const timedOut = response.error_code === 1050;
  const complete = isCompleteMiss(response, requestId);
  requests.add(1);
  clientElapsed.add(Date.now() - started);
  completedMisses.add(complete);
  clientTimeouts.add(timedOut ? 1 : 0);
  unexpectedResponses.add(!complete && !timedOut ? 1 : 0);
}

export function handleSummary(data) {
  return { stdout: JSON.stringify({
    workload: 'signature_statuses_misses', run: runLabel,
    batchSize, corpusSize: signatures.length, rate, duration, timeoutMs,
    cancellationVerified: false,
    note: 'Timeouts are expected observations; inspect coordinator and leaf telemetry separately.',
    metrics: data.metrics,
  }, null, 2) + '\n' };
}
