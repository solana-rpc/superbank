// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

// Disk-cache performance comparison for superbank-rpc.
//
// Purpose: compare a disk-cache-enabled target against the same service with
// disk cache disabled, while proving the target requests are real disk-cache
// hits via X-Superbank-Sources. The summary reports target/reference latency,
// latency deltas, and speedup ratios per disk-served method.
//
// Usage:
//   k6 run tests/k6/scenarios/performance/superbank-rpc-disk-cache-compare.js \
//     -e RPC_URL=http://disk-enabled:8899 \
//     -e REFERENCE_RPC_URL=http://disk-disabled:8899 \
//     -e ADDRESS_FILE=tests/k6/data/pools/addresses.txt
//
// Tuning:
//   PERF_SLOT_SPAN          slots below shared tip to scan (default 512)
//   PERF_BLOCK_SAMPLES      max block slots to retain (default 32)
//   PERF_ADDRESS_SAMPLES    max addresses to probe (default 20)
//   PERF_PAGE_SIZE          address page size (default 100)
//   PERF_METHODS            comma list of method labels to include
//   PERF_ALLOW_MIXED_SOURCES=1 accepts disk-cache plus ClickHouse
//   PERF_VALIDATE_RESULTS=0 disables response parity checks
//   PERF_NORMALIZE_PROVIDER_DIFFS=0 disables live-reference normalization
//   PERF_DEBUG_MISMATCHES=1 logs mismatch metadata and result snippets

import http from 'k6/http';
import { check, fail } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { config, parseNonNegativeIntEnv, scenarios } from '../../lib/config.js';
import { initAddressPool } from '../../lib/addresses.js';
import { deepEqual } from '../../lib/compare.js';

const rpcUrl = config.rpcUrl;
const referenceUrl = config.referenceRpcUrl;
const slotSpan = parsePositiveIntEnv('PERF_SLOT_SPAN', 512);
const blockSamples = parsePositiveIntEnv('PERF_BLOCK_SAMPLES', 32);
const addressSamples = parseNonNegativeIntEnv('PERF_ADDRESS_SAMPLES', 20);
const pageSize = parsePositiveIntEnv('PERF_PAGE_SIZE', 100);
const signaturesPerBlock = parsePositiveIntEnv('PERF_SIGNATURES_PER_BLOCK', 4);
const signatureStatusBatch = parsePositiveIntEnv('PERF_SIGNATURE_STATUS_BATCH', 16);
const blocksRange = parsePositiveIntEnv('PERF_BLOCKS_RANGE', 32);
const blocksLimit = parsePositiveIntEnv('PERF_BLOCKS_LIMIT', 32);
const tipLag = parseNonNegativeIntEnv('PERF_TIP_LAG', 64);
const setupTimeout = __ENV.PERF_SETUP_TIMEOUT || '30s';
const requestTimeout = __ENV.PERF_REQUEST_TIMEOUT || '30s';
const validateResults = __ENV.PERF_VALIDATE_RESULTS !== '0';
const normalizeProviderDiffs = __ENV.PERF_NORMALIZE_PROVIDER_DIFFS !== '0';
const allowMixedSources =
  __ENV.PERF_ALLOW_MIXED_SOURCES === '1' ||
  __ENV.PERF_ALLOW_MIXED_SOURCES === 'true';
const debugMismatches =
  __ENV.PERF_DEBUG_MISMATCHES === '1' ||
  __ENV.PERF_DEBUG_MISMATCHES === 'true';
const debugMismatchesMax = parseNonNegativeIntEnv('PERF_DEBUG_MISMATCHES_MAX', 10);
const debugBodyChars = parseNonNegativeIntEnv('PERF_DEBUG_BODY_CHARS', 2000);

const methodLabels = [
  'get_block_full',
  'get_block_signatures',
  'get_block_time',
  'get_blocks',
  'get_blocks_with_limit',
  'get_transaction',
  'get_signature_statuses',
  'get_signatures_for_address',
  'get_transactions_for_address',
];

const gsfaIgnoredAddresses = new Set([
  '11111111111111111111111111111111',
  'Vote111111111111111111111111111111111111111',
  'SysvarC1ock11111111111111111111111111111111',
  'SysvarS1otHashes111111111111111111111111111',
]);

const defaultMethods = new Set(methodLabels);
const enabledMethods = parseMethodSet(__ENV.PERF_METHODS || '');
const addressMethodsEnabled =
  enabledMethods.has('get_signatures_for_address') ||
  enabledMethods.has('get_transactions_for_address');
const addressPool =
  addressMethodsEnabled && addressSamples > 0 ? initAddressPool() : [];

const metricByMethod = {
  get_block_full: makeMethodMetrics('get_block_full'),
  get_block_signatures: makeMethodMetrics('get_block_signatures'),
  get_block_time: makeMethodMetrics('get_block_time'),
  get_blocks: makeMethodMetrics('get_blocks'),
  get_blocks_with_limit: makeMethodMetrics('get_blocks_with_limit'),
  get_transaction: makeMethodMetrics('get_transaction'),
  get_signature_statuses: makeMethodMetrics('get_signature_statuses'),
  get_signatures_for_address: makeMethodMetrics('get_signatures_for_address'),
  get_transactions_for_address: makeMethodMetrics('get_transactions_for_address'),
};

const targetDiskHitRate = new Rate('diskcache_perf_target_disk_hit_rate');
const targetClickhouseTouchRate = new Rate('diskcache_perf_target_clickhouse_touch_rate');
const referenceDiskCacheRate = new Rate('diskcache_perf_reference_disk_cache_rate');
const responseParityRate = new Rate('diskcache_perf_response_parity_rate');
const targetSuccessRate = new Rate('diskcache_perf_target_success_rate');
const referenceSuccessRate = new Rate('diskcache_perf_reference_success_rate');
const comparisonsTotal = new Counter('diskcache_perf_comparisons_total');
const speedupWinsTotal = new Counter('diskcache_perf_speedup_wins_total');

let requestId = 0;
let mismatchLogs = 0;

export const options = {
  vus: scenarios.basic.vus,
  duration: scenarios.basic.duration,
  thresholds: {
    http_req_failed: [`rate<${config.thresholds.httpFailRate}`],
    diskcache_perf_target_success_rate: ['rate>=0.99'],
    diskcache_perf_reference_success_rate: ['rate>=0.99'],
    diskcache_perf_target_disk_hit_rate: [
      allowMixedSources ? 'rate>=0.99' : 'rate==1.0',
    ],
    diskcache_perf_reference_disk_cache_rate: ['rate==0.0'],
    diskcache_perf_response_parity_rate: [
      validateResults ? 'rate>=0.99' : 'rate>=0.0',
    ],
  },
};

function parsePositiveIntEnv(name, defaultValue) {
  const raw = __ENV[name];
  if (raw === undefined || raw === null || raw === '') {
    return defaultValue;
  }
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    return defaultValue;
  }
  return parsed;
}

function parseMethodSet(raw) {
  if (!raw) {
    return defaultMethods;
  }
  const requested = new Set(
    raw
      .split(',')
      .map((entry) => entry.trim())
      .filter(Boolean),
  );
  const unknown = Array.from(requested).filter((entry) => !defaultMethods.has(entry));
  if (unknown.length > 0) {
    throw new Error(`Unknown PERF_METHODS entries: ${unknown.join(', ')}`);
  }
  return requested;
}

function makeMethodMetrics(label) {
  return {
    targetLatency: new Trend(`diskcache_perf_${label}_target_ms`, true),
    referenceLatency: new Trend(`diskcache_perf_${label}_reference_ms`, true),
    delta: new Trend(`diskcache_perf_${label}_delta_ms`, true),
    speedup: new Trend(`diskcache_perf_${label}_speedup_ratio`),
    targetRequests: new Counter(`diskcache_perf_${label}_target_requests_total`),
    referenceRequests: new Counter(`diskcache_perf_${label}_reference_requests_total`),
    diskHits: new Counter(`diskcache_perf_${label}_disk_hits_total`),
    parityMismatches: new Counter(`diskcache_perf_${label}_parity_mismatches_total`),
  };
}

function getHeaderValue(headers, ...keys) {
  if (!headers) {
    return null;
  }
  for (const key of keys) {
    const value = headers[key] ?? headers[key.toLowerCase()];
    if (value !== undefined && value !== null) {
      return Array.isArray(value) ? value[0] : value;
    }
  }
  return null;
}

function sourceContains(source, token) {
  const normalized = String(source || '').toLowerCase().trim();
  if (normalized === 'all') {
    return ['head-cache', 'disk-cache', 'clickhouse'].includes(token);
  }
  return normalized
    .split(',')
    .map((entry) => entry.trim())
    .includes(token);
}

function isAcceptedDiskSource(source) {
  if (!sourceContains(source, 'disk-cache')) {
    return false;
  }
  if (!allowMixedSources && sourceContains(source, 'clickhouse')) {
    return false;
  }
  if (allowMixedSources) {
    return true;
  }
  return true;
}

function rpc(url, method, params, timeout = requestTimeout) {
  requestId += 1;
  const response = http.post(
    url,
    JSON.stringify({ jsonrpc: '2.0', id: requestId, method, params }),
    { headers: { 'Content-Type': 'application/json' }, timeout },
  );
  let body = null;
  try {
    body = JSON.parse(response.body);
  } catch (_) {
    body = null;
  }
  return {
    response,
    body,
    source: getHeaderValue(
      response.headers,
      'X-Superbank-Sources',
      'x-superbank-sources',
    ),
    duration: response.timings.duration,
    ok: response.status === 200 && body !== null && !body.error,
  };
}

function setupRpc(url, method, params) {
  return rpc(url, method, params, setupTimeout);
}

function resultForComparison(task, body) {
  const result = body?.result ?? null;
  if (task.label === 'get_transactions_for_address') {
    return result && typeof result === 'object' ? result.data ?? null : result;
  }
  return result;
}

function hasOwn(value, key) {
  return Object.prototype.hasOwnProperty.call(value, key);
}

function isTokenUiAmount(value) {
  return (
    value !== null &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    hasOwn(value, 'amount') &&
    hasOwn(value, 'decimals') &&
    hasOwn(value, 'uiAmountString') &&
    hasOwn(value, 'uiAmount')
  );
}

function normalizeProviderComparable(task, value, path = '$') {
  if (!normalizeProviderDiffs) {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((entry, idx) =>
      normalizeProviderComparable(task, entry, `${path}[${idx}]`),
    );
  }
  if (value === null || typeof value !== 'object') {
    return value;
  }

  const output = {};
  const ignoreContextSlot =
    task.label === 'get_signature_statuses' && path === '$.context';
  const ignoreTokenUiAmount = isTokenUiAmount(value);

  for (const key of Object.keys(value)) {
    if (ignoreContextSlot && key === 'slot') {
      continue;
    }
    if (ignoreTokenUiAmount && key === 'uiAmount') {
      continue;
    }
    output[key] = normalizeProviderComparable(task, value[key], `${path}.${key}`);
  }
  return output;
}

function comparableResult(task, body) {
  return normalizeProviderComparable(task, resultForComparison(task, body));
}

function valueKind(value) {
  if (value === null) {
    return 'null';
  }
  if (Array.isArray(value)) {
    return 'array';
  }
  return typeof value;
}

function firstDiff(left, right, path = '$') {
  if (left === right) {
    return null;
  }
  if (valueKind(left) !== valueKind(right)) {
    return {
      path,
      reason: 'type_mismatch',
      targetKind: valueKind(left),
      referenceKind: valueKind(right),
    };
  }
  if (Array.isArray(left)) {
    if (left.length !== right.length) {
      return {
        path,
        reason: 'array_length_mismatch',
        targetLength: left.length,
        referenceLength: right.length,
      };
    }
    for (let idx = 0; idx < left.length; idx += 1) {
      const diff = firstDiff(left[idx], right[idx], `${path}[${idx}]`);
      if (diff) {
        return diff;
      }
    }
    return null;
  }
  if (left && typeof left === 'object') {
    const leftKeys = Object.keys(left).sort();
    const rightKeys = Object.keys(right).sort();
    const leftJoined = leftKeys.join('\u0000');
    const rightJoined = rightKeys.join('\u0000');
    if (leftJoined !== rightJoined) {
      return {
        path,
        reason: 'object_keys_mismatch',
        targetOnlyKeys: leftKeys.filter((key) => !rightKeys.includes(key)).slice(0, 20),
        referenceOnlyKeys: rightKeys.filter((key) => !leftKeys.includes(key)).slice(0, 20),
      };
    }
    for (const key of leftKeys) {
      const diff = firstDiff(left[key], right[key], `${path}.${key}`);
      if (diff) {
        return diff;
      }
    }
    return null;
  }
  return {
    path,
    reason: 'value_mismatch',
    targetValue: left,
    referenceValue: right,
  };
}

function snippet(value) {
  const text = JSON.stringify(value);
  if (debugBodyChars === 0 || text.length <= debugBodyChars) {
    return text;
  }
  return `${text.slice(0, debugBodyChars)}...<truncated ${text.length - debugBodyChars} chars>`;
}

function resultsMatch(task, target, reference) {
  if (!target.body || !reference.body) {
    return false;
  }
  if (Boolean(target.body.error) || Boolean(reference.body.error)) {
    return (
      Boolean(target.body.error) === Boolean(reference.body.error) &&
      target.body.error?.code === reference.body.error?.code
    );
  }
  return deepEqual(
    comparableResult(task, target.body),
    comparableResult(task, reference.body),
  );
}

function taskKey(task) {
  return `${task.label}:${JSON.stringify(task.params)}`;
}

function addTask(tasks, seen, task, probe = null) {
  if (!enabledMethods.has(task.label)) {
    return false;
  }
  const key = taskKey(task);
  if (seen.has(key)) {
    return false;
  }
  const target = probe || setupRpc(rpcUrl, task.method, task.params);
  if (!target.ok || !isAcceptedDiskSource(target.source)) {
    return false;
  }
  seen.add(key);
  tasks.push(task);
  return true;
}

function blockOptions(transactionDetails, rewards) {
  return {
    transactionDetails,
    rewards,
    maxSupportedTransactionVersion: config.blockMaxSupportedTransactionVersion ?? 0,
    commitment: 'finalized',
  };
}

function transactionOptions() {
  return {
    encoding: config.transactionEncoding || 'json',
    maxSupportedTransactionVersion: config.maxSupportedTransactionVersion ?? 0,
    commitment: 'finalized',
  };
}

function signaturesForAddressOptions(before = null, until = null) {
  const opts = {
    limit: pageSize,
    commitment: 'finalized',
  };
  if (before) {
    opts.before = before;
  }
  if (until) {
    opts.until = until;
  }
  return opts;
}

function tfaWindowOptions(startSlot, endSlot) {
  return {
    limit: pageSize,
    transactionDetails: 'signatures',
    beforeSlot: endSlot + 1,
    untilSlot: startSlot,
  };
}

function extractSignatures(blockResult) {
  const signatures = [];
  const txs = blockResult?.transactions;
  if (!Array.isArray(txs)) {
    return signatures;
  }
  for (const tx of txs) {
    const signature = tx?.transaction?.signatures?.[0];
    if (signature) {
      signatures.push(signature);
    }
    if (signatures.length >= signaturesPerBlock) {
      break;
    }
  }
  return signatures;
}

function accountKeyToString(key) {
  if (typeof key === 'string') {
    return key;
  }
  if (key && typeof key === 'object' && typeof key.pubkey === 'string') {
    return key.pubkey;
  }
  return null;
}

function extractAddressSignaturePairs(blockResult) {
  const pairs = [];
  const txs = blockResult?.transactions;
  if (!Array.isArray(txs)) {
    return pairs;
  }

  for (const tx of txs) {
    const signature = tx?.transaction?.signatures?.[0];
    if (!signature) {
      continue;
    }

    const txAddresses = [];
    const accountKeys = tx?.transaction?.message?.accountKeys;
    if (Array.isArray(accountKeys)) {
      for (const key of accountKeys) {
        const address = accountKeyToString(key);
        if (address) {
          txAddresses.push(address);
        }
      }
    }

    const loaded = tx?.meta?.loadedAddresses;
    for (const key of loaded?.writable || []) {
      if (typeof key === 'string') {
        txAddresses.push(key);
      }
    }
    for (const key of loaded?.readonly || []) {
      if (typeof key === 'string') {
        txAddresses.push(key);
      }
    }

    const seen = new Set();
    for (const address of txAddresses) {
      if (!seen.has(address)) {
        seen.add(address);
        pairs.push({ address, signature });
      }
    }
  }
  return pairs;
}

function addAddressCandidate(candidates, seen, address) {
  if (
    typeof address !== 'string' ||
    gsfaIgnoredAddresses.has(address) ||
    seen.has(address)
  ) {
    return false;
  }
  seen.add(address);
  candidates.push(address);
  return true;
}

function addAddressSignatureHint(hints, address, signature) {
  if (
    typeof address !== 'string' ||
    gsfaIgnoredAddresses.has(address) ||
    typeof signature !== 'string'
  ) {
    return;
  }
  let signatures = hints.get(address);
  if (!signatures) {
    signatures = [];
    hints.set(address, signatures);
  }
  if (!signatures.includes(signature)) {
    signatures.push(signature);
  }
}

function addBlockTasks(tasks, seen, slot, fullProbe) {
  addTask(
    tasks,
    seen,
    {
      label: 'get_block_full',
      method: 'getBlock',
      params: [slot, blockOptions('full', true)],
    },
    fullProbe,
  );
  addTask(tasks, seen, {
    label: 'get_block_signatures',
    method: 'getBlock',
    params: [slot, blockOptions('signatures', false)],
  });
  addTask(tasks, seen, {
    label: 'get_block_time',
    method: 'getBlockTime',
    params: [slot],
  });
}

function addRangeTasks(tasks, seen, startSlot, endSlot) {
  addTask(tasks, seen, {
    label: 'get_blocks',
    method: 'getBlocks',
    params: [startSlot, endSlot],
  });
  addTask(tasks, seen, {
    label: 'get_blocks_with_limit',
    method: 'getBlocksWithLimit',
    params: [startSlot, Math.min(blocksLimit, endSlot - startSlot + 1)],
  });
}

function addSignatureTasks(tasks, seen, signatures) {
  for (const signature of signatures) {
    addTask(tasks, seen, {
      label: 'get_transaction',
      method: 'getTransaction',
      params: [signature, transactionOptions()],
    });
  }

  for (let idx = 0; idx < signatures.length; idx += signatureStatusBatch) {
    const batch = signatures.slice(idx, idx + signatureStatusBatch);
    if (batch.length === 0) {
      continue;
    }
    addTask(tasks, seen, {
      label: 'get_signature_statuses',
      method: 'getSignatureStatuses',
      params: [batch],
    });
  }
}

function addSignaturesForAddressTask(tasks, seen, address, signatureHints) {
  const signatures = signatureHints.get(address) || [];
  const before = signatures[0];
  const until = signatures.length > 1 ? signatures[signatures.length - 1] : null;
  if (!before) {
    return false;
  }

  return addTask(tasks, seen, {
    label: 'get_signatures_for_address',
    method: 'getSignaturesForAddress',
    params: [address, signaturesForAddressOptions(before, before === until ? null : until)],
  });
}

function addAddressTasks(tasks, seen, addresses, startSlot, endSlot, signatureHints) {
  for (const address of addresses.slice(0, addressSamples)) {
    addSignaturesForAddressTask(tasks, seen, address, signatureHints);
    addTask(tasks, seen, {
      label: 'get_transactions_for_address',
      method: 'getTransactionsForAddress',
      params: [address, tfaWindowOptions(startSlot, endSlot)],
    });
  }
}

export function setup() {
  if (!referenceUrl) {
    fail('REFERENCE_RPC_URL is required for disk-cache performance comparison');
  }

  const targetTip = setupRpc(rpcUrl, 'getSlot', [{ commitment: 'finalized' }]);
  const referenceTip = setupRpc(referenceUrl, 'getSlot', [{ commitment: 'finalized' }]);
  if (!targetTip.ok || !referenceTip.ok) {
    fail('could not resolve finalized tips for target and reference');
  }

  const tip = Math.min(targetTip.body.result, referenceTip.body.result) - tipLag;
  if (!Number.isFinite(tip) || tip <= 0) {
    fail(`invalid shared finalized tip after PERF_TIP_LAG=${tipLag}`);
  }
  const startSlot = Math.max(0, tip - slotSpan);
  const step = Math.max(1, Math.floor(slotSpan / blockSamples));

  const tasks = [];
  const seen = new Set();
  const blockSlots = [];
  const signatures = [];
  const signatureSet = new Set();
  const addressCandidates = [];
  const addressCandidateSet = new Set();
  const addressSignatureHints = new Map();

  for (let slot = tip; slot >= startSlot && blockSlots.length < blockSamples; slot -= step) {
    const probe = setupRpc(rpcUrl, 'getBlock', [slot, blockOptions('full', true)]);
    if (!probe.ok || !isAcceptedDiskSource(probe.source) || probe.body?.result === null) {
      continue;
    }
    blockSlots.push(slot);
    addBlockTasks(tasks, seen, slot, probe);
    for (const signature of extractSignatures(probe.body.result)) {
      if (!signatureSet.has(signature)) {
        signatureSet.add(signature);
        signatures.push(signature);
      }
    }
    if (addressMethodsEnabled) {
      for (const { address, signature } of extractAddressSignaturePairs(probe.body.result)) {
        addAddressSignatureHint(addressSignatureHints, address, signature);
      }
    }

    const rangeEnd = slot;
    const rangeStart = Math.max(startSlot, rangeEnd - blocksRange + 1);
    addRangeTasks(tasks, seen, rangeStart, rangeEnd);
  }

  addSignatureTasks(tasks, seen, signatures);
  if (addressMethodsEnabled) {
    const hintedAddresses = Array.from(addressSignatureHints.entries())
      .sort((left, right) => right[1].length - left[1].length)
      .map(([address]) => address);
    for (const address of hintedAddresses) {
      addAddressCandidate(addressCandidates, addressCandidateSet, address);
    }
    for (const address of addressPool) {
      addAddressCandidate(addressCandidates, addressCandidateSet, address);
    }
    if (addressCandidates.length > 0) {
      addAddressTasks(
        tasks,
        seen,
        addressCandidates,
        startSlot,
        tip,
        addressSignatureHints,
      );
    }
  }

  if (tasks.length === 0) {
    fail(
      'No disk-cache-hit workload items found. Ensure the target disk cache is warmed, ' +
        'RPC_URL points at the disk-enabled server, and PERF_ALLOW_MIXED_SOURCES=1 is set ' +
        'if you intentionally want to include requests that also touch ClickHouse.',
    );
  }

  const taskCounts = {};
  for (const task of tasks) {
    taskCounts[task.label] = (taskCounts[task.label] || 0) + 1;
  }

  console.log(
    `Prepared ${tasks.length} disk-cache performance tasks: ${JSON.stringify(taskCounts)}`,
  );

  return {
    tip,
    startSlot,
    taskCounts,
    tasks,
    blockSlots,
    signatureCount: signatures.length,
  };
}

function recordMetrics(task, target, reference, parityOk, diskHit) {
  const metrics = metricByMethod[task.label];
  metrics.targetRequests.add(1);
  metrics.referenceRequests.add(1);
  metrics.targetLatency.add(target.duration);
  metrics.referenceLatency.add(reference.duration);

  if (Number.isFinite(target.duration) && Number.isFinite(reference.duration)) {
    metrics.delta.add(reference.duration - target.duration);
    if (target.duration > 0) {
      const speedup = reference.duration / target.duration;
      metrics.speedup.add(speedup);
      if (speedup > 1) {
        speedupWinsTotal.add(1);
      }
    }
  }
  if (diskHit) {
    metrics.diskHits.add(1);
  }
  if (!parityOk) {
    metrics.parityMismatches.add(1);
  }
}

function maybeLogMismatch(task, target, reference, parityOk, diskHit) {
  if (!debugMismatches || mismatchLogs >= debugMismatchesMax) {
    return;
  }
  if (parityOk && diskHit && target.ok && reference.ok) {
    return;
  }
  mismatchLogs += 1;
  const targetComparable = comparableResult(task, target.body);
  const referenceComparable = comparableResult(task, reference.body);
  console.warn(
    `DISK_CACHE_PERF_MISMATCH ${JSON.stringify({
      task,
      targetStatus: target.response.status,
      referenceStatus: reference.response.status,
      targetSource: target.source,
      referenceSource: reference.source,
      targetError: target.body?.error ?? null,
      referenceError: reference.body?.error ?? null,
      parityOk,
      diskHit,
      firstDiff: firstDiff(targetComparable, referenceComparable),
      targetResult: snippet(targetComparable),
      referenceResult: snippet(referenceComparable),
    })}`,
  );
}

export default function (data) {
  const task = data.tasks[((__ITER * 997) + (__VU - 1)) % data.tasks.length];
  const targetFirst = (__ITER + __VU) % 2 === 0;
  const first = targetFirst
    ? { name: 'target', url: rpcUrl }
    : { name: 'reference', url: referenceUrl };
  const second = targetFirst
    ? { name: 'reference', url: referenceUrl }
    : { name: 'target', url: rpcUrl };

  const firstResult = rpc(first.url, task.method, task.params);
  const secondResult = rpc(second.url, task.method, task.params);
  const target = first.name === 'target' ? firstResult : secondResult;
  const reference = first.name === 'reference' ? firstResult : secondResult;

  const diskHit = isAcceptedDiskSource(target.source);
  const referenceDisk = sourceContains(reference.source, 'disk-cache');
  const parityOk = !validateResults || resultsMatch(task, target, reference);

  comparisonsTotal.add(1);
  targetDiskHitRate.add(diskHit ? 1 : 0);
  targetClickhouseTouchRate.add(sourceContains(target.source, 'clickhouse') ? 1 : 0);
  referenceDiskCacheRate.add(referenceDisk ? 1 : 0);
  responseParityRate.add(parityOk ? 1 : 0);
  targetSuccessRate.add(target.ok ? 1 : 0);
  referenceSuccessRate.add(reference.ok ? 1 : 0);
  recordMetrics(task, target, reference, parityOk, diskHit);

  check(null, {
    'target request succeeded': () => target.ok,
    'reference request succeeded': () => reference.ok,
    'target was served by disk cache': () => diskHit,
    'reference is not disk-cache enabled': () => !referenceDisk,
    'target/reference response parity': () => parityOk,
  });

  maybeLogMismatch(task, target, reference, parityOk, diskHit);
}

function trendSummary(data, name) {
  const values = data.metrics[name]?.values;
  if (!values) {
    return null;
  }
  return {
    avg: values.avg || 0,
    med: values.med || 0,
    p90: values['p(90)'] || 0,
    p95: values['p(95)'] || 0,
    p99: values['p(99)'] || 0,
    max: values.max || 0,
  };
}

function countSummary(data, name) {
  return data.metrics[name]?.values?.count || 0;
}

function rateSummary(data, name) {
  return data.metrics[name]?.values?.rate || 0;
}

function summarizeMethod(data, label) {
  const target = trendSummary(data, `diskcache_perf_${label}_target_ms`);
  const reference = trendSummary(data, `diskcache_perf_${label}_reference_ms`);
  if (!target && !reference) {
    return null;
  }
  const speedup = trendSummary(data, `diskcache_perf_${label}_speedup_ratio`);
  const delta = trendSummary(data, `diskcache_perf_${label}_delta_ms`);
  const p95Speedup =
    target?.p95 && reference?.p95 ? reference.p95 / target.p95 : 0;
  const avgSpeedup =
    target?.avg && reference?.avg ? reference.avg / target.avg : 0;
  return {
    requests: {
      target: countSummary(data, `diskcache_perf_${label}_target_requests_total`),
      reference: countSummary(data, `diskcache_perf_${label}_reference_requests_total`),
      diskHits: countSummary(data, `diskcache_perf_${label}_disk_hits_total`),
      parityMismatches: countSummary(
        data,
        `diskcache_perf_${label}_parity_mismatches_total`,
      ),
    },
    targetLatencyMs: target,
    referenceLatencyMs: reference,
    deltaMs: delta,
    speedupRatio: {
      trend: speedup,
      avgFromAverages: avgSpeedup,
      p95FromP95s: p95Speedup,
    },
  };
}

export function handleSummary(data) {
  const byMethod = {};
  for (const label of methodLabels) {
    const summary = summarizeMethod(data, label);
    if (summary) {
      byMethod[label] = summary;
    }
  }

  const summary = {
    testType: 'disk-cache-performance-compare',
    timestamp: new Date().toISOString(),
    config: {
      rpcUrl,
      referenceRpcUrl: referenceUrl,
      vus: scenarios.basic.vus,
      duration: scenarios.basic.duration,
      slotSpan,
      blockSamples,
      addressSamples,
      pageSize,
      signaturesPerBlock,
      signatureStatusBatch,
      blocksRange,
      blocksLimit,
      tipLag,
      validateResults,
      normalizeProviderDiffs,
      allowMixedSources,
      enabledMethods: Array.from(enabledMethods),
    },
    totals: {
      comparisons: countSummary(data, 'diskcache_perf_comparisons_total'),
      speedupWins: countSummary(data, 'diskcache_perf_speedup_wins_total'),
      targetDiskHitRate: rateSummary(data, 'diskcache_perf_target_disk_hit_rate'),
      targetClickhouseTouchRate: rateSummary(
        data,
        'diskcache_perf_target_clickhouse_touch_rate',
      ),
      referenceDiskCacheRate: rateSummary(
        data,
        'diskcache_perf_reference_disk_cache_rate',
      ),
      responseParityRate: rateSummary(data, 'diskcache_perf_response_parity_rate'),
      targetSuccessRate: rateSummary(data, 'diskcache_perf_target_success_rate'),
      referenceSuccessRate: rateSummary(
        data,
        'diskcache_perf_reference_success_rate',
      ),
    },
    byMethod,
  };

  return {
    stdout: JSON.stringify(summary, null, 2) + '\n',
  };
}
