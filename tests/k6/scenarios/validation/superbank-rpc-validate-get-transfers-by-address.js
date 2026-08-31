// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

// Validation + latency comparison test for superbank-rpc getTransfersByAddress.
//
// Purpose: Compare two endpoints that both implement getTransfersByAddress,
// validate response parity, and report which one is faster.
//
// Usage:
//   k6 run tests/k6/scenarios/validation/superbank-rpc-validate-get-transfers-by-address.js \
//     -e RPC_URL=http://localhost:8899 \
//     -e REFERENCE_RPC_URL=http://localhost:8898 \
//     -e ADDRESS_FILE=./tests/k6/data/pools/addresses.txt

import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { config, scenarios, transfersByAddressOptions } from '../../lib/config.js';
import { initAddressPool, randomAddress } from '../../lib/addresses.js';
import { deepEqualWithLogSuperset, summarizeJson } from '../../lib/compare.js';
import { executeRequest, makeGetTransfersByAddressRequest } from '../../lib/rpc.js';

if (!config.referenceRpcUrl) {
  throw new Error('REFERENCE_RPC_URL is required for validation tests.');
}

const addressPool = initAddressPool();
const logMismatches = __ENV.VALIDATION_LOG_MISMATCHES !== '0';

const primaryLatency = new Trend('transfers_compare_primary_latency_ms', true);
const referenceLatency = new Trend('transfers_compare_reference_latency_ms', true);
const latencyDelta = new Trend('transfers_compare_latency_delta_ms', true);

const primaryRequests = new Counter('transfers_compare_primary_requests_total');
const primarySuccessful = new Counter('transfers_compare_primary_success_total');
const primaryErrors = new Counter('transfers_compare_primary_errors_total');
const referenceRequests = new Counter('transfers_compare_reference_requests_total');
const referenceSuccessful = new Counter('transfers_compare_reference_success_total');
const referenceErrors = new Counter('transfers_compare_reference_errors_total');

const comparisonsCompared = new Counter('transfers_compare_compared_total');
const comparisonsSkipped = new Counter('transfers_compare_skipped_total');
const comparisonsMatched = new Counter('transfers_compare_matches_total');
const comparisonsMismatched = new Counter('transfers_compare_mismatches_total');
const comparisonMatchRate = new Rate('transfers_compare_match_rate');
const primaryFaster = new Counter('transfers_compare_primary_faster_total');
const referenceFaster = new Counter('transfers_compare_reference_faster_total');
const comparisonTies = new Counter('transfers_compare_ties_total');

export const options = {
  vus: scenarios.basic.vus,
  duration: scenarios.basic.duration,
  thresholds: {
    http_req_failed: ['rate==0.0'],
    checks: ['rate==1.0'],
  },
};

function recordEndpoint(kind, result) {
  const latency = kind === 'primary' ? primaryLatency : referenceLatency;
  const requests = kind === 'primary' ? primaryRequests : referenceRequests;
  const successful = kind === 'primary' ? primarySuccessful : referenceSuccessful;
  const errors = kind === 'primary' ? primaryErrors : referenceErrors;

  requests.add(1);
  const duration = result?.response?.timings?.duration;
  if (typeof duration === 'number' && Number.isFinite(duration)) {
    latency.add(duration);
  }

  if (result?.response?.status === 200 && result.body && !result.body.error) {
    successful.add(1);
  } else {
    errors.add(1);
  }
}

function recordLatencyComparison(primary, reference) {
  const primaryDuration = primary?.response?.timings?.duration;
  const referenceDuration = reference?.response?.timings?.duration;
  if (
    typeof primaryDuration !== 'number' ||
    typeof referenceDuration !== 'number' ||
    !Number.isFinite(primaryDuration) ||
    !Number.isFinite(referenceDuration)
  ) {
    return;
  }

  const delta = primaryDuration - referenceDuration;
  latencyDelta.add(delta);
  if (delta < 0) {
    primaryFaster.add(1);
  } else if (delta > 0) {
    referenceFaster.add(1);
  } else {
    comparisonTies.add(1);
  }
}

function maybeLogMismatch(address, primaryBody, referenceBody) {
  if (!logMismatches) {
    return;
  }
  console.error(`getTransfersByAddress mismatch for ${address} (vu ${__VU}, iter ${__ITER})`);
  console.error(`Primary: ${summarizeJson(primaryBody)}`);
  console.error(`Reference: ${summarizeJson(referenceBody)}`);
}

function summarizeTrend(data, metricName) {
  const values = data.metrics[metricName]?.values;
  return {
    avg: values?.avg || 0,
    p95: values?.['p(95)'] || 0,
    min: values?.min || 0,
    max: values?.max || 0,
  };
}

function summarizeCount(data, metricName) {
  return data.metrics[metricName]?.values?.count || 0;
}

export default function () {
  const address = randomAddress();
  const requestId = Math.floor(Math.random() * 1_000_000_000);
  const payload = makeGetTransfersByAddressRequest(
    address,
    transfersByAddressOptions(),
    requestId
  );

  const primaryFirst = (__ITER + __VU) % 2 === 0;
  const first = primaryFirst
    ? { kind: 'primary', rpcUrl: config.rpcUrl }
    : { kind: 'reference', rpcUrl: config.referenceRpcUrl };
  const second = primaryFirst
    ? { kind: 'reference', rpcUrl: config.referenceRpcUrl }
    : { kind: 'primary', rpcUrl: config.rpcUrl };

  const firstResult = executeRequest(payload, {
    rpcUrl: first.rpcUrl,
    recordMetrics: false,
  });
  recordEndpoint(first.kind, firstResult);

  const secondResult = executeRequest(payload, {
    rpcUrl: second.rpcUrl,
    recordMetrics: false,
  });
  recordEndpoint(second.kind, secondResult);

  const primary = first.kind === 'primary' ? firstResult : secondResult;
  const reference = first.kind === 'reference' ? firstResult : secondResult;
  const basicChecks = check(null, {
    'primary status is 200': () => primary.response.status === 200,
    'reference status is 200': () => reference.response.status === 200,
    'primary response is json': () => primary.body !== null,
    'reference response is json': () => reference.body !== null,
    'primary has no rpc error': () => primary.body && !primary.body.error,
    'reference has no rpc error': () => reference.body && !reference.body.error,
  });

  if (!basicChecks) {
    comparisonsSkipped.add(1);
    maybeLogMismatch(address, primary.body, reference.body);
    return;
  }

  recordLatencyComparison(primary, reference);
  comparisonsCompared.add(1);
  const match = deepEqualWithLogSuperset(primary.body, reference.body);
  comparisonMatchRate.add(match);
  if (match) {
    comparisonsMatched.add(1);
  } else {
    comparisonsMismatched.add(1);
    maybeLogMismatch(address, primary.body, reference.body);
  }

  check(null, {
    'responses match': () => match,
  });
}

export function handleSummary(data) {
  return {
    stdout:
      JSON.stringify(
        {
          testType: 'validate-get-transfers-by-address',
          timestamp: new Date().toISOString(),
          config: {
            rpcUrl: config.rpcUrl,
            referenceRpcUrl: config.referenceRpcUrl,
            vus: scenarios.basic.vus,
            duration: scenarios.basic.duration,
            addressPoolSize: addressPool.length,
            sortOrder: config.transfersByAddressSortOrder,
            limit: config.transfersByAddressLimit,
            commitment: config.transfersByAddressCommitment,
            solMode: config.transfersByAddressSolMode,
            minContextSlot: config.transfersByAddressMinContextSlot,
            paginationToken: config.transfersByAddressPaginationToken,
            direction: config.transfersByAddressDirection,
            mint: config.transfersByAddressMint,
            with: config.transfersByAddressWith,
            amountGte: config.transfersByAddressAmountGte,
            amountLte: config.transfersByAddressAmountLte,
          },
          metrics: {
            primary: {
              requests: summarizeCount(data, 'transfers_compare_primary_requests_total'),
              successful: summarizeCount(data, 'transfers_compare_primary_success_total'),
              errors: summarizeCount(data, 'transfers_compare_primary_errors_total'),
              latencyMs: summarizeTrend(data, 'transfers_compare_primary_latency_ms'),
            },
            reference: {
              requests: summarizeCount(data, 'transfers_compare_reference_requests_total'),
              successful: summarizeCount(data, 'transfers_compare_reference_success_total'),
              errors: summarizeCount(data, 'transfers_compare_reference_errors_total'),
              latencyMs: summarizeTrend(data, 'transfers_compare_reference_latency_ms'),
            },
            comparison: {
              compared: summarizeCount(data, 'transfers_compare_compared_total'),
              skipped: summarizeCount(data, 'transfers_compare_skipped_total'),
              matches: summarizeCount(data, 'transfers_compare_matches_total'),
              mismatches: summarizeCount(data, 'transfers_compare_mismatches_total'),
              matchRate: data.metrics.transfers_compare_match_rate?.values?.rate || 0,
              primaryFaster: summarizeCount(data, 'transfers_compare_primary_faster_total'),
              referenceFaster: summarizeCount(data, 'transfers_compare_reference_faster_total'),
              ties: summarizeCount(data, 'transfers_compare_ties_total'),
              latencyDeltaMs: summarizeTrend(data, 'transfers_compare_latency_delta_ms'),
            },
          },
        },
        null,
        2
      ) + '\n',
  };
}
