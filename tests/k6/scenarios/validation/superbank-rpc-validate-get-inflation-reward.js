// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

// Validation test for superbank-rpc getInflationReward
//
// Purpose: Compare Superbank RPC responses against a reference RPC.
//
// Usage:
//   k6 run tests/k6/scenarios/validation/superbank-rpc-validate-get-inflation-reward.js \
//     -e RPC_URL=http://localhost:8899 \
//     -e REFERENCE_RPC_URL=https://api.mainnet-beta.solana.com \
//     -e ADDRESS_FILE=./tests/k6/data/pools/addresses.txt \
//     -e INFLATION_REWARD_EPOCH=760

import { check } from 'k6';
import { config, scenarios } from '../../lib/config.js';
import { initAddressPool } from '../../lib/addresses.js';
import { deepEqual, summarizeJson } from '../../lib/compare.js';
import {
  executeRequest,
  makeGetInflationRewardRequest,
  makeGetSlotRequest,
} from '../../lib/rpc.js';
import {
  addDownstreamMetrics,
  collectJsonrpcErrorCodeCounts,
  makeJsonrpcErrorCodeThresholds,
} from '../../lib/summary.js';

if (!config.referenceRpcUrl) {
  throw new Error('REFERENCE_RPC_URL is required for validation tests.');
}

const addressPool = initAddressPool();
const logMismatches = __ENV.VALIDATION_LOG_MISMATCHES !== '0';

export const options = {
  vus: scenarios.basic.vus,
  duration: scenarios.basic.duration,
  thresholds: {
    http_req_failed: ['rate==0.0'],
    checks: ['rate==1.0'],
    ...makeJsonrpcErrorCodeThresholds(),
  },
};

function normalizeInflationRewardResponse(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    return body;
  }
  if (!Array.isArray(body.result)) {
    return body;
  }

  const normalizedResult = body.result.map((entry) => {
    if (entry === null || typeof entry !== 'object' || Array.isArray(entry)) {
      return entry;
    }

    return {
      epoch: entry.epoch,
      effectiveSlot: entry.effectiveSlot,
      amount: entry.amount,
      postBalance: entry.postBalance,
      commission:
        Object.prototype.hasOwnProperty.call(entry, 'commission') &&
        entry.commission !== undefined
          ? entry.commission
          : null,
      commissionBps:
        Object.prototype.hasOwnProperty.call(entry, 'commissionBps') &&
        entry.commissionBps !== undefined
          ? entry.commissionBps
          : null,
    };
  });

  return {
    ...body,
    result: normalizedResult,
  };
}

function pickAddresses(count) {
  const target = Math.max(1, Math.min(count, addressPool.length));
  const picked = [];
  const seen = new Set();
  while (picked.length < target) {
    const address = addressPool[Math.floor(Math.random() * addressPool.length)];
    if (seen.has(address)) {
      continue;
    }
    seen.add(address);
    picked.push(address);
  }
  return picked;
}

function resolveEpoch(requestId) {
  if (config.inflationRewardEpoch !== null) {
    return config.inflationRewardEpoch;
  }

  const slotPayload = makeGetSlotRequest(
    { commitment: config.inflationRewardCommitment },
    requestId
  );
  const slotResponse = executeRequest(slotPayload, {
    rpcUrl: config.rpcUrl,
    recordMetrics: false,
  });

  if (
    slotResponse.response.status !== 200 ||
    !slotResponse.body ||
    slotResponse.body.error ||
    typeof slotResponse.body.result !== 'number'
  ) {
    return null;
  }

  const currentEpoch = Math.floor(
    slotResponse.body.result / config.slotSlotsPerEpoch
  );
  return Math.max(0, currentEpoch - 1);
}

function buildOptions(epoch) {
  const options = {
    commitment: config.inflationRewardCommitment,
  };
  if (epoch !== null && epoch !== undefined) {
    options.epoch = epoch;
  }
  if (config.inflationRewardMinContextSlot !== null) {
    options.minContextSlot = config.inflationRewardMinContextSlot;
  }
  return options;
}

export default function () {
  const requestId = Math.floor(Math.random() * 1_000_000_000);
  const epoch = resolveEpoch(requestId);
  const addresses = pickAddresses(config.inflationRewardAddressCount);

  const preconditions = check(null, {
    'address pool is not empty': () => addressPool.length > 0,
    'epoch resolved': () => epoch !== null && epoch !== undefined,
  });

  if (!preconditions) {
    return;
  }

  const payload = makeGetInflationRewardRequest(
    addresses,
    buildOptions(epoch),
    requestId + 1
  );

  const superbank = executeRequest(payload, { rpcUrl: config.rpcUrl });
  const reference = executeRequest(payload, {
    rpcUrl: config.referenceRpcUrl,
    recordMetrics: false,
  });

  const basicChecks = check(null, {
    'superbank status is 200': () => superbank.response.status === 200,
    'reference status is 200': () => reference.response.status === 200,
    'superbank response is json': () => superbank.body !== null,
    'reference response is json': () => reference.body !== null,
  });

  let match = false;
  if (basicChecks) {
    const normalizedSuperbank = normalizeInflationRewardResponse(superbank.body);
    const normalizedReference = normalizeInflationRewardResponse(reference.body);
    match = deepEqual(normalizedSuperbank, normalizedReference);
  }

  if (!match && basicChecks && logMismatches) {
    console.error(
      `Response mismatch for epoch ${epoch} (vu ${__VU}, iter ${__ITER}, addresses=${addresses.length})`
    );
    console.error(`Superbank: ${summarizeJson(superbank.body)}`);
    console.error(`Reference: ${summarizeJson(reference.body)}`);
  }

  check(null, {
    'responses match': () => match,
  });
}

export function handleSummary(data) {
  const jsonrpcCodes = collectJsonrpcErrorCodeCounts(data);
  const summary = {
    testType: 'validate-get-inflation-reward',
    timestamp: new Date().toISOString(),
    config: {
      rpcUrl: config.rpcUrl,
      referenceRpcUrl: config.referenceRpcUrl,
      vus: scenarios.basic.vus,
      duration: scenarios.basic.duration,
      addressPoolSize: addressPool.length,
      addressCount: config.inflationRewardAddressCount,
      epoch: config.inflationRewardEpoch,
      commitment: config.inflationRewardCommitment,
      minContextSlot: config.inflationRewardMinContextSlot,
    },
    metrics: {
      checks: {
        rate: data.metrics.checks?.values?.rate || 0,
        passed: data.metrics.checks?.values?.passes || 0,
        failed: data.metrics.checks?.values?.fails || 0,
      },
      requests: {
        total: data.metrics.rpc_requests_total?.values?.count || 0,
        successful: data.metrics.rpc_requests_success?.values?.count || 0,
        failed: data.metrics.http_req_failed?.values?.passes || 0,
      },
      errors: {
        http: data.metrics.rpc_errors_http?.values?.count || 0,
        rpc: data.metrics.rpc_errors_rpc?.values?.count || 0,
        timeout: data.metrics.rpc_errors_timeout?.values?.count || 0,
        jsonrpcTotal: jsonrpcCodes.total,
        jsonrpcUntracked: jsonrpcCodes.untracked,
        jsonrpcByCode: jsonrpcCodes.byCode,
      },
    },
  };

  addDownstreamMetrics(data, summary.metrics);

  return {
    stdout: JSON.stringify(summary, null, 2) + '\n',
  };
}
