// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

// Stress test for the getInflationReward admission and ClickHouse resource guards.
// An explicit historical epoch is required so this scenario does not add getSlot load.
//
// Usage:
//   k6 run tests/k6/scenarios/stress/stress-test-get-inflation-reward.js \
//     -e RPC_URL=http://localhost:8899 \
//     -e ADDRESS_FILE=./tests/k6/data/pools/addresses.txt \
//     -e INFLATION_REWARD_EPOCH=280 \
//     -e INFLATION_REWARD_ADDRESS_COUNT=18

import { check } from 'k6';
import { Trend } from 'k6/metrics';
import { config, scenarios } from '../../lib/config.js';
import { initAddressPool } from '../../lib/addresses.js';
import { executeRequest, makeGetInflationRewardRequest } from '../../lib/rpc.js';
import {
  addDownstreamMetrics,
  collectJsonrpcErrorCodeCounts,
} from '../../lib/summary.js';

if (config.inflationRewardEpoch === null) {
  throw new Error('INFLATION_REWARD_EPOCH is required for this stress scenario.');
}

const addressPool = initAddressPool();
if (addressPool.length === 0) {
  throw new Error('The configured address pool is empty.');
}

const addressCount = Math.min(
  config.inflationRewardAddressCount,
  addressPool.length
);
const inflationRewardLatency = new Trend(
  'rpc_getInflationReward_latency',
  true
);

export const options = {
  scenarios: {
    stress: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: scenarios.stress.stages,
      gracefulRampDown: '30s',
    },
  },
  thresholds: {
    http_req_failed: ['rate==0.0'],
    checks: ['rate==1.0'],
    rpc_getInflationReward_latency: ['p(95)<5000'],
  },
};

function addressesForVu() {
  const start = (__VU * addressCount) % addressPool.length;
  const addresses = [];
  for (let offset = 0; offset < addressCount; offset += 1) {
    addresses.push(addressPool[(start + offset) % addressPool.length]);
  }
  return addresses;
}

export default function () {
  const payload = makeGetInflationRewardRequest(
    addressesForVu(),
    {
      epoch: config.inflationRewardEpoch,
      commitment: config.inflationRewardCommitment,
    }
  );
  const result = executeRequest(payload, {
    rpcUrl: config.rpcUrl,
    latencyMetric: inflationRewardLatency,
  });
  const errorCode = result.body?.error?.code;

  check(result, {
    'status is 200': ({ response }) => response.status === 200,
    'request succeeds or is explicitly shed': () =>
      errorCode === undefined || errorCode === -32005,
  });
}

export function handleSummary(data) {
  const jsonrpcCodes = collectJsonrpcErrorCodeCounts(data);
  const summary = {
    testType: 'stress-get-inflation-reward',
    timestamp: new Date().toISOString(),
    config: {
      rpcUrl: config.rpcUrl,
      epoch: config.inflationRewardEpoch,
      addressCount,
      commitment: config.inflationRewardCommitment,
    },
    metrics: {
      requests: {
        total: data.metrics.rpc_requests_total?.values?.count || 0,
        successful: data.metrics.rpc_requests_success?.values?.count || 0,
      },
      latency: {
        p95:
          data.metrics.rpc_getInflationReward_latency?.values['p(95)'] || 0,
        p99:
          data.metrics.rpc_getInflationReward_latency?.values['p(99)'] || 0,
        max: data.metrics.rpc_getInflationReward_latency?.values?.max || 0,
      },
      errors: {
        jsonrpcTotal: jsonrpcCodes.total,
        jsonrpcByCode: jsonrpcCodes.byCode,
      },
    },
  };

  addDownstreamMetrics(data, summary.metrics);
  return { stdout: JSON.stringify(summary, null, 2) + '\n' };
}
