// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

// Basic load test for superbank-rpc getEpochInfo.

import { check } from 'k6';

import { config, epochInfoOptions, scenarios } from '../../lib/config.js';
import { getEpochInfo } from '../../lib/rpc.js';
import { addDownstreamMetrics } from '../../lib/summary.js';

export const options = {
  vus: scenarios.basic.vus,
  duration: scenarios.basic.duration,
  thresholds: {
    http_req_failed: [`rate<${config.thresholds.httpFailRate}`],
    rpc_getEpochInfo_latency: [`p(95)<${config.thresholds.p95Latency}`],
  },
};

export default function () {
  const { body } = getEpochInfo(epochInfoOptions());
  check(body, {
    'epoch info has a result': (value) => value && value.result,
    'epoch info has numeric slot fields': (value) =>
      value &&
      value.result &&
      Number.isInteger(value.result.absoluteSlot) &&
      Number.isInteger(value.result.blockHeight) &&
      Number.isInteger(value.result.epoch) &&
      Number.isInteger(value.result.slotIndex) &&
      Number.isInteger(value.result.slotsInEpoch) &&
      value.result.slotsInEpoch > 0,
  });
}

export function handleSummary(data) {
  const summary = {
    testType: 'basic-get-epoch-info',
    timestamp: new Date().toISOString(),
    config: {
      rpcUrl: config.rpcUrl,
      vus: scenarios.basic.vus,
      duration: scenarios.basic.duration,
      commitment: config.epochInfoCommitment,
      minContextSlot: config.epochInfoMinContextSlot,
    },
    metrics: {
      requests: {
        total: data.metrics.rpc_requests_total?.values?.count || 0,
        successful: data.metrics.rpc_requests_success?.values?.count || 0,
        failed: data.metrics.http_req_failed?.values?.passes || 0,
      },
      latency: {
        avg: data.metrics.rpc_getEpochInfo_latency?.values?.avg || 0,
        p95: data.metrics.rpc_getEpochInfo_latency?.values['p(95)'] || 0,
        p99: data.metrics.rpc_getEpochInfo_latency?.values['p(99)'] || 0,
        max: data.metrics.rpc_getEpochInfo_latency?.values?.max || 0,
      },
      errors: {
        http: data.metrics.rpc_errors_http?.values?.count || 0,
        rpc: data.metrics.rpc_errors_rpc?.values?.count || 0,
        timeout: data.metrics.rpc_errors_timeout?.values?.count || 0,
      },
    },
  };

  addDownstreamMetrics(data, summary.metrics);

  return {
    stdout: JSON.stringify(summary, null, 2) + '\n',
  };
}
