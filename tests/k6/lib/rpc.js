// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

// RPC request helpers for k6 load tests

import http from 'k6/http';
import { check } from 'k6';
import { config } from './config.js';
import {
  rpcLatency,
  rpcGetTransactionLatency,
  rpcGetBlockLatency,
  rpcGetBlockHeightLatency,
  rpcGetSlotLatency,
  rpcGetEpochInfoLatency,
  rpcGetTransactionCountLatency,
  rpcGetLatestBlockhashLatency,
  rpcGetBlockTimeLatency,
  rpcGetBlocksLatency,
  rpcGetBlocksWithLimitLatency,
  rpcGetSignatureStatusesLatency,
  rpcGetTransactionsForAddressLatency,
  rpcGetFirstAvailableBlockLatency,
  responseSize,
  signaturesCount,
  downstreamClickhouseElapsedMs,
  downstreamReceivedBytes,
  downstreamDecodedBytes,
  downstreamRowsRead,
  downstreamRowsReturned,
  downstreamDataReadBytes,
  errorRate,
  timeoutErrors,
  rpcErrors,
  httpErrors,
  jsonrpcErrorCodes,
  totalRequests,
  successfulRequests,
} from './metrics.js';

/**
 * Build a JSON-RPC request payload for getSignaturesForAddress
 * @param {string} address - Solana address
 * @param {object} options - Request options
 * @param {number} [options.limit] - Max signatures to return
 * @param {string} [options.before] - Get signatures before this signature
 * @param {number} [options.beforeSlot] - Superbank extension: get signatures before this slot
 * @param {string} [options.until] - Get signatures until this signature
 * @param {number} [options.untilSlot] - Superbank extension: get signatures until this slot
 * @param {number} [requestId] - JSON-RPC request id override
 * @returns {string} JSON stringified payload
 */
export function makeGetSignaturesRequest(address, options = {}, requestId = null) {
  const params = [address];

  const opts = {};
  if (options.limit !== undefined) {
    opts.limit = options.limit;
  } else {
    opts.limit = config.limit;
  }
  if (options.before) {
    opts.before = options.before;
  }
  if (options.beforeSlot !== undefined) {
    opts.beforeSlot = options.beforeSlot;
  }
  if (options.until) {
    opts.until = options.until;
  }
  if (options.untilSlot !== undefined) {
    opts.untilSlot = options.untilSlot;
  }

  params.push(opts);

  return JSON.stringify({
    jsonrpc: '2.0',
    id: requestId === null ? Math.floor(Math.random() * 1_000_000_000) : requestId,
    method: 'getSignaturesForAddress',
    params,
  });
}

/**
 * Build a JSON-RPC request payload for getSignatureStatuses
 * @param {string[]} signatures - Signatures to check
 * @param {object} options - Request options
 * @param {boolean} [options.searchTransactionHistory]
 * @param {number} [requestId] - JSON-RPC request id override
 * @returns {string} JSON stringified payload
 */
export function makeGetSignatureStatusesRequest(signatures, options = {}, requestId = null) {
  const params = [signatures];
  if (options && Object.keys(options).length > 0) {
    params.push(options);
  }
  return JSON.stringify({
    jsonrpc: '2.0',
    id: requestId === null ? Math.floor(Math.random() * 1_000_000_000) : requestId,
    method: 'getSignatureStatuses',
    params,
  });
}

/**
 * Build a JSON-RPC request payload for getTransactionsForAddress
 * @param {string} address - Solana address
 * @param {object} options - Request options
 * @param {number} [options.beforeSlot] - Superbank extension: alias for filters.slot.lt
 * @param {number} [options.untilSlot] - Superbank extension: alias for filters.slot.gt
 * @param {number} [requestId] - JSON-RPC request id override
 * @returns {string} JSON stringified payload
 */
export function makeGetTransactionsForAddressRequest(address, options = {}, requestId = null) {
  const params = [address];
  if (options && Object.keys(options).length > 0) {
    params.push(options);
  }
  return JSON.stringify({
    jsonrpc: '2.0',
    id: requestId === null ? Math.floor(Math.random() * 1_000_000_000) : requestId,
    method: 'getTransactionsForAddress',
    params,
  });
}

/**
 * Build a JSON-RPC request payload for getBlockTime
 * @param {number} slot - Solana slot
 * @param {number} [requestId] - JSON-RPC request id override
 * @returns {string} JSON stringified payload
 */
export function makeGetBlockTimeRequest(slot, requestId = null) {
  return JSON.stringify({
    jsonrpc: '2.0',
    id: requestId === null ? Math.floor(Math.random() * 1_000_000_000) : requestId,
    method: 'getBlockTime',
    params: [slot],
  });
}

/**
 * Build a JSON-RPC request payload for getBlocks
 * @param {number} startSlot - Start slot
 * @param {number|null} endSlot - End slot (optional)
 * @param {object} options - Request options
 * @param {number} [requestId] - JSON-RPC request id override
 * @returns {string} JSON stringified payload
 */
export function makeGetBlocksRequest(startSlot, endSlot = null, options = {}, requestId = null) {
  const params = [startSlot];
  if (endSlot !== null && endSlot !== undefined) {
    params.push(endSlot);
  }
  if (options && Object.keys(options).length > 0) {
    params.push(options);
  }
  return JSON.stringify({
    jsonrpc: '2.0',
    id: requestId === null ? Math.floor(Math.random() * 1_000_000_000) : requestId,
    method: 'getBlocks',
    params,
  });
}

/**
 * Build a JSON-RPC request payload for getBlocksWithLimit
 * @param {number} startSlot - Start slot
 * @param {number} limit - Maximum slots to return
 * @param {object} options - Request options
 * @param {number} [requestId] - JSON-RPC request id override
 * @returns {string} JSON stringified payload
 */
export function makeGetBlocksWithLimitRequest(
  startSlot,
  limit,
  options = {},
  requestId = null
) {
  const params = [startSlot, limit];
  if (options && Object.keys(options).length > 0) {
    params.push(options);
  }
  return JSON.stringify({
    jsonrpc: '2.0',
    id: requestId === null ? Math.floor(Math.random() * 1_000_000_000) : requestId,
    method: 'getBlocksWithLimit',
    params,
  });
}

/**
 * Build a JSON-RPC request payload for getFirstAvailableBlock
 * @param {number} [requestId] - JSON-RPC request id override
 * @returns {string} JSON stringified payload
 */
export function makeGetFirstAvailableBlockRequest(requestId = null) {
  return JSON.stringify({
    jsonrpc: '2.0',
    id: requestId === null ? Math.floor(Math.random() * 1_000_000_000) : requestId,
    method: 'getFirstAvailableBlock',
    params: [],
  });
}

/**
 * Build a JSON-RPC request payload for getInflationReward
 * @param {string[]} addresses - Solana addresses
 * @param {object} options - Request options
 * @param {number} [options.epoch] - Epoch to query
 * @param {string} [options.commitment] - confirmed | finalized
 * @param {number} [options.minContextSlot] - Minimum context slot (optional)
 * @param {number} [requestId] - JSON-RPC request id override
 * @returns {string} JSON stringified payload
 */
export function makeGetInflationRewardRequest(addresses, options = {}, requestId = null) {
  const params = [addresses];
  if (options && Object.keys(options).length > 0) {
    params.push(options);
  }
  return JSON.stringify({
    jsonrpc: '2.0',
    id: requestId === null ? Math.floor(Math.random() * 1_000_000_000) : requestId,
    method: 'getInflationReward',
    params,
  });
}

/**
 * Build a JSON-RPC request payload for getBlockHeight
 * @param {object} options - Request options
 * @param {string} [options.commitment] - processed | confirmed | finalized
 * @param {number} [options.minContextSlot] - Minimum context slot (optional)
 * @param {number} [requestId] - JSON-RPC request id override
 * @returns {string} JSON stringified payload
 */
export function makeGetBlockHeightRequest(options = {}, requestId = null) {
  const params = [];
  if (options && Object.keys(options).length > 0) {
    params.push(options);
  }

  return JSON.stringify({
    jsonrpc: '2.0',
    id: requestId === null ? Math.floor(Math.random() * 1_000_000_000) : requestId,
    method: 'getBlockHeight',
    params,
  });
}

/**
 * Build a JSON-RPC request payload for getSlot
 * @param {object} options - Request options
 * @param {string} [options.commitment] - processed | confirmed | finalized
 * @param {number} [options.minContextSlot] - Minimum context slot (optional)
 * @param {number} [requestId] - JSON-RPC request id override
 * @returns {string} JSON stringified payload
 */
export function makeGetSlotRequest(options = {}, requestId = null) {
  const params = [];
  if (options && Object.keys(options).length > 0) {
    params.push(options);
  }

  return JSON.stringify({
    jsonrpc: '2.0',
    id: requestId === null ? Math.floor(Math.random() * 1_000_000_000) : requestId,
    method: 'getSlot',
    params,
  });
}

/**
 * Build a JSON-RPC request payload for getEpochInfo.
 * @param {object} options - Request options
 * @param {string} [options.commitment] - processed | confirmed | finalized
 * @param {number} [options.minContextSlot] - Minimum context slot (optional)
 * @param {number} [requestId] - JSON-RPC request id override
 * @returns {string} JSON stringified payload
 */
export function makeGetEpochInfoRequest(options = {}, requestId = null) {
  const params = [];
  if (options && Object.keys(options).length > 0) {
    params.push(options);
  }

  return JSON.stringify({
    jsonrpc: '2.0',
    id: requestId === null ? Math.floor(Math.random() * 1_000_000_000) : requestId,
    method: 'getEpochInfo',
    params,
  });
}

/**
 * Build a JSON-RPC request payload for getTransactionCount
 * @param {object} options - Request options
 * @param {string} [options.commitment] - processed | confirmed | finalized
 * @param {number} [options.minContextSlot] - Minimum context slot (optional)
 * @param {number} [requestId] - JSON-RPC request id override
 * @returns {string} JSON stringified payload
 */
export function makeGetTransactionCountRequest(options = {}, requestId = null) {
  const params = [];
  if (options && Object.keys(options).length > 0) {
    params.push(options);
  }

  return JSON.stringify({
    jsonrpc: '2.0',
    id: requestId === null ? Math.floor(Math.random() * 1_000_000_000) : requestId,
    method: 'getTransactionCount',
    params,
  });
}

/**
 * Build a JSON-RPC request payload for getLatestBlockhash
 * @param {object} options - Request options
 * @param {string} [options.commitment] - processed | confirmed | finalized
 * @param {number} [options.minContextSlot] - Minimum context slot (optional)
 * @param {number} [requestId] - JSON-RPC request id override
 * @returns {string} JSON stringified payload
 */
export function makeGetLatestBlockhashRequest(options = {}, requestId = null) {
  const params = [];
  if (options && Object.keys(options).length > 0) {
    params.push(options);
  }

  return JSON.stringify({
    jsonrpc: '2.0',
    id: requestId === null ? Math.floor(Math.random() * 1_000_000_000) : requestId,
    method: 'getLatestBlockhash',
    params,
  });
}

function selectRpcUrl() {
  const urls = config.rpcUrls;
  if (urls && urls.length > 0) {
    return urls[(__ITER + __VU) % urls.length];
  }
  return config.rpcUrl;
}

function getHeaderValue(headers, ...keys) {
  for (const key of keys) {
    const value = headers[key];
    if (value !== undefined && value !== null) {
      return Array.isArray(value) ? value[0] : value;
    }
  }
  return null;
}

function parseDelimitedHeader(rawValue) {
  const parsed = {};
  if (rawValue === null || rawValue === undefined) {
    return parsed;
  }

  const parts = String(rawValue).split(';');
  for (const part of parts) {
    const [key, value] = part.split('=');
    if (!key || value === undefined) {
      continue;
    }
    parsed[key.trim()] = value.trim();
  }

  return parsed;
}

/**
 * Parse response-side Superbank metrics headers.
 *
 * Current Superbank emits `X-Superbank-Metrics`; older deployments may still emit
 * `X-Downstream-Timings`, so accept both.
 */
export function parseResponseMetricsHeaders(res) {
  if (!res || !res.headers) {
    return null;
  }

  const headers = res.headers;
  const legacyHeader = getHeaderValue(
    headers,
    'X-Downstream-Timings',
    'x-downstream-timings'
  );
  const superbankHeader = getHeaderValue(headers, 'X-Superbank-Metrics', 'x-superbank-metrics');

  if (!legacyHeader && !superbankHeader) {
    return null;
  }

  const metrics = {
    clickhouseElapsedMs: null,
    receivedBytes: null,
    decodedBytes: null,
    rowsRead: null,
    rowsReturned: null,
    dataReadBytes: null,
  };

  const legacyFields = parseDelimitedHeader(legacyHeader);
  const superbankFields = parseDelimitedHeader(superbankHeader);

  const addNumber = (targetKey, value) => {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      metrics[targetKey] = parsed;
    }
  };

  addNumber('clickhouseElapsedMs', legacyFields.clickhouse_elapsed_ms);
  addNumber('receivedBytes', legacyFields.received_bytes);
  addNumber('decodedBytes', legacyFields.decoded_bytes);
  addNumber('rowsRead', superbankFields.rows_read);
  addNumber('rowsReturned', superbankFields.rows_returned);
  addNumber('dataReadBytes', superbankFields.data_read_bytes);

  return metrics;
}

/**
 * Execute a JSON-RPC request and record metrics
 * @param {string} payload - JSON-RPC payload
 * @param {object} options - Additional options
 * @param {boolean} [options.recordMetrics=true] - Whether to record custom metrics
 * @param {string} [options.rpcUrl] - Override RPC URL
 * @returns {object} { response, body, success }
 */
export function executeRequest(payload, options = {}) {
  const {
    recordMetrics = true,
    latencyMetric = rpcLatency,
    rpcUrl,
    requestOptions,
  } = options;
  const targetUrl = rpcUrl || selectRpcUrl();

  if (recordMetrics) {
    totalRequests.add(1);
  }

  const httpOptions = {
    headers: { 'Content-Type': 'application/json' },
  };
  if (requestOptions && typeof requestOptions === 'object') {
    Object.assign(httpOptions, requestOptions);
  }

  const res = http.post(targetUrl, payload, httpOptions);

  let body = null;
  try {
    body = res.json();
  } catch (e) {
    // JSON parsing failed
  }

  if (recordMetrics) {
    recordRequestMetrics(res, body, latencyMetric);
  }

  const success = res.status === 200 && body !== null && !body.error;

  return { response: res, body, success };
}

/**
 * Record metrics for a completed request
 * @param {object} res - k6 HTTP response
 * @param {object|null} body - Parsed JSON body or null
 */
export function recordRequestMetrics(res, body, latencyMetric = rpcLatency) {
  // Always record latency
  latencyMetric.add(res.timings.duration);

  // Record response size
  if (res.body) {
    responseSize.add(res.body.length);
  }

  const downstreamMetrics = parseResponseMetricsHeaders(res);
  if (downstreamMetrics) {
    if (downstreamMetrics.clickhouseElapsedMs !== null) {
      downstreamClickhouseElapsedMs.add(downstreamMetrics.clickhouseElapsedMs);
    }
    if (downstreamMetrics.receivedBytes !== null) {
      downstreamReceivedBytes.add(downstreamMetrics.receivedBytes);
    }
    if (downstreamMetrics.decodedBytes !== null) {
      downstreamDecodedBytes.add(downstreamMetrics.decodedBytes);
    }
    if (downstreamMetrics.rowsRead !== null) {
      downstreamRowsRead.add(downstreamMetrics.rowsRead);
    }
    if (downstreamMetrics.rowsReturned !== null) {
      downstreamRowsReturned.add(downstreamMetrics.rowsReturned);
    }
    if (downstreamMetrics.dataReadBytes !== null) {
      downstreamDataReadBytes.add(downstreamMetrics.dataReadBytes);
    }
  }

  // Categorize the result
  if (res.status !== 200) {
    // HTTP-level error
    errorRate.add(1);
    httpErrors.add(1);
  } else if (!body) {
    // Failed to parse JSON
    errorRate.add(1);
    httpErrors.add(1);
  } else if (body.error) {
    // RPC-level error
    errorRate.add(1);
    rpcErrors.add(1);
    if (body.error && body.error.code !== undefined) {
      // k6 only exposes tagged sub-metrics in handleSummary when thresholds reference them.
      // Validation scenarios add no-op thresholds so we can report per-code counts.
      jsonrpcErrorCodes.add(1, { code: String(body.error.code) });
    } else {
      jsonrpcErrorCodes.add(1, { code: 'missing' });
    }
  } else {
    // Success
    errorRate.add(0);
    successfulRequests.add(1);

    // Record signatures count
    if (body.result && Array.isArray(body.result)) {
      signaturesCount.add(body.result.length);
    }
  }

  // Check for slow requests (potential timeouts)
  if (res.timings.duration > config.thresholds.slowRequestThreshold) {
    timeoutErrors.add(1);
  }
}

/**
 * Run standard checks on a response
 * @param {object} res - k6 HTTP response
 * @param {object|null} body - Parsed JSON body or null
 * @returns {boolean} Whether all checks passed
 */
export function runChecks(res, body) {
  return check(res, {
    'status is 200': (r) => r.status === 200,
    'response is json': () => body !== null,
    'no rpc error': () => body && !body.error,
  });
}

/**
 * Execute getSignaturesForAddress and run all checks
 * @param {string} address - Solana address
 * @param {object} options - Request options
 * @returns {object} { response, body, success, checksPass }
 */
export function getSignaturesForAddress(address, options = {}) {
  const payload = makeGetSignaturesRequest(address, options);
  const { response, body, success } = executeRequest(payload);
  const checksPass = runChecks(response, body);

  return { response, body, success, checksPass };
}

/**
 * Execute getSignatureStatuses and run all checks
 * @param {string[]} signatures - Signature list
 * @param {object} options - Request options
 * @returns {object} { response, body, success, checksPass }
 */
export function getSignatureStatuses(signatures, options = {}) {
  const payload = makeGetSignatureStatusesRequest(signatures, options);
  const { response, body, success } = executeRequest(payload, {
    latencyMetric: rpcGetSignatureStatusesLatency,
  });
  const checksPass = runChecks(response, body);

  return { response, body, success, checksPass };
}

/**
 * Execute getTransactionsForAddress and run all checks
 * @param {string} address - Solana address
 * @param {object} options - Request options
 * @returns {object} { response, body, success, checksPass }
 */
export function getTransactionsForAddress(address, options = {}) {
  const payload = makeGetTransactionsForAddressRequest(address, options);
  const { response, body, success } = executeRequest(payload, {
    latencyMetric: rpcGetTransactionsForAddressLatency,
  });
  const checksPass = runChecks(response, body);

  return { response, body, success, checksPass };
}

/**
 * Build a JSON-RPC request payload for getTransaction
 * @param {string} signature - Solana transaction signature
 * @param {object} options - Request options
 * @param {string} [options.encoding] - json | jsonParsed | base58 | base64
 * @param {string} [options.commitment] - confirmed | finalized
 * @param {number} [options.maxSupportedTransactionVersion] - max version to return
 * @param {number} [options.slot] - Superbank extension: strict slot filter
 * @param {number} [requestId] - JSON-RPC request id override
 * @returns {string} JSON stringified payload
 */
export function makeGetTransactionRequest(signature, options = {}, requestId = null) {
  const params = [signature];
  if (options && Object.keys(options).length > 0) {
    params.push(options);
  }
  return JSON.stringify({
    jsonrpc: '2.0',
    id: requestId === null ? Math.floor(Math.random() * 1_000_000_000) : requestId,
    method: 'getTransaction',
    params,
  });
}

/**
 * Execute getTransaction and run all checks
 * @param {string} signature - Solana transaction signature
 * @param {object} options - Request options
 * @returns {object} { response, body, success, checksPass }
 */
export function getTransaction(signature, options = {}) {
  const payload = makeGetTransactionRequest(signature, options);
  const { response, body, success } = executeRequest(payload, {
    latencyMetric: rpcGetTransactionLatency,
  });
  const checksPass = runChecks(response, body);

  return { response, body, success, checksPass };
}

/**
 * Execute getBlockTime and run all checks
 * @param {number} slot - Solana slot
 * @returns {object} { response, body, success, checksPass }
 */
export function getBlockTime(slot) {
  const payload = makeGetBlockTimeRequest(slot);
  const { response, body, success } = executeRequest(payload, {
    latencyMetric: rpcGetBlockTimeLatency,
  });
  const checksPass = runChecks(response, body);

  return { response, body, success, checksPass };
}

/**
 * Execute getBlockHeight and run all checks
 * @param {object} options - Request options
 * @returns {object} { response, body, success, checksPass }
 */
export function getBlockHeight(options = {}) {
  const payload = makeGetBlockHeightRequest(options);
  const { response, body, success } = executeRequest(payload, {
    latencyMetric: rpcGetBlockHeightLatency,
  });
  const checksPass = runChecks(response, body);

  return { response, body, success, checksPass };
}

/**
 * Execute getSlot and run all checks
 * @param {object} options - Request options
 * @returns {object} { response, body, success, checksPass }
 */
export function getSlot(options = {}) {
  const payload = makeGetSlotRequest(options);
  const { response, body, success } = executeRequest(payload, {
    latencyMetric: rpcGetSlotLatency,
  });
  const checksPass = runChecks(response, body);

  return { response, body, success, checksPass };
}

/**
 * Execute getEpochInfo and run all checks
 * @param {object} options - Request options
 * @returns {object} { response, body, success, checksPass }
 */
export function getEpochInfo(options = {}) {
  const payload = makeGetEpochInfoRequest(options);
  const { response, body, success } = executeRequest(payload, {
    latencyMetric: rpcGetEpochInfoLatency,
  });
  const checksPass = runChecks(response, body);

  return { response, body, success, checksPass };
}

/**
 * Execute getTransactionCount and run all checks
 * @param {object} options - Request options
 * @returns {object} { response, body, success, checksPass }
 */
export function getTransactionCount(options = {}) {
  const payload = makeGetTransactionCountRequest(options);
  const { response, body, success } = executeRequest(payload, {
    latencyMetric: rpcGetTransactionCountLatency,
  });
  const checksPass = runChecks(response, body);

  return { response, body, success, checksPass };
}

/**
 * Execute getLatestBlockhash and run all checks
 * @param {object} options - Request options
 * @returns {object} { response, body, success, checksPass }
 */
export function getLatestBlockhash(options = {}) {
  const payload = makeGetLatestBlockhashRequest(options);
  const { response, body, success } = executeRequest(payload, {
    latencyMetric: rpcGetLatestBlockhashLatency,
  });
  const checksPass = runChecks(response, body);

  return { response, body, success, checksPass };
}

/**
 * Execute getBlocks and run all checks
 * @param {number} startSlot - Start slot
 * @param {number|null} endSlot - End slot (optional)
 * @param {object} options - Request options
 * @returns {object} { response, body, success, checksPass }
 */
export function getBlocks(startSlot, endSlot = null, options = {}) {
  const payload = makeGetBlocksRequest(startSlot, endSlot, options);
  const { response, body, success } = executeRequest(payload, {
    latencyMetric: rpcGetBlocksLatency,
  });
  const checksPass = runChecks(response, body);

  return { response, body, success, checksPass };
}

/**
 * Execute getBlocksWithLimit and run all checks
 * @param {number} startSlot - Start slot
 * @param {number} limit - Maximum slots to return
 * @param {object} options - Request options
 * @returns {object} { response, body, success, checksPass }
 */
export function getBlocksWithLimit(startSlot, limit, options = {}) {
  const payload = makeGetBlocksWithLimitRequest(startSlot, limit, options);
  const { response, body, success } = executeRequest(payload, {
    latencyMetric: rpcGetBlocksWithLimitLatency,
  });
  const checksPass = runChecks(response, body);

  return { response, body, success, checksPass };
}

/**
 * Build a JSON-RPC request payload for getBlock
 * @param {number} slot - Solana slot
 * @param {object} options - Request options
 * @param {string} [options.encoding] - json | jsonParsed | base58 | base64
 * @param {string} [options.transactionDetails] - full | accounts | signatures | none
 * @param {boolean} [options.rewards] - include rewards
 * @param {string} [options.commitment] - confirmed | finalized
 * @param {number} [options.maxSupportedTransactionVersion] - max version to return
 * @param {number} [requestId] - JSON-RPC request id override
 * @returns {string} JSON stringified payload
 */
export function makeGetBlockRequest(slot, options = {}, requestId = null) {
  const params = [slot];
  if (options && Object.keys(options).length > 0) {
    params.push(options);
  }
  return JSON.stringify({
    jsonrpc: '2.0',
    id: requestId === null ? Math.floor(Math.random() * 1_000_000_000) : requestId,
    method: 'getBlock',
    params,
  });
}

/**
 * Execute getBlock and run all checks
 * @param {number} slot - Solana slot
 * @param {object} options - Request options
 * @returns {object} { response, body, success, checksPass }
 */
export function getBlock(slot, options = {}) {
  const payload = makeGetBlockRequest(slot, options);
  const { response, body, success } = executeRequest(payload, {
    latencyMetric: rpcGetBlockLatency,
  });
  const checksPass = runChecks(response, body);

  return { response, body, success, checksPass };
}

/**
 * Execute getFirstAvailableBlock and run all checks
 * @returns {object} { response, body, success, checksPass }
 */
export function getFirstAvailableBlock() {
  const payload = makeGetFirstAvailableBlockRequest();
  const { response, body, success } = executeRequest(payload, {
    latencyMetric: rpcGetFirstAvailableBlockLatency,
  });
  const checksPass = runChecks(response, body);

  return { response, body, success, checksPass };
}
