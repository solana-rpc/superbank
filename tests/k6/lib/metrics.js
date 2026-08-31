// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

// Custom metrics for k6 load tests

import { Trend, Counter, Rate } from 'k6/metrics';

// Latency metrics
export const rpcLatency = new Trend('rpc_getSignatures_latency', true);
export const rpcGetTransactionLatency = new Trend('rpc_getTransaction_latency', true);
export const rpcGetBlockLatency = new Trend('rpc_getBlock_latency', true);
export const rpcGetBlockHeightLatency = new Trend('rpc_getBlockHeight_latency', true);
export const rpcGetSlotLatency = new Trend('rpc_getSlot_latency', true);
export const rpcGetEpochInfoLatency = new Trend('rpc_getEpochInfo_latency', true);
export const rpcGetTransactionCountLatency = new Trend(
  'rpc_getTransactionCount_latency',
  true
);
export const rpcGetLatestBlockhashLatency = new Trend(
  'rpc_getLatestBlockhash_latency',
  true
);
export const rpcGetBlockTimeLatency = new Trend('rpc_getBlockTime_latency', true);
export const rpcGetBlocksLatency = new Trend('rpc_getBlocks_latency', true);
export const rpcGetBlocksWithLimitLatency = new Trend(
  'rpc_getBlocksWithLimit_latency',
  true
);
export const rpcGetSignatureStatusesLatency = new Trend('rpc_getSignatureStatuses_latency', true);
export const rpcGetTransactionsForAddressLatency = new Trend(
  'rpc_getTransactionsForAddress_latency',
  true
);
export const rpcGetFirstAvailableBlockLatency = new Trend(
  'rpc_getFirstAvailableBlock_latency',
  true
);

// WebSocket + head-cache metrics (used by head-cache k6 scenarios)
export const wsConnectMs = new Trend('ws_connect_ms', true);
export const wsSubscribeOk = new Rate('ws_subscribe_ok');
export const wsMessagesTotal = new Counter('ws_messages_total');
export const wsSignaturesTotal = new Counter('ws_signatures_total');
export const wsQueueDroppedTotal = new Counter('ws_queue_dropped_total');

export const headcacheSigToProcessedRequestMs = new Trend(
  'headcache_sig_to_processed_request_ms',
  true
);
export const headcacheGetTransactionProcessedMs = new Trend(
  'headcache_getTransaction_processed_ms',
  true
);
export const headcacheGetTransactionConfirmedMs = new Trend(
  'headcache_getTransaction_confirmed_ms',
  true
);
export const headcacheGetTransactionFinalizedMs = new Trend(
  'headcache_getTransaction_finalized_ms',
  true
);

export const headcacheAvailabilityProcessedMs = new Trend(
  'headcache_availability_processed_ms',
  true
);
export const headcacheAvailabilityConfirmedMs = new Trend(
  'headcache_availability_confirmed_ms',
  true
);
export const headcacheAvailabilityFinalizedMs = new Trend(
  'headcache_availability_finalized_ms',
  true
);

export const headcacheAvailabilityProcessedTimeoutTotal = new Counter(
  'headcache_availability_processed_timeout_total'
);
export const headcacheAvailabilityConfirmedTimeoutTotal = new Counter(
  'headcache_availability_confirmed_timeout_total'
);
export const headcacheAvailabilityFinalizedTimeoutTotal = new Counter(
  'headcache_availability_finalized_timeout_total'
);

export const headcacheProcessedAvailableRate = new Rate(
  'headcache_processed_available_rate'
);
export const headcacheConfirmedAvailableRate = new Rate(
  'headcache_confirmed_available_rate'
);
export const headcacheFinalizedAvailableRate = new Rate(
  'headcache_finalized_available_rate'
);
export const headcacheProcessedBlockTimePresentRate = new Rate(
  'headcache_processed_block_time_present_rate'
);
export const headcacheProcessedBlockTimeMissingTotal = new Counter(
  'headcache_processed_block_time_missing_total'
);
export const headcacheProcessedBlockTimePendingTotal = new Counter(
  'headcache_processed_block_time_pending_total'
);
export const headcacheProcessedBlockTimeFillMs = new Trend(
  'headcache_processed_block_time_fill_ms',
  true
);
export const headcacheProcessedBlockTimeTimeoutTotal = new Counter(
  'headcache_processed_block_time_timeout_total'
);

// Response metrics
export const responseSize = new Trend('rpc_response_size');
export const signaturesCount = new Trend('rpc_signatures_count');
export const downstreamClickhouseElapsedMs = new Trend('downstream_clickhouse_elapsed_ms', true);
export const downstreamReceivedBytes = new Trend('downstream_received_bytes');
export const downstreamDecodedBytes = new Trend('downstream_decoded_bytes');
export const downstreamRowsRead = new Trend('downstream_rows_read');
export const downstreamRowsReturned = new Trend('downstream_rows_returned');
export const downstreamDataReadBytes = new Trend('downstream_data_read_bytes');

// Error tracking
export const errorRate = new Rate('rpc_error_rate');
export const timeoutErrors = new Counter('rpc_errors_timeout');
export const rpcErrors = new Counter('rpc_errors_rpc');
export const httpErrors = new Counter('rpc_errors_http');
// RPC error codes are emitted in JSON-RPC error objects. Use tags to track counts by code.
export const jsonrpcErrorCodes = new Counter('rpc_errors_jsonrpc_code');

// Request counters
export const totalRequests = new Counter('rpc_requests_total');
export const successfulRequests = new Counter('rpc_requests_success');

/**
 * All custom metrics exported as an object for convenience
 */
export const metrics = {
  rpcLatency,
  rpcGetTransactionLatency,
  rpcGetBlockLatency,
  rpcGetBlockHeightLatency,
  rpcGetSlotLatency,
  rpcGetEpochInfoLatency,
  rpcGetLatestBlockhashLatency,
  rpcGetBlockTimeLatency,
  rpcGetBlocksLatency,
  rpcGetBlocksWithLimitLatency,
  rpcGetSignatureStatusesLatency,
  rpcGetTransactionsForAddressLatency,
  rpcGetFirstAvailableBlockLatency,
  wsConnectMs,
  wsSubscribeOk,
  wsMessagesTotal,
  wsSignaturesTotal,
  wsQueueDroppedTotal,
  headcacheSigToProcessedRequestMs,
  headcacheGetTransactionProcessedMs,
  headcacheGetTransactionConfirmedMs,
  headcacheGetTransactionFinalizedMs,
  headcacheAvailabilityProcessedMs,
  headcacheAvailabilityConfirmedMs,
  headcacheAvailabilityFinalizedMs,
  headcacheAvailabilityProcessedTimeoutTotal,
  headcacheAvailabilityConfirmedTimeoutTotal,
  headcacheAvailabilityFinalizedTimeoutTotal,
  headcacheProcessedAvailableRate,
  headcacheConfirmedAvailableRate,
  headcacheFinalizedAvailableRate,
  headcacheProcessedBlockTimePresentRate,
  headcacheProcessedBlockTimeMissingTotal,
  headcacheProcessedBlockTimePendingTotal,
  headcacheProcessedBlockTimeFillMs,
  headcacheProcessedBlockTimeTimeoutTotal,
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
};
