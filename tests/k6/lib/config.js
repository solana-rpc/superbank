// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

// Shared configuration for k6 load tests

export const config = {
    // RPC endpoint
    rpcUrl: __ENV.RPC_URL || "http://localhost:8899",
    // Optional list of RPC endpoints for per-iteration rotation
    rpcUrls: (__ENV.RPC_URLS || "")
        .split(/[,\s]+/)
        .map((entry) => entry.trim())
        .filter(Boolean),
    // Reference RPC endpoint (validation tests)
    referenceRpcUrl: __ENV.REFERENCE_RPC_URL || null,

    // Request parameters
    limit: Number(__ENV.LIMIT || 25),

    // Address pool configuration
    addressFile: __ENV.ADDRESS_FILE || null,
    addressPoolSize: Number(__ENV.ADDRESS_POOL_SIZE || 200),

  // Signature pool configuration (for getTransaction tests)
  signatureFile: __ENV.SIGNATURE_FILE || null,
  signaturePoolSize: Number(__ENV.SIGNATURE_POOL_SIZE || 200),
  signatureStatusesBatch: Number(__ENV.SIGNATURE_STATUSES_BATCH || 10),
  signatureStatusesSearchHistory:
    __ENV.SIGNATURE_STATUSES_SEARCH_HISTORY !== undefined
      ? __ENV.SIGNATURE_STATUSES_SEARCH_HISTORY === 'true' ||
        __ENV.SIGNATURE_STATUSES_SEARCH_HISTORY === '1'
      : null,

    // Slot pool configuration (for getBlock tests)
    slotFile: __ENV.SLOT_FILE || null,
    slotPoolSize: Number(__ENV.SLOT_POOL_SIZE || 200),
    slotMin: Number(__ENV.SLOT_MIN || 0),
    slotMax: __ENV.SLOT_MAX !== undefined ? Number(__ENV.SLOT_MAX) : null,
    slotEpochMin:
        __ENV.SLOT_EPOCH_MIN !== undefined
            ? Number(__ENV.SLOT_EPOCH_MIN)
            : null,
    slotEpochMax:
        __ENV.SLOT_EPOCH_MAX !== undefined
            ? Number(__ENV.SLOT_EPOCH_MAX)
            : null,
    slotSlotsPerEpoch: Number(__ENV.SLOT_SLOTS_PER_EPOCH || 432000),

    // getTransaction options
    transactionEncoding: __ENV.TX_ENCODING || "json",
    transactionCommitment: __ENV.TX_COMMITMENT || null,
  maxSupportedTransactionVersion: (() => {
    const value = Number(__ENV.MAX_SUPPORTED_TX_VERSION);
    return Number.isFinite(value) ? value : 1;
  })(),

    // getBlock options
    blockEncoding: __ENV.BLOCK_ENCODING || null,
    blockTransactionDetails: __ENV.BLOCK_TRANSACTION_DETAILS || null,
    blockRewards:
        __ENV.BLOCK_REWARDS !== undefined
            ? __ENV.BLOCK_REWARDS === "true" || __ENV.BLOCK_REWARDS === "1"
            : null,
  blockCommitment: __ENV.BLOCK_COMMITMENT || null,
  blockMaxSupportedTransactionVersion:
    __ENV.BLOCK_MAX_SUPPORTED_TX_VERSION !== undefined
      ? Number(__ENV.BLOCK_MAX_SUPPORTED_TX_VERSION)
      : null,
  blocksCommitment: __ENV.BLOCKS_COMMITMENT || null,
  blocksRange: Number(__ENV.BLOCKS_RANGE || 100),

  // getBlockHeight options
  blockHeightCommitment: __ENV.BLOCK_HEIGHT_COMMITMENT || null,
  blockHeightMinContextSlot: (() => {
    const value = Number(__ENV.BLOCK_HEIGHT_MIN_CONTEXT_SLOT);
    return Number.isFinite(value) ? value : null;
  })(),

  // getSlot options
  slotCommitment: __ENV.SLOT_COMMITMENT || null,
  slotMinContextSlot: (() => {
    const value = Number(__ENV.SLOT_MIN_CONTEXT_SLOT);
    return Number.isFinite(value) ? value : null;
  })(),

  // getTransactionCount options
  transactionCountCommitment: __ENV.TRANSACTION_COUNT_COMMITMENT || null,
  transactionCountMinContextSlot: (() => {
    const value = Number(__ENV.TRANSACTION_COUNT_MIN_CONTEXT_SLOT);
    return Number.isFinite(value) ? value : null;
  })(),

  // getLatestBlockhash options
  latestBlockhashCommitment: __ENV.LATEST_BLOCKHASH_COMMITMENT || null,
  latestBlockhashMinContextSlot: (() => {
    const value = Number(__ENV.LATEST_BLOCKHASH_MIN_CONTEXT_SLOT);
    return Number.isFinite(value) ? value : null;
  })(),

  // getInflationReward options
  inflationRewardCommitment: __ENV.INFLATION_REWARD_COMMITMENT || 'finalized',
  inflationRewardEpoch: (() => {
    const value = Number(__ENV.INFLATION_REWARD_EPOCH);
    return Number.isFinite(value) && value >= 0 ? Math.floor(value) : null;
  })(),
  inflationRewardMinContextSlot: (() => {
    const value = Number(__ENV.INFLATION_REWARD_MIN_CONTEXT_SLOT);
    return Number.isFinite(value) && value >= 0 ? Math.floor(value) : null;
  })(),
  inflationRewardAddressCount: (() => {
    const value = Number(__ENV.INFLATION_REWARD_ADDRESS_COUNT);
    return Number.isFinite(value) && value > 0 ? Math.floor(value) : 8;
  })(),

  // getTransactionsForAddress options
  transactionsForAddressDetails: __ENV.TFA_TRANSACTION_DETAILS || 'signatures',
  transactionsForAddressSortOrder: __ENV.TFA_SORT_ORDER || 'desc',
  transactionsForAddressLimit: Number(__ENV.TFA_LIMIT || 25),
  transactionsForAddressCommitment: __ENV.TFA_COMMITMENT || null,
  transactionsForAddressEncoding: __ENV.TFA_ENCODING || null,
  transactionsForAddressMaxSupportedTransactionVersion:
    __ENV.TFA_MAX_SUPPORTED_TX_VERSION !== undefined
      ? Number(__ENV.TFA_MAX_SUPPORTED_TX_VERSION)
      : null,
  transactionsForAddressMinContextSlot:
    __ENV.TFA_MIN_CONTEXT_SLOT !== undefined
      ? Number(__ENV.TFA_MIN_CONTEXT_SLOT)
      : null,
  transactionsForAddressPaginationToken: __ENV.TFA_PAGINATION_TOKEN || null,
  transactionsForAddressStatus: __ENV.TFA_STATUS || null,
  transactionsForAddressTokenAccounts: __ENV.TFA_TOKEN_ACCOUNTS || null,

  // getTransfersByAddress options
  transfersByAddressSortOrder: __ENV.TRANSFERS_SORT_ORDER || 'desc',
  transfersByAddressLimit: Number(__ENV.TRANSFERS_LIMIT || 100),
  transfersByAddressCommitment: __ENV.TRANSFERS_COMMITMENT || 'finalized',
  transfersByAddressSolMode: __ENV.TRANSFERS_SOL_MODE || 'merged',
  transfersByAddressMinContextSlot:
    __ENV.TRANSFERS_MIN_CONTEXT_SLOT !== undefined
      ? Number(__ENV.TRANSFERS_MIN_CONTEXT_SLOT)
      : null,
  transfersByAddressPaginationToken: __ENV.TRANSFERS_PAGINATION_TOKEN || null,
  transfersByAddressDirection: __ENV.TRANSFERS_DIRECTION || 'any',
  transfersByAddressMint: __ENV.TRANSFERS_MINT || null,
  transfersByAddressWith: __ENV.TRANSFERS_WITH || null,
  transfersByAddressAmountGte:
    __ENV.TRANSFERS_AMOUNT_GTE !== undefined
      ? Number(__ENV.TRANSFERS_AMOUNT_GTE)
      : null,
  transfersByAddressAmountLte:
    __ENV.TRANSFERS_AMOUNT_LTE !== undefined
      ? Number(__ENV.TRANSFERS_AMOUNT_LTE)
      : null,

  // WebSocket signature stream (used by head-cache tests)
  solanaWsUrl: __ENV.SOLANA_WS_URL || null,
  wsMention:
    __ENV.WS_MENTION ||
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' /* USDC mint */,
  wsCommitment: __ENV.WS_COMMITMENT || 'processed',
  wsSubscribeTimeoutMs: Number(__ENV.WS_SUBSCRIBE_TIMEOUT_MS || 5000),
  wsNoSignatureTimeoutMs: Number(__ENV.WS_NO_SIGNATURE_TIMEOUT_MS || 5000),
  wsQueueMax: Number(__ENV.WS_QUEUE_MAX || 2000),
  wsMaxSigsPerSec: Number(__ENV.WS_MAX_SIGS_PER_SEC || 2),
  metricsUrl: __ENV.METRICS_URL || null,

  // Head-cache polling behavior
  headCachePollIntervalMs: Number(__ENV.HEADCACHE_POLL_INTERVAL_MS || 250),
  headCacheProcessedMaxWaitMs: Number(__ENV.HEADCACHE_PROCESSED_MAX_WAIT_MS || 2000),
  headCacheConfirmedMaxWaitMs: Number(__ENV.HEADCACHE_CONFIRMED_MAX_WAIT_MS || 30000),
  headCacheFinalizedMaxWaitMs: Number(__ENV.HEADCACHE_FINALIZED_MAX_WAIT_MS || 120000),
  headCacheMaxPollsPerTick: Number(__ENV.HEADCACHE_MAX_POLLS_PER_TICK || 10),

    // Log file configuration (CSV format from HAProxy logs)
    logFile: __ENV.LOG_FILE || null,

    // Traffic replay multiplier (1 = original rate, 2 = 2x speed, 0.5 = half speed)
    trafficMultiplier: Number(__ENV.TRAFFIC_MULTIPLIER || 1),

    // Default thresholds
    thresholds: {
        // HTTP failure rate
        httpFailRate: 0.01, // 1%

        // Latency thresholds (ms)
        p95Latency: 500,
        p99Latency: 1000,

        // Stress test thresholds (more lenient)
        stressHttpFailRate: 0.1, // 10%
        stressP95Latency: 2000,

        // Spike test thresholds
        spikeHttpFailRate: 0.05, // 5%
        spikeP95Latency: 1000,
    },

    // Timeout threshold for categorizing slow requests (ms)
  slowRequestThreshold: 10000,
};

export function latestBlockhashOptions() {
  const options = {};
  if (config.latestBlockhashCommitment) {
    options.commitment = config.latestBlockhashCommitment;
  }
  if (config.latestBlockhashMinContextSlot !== null) {
    options.minContextSlot = config.latestBlockhashMinContextSlot;
  }
  return options;
}

export function transactionCountOptions() {
  const options = {};
  if (config.transactionCountCommitment) {
    options.commitment = config.transactionCountCommitment;
  }
  if (config.transactionCountMinContextSlot !== null) {
    options.minContextSlot = config.transactionCountMinContextSlot;
  }
  return options;
}

export function transactionsForAddressOptions() {
  const options = {
    transactionDetails: config.transactionsForAddressDetails,
    sortOrder: config.transactionsForAddressSortOrder,
    limit: config.transactionsForAddressLimit,
  };

  if (config.transactionsForAddressCommitment) {
    options.commitment = config.transactionsForAddressCommitment;
  }
  if (config.transactionsForAddressEncoding) {
    options.encoding = config.transactionsForAddressEncoding;
  }
  if (config.transactionsForAddressMaxSupportedTransactionVersion !== null) {
    options.maxSupportedTransactionVersion =
      config.transactionsForAddressMaxSupportedTransactionVersion;
  }
  if (config.transactionsForAddressMinContextSlot !== null) {
    options.minContextSlot = config.transactionsForAddressMinContextSlot;
  }
  if (config.transactionsForAddressPaginationToken) {
    options.paginationToken = config.transactionsForAddressPaginationToken;
  }

  const filters = {};
  if (config.transactionsForAddressStatus) {
    filters.status = config.transactionsForAddressStatus;
  }
  if (config.transactionsForAddressTokenAccounts) {
    filters.tokenAccounts = config.transactionsForAddressTokenAccounts;
  }
  if (Object.keys(filters).length > 0) {
    options.filters = filters;
  }

  return options;
}

export function transfersByAddressOptions() {
  const options = {
    sortOrder: config.transfersByAddressSortOrder,
    limit: config.transfersByAddressLimit,
  };

  if (config.transfersByAddressCommitment) {
    options.commitment = config.transfersByAddressCommitment;
  }
  if (config.transfersByAddressSolMode) {
    options.solMode = config.transfersByAddressSolMode;
  }
  if (config.transfersByAddressMinContextSlot !== null) {
    options.minContextSlot = config.transfersByAddressMinContextSlot;
  }
  if (config.transfersByAddressPaginationToken) {
    options.paginationToken = config.transfersByAddressPaginationToken;
  }
  if (config.transfersByAddressDirection) {
    options.direction = config.transfersByAddressDirection;
  }
  if (config.transfersByAddressMint) {
    options.mint = config.transfersByAddressMint;
  }
  if (config.transfersByAddressWith) {
    options.with = config.transfersByAddressWith;
  }

  const filters = {};
  const amount = {};
  if (config.transfersByAddressAmountGte !== null) {
    amount.gte = config.transfersByAddressAmountGte;
  }
  if (config.transfersByAddressAmountLte !== null) {
    amount.lte = config.transfersByAddressAmountLte;
  }
  if (Object.keys(amount).length > 0) {
    filters.amount = amount;
  }
  if (Object.keys(filters).length > 0) {
    options.filters = filters;
  }

  return options;
}

export function parseNonNegativeIntEnv(name, defaultValue) {
  const raw = __ENV[name];
  if (raw === undefined || raw === null || raw === '') {
    return defaultValue;
  }

  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    return defaultValue;
  }

  return parsed;
}

function parsePositiveIntEnv(name) {
  const raw = __ENV[name];
  if (raw === undefined || raw === null || raw === '') {
    return null;
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return null;
  }
  return Math.floor(parsed);
}

function stressStages() {
  const overrideVus = parsePositiveIntEnv('STRESS_VUS');
  if (overrideVus === null) {
    return [
      { duration: '2m', target: 10 }, // Warm up
      { duration: '5m', target: 100 }, // Ramp to moderate
      { duration: '5m', target: 250 }, // Ramp to high
      { duration: '5m', target: 500 }, // Push limits
      { duration: '5m', target: 1000 }, // Find breaking point
      { duration: '2m', target: 0 }, // Cool down
    ];
  }

  const rampUp = __ENV.STRESS_RAMP_UP || '30s';
  const hold = __ENV.STRESS_DURATION || '5m';
  const rampDown = __ENV.STRESS_RAMP_DOWN || '30s';

  return [
    { duration: rampUp, target: overrideVus },
    { duration: hold, target: overrideVus },
    { duration: rampDown, target: 0 },
  ];
}

function spikeStages() {
  const overrideVus = parsePositiveIntEnv('SPIKE_VUS');
  if (overrideVus === null) {
    return [
      { duration: "1m", target: 5 }, // Baseline
      { duration: "10s", target: 100 }, // Spike!
      { duration: "2m", target: 100 }, // Stay at peak
      { duration: "10s", target: 5 }, // Drop back
      { duration: "2m", target: 5 }, // Recovery period
      { duration: "10s", target: 150 }, // Bigger spike!
      { duration: "2m", target: 150 }, // Stay at peak
      { duration: "10s", target: 5 }, // Drop back
      { duration: "1m", target: 5 }, // Final recovery
    ];
  }

  const baselineVus = parsePositiveIntEnv('SPIKE_BASELINE_VUS') || 1;
  const baseline = __ENV.SPIKE_BASELINE_DURATION || '10s';
  const rampUp = __ENV.SPIKE_RAMP_UP || '10s';
  const hold = __ENV.SPIKE_DURATION || '30s';
  const recovery = __ENV.SPIKE_RECOVERY_DURATION || '10s';
  const rampDown = __ENV.SPIKE_RAMP_DOWN || '10s';

  return [
    { duration: baseline, target: baselineVus },
    { duration: rampUp, target: overrideVus },
    { duration: hold, target: overrideVus },
    { duration: recovery, target: baselineVus },
    { duration: rampDown, target: 0 },
  ];
}

// Scenario presets
export const scenarios = {
    // Basic load test defaults
    basic: {
        vus: Number(__ENV.VUS || 5),
        duration: __ENV.DURATION || "30s",
    },

    // Stress test stages
    stress: {
        stages: stressStages(),
    },

    // Soak test defaults
    soak: {
        vus: Number(__ENV.SOAK_VUS || 20),
        duration: __ENV.DURATION || "30m",
    },

    // Spike test stages
    spike: {
        stages: spikeStages(),
    },
};
