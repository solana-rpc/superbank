# superbank-rpc k6 Load Tests

Comprehensive load testing suite for superbank-rpc methods, including stress tests for many supported RPCs.
Includes basic tests plus validation tests for `getSignaturesForAddress`, `getTransaction`, `getBlock`, `getTransactionCount`, `getInflationReward`, a dedicated endpoint-comparison scenario for `getTransactionsForAddress`, a disk-cache parity scenario (`superbank-rpc-disk-cache-parity.js`) that diffs a disk-cache-enabled target against a reference across every method the disk tier serves, and a disk-cache performance comparison scenario (`superbank-rpc-disk-cache-compare.js`) that reports latency deltas and speedup ratios versus a non-disk reference.

## Quick Start

Install [k6](https://k6.io/) locally, then run:

```bash
# Basic load test
k6 run tests/k6/scenarios/basic/superbank-rpc-get-signatures.js -e RPC_URL=http://localhost:8899 -e ADDRESS_FILE=./tests/k6/data/pools/addresses.txt

# Basic JSON-RPC batch load test
k6 run tests/k6/scenarios/basic/superbank-rpc-batch-load.js -e RPC_URL=http://localhost:8899 -e BATCH_SIZE=3

# Hot address before/after test (specific pagination shape)
k6 run tests/k6/scenarios/basic/superbank-rpc-get-signatures-hot-before-after.js \
  -e RPC_URL=http://localhost:8899 \
  -e HOT_ADDRESS=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v \
  -e HOT_BEFORE=<signature> \
  -e HOT_AFTER=<signature> \
  -e HOT_BEFORE_AFTER_VUS=1 \
  -e HOT_BEFORE_AFTER_ITERATIONS=1

# Hot address pagination test (bounded paging)
k6 run tests/k6/scenarios/basic/superbank-rpc-get-signatures-hot-pagination.js \
  -e RPC_URL=http://localhost:8899 \
  -e HOT_ADDRESS=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v \
  -e HOT_LIMIT=1000 \
  -e HOT_MAX_PAGES=50

# Basic getTransaction test
k6 run tests/k6/scenarios/basic/superbank-rpc-get-transaction.js -e RPC_URL=http://localhost:8899 -e SIGNATURE_FILE=./tests/k6/data/pools/signatures.txt

# Head-cache WebSocket test (subscribe to upstream Solana WS for fresh signatures)
k6 run tests/k6/scenarios/basic/superbank-rpc-head-cache-ws-get-transaction.js \
  -e RPC_URL=http://localhost:8899 \
  -e SOLANA_WS_URL=wss://api.mainnet-beta.solana.com \
  -e WS_MENTION=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v \
  -e WS_MAX_SIGS_PER_SEC=2

# Validation test (compare against reference RPC)
k6 run tests/k6/scenarios/validation/superbank-rpc-validate-get-signatures.js -e RPC_URL=http://localhost:8899 -e REFERENCE_RPC_URL=https://api.mainnet-beta.solana.com -e ADDRESS_FILE=./tests/k6/data/pools/addresses.txt

# Validation test for getTransaction
k6 run tests/k6/scenarios/validation/superbank-rpc-validate-get-transaction.js -e RPC_URL=http://localhost:8899 -e REFERENCE_RPC_URL=https://api.mainnet-beta.solana.com -e SIGNATURE_FILE=./tests/k6/data/pools/signatures.txt

# Validation test for getBlock
k6 run tests/k6/scenarios/validation/superbank-rpc-validate-get-block.js -e RPC_URL=http://localhost:8899 -e REFERENCE_RPC_URL=https://api.mainnet-beta.solana.com -e SLOT_FILE=./tests/k6/data/pools/slots.txt

# Validation test for getInflationReward
k6 run tests/k6/scenarios/validation/superbank-rpc-validate-get-inflation-reward.js \
  -e RPC_URL=http://localhost:8899 \
  -e REFERENCE_RPC_URL=https://api.mainnet-beta.solana.com \
  -e ADDRESS_FILE=./tests/k6/data/pools/addresses.txt \
  -e INFLATION_REWARD_EPOCH=760

# Validation test for getTransactionCount
k6 run tests/k6/scenarios/validation/superbank-rpc-validate-get-transaction-count.js \
  -e RPC_URL=http://localhost:8899 \
  -e REFERENCE_RPC_URL=https://api.mainnet-beta.solana.com

# Validation test for getLatestBlockhash
k6 run tests/k6/scenarios/validation/superbank-rpc-validate-get-latest-blockhash.js \
  -e RPC_URL=http://localhost:8899 \
  -e REFERENCE_RPC_URL=https://api.mainnet-beta.solana.com

# Validation + latency comparison for getTransactionsForAddress
k6 run tests/k6/scenarios/validation/superbank-rpc-validate-get-transactions-for-address.js \
  -e RPC_URL=http://localhost:8899 \
  -e REFERENCE_RPC_URL=http://localhost:8898 \
  -e ADDRESS_FILE=./tests/k6/data/pools/addresses.txt

# Validation test for JSON-RPC batch semantics (no reference RPC required)
k6 run tests/k6/scenarios/validation/superbank-rpc-validate-batch.js -e RPC_URL=http://localhost:8899

# Disk-cache performance comparison against a non-disk reference
k6 run tests/k6/scenarios/performance/superbank-rpc-disk-cache-compare.js \
  -e RPC_URL=http://disk-enabled:8899 \
  -e REFERENCE_RPC_URL=http://disk-disabled:8899 \
  -e ADDRESS_FILE=./tests/k6/data/pools/addresses.txt \
  -e VUS=10 \
  -e DURATION=60s

# Stress test for getBlock
k6 run tests/k6/scenarios/stress/stress-test-get-block.js -e RPC_URL=http://localhost:8899 -e SLOT_FILE=./tests/k6/data/pools/slots.txt

# Stress tests by method
k6 run tests/k6/scenarios/stress/stress-test-get-transaction.js -e RPC_URL=http://localhost:8899 -e SIGNATURE_FILE=./tests/k6/data/pools/signatures.txt
k6 run tests/k6/scenarios/stress/stress-test-get-signature-statuses.js -e RPC_URL=http://localhost:8899 -e SIGNATURE_FILE=./tests/k6/data/pools/signatures.txt
k6 run tests/k6/scenarios/stress/stress-test-get-block-height.js -e RPC_URL=http://localhost:8899
k6 run tests/k6/scenarios/stress/stress-test-get-slot.js -e RPC_URL=http://localhost:8899
k6 run tests/k6/scenarios/stress/stress-test-get-transaction-count.js -e RPC_URL=http://localhost:8899
k6 run tests/k6/scenarios/stress/stress-test-get-block-time.js -e RPC_URL=http://localhost:8899 -e SLOT_FILE=./tests/k6/data/pools/slots.txt
k6 run tests/k6/scenarios/stress/stress-test-get-blocks.js -e RPC_URL=http://localhost:8899 -e SLOT_FILE=./tests/k6/data/pools/slots.txt
k6 run tests/k6/scenarios/stress/stress-test-get-first-available-block.js -e RPC_URL=http://localhost:8899
k6 run tests/k6/scenarios/stress/stress-test-get-latest-blockhash.js -e RPC_URL=http://localhost:8899
k6 run tests/k6/scenarios/stress/stress-test-get-transactions-for-address.js -e RPC_URL=http://localhost:8899 -e ADDRESS_FILE=./tests/k6/data/pools/addresses.txt

# Stress test - find breaking point
k6 run tests/k6/scenarios/stress/stress-test.js -e RPC_URL=http://localhost:8899

# Soak test - endurance testing
k6 run tests/k6/scenarios/soak/soak-test.js -e RPC_URL=http://localhost:8899

# Spike test - traffic bursts
k6 run tests/k6/scenarios/spike/spike-test.js -e RPC_URL=http://localhost:8899

# Fuzz test - parameter validation across RPC methods
k6 run tests/k6/scenarios/fuzz/fuzz-test-rpc-params.js -e RPC_URL=http://localhost:8899

# Fuzz options test - exhaust valid options/combos for each RPC method
k6 run tests/k6/scenarios/fuzz/fuzz-options-rpc-methods.js -e RPC_URL=http://localhost:8899
```

## Test Scenarios

### Basic Load Test (`superbank-rpc-get-signatures.js`)

Simple constant-load test with configurable VUs and duration.

- **Purpose**: Quick baseline performance measurement
- **Default**: 5 VUs for 30 seconds
- **Thresholds**: p95 latency < 500ms, HTTP failure rate < 1%

```bash
k6 run tests/k6/scenarios/basic/superbank-rpc-get-signatures.js \
  -e RPC_URL=http://localhost:8899 \
  -e VUS=10 \
  -e DURATION=60s
```

### Basic Batch Load Test (`superbank-rpc-batch-load.js`)

Constant-load test for valid JSON-RPC batch envelopes.

- **Purpose**: baseline throughput and response-order checks for batch requests
- **Default**: 5 VUs for 30 seconds
- **Config**: `BATCH_SIZE` controls items per batch envelope (default `3`)

```bash
k6 run tests/k6/scenarios/basic/superbank-rpc-batch-load.js \
  -e RPC_URL=http://localhost:8899 \
  -e BATCH_SIZE=3 \
  -e VUS=10 \
  -e DURATION=60s
```

### Basic getTransaction Test (`superbank-rpc-get-transaction.js`)

Simple constant-load test for `getTransaction` with a signature pool.

- **Purpose**: Quick baseline for transaction hydration/encoding performance
- **Default**: 5 VUs for 30 seconds
- **Thresholds**: p95 latency < 500ms, HTTP failure rate < 1%

```bash
k6 run tests/k6/scenarios/basic/superbank-rpc-get-transaction.js \
  -e RPC_URL=http://localhost:8899 \
  -e SIGNATURE_FILE=./tests/k6/data/pools/signatures.txt \
  -e VUS=10 \
  -e DURATION=60s
```

### Head-cache WS Test (`superbank-rpc-head-cache-ws-get-transaction.js`)

Subscribe to an upstream Solana WebSocket (`logsSubscribe`) to harvest fresh transaction
signatures, then call superbank-rpc `getTransaction` at `processed`, `confirmed`, and `finalized`.

- **Purpose**: Exercise the optional `grpc-head-cache` path (processed commitment support) and
  measure time-to-availability at higher commitments.
- **Requires**: `SOLANA_WS_URL` (upstream WS endpoint) and a running `superbank-rpc` at `RPC_URL`.
- **Notes**:
  - If `processed` commitment is rejected, the test fails fast (head cache likely disabled).
  - If `processed` returns a transaction with `blockTime=null` (or non-numeric), the test keeps
    polling and records pending/fill/timeout metrics.
  - Set `HEADCACHE_STRICT_PROCESSED_BLOCK_TIME=1` to fail the test when processed responses miss
    valid `blockTime`.
  - The default `WS_MENTION` is the USDC mint (high volume). Keep `WS_MAX_SIGS_PER_SEC` low.

```bash
k6 run tests/k6/scenarios/basic/superbank-rpc-head-cache-ws-get-transaction.js \
  -e RPC_URL=http://localhost:8899 \
  -e SOLANA_WS_URL=wss://api.mainnet-beta.solana.com \
  -e WS_MENTION=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v \
  -e WS_MAX_SIGS_PER_SEC=2 \
  -e VUS=5 \
  -e DURATION=60s
```

### Validation Tests (`superbank-rpc-validate-*.js`)

Compare Superbank RPC responses to a reference RPC. Responses must match exactly; JSON key order is ignored.
The `getTransactionsForAddress` comparison scenario is intended for Superbank-to-Superbank comparisons,
not official Solana RPC endpoints, because the method is custom.

```bash
# getSignaturesForAddress validation
k6 run tests/k6/scenarios/validation/superbank-rpc-validate-get-signatures.js \
  -e RPC_URL=http://localhost:8899 \
  -e REFERENCE_RPC_URL=https://api.mainnet-beta.solana.com

# getTransaction validation
k6 run tests/k6/scenarios/validation/superbank-rpc-validate-get-transaction.js \
  -e RPC_URL=http://localhost:8899 \
  -e REFERENCE_RPC_URL=https://api.mainnet-beta.solana.com \
  -e SIGNATURE_FILE=./tests/k6/data/pools/signatures.txt

# getBlock validation
k6 run tests/k6/scenarios/validation/superbank-rpc-validate-get-block.js \
  -e RPC_URL=http://localhost:8899 \
  -e REFERENCE_RPC_URL=https://api.mainnet-beta.solana.com \
  -e SLOT_FILE=./tests/k6/data/pools/slots.txt

# getInflationReward validation
k6 run tests/k6/scenarios/validation/superbank-rpc-validate-get-inflation-reward.js \
  -e RPC_URL=http://localhost:8899 \
  -e REFERENCE_RPC_URL=https://api.mainnet-beta.solana.com \
  -e ADDRESS_FILE=./tests/k6/data/pools/addresses.txt \
  -e INFLATION_REWARD_EPOCH=760
# If INFLATION_REWARD_EPOCH is omitted, the scenario derives epoch from Superbank getSlot
# and validates the previous epoch for stable cross-node comparison.

# getTransactionsForAddress validation + latency comparison
k6 run tests/k6/scenarios/validation/superbank-rpc-validate-get-transactions-for-address.js \
  -e RPC_URL=http://localhost:8899 \
  -e REFERENCE_RPC_URL=http://localhost:8898 \
  -e ADDRESS_FILE=./tests/k6/data/pools/addresses.txt
# This alternates call order per iteration and reports which endpoint is faster by avg latency.

# JSON-RPC batch protocol validation (no reference RPC required)
k6 run tests/k6/scenarios/validation/superbank-rpc-validate-batch.js \
  -e RPC_URL=http://localhost:8899
```

### Disk-Cache Performance Comparison (`superbank-rpc-disk-cache-compare.js`)

Compare a disk-cache-enabled target (`RPC_URL`) with the same server build running without disk
cache (`REFERENCE_RPC_URL`). The setup phase probes recent finalized slots, derives stable
`getSignaturesForAddress` signature cursors from the target coverage span, and keeps only requests
whose target response reports `X-Superbank-Sources: disk-cache`. The measured
`getSignaturesForAddress` workload uses standard Solana `before`/`until` cursors. The load phase
alternates call order between the two endpoints and reports per-method target/reference latency,
latency delta, and speedup ratio.

```bash
k6 run tests/k6/scenarios/performance/superbank-rpc-disk-cache-compare.js \
  -e RPC_URL=http://disk-enabled:8899 \
  -e REFERENCE_RPC_URL=http://disk-disabled:8899 \
  -e ADDRESS_FILE=./tests/k6/data/pools/addresses.txt \
  -e VUS=10 \
  -e DURATION=60s
```

Useful knobs:
- `PERF_SLOT_SPAN` / `PERF_BLOCK_SAMPLES`: recent finalized slot window and sampled block count.
- `PERF_ADDRESS_SAMPLES` / `PERF_PAGE_SIZE`: address workload size for gSFA/gTFA.
- `PERF_METHODS`: comma-separated labels such as `get_block_full,get_transaction`.
- `PERF_ALLOW_MIXED_SOURCES=1`: accepts `disk-cache,clickhouse` responses when intentionally testing tier-boundary paths. `head-cache,disk-cache` is accepted by default because the source header reports every tier touched.
- `PERF_NORMALIZE_PROVIDER_DIFFS=0`: disables default normalization for live-reference differences. By default the comparator ignores `getSignatureStatuses.context.slot` drift and token-balance `uiTokenAmount.uiAmount` null-vs-number differences when canonical token fields are present.
- `PERF_DEBUG_MISMATCHES=1`: logs mismatched request params, source headers, first differing JSON path, and trimmed target/reference result snippets.
- `PERF_DEBUG_MISMATCHES_MAX` / `PERF_DEBUG_BODY_CHARS`: cap mismatch log count and result snippet size.

### Stress Test (`stress-test.js`)

Ramp up load progressively to find the system's breaking point.

- **Purpose**: Identify maximum capacity and failure modes
- **Stages**: 10 → 100 → 250 → 500 → 1000 → 0 VUs over ~24 minutes
- **Thresholds**: More lenient (p95 < 2s, 10% failure rate allowed)

```bash
k6 run tests/k6/scenarios/stress/stress-test.js -e RPC_URL=http://localhost:8899

# Cap the stress run at 10 VUs instead of using the default ramp
k6 run tests/k6/scenarios/stress/stress-test.js \
  -e RPC_URL=http://localhost:8899 \
  -e STRESS_VUS=10 \
  -e STRESS_DURATION=5m
```

### Stress Test for getBlock (`stress-test-get-block.js`)

Ramp up `getBlock` load to find the breaking point.

```bash
k6 run tests/k6/scenarios/stress/stress-test-get-block.js \
  -e RPC_URL=http://localhost:8899 \
  -e SLOT_FILE=./tests/k6/data/pools/slots.txt

# Hold getBlock at 10 VUs
k6 run tests/k6/scenarios/stress/stress-test-get-block.js \
  -e RPC_URL=http://localhost:8899 \
  -e SLOT_FILE=./tests/k6/data/pools/slots.txt \
  -e STRESS_VUS=10 \
  -e STRESS_DURATION=5m

# Log a small sample of failing slots / JSON-RPC errors during the run
k6 run tests/k6/scenarios/stress/stress-test-get-block.js \
  -e RPC_URL=http://localhost:8899 \
  -e SLOT_FILE=./tests/k6/data/pools/slots.txt \
  -e STRESS_VUS=10 \
  -e STRESS_DURATION=5m \
  -e STRESS_DEBUG_FAILURES=1 \
  -e STRESS_DEBUG_FAILURES_MAX=5
```

### Stress Tests by Method

```bash
# getTransaction
k6 run tests/k6/scenarios/stress/stress-test-get-transaction.js \
  -e RPC_URL=http://localhost:8899 \
  -e SIGNATURE_FILE=./tests/k6/data/pools/signatures.txt

# getSignatureStatuses
k6 run tests/k6/scenarios/stress/stress-test-get-signature-statuses.js \
  -e RPC_URL=http://localhost:8899 \
  -e SIGNATURE_FILE=./tests/k6/data/pools/signatures.txt

# getBlockHeight
k6 run tests/k6/scenarios/stress/stress-test-get-block-height.js \
  -e RPC_URL=http://localhost:8899

# getSlot
k6 run tests/k6/scenarios/stress/stress-test-get-slot.js \
  -e RPC_URL=http://localhost:8899

# getBlockTime
k6 run tests/k6/scenarios/stress/stress-test-get-block-time.js \
  -e RPC_URL=http://localhost:8899 \
  -e SLOT_FILE=./tests/k6/data/pools/slots.txt

# getBlocks
k6 run tests/k6/scenarios/stress/stress-test-get-blocks.js \
  -e RPC_URL=http://localhost:8899 \
  -e SLOT_FILE=./tests/k6/data/pools/slots.txt

# getFirstAvailableBlock
k6 run tests/k6/scenarios/stress/stress-test-get-first-available-block.js \
  -e RPC_URL=http://localhost:8899

# getLatestBlockhash
k6 run tests/k6/scenarios/stress/stress-test-get-latest-blockhash.js \
  -e RPC_URL=http://localhost:8899

# getSignaturesForAddress
k6 run tests/k6/scenarios/stress/stress-test.js \
  -e RPC_URL=http://localhost:8899

# getTransactionsForAddress
k6 run tests/k6/scenarios/stress/stress-test-get-transactions-for-address.js \
  -e RPC_URL=http://localhost:8899 \
  -e ADDRESS_FILE=./tests/k6/data/pools/addresses.txt
```

### Soak Test (`soak-test.js`)

Sustained constant load over an extended period.

- **Purpose**: Find memory leaks, connection pool exhaustion, or performance degradation
- **Default**: 20 VUs for 30 minutes (configurable up to hours)
- **Thresholds**: Strict (p95 < 500ms, p99 < 1s, 1% failure rate)

```bash
# 30 minute soak test
k6 run tests/k6/scenarios/soak/soak-test.js -e RPC_URL=http://localhost:8899

# 2 hour soak test with more VUs
k6 run tests/k6/scenarios/soak/soak-test.js \
  -e RPC_URL=http://localhost:8899 \
  -e SOAK_VUS=30 \
  -e DURATION=2h
```

### Spike Test (`spike-test.js`)

Sudden traffic bursts to test resilience and recovery.

- **Purpose**: Verify system handles sudden load spikes and recovers gracefully
- **Pattern**: Baseline → Spike to 100 VUs → Recovery → Bigger spike to 150 VUs → Recovery
- **Thresholds**: Moderate (p95 < 1s, 5% failure rate allowed)

```bash
k6 run tests/k6/scenarios/spike/spike-test.js -e RPC_URL=http://localhost:8899
```

### Fuzz Test (`fuzz-test-rpc-params.js`)

Randomized parameter fuzzing across the supported RPC methods. Designed to surface
validation bugs and server-side edge cases without requiring known-good data.

- **Purpose**: Validate parameter parsing and error handling
- **Default**: 5 VUs for 30 seconds
- **Thresholds**: 99% of responses should avoid 5xx; 95% should be JSON

### Fuzz Options Test (`fuzz-options-rpc-methods.js`)

Systematically exercises valid option values and representative option combinations per RPC
method. Designed to surface combinations of valid params that still fail (e.g., unsupported
encodings, transaction version mismatches, or missing token-owner data).

- **Purpose**: Validate supported option values and option interactions
- **Default**: 1 VU with iterations equal to the number of generated cases
- **Notes**:
  - Only official Solana RPC options are exercised; non-official options are excluded.
  - Custom methods (e.g. `getTransactionsForAddress`) are not included in this fuzz suite.
  - `FUZZ_OPTIONS_METHODS` must list official RPC methods only.
  - Failures are logged to stdout (case id, method, params summary, status, error).
  - Optional env vars: `FUZZ_OPTIONS_METHODS`, `FUZZ_OPTIONS_MAX_CASES`,
    `FUZZ_OPTIONS_LOG_LIMIT`, `FUZZ_OPTIONS_VUS`.

```bash
# Full suite (all methods)
k6 run tests/k6/scenarios/fuzz/fuzz-options-rpc-methods.js \
  -e RPC_URL=http://localhost:8899

# Single method
k6 run tests/k6/scenarios/fuzz/fuzz-options-rpc-methods.js \
  -e RPC_URL=http://localhost:8899 \
  -e FUZZ_OPTIONS_METHODS=getBlocksWithLimit

# Cap total cases (quick run)
k6 run tests/k6/scenarios/fuzz/fuzz-options-rpc-methods.js \
  -e RPC_URL=http://localhost:8899 \
  -e FUZZ_OPTIONS_MAX_CASES=500
```

```bash
k6 run tests/k6/scenarios/fuzz/fuzz-test-rpc-params.js -e RPC_URL=http://localhost:8899

# Bias towards invalid inputs and focus on specific methods
k6 run tests/k6/scenarios/fuzz/fuzz-test-rpc-params.js \
  -e RPC_URL=http://localhost:8899 \
  -e FUZZ_VALID_RATIO=0.2 \
  -e FUZZ_METHODS=getTransaction,getBlock
```

### Replay Test (`replay-test.js`)

Replay HAProxy-compatible CSV data at configurable speed. The checked-in sample is synthetic and
contains only the `Time` and `body` columns required by the parser.

```bash
k6 run tests/k6/scenarios/replay/replay-test.js \
  -e RPC_URL=http://localhost:8899 \
  -e LOG_FILE=./tests/k6/data/replay/synthetic-gsfa-replay.csv
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RPC_URL` | `http://localhost:8899` | Superbank RPC endpoint URL |
| `RPC_URLS` | (none) | Comma/space-separated list of RPC endpoints to rotate per iteration |
| `REFERENCE_RPC_URL` | (none) | Reference RPC endpoint URL for validation tests |
| `TFA_REFERENCE_RPC_URL` | (none) | Reference Superbank endpoint URL for `getTransactionsForAddress` validation |
| `VALIDATE_LATEST_BLOCKHASH` | `1` | Set to `0` to skip `validate:getLatestBlockhash` when no live `isBlockhashValid` reference is available |
| `LIMIT` | `25` | `getSignaturesForAddress` limit parameter |
| `ADDRESS_FILE` | (none) | Path to file with known Solana addresses |
| `ADDRESS_POOL_SIZE` | `200` | Number of random addresses to generate |
| `SIGNATURE_FILE` | (none) | Path to file with known Solana transaction signatures |
| `SIGNATURE_POOL_SIZE` | `200` | Number of random signatures to generate |
| `SIGNATURE_STATUSES_BATCH` | `10` | Signatures per getSignatureStatuses request (max 256) |
| `SIGNATURE_STATUSES_SEARCH_HISTORY` | (none) | getSignatureStatuses `searchTransactionHistory` |
| `SLOT_FILE` | (none) | Path to file with known Solana slots |
| `SLOT_POOL_SIZE` | `200` | Number of random slots to generate |
| `SLOT_MIN` | `0` | Minimum slot for random slot generation |
| `SLOT_MAX` | (none) | Maximum slot for random slot generation (defaults to `SLOT_MIN + 1_000_000`) |
| `SLOT_EPOCH_MIN` | (none) | Minimum epoch for random slot generation |
| `SLOT_EPOCH_MAX` | (none) | Maximum epoch for random slot generation |
| `SLOT_SLOTS_PER_EPOCH` | `432000` | Slots per epoch when using epoch-based slot generation |
| `TX_ENCODING` | `json` | getTransaction encoding (`json`, `jsonParsed`, `base64`, `base58`) |
| `TX_COMMITMENT` | (none) | getTransaction commitment (`confirmed`, `finalized`) |
| `MAX_SUPPORTED_TX_VERSION` | `0` | getTransaction `maxSupportedTransactionVersion` |
| `BLOCK_ENCODING` | (none) | getBlock encoding (`json`, `jsonParsed`, `base64`, `base58`) |
| `BLOCK_TRANSACTION_DETAILS` | (none) | getBlock `transactionDetails` (`full`, `accounts`, `signatures`, `none`) |
| `BLOCK_REWARDS` | (none) | getBlock `rewards` (`true` or `false`) |
| `BLOCK_COMMITMENT` | (none) | getBlock commitment (`confirmed`, `finalized`) |
| `BLOCK_MAX_SUPPORTED_TX_VERSION` | (none) | getBlock `maxSupportedTransactionVersion` |
| `BLOCKS_COMMITMENT` | (none) | getBlocks commitment (`confirmed`, `finalized`) |
| `BLOCKS_RANGE` | `100` | getBlocks end slot offset from start slot |
| `BLOCK_HEIGHT_COMMITMENT` | (none) | getBlockHeight commitment (`processed`, `confirmed`, `finalized`) |
| `BLOCK_HEIGHT_MIN_CONTEXT_SLOT` | (none) | getBlockHeight `minContextSlot` |
| `SLOT_COMMITMENT` | (none) | getSlot commitment (`processed`, `confirmed`, `finalized`) |
| `SLOT_MIN_CONTEXT_SLOT` | (none) | getSlot `minContextSlot` |
| `TRANSACTION_COUNT_COMMITMENT` | (none) | getTransactionCount commitment (`processed`, `confirmed`, `finalized`) |
| `TRANSACTION_COUNT_MIN_CONTEXT_SLOT` | (none) | getTransactionCount `minContextSlot` |
| `LATEST_BLOCKHASH_COMMITMENT` | (none) | getLatestBlockhash commitment (`processed`, `confirmed`, `finalized`) |
| `LATEST_BLOCKHASH_MIN_CONTEXT_SLOT` | (none) | getLatestBlockhash `minContextSlot` |
| `INFLATION_REWARD_COMMITMENT` | `finalized` | getInflationReward commitment (`confirmed`, `finalized`) |
| `INFLATION_REWARD_EPOCH` | (none) | getInflationReward epoch; when omitted, validation derives a stable previous epoch |
| `INFLATION_REWARD_MIN_CONTEXT_SLOT` | (none) | getInflationReward `minContextSlot` |
| `INFLATION_REWARD_ADDRESS_COUNT` | `8` | Number of addresses from the pool to include in getInflationReward requests |
| `TFA_TRANSACTION_DETAILS` | `signatures` | getTransactionsForAddress `transactionDetails` (`signatures`, `full`) |
| `TFA_SORT_ORDER` | `desc` | getTransactionsForAddress `sortOrder` (`asc`, `desc`) |
| `TFA_LIMIT` | `25` | getTransactionsForAddress `limit` |
| `TFA_COMMITMENT` | (none) | getTransactionsForAddress commitment (`confirmed`, `finalized`) |
| `TFA_ENCODING` | (none) | getTransactionsForAddress encoding (when `transactionDetails=full`) |
| `TFA_MAX_SUPPORTED_TX_VERSION` | (none) | getTransactionsForAddress `maxSupportedTransactionVersion` |
| `TFA_MIN_CONTEXT_SLOT` | (none) | getTransactionsForAddress `minContextSlot` |
| `TFA_PAGINATION_TOKEN` | (none) | getTransactionsForAddress `paginationToken` |
| `TFA_STATUS` | (none) | getTransactionsForAddress `filters.status` |
| `TFA_TOKEN_ACCOUNTS` | (none) | getTransactionsForAddress `filters.tokenAccounts` |
| `RPC_TIMEOUT` | (none) | Fallback per-request timeout for scenarios that support it |
| `HOT_ADDRESS` | (USDC mint) | Hot address for the hot pagination scenarios |
| `HOT_LIMIT` | `1000` | Page size for hot pagination |
| `HOT_MAX_PAGES` | `0` | Max pages to fetch (0 = unlimited) |
| `HOT_MAX_SIGNATURES` | `0` | Max signatures to fetch (0 = unlimited) |
| `HOT_SLEEP_MS` | `0` | Sleep between hot-pagination requests |
| `HOT_P95_MS` | `0` | Optional p95 threshold (ms) for hot-pagination scenarios |
| `HOT_TIMEOUT` | `180s` | Per-request timeout for hot-pagination scenarios |
| `HOT_BEFORE` | (none) | Hot before/after scenario `before` signature |
| `HOT_AFTER` | (none) | Hot before/after scenario `after` signature (maps to Solana `until`) |
| `HOT_UNTIL` | (none) | Alias for `HOT_AFTER` |
| `HOT_COMMITMENT` | (none) | Hot before/after scenario `commitment` |
| `HOT_BEFORE_AFTER_VUS` | `1` | Hot before/after scenario VUs; capped to iterations |
| `HOT_BEFORE_AFTER_ITERATIONS` | `1` | Hot before/after scenario iterations |
| `ITERATIONS` | `1` | Fallback hot before/after scenario iterations |
| `SOLANA_WS_URL` | (none) | Upstream Solana WebSocket endpoint (head-cache scenario) |
| `WS_MENTION` | (USDC mint) | logsSubscribe mention filter for head-cache scenario |
| `WS_COMMITMENT` | `processed` | WebSocket subscription commitment |
| `WS_SUBSCRIBE_TIMEOUT_MS` | `5000` | WebSocket subscribe timeout (ms) |
| `WS_NO_SIGNATURE_TIMEOUT_MS` | `5000` | Time to wait for a new signature (ms) |
| `WS_QUEUE_MAX` | `2000` | Max signatures buffered in the queue |
| `WS_MAX_SIGS_PER_SEC` | `2` | Throttle for harvested signatures |
| `METRICS_URL` | (none) | Optional metrics endpoint for head-cache preflight checks |
| `HEADCACHE_POLL_INTERVAL_MS` | `250` | Head-cache poll interval (ms) |
| `HEADCACHE_PROCESSED_MAX_WAIT_MS` | `2000` | Max wait for processed availability (ms) |
| `HEADCACHE_CONFIRMED_MAX_WAIT_MS` | `30000` | Max wait for confirmed availability (ms) |
| `HEADCACHE_FINALIZED_MAX_WAIT_MS` | `120000` | Max wait for finalized availability (ms) |
| `HEADCACHE_MAX_POLLS_PER_TICK` | `10` | Polls per tick |
| `HEADCACHE_STRICT_PROCESSED_BLOCK_TIME` | `0` | Fail when processed tx has missing/invalid `blockTime` |
| `LOG_FILE` | (none) | Replay test input CSV path |
| `TRAFFIC_MULTIPLIER` | `1` | Replay test speed multiplier |
| `FUZZ_VALID_RATIO` | `0.35` | Ratio of valid requests in fuzz test (0-1) |
| `FUZZ_METHODS` | (none) | Comma/space-separated list of RPC methods to fuzz |
| `FUZZ_DEBUG_FAILURES` | `0` | When set, prints failure details (fuzz-test-rpc-params) |
| `FUZZ_DEBUG_FAILURES_MAX` | `20` | Max failures to print when `FUZZ_DEBUG_FAILURES=1` |
| `FUZZ_DEBUG_PAYLOAD_MAX` | `300` | Max request payload bytes to print |
| `FUZZ_DEBUG_BODY_MAX` | `300` | Max response body bytes to print |
| `FUZZ_DEBUG_RPC_ERRORS` | `0` | When set, prints JSON-RPC error objects |
| `FUZZ_OPTIONS_METHODS` | (none) | Comma/space-separated list of methods (fuzz-options-rpc-methods) |
| `FUZZ_OPTIONS_MAX_CASES` | (none) | Cap total fuzz-options cases |
| `FUZZ_OPTIONS_LOG_LIMIT` | `25` | Max failures to log (fuzz-options-rpc-methods) |
| `FUZZ_OPTIONS_VUS` | `1` | Virtual users for fuzz-options (increases case repetition) |
| `FUZZ_OPTIONS_LOG_DOWNSTREAM` | `0` | When set, logs downstream timings/bytes for slow requests |
| `FUZZ_OPTIONS_LOG_DOWNSTREAM_MAX` | `50` | Max downstream logs to print |
| `FUZZ_OPTIONS_LOG_DOWNSTREAM_THRESHOLD_MS` | `0` | Only log downstream if legacy `clickhouse_elapsed_ms` >= threshold |
| `FUZZ_OPTIONS_LOG_DOWNSTREAM_THRESHOLD_BYTES` | `0` | Only log downstream if `received_bytes`, `decoded_bytes`, or `data_read_bytes` >= threshold |
| `STRESS_VUS` | (none) | Override stress scenarios to hold a fixed VU count instead of the default ramp |
| `STRESS_DURATION` | `5m` | Hold duration when `STRESS_VUS` is set |
| `STRESS_RAMP_UP` | `30s` | Ramp-up duration when `STRESS_VUS` is set |
| `STRESS_RAMP_DOWN` | `30s` | Ramp-down duration when `STRESS_VUS` is set |
| `STRESS_DEBUG_FAILURES` | `0` | For `stress-test-get-block.js`, log a small sample of failing slots and JSON-RPC errors |
| `STRESS_DEBUG_FAILURES_MAX` | `10` | Max sampled failure logs per VU when `STRESS_DEBUG_FAILURES=1` |
| `SPIKE_VUS` | (none) | Override spike scenarios with a bounded CI-friendly spike profile |
| `SPIKE_BASELINE_VUS` | `1` | Baseline VUs when `SPIKE_VUS` is set |
| `SPIKE_BASELINE_DURATION` | `10s` | Baseline duration when `SPIKE_VUS` is set |
| `SPIKE_RAMP_UP` | `10s` | Ramp-up duration when `SPIKE_VUS` is set |
| `SPIKE_DURATION` | `30s` | Hold duration when `SPIKE_VUS` is set |
| `SPIKE_RECOVERY_DURATION` | `10s` | Recovery duration when `SPIKE_VUS` is set |
| `SPIKE_RAMP_DOWN` | `10s` | Ramp-down duration when `SPIKE_VUS` is set |
| `VUS` | varies | Virtual users (for basic test) |
| `DURATION` | varies | Test duration (for basic/soak tests) |
| `SOAK_VUS` | `20` | Virtual users for soak test |
| `VALIDATION_LOG_MISMATCHES` | `1` | Set to `0` to disable mismatch logging |
| `K6_JSONRPC_INCLUDE_RESERVED_SERVER_CODES` | `0` | Set to `1` to include `-32099..-32000` code buckets |

### Using Real Addresses

For more realistic testing with addresses that have historical data:

1. Create a file with one address per line:
   ```
   7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU
   Stake11111111111111111111111111111111111111
   Vote111111111111111111111111111111111111111
   ```

2. Run tests with the address file:
   ```bash
   k6 run tests/k6/scenarios/stress/stress-test.js \
     -e RPC_URL=http://localhost:8899 \
     -e ADDRESS_FILE=./known-addresses.txt
   ```

Random addresses are syntactically valid Solana public keys but may not exist in ClickHouse; use `ADDRESS_FILE` when you want hits with historical data.

### Using Real Slots

For `getBlock` validation, provide known slots to avoid pruned or empty responses:

1. Create a file with one slot per line:
   ```
   267510000
   267510001
   267510002
   ```

2. Run the validation test with the slot file:
   ```bash
   k6 run tests/k6/scenarios/validation/superbank-rpc-validate-get-block.js \
     -e RPC_URL=http://localhost:8899 \
     -e REFERENCE_RPC_URL=https://api.mainnet-beta.solana.com \
     -e SLOT_FILE=./tests/k6/data/pools/slots.txt
   ```

If no `SLOT_FILE` is provided, the test will generate random slots (which may not exist on either RPC).

To generate slots by epoch (e.g. epochs 0-915), set:
```bash
k6 run tests/k6/scenarios/validation/superbank-rpc-validate-get-block.js \
  -e RPC_URL=http://localhost:8899 \
  -e REFERENCE_RPC_URL=https://api.mainnet-beta.solana.com \
  -e SLOT_EPOCH_MIN=0 \
  -e SLOT_EPOCH_MAX=915 \
  -e SLOT_SLOTS_PER_EPOCH=432000
```

## Metrics

### Latency Metrics
- `rpc_getSignatures_latency` - Response time in milliseconds (Trend)
- `rpc_getTransaction_latency` - Response time in milliseconds (Trend)
- `rpc_getBlock_latency` - Response time in milliseconds (Trend)
- `rpc_getBlockHeight_latency` - Response time in milliseconds (Trend)
- `rpc_getSlot_latency` - Response time in milliseconds (Trend)
- `rpc_getTransactionCount_latency` - Response time in milliseconds (Trend)
- `rpc_getLatestBlockhash_latency` - Response time in milliseconds (Trend)
- `rpc_getBlockTime_latency` - Response time in milliseconds (Trend)
- `rpc_getBlocks_latency` - Response time in milliseconds (Trend)
- `rpc_getBlocksWithLimit_latency` - Response time in milliseconds (Trend)
- `rpc_getSignatureStatuses_latency` - Response time in milliseconds (Trend)
- `rpc_getTransactionsForAddress_latency` - Response time in milliseconds (Trend)
- `rpc_getFirstAvailableBlock_latency` - Response time in milliseconds (Trend)

### Response Metrics

Current Superbank versions emit `X-Superbank-Metrics`; older deployments may still emit
`X-Downstream-Timings`. The k6 helpers accept both headers and record whichever metrics are present.

- `rpc_response_size` - Response body size in bytes (Trend)
- `rpc_signatures_count` - Number of signatures returned per request (Trend)
- `downstream_rows_read` - `X-Superbank-Metrics` `rows_read` counter when present (Trend)
- `downstream_rows_returned` - `X-Superbank-Metrics` `rows_returned` counter when present (Trend)
- `downstream_data_read_bytes` - `X-Superbank-Metrics` `data_read_bytes` counter when present (Trend)
- `downstream_clickhouse_elapsed_ms` - Legacy `X-Downstream-Timings` `clickhouse_elapsed_ms` when present (Trend)
- `downstream_received_bytes` - Legacy `X-Downstream-Timings` `received_bytes` when present (Trend)
- `downstream_decoded_bytes` - Legacy `X-Downstream-Timings` `decoded_bytes` when present (Trend)

### WebSocket + Head Cache Metrics
- `ws_connect_ms` - WebSocket connect time (Trend)
- `ws_subscribe_ok` - WebSocket subscribe success rate (Rate)
- `ws_messages_total` - WebSocket messages received (Counter)
- `ws_signatures_total` - Signatures harvested from WS stream (Counter)
- `ws_queue_dropped_total` - Signatures dropped due to queue overflow (Counter)
- `headcache_sig_to_processed_request_ms` - Time from signature observation to processed request (Trend)
- `headcache_getTransaction_processed_ms` - processed getTransaction latency (Trend)
- `headcache_getTransaction_confirmed_ms` - confirmed getTransaction latency (Trend)
- `headcache_getTransaction_finalized_ms` - finalized getTransaction latency (Trend)
- `headcache_availability_processed_ms` - Time to processed availability (Trend)
- `headcache_availability_confirmed_ms` - Time to confirmed availability (Trend)
- `headcache_availability_finalized_ms` - Time to finalized availability (Trend)
- `headcache_availability_processed_timeout_total` - Processed availability timeouts (Counter)
- `headcache_availability_confirmed_timeout_total` - Confirmed availability timeouts (Counter)
- `headcache_availability_finalized_timeout_total` - Finalized availability timeouts (Counter)
- `headcache_processed_available_rate` - Processed availability success rate (Rate)
- `headcache_confirmed_available_rate` - Confirmed availability success rate (Rate)
- `headcache_finalized_available_rate` - Finalized availability success rate (Rate)
- `headcache_processed_block_time_present_rate` - Processed tasks that reached valid `blockTime` before timeout (Rate)
- `headcache_processed_block_time_missing_total` - Processed available responses missing valid `blockTime` (Counter; attempts)
- `headcache_processed_block_time_pending_total` - Processed signatures that entered pending `blockTime` state (Counter)
- `headcache_processed_block_time_fill_ms` - Time from first processed availability (missing `blockTime`) to valid `blockTime` (Trend)
- `headcache_processed_block_time_timeout_total` - Pending processed signatures that timed out before valid `blockTime` (Counter)

### Hot Pagination Metrics
- `rpc_hot_pagination_pages` - Pages fetched (Counter)
- `rpc_hot_pagination_signatures` - Signatures fetched (Counter)

### Error Metrics
- `rpc_error_rate` - Overall error rate (Rate)
- `rpc_errors_http` - HTTP-level errors (Counter)
- `rpc_errors_rpc` - JSON-RPC level errors (Counter)
- `rpc_errors_jsonrpc_code` - JSON-RPC errors by code (Counter; tag: `code`)
- `rpc_errors_timeout` - Slow requests > 10 seconds (Counter)

#### JSON-RPC Error Code Breakdown

k6 only includes per-tag submetrics in the end-of-test JSON output when a threshold references
them. The validation scenarios use `makeJsonrpcErrorCodeThresholds()` to generate no-op
thresholds so `rpc_errors_jsonrpc_code{code:<...>}` appears in the summary output.

By default, it tracks the JSON-RPC 2.0 standard codes plus `-32000` (common server-side error)
and the `missing` fallback bucket. To also include the full reserved server range
(`-32099..-32000`), set `K6_JSONRPC_INCLUDE_RESERVED_SERVER_CODES=1`.

### Request Counters
- `rpc_requests_total` - Total requests made (Counter)
- `rpc_requests_success` - Successful requests (Counter)

## Output

Each test produces a JSON summary at the end with:
- Test type and timestamp
- Configuration used
- Aggregated metrics (latency percentiles, error counts, response sizes)

Example output:
```json
{
  "testType": "stress",
  "timestamp": "2024-01-15T10:30:00.000Z",
  "config": {
    "rpcUrl": "http://localhost:8899",
    "limit": 25,
    "addressPoolSize": 200
  },
  "metrics": {
    "requests": {
      "total": 15420,
      "successful": 15380,
      "failed": 40
    },
    "latency": {
      "avg": 45.2,
      "p95": 120.5,
      "p99": 250.3,
      "max": 980.1
    },
    "errors": {
      "http": 10,
      "rpc": 30,
      "timeout": 0
    }
  }
}
```

## Project Structure

```
tests/k6/
├── data/
│   ├── pools/
│   │   ├── addresses.txt           # Sample address pool
│   │   ├── signatures.txt          # Sample signature pool
│   │   └── slots.txt               # Sample slot pool
│   └── replay/
│       └── synthetic-gsfa-replay.csv # Synthetic replay input
├── lib/
│   ├── config.js                  # Shared configuration
│   ├── addresses.js               # Address generation/loading
│   ├── compare.js                 # JSON comparison helpers
│   ├── signatures.js              # Signature generation/loading
│   ├── slots.js                   # Slot generation/loading
│   ├── metrics.js                 # Custom metrics definitions
│   ├── official-rpc-spec.js        # Official method/option enums for fuzzing
│   ├── path.js                     # Shared path helpers
│   ├── rpc.js                     # RPC request helpers
│   ├── logs.js                    # Replay log parsing
│   └── summary.js                 # Summary helpers
├── scenarios/
│   ├── basic/
│   │   ├── superbank-rpc-get-signatures.js # Basic load test
│   │   ├── superbank-rpc-get-signatures-hot-pagination.js # Hot pagination test
│   │   ├── superbank-rpc-get-signatures-hot-before-after.js # Hot before/after test
│   │   ├── superbank-rpc-get-transaction.js # Basic getTransaction test
│   │   └── superbank-rpc-head-cache-ws-get-transaction.js # Head-cache WS test
│   ├── validation/
│   │   ├── superbank-rpc-validate-batch.js # JSON-RPC batch protocol validation
│   │   ├── superbank-rpc-validate-get-signatures.js # Validation test for getSignaturesForAddress
│   │   ├── superbank-rpc-validate-get-transaction.js # Validation test for getTransaction
│   │   ├── superbank-rpc-validate-get-block.js # Validation test for getBlock
│   │   ├── superbank-rpc-validate-get-inflation-reward.js
│   │   ├── superbank-rpc-validate-get-latest-blockhash.js
│   │   ├── superbank-rpc-validate-get-transaction-count.js
│   │   └── superbank-rpc-validate-get-transactions-for-address.js # TFA endpoint comparison
│   ├── stress/
│   │   ├── stress-test.js          # Stress test (find breaking point)
│   │   ├── stress-test-get-block.js
│   │   ├── stress-test-get-block-height.js
│   │   ├── stress-test-get-block-time.js
│   │   ├── stress-test-get-blocks.js
│   │   ├── stress-test-get-first-available-block.js
│   │   ├── stress-test-get-latest-blockhash.js
│   │   ├── stress-test-get-signature-statuses.js
│   │   ├── stress-test-get-slot.js
│   │   ├── stress-test-get-transaction-count.js
│   │   ├── stress-test-get-transaction.js
│   │   └── stress-test-get-transactions-for-address.js
│   ├── soak/
│   │   └── soak-test.js            # Soak/endurance test
│   ├── spike/
│   │   └── spike-test.js           # Spike test (traffic bursts)
│   ├── fuzz/
│   │   ├── fuzz-test-rpc-params.js # Fuzz test for RPC parameter validation
│   │   └── fuzz-options-rpc-methods.js # Fuzz test for official option combinations
│   └── replay/
│       └── replay-test.js          # Replay HAProxy-compatible CSV data
└── README.md                      # This file
```

## Interpreting Results

### What to Look For

1. **Stress Test**: Note at which VU count latency starts degrading or errors increase
2. **Soak Test**: Watch for gradual latency increase over time (memory leak indicator)
3. **Spike Test**: Check if system recovers to baseline after spikes

### Common Issues

| Symptom | Possible Cause |
|---------|----------------|
| High p99 but low p95 | Occasional slow queries, check ClickHouse execution time |
| Increasing latency over time | Memory leak or connection pool exhaustion |
| Errors during spikes only | Connection limits or thread pool saturation |
| All requests return empty | Random addresses with no data (use `ADDRESS_FILE`) |

### Threshold Failures

If thresholds fail, k6 will exit with a non-zero code. Check the output for which thresholds failed and adjust your expectations or fix the underlying issue.
