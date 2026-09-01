#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Copyright 2025-2026 Triton One Limited. All rights reserved.
#

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

usage() {
  cat <<'EOF'
Run Superbank k6 scenarios sequentially (basic + validation + fuzz + replay; optional stress/soak/spike).

Usage:
  scripts/test/run-k6.sh [--stress] [--soak] [--spike]

Environment (common):
  RPC_URL                  Superbank RPC endpoint (default: http://localhost:8899)
  REFERENCE_RPC_URL        Reference RPC endpoint for validation scenarios (optional)
  TFA_REFERENCE_RPC_URL    Reference RPC endpoint for getTransactionsForAddress validation (optional)
  VALIDATE_LATEST_BLOCKHASH Set to 0 to skip reference isBlockhashValid validation (default: 1)

Pools (defaults point at sample data in this repo):
  ADDRESS_FILE             (default: ./tests/k6/data/pools/addresses.txt)
  SIGNATURE_FILE           (default: ./tests/k6/data/pools/signatures.txt)
  SLOT_FILE                (default: ./tests/k6/data/pools/slots.txt)
  LOG_FILE                 (default for replay only: ./tests/k6/data/replay/synthetic-gsfa-replay.csv)

Suite defaults (override as needed):
  VUS                      (default: 5)
  DURATION                 (default: 30s)
  TX_ENCODING              (default: jsonParsed)

Notes:
  - Validation tests are skipped unless REFERENCE_RPC_URL is set.
  - Head-cache WS scenario runs only if RPC supports `commitment=processed` and SOLANA_WS_URL is set.
  - The getInflationReward stress scenario is skipped unless INFLATION_REWARD_EPOCH is set.
  - Stress/soak/spike scenarios are long-running; enable them explicitly via flags.
EOF
}

include_stress=false
include_soak=false
include_spike=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --stress) include_stress=true ;;
    --soak) include_soak=true ;;
    --spike) include_spike=true ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if command -v k6 >/dev/null 2>&1; then
  : # ok
else
  echo "k6 not found (install https://k6.io/ or add it to PATH)." >&2
  exit 1
fi

# Defaults for stable, repo-local pools.
: "${RPC_URL:=http://localhost:8899}"
: "${ADDRESS_FILE:=./tests/k6/data/pools/addresses.txt}"
: "${SIGNATURE_FILE:=./tests/k6/data/pools/signatures.txt}"
: "${SLOT_FILE:=./tests/k6/data/pools/slots.txt}"

# Defaults for suite runtime.
: "${VUS:=5}"
: "${DURATION:=30s}"

# Prefer `jsonParsed` by default because it's the most strict / most expressive encoding
# for response-shape validation.
: "${TX_ENCODING:=jsonParsed}"

export RPC_URL
export ADDRESS_FILE
export SIGNATURE_FILE
export SLOT_FILE
export VUS
export DURATION
export TX_ENCODING

failures=0
failed=()
skipped=()

run_k6() {
  local name="$1"
  local script="$2"

  echo
  echo "==> ${name}"
  echo "    ${script}"

  if k6 run "${script}"; then
    return 0
  fi

  failures=$((failures + 1))
  failed+=("${name}")
  return 0
}

skip() {
  local name="$1"
  local reason="$2"

  echo
  echo "==> ${name}"
  echo "    skipped: ${reason}"
  skipped+=("${name}")
}

first_signature_from_pool() {
  local file="$1"
  # Print first non-empty token (robust against blank lines/whitespace).
  awk 'NF{print $1; exit 0}' "${file}"
}

two_signatures_from_pool() {
  local file="$1"
  awk 'NF{print $1}' "${file}" | head -n 2
}

rpc_supports_processed_commitment() {
  local sig
  sig="$(first_signature_from_pool "${SIGNATURE_FILE}")"
  if [[ -z "${sig}" ]]; then
    return 1
  fi

  local payload
  payload="$(
    python3 - <<PY
import json
sig = ${sig@Q}
print(json.dumps({
  "jsonrpc": "2.0",
  "id": 1,
  "method": "getTransaction",
  "params": [sig, {"encoding": "json", "commitment": "processed", "maxSupportedTransactionVersion": 0}],
}))
PY
  )"

  # If the endpoint rejects `processed` commitment, head-cache WS tests will fail.
  curl -sS --max-time 5 \
    -H 'content-type: application/json' \
    -d "${payload}" \
    "${RPC_URL}" \
    | python3 -c '
import json, sys
try:
    body = json.load(sys.stdin)
except Exception:
    sys.exit(1)

err = body.get("error")
if not err:
    sys.exit(0)

msg = str(err.get("message") or "")
if "Only confirmed or finalized commitments are supported" in msg:
    sys.exit(1)
sys.exit(0)
'
}

run_basic_suite() {
  # Basic tests.
  run_k6 "basic:getSignaturesForAddress" \
    "tests/k6/scenarios/basic/superbank-rpc-get-signatures.js"

  run_k6 "basic:json-rpc batch load" \
    "tests/k6/scenarios/basic/superbank-rpc-batch-load.js"

  run_k6 "basic:getTransaction" \
    "tests/k6/scenarios/basic/superbank-rpc-get-transaction.js"

  # Hot pagination (bounded).
  if [[ -z "${HOT_MAX_PAGES:-}" ]]; then
    HOT_MAX_PAGES=1
  fi
  export HOT_MAX_PAGES
  run_k6 "basic:getSignaturesForAddress hot-pagination" \
    "tests/k6/scenarios/basic/superbank-rpc-get-signatures-hot-pagination.js"
  unset HOT_MAX_PAGES || true

  # Hot before/after requires signatures; default to first 2 in SIGNATURE_FILE if unset.
  if [[ -z "${HOT_BEFORE:-}" || -z "${HOT_AFTER:-}" ]]; then
    mapfile -t sigs < <(two_signatures_from_pool "${SIGNATURE_FILE}" || true)
    if [[ ${#sigs[@]} -ge 2 && -n "${sigs[0]}" && -n "${sigs[1]}" ]]; then
      HOT_BEFORE="${sigs[0]}"
      HOT_AFTER="${sigs[1]}"
      export HOT_BEFORE
      export HOT_AFTER
    else
      skip "basic:getSignaturesForAddress hot-before-after" "HOT_BEFORE/HOT_AFTER not set and SIGNATURE_FILE has <2 signatures"
      return 0
    fi
  else
    export HOT_BEFORE
    export HOT_AFTER
  fi
  run_k6 "basic:getSignaturesForAddress hot-before-after" \
    "tests/k6/scenarios/basic/superbank-rpc-get-signatures-hot-before-after.js"
  unset HOT_BEFORE HOT_AFTER || true

  # Head-cache WS test is only meaningful if the server supports processed commitment.
  if [[ -z "${SOLANA_WS_URL:-}" ]]; then
    skip "basic:head-cache ws getTransaction" "SOLANA_WS_URL not set"
  elif rpc_supports_processed_commitment; then
    export SOLANA_WS_URL
    run_k6 "basic:head-cache ws getTransaction" \
      "tests/k6/scenarios/basic/superbank-rpc-head-cache-ws-get-transaction.js"
  else
    skip "basic:head-cache ws getTransaction" "RPC does not support commitment=processed (head cache disabled)"
  fi
}

run_validation_suite() {
  if [[ -z "${REFERENCE_RPC_URL:-}" ]]; then
    skip "validation suite" "REFERENCE_RPC_URL not set"
  else
    export REFERENCE_RPC_URL

    run_k6 "validate:getSignaturesForAddress" \
      "tests/k6/scenarios/validation/superbank-rpc-validate-get-signatures.js"
    run_k6 "validate:getTransaction" \
      "tests/k6/scenarios/validation/superbank-rpc-validate-get-transaction.js"
    run_k6 "validate:getBlock" \
      "tests/k6/scenarios/validation/superbank-rpc-validate-get-block.js"
    run_k6 "validate:getTransactionCount" \
      "tests/k6/scenarios/validation/superbank-rpc-validate-get-transaction-count.js"
    if [[ "${VALIDATE_LATEST_BLOCKHASH:-1}" == "0" ]]; then
      skip "validate:getLatestBlockhash" "VALIDATE_LATEST_BLOCKHASH=0"
    else
      run_k6 "validate:getLatestBlockhash" \
        "tests/k6/scenarios/validation/superbank-rpc-validate-get-latest-blockhash.js"
    fi
    run_k6 "validate:getInflationReward" \
      "tests/k6/scenarios/validation/superbank-rpc-validate-get-inflation-reward.js"

    if [[ -n "${TFA_REFERENCE_RPC_URL:-}" ]]; then
      local original_reference_rpc_url="${REFERENCE_RPC_URL}"
      REFERENCE_RPC_URL="${TFA_REFERENCE_RPC_URL}"
      export REFERENCE_RPC_URL
      run_k6 "validate:getTransactionsForAddress" \
        "tests/k6/scenarios/validation/superbank-rpc-validate-get-transactions-for-address.js"
      REFERENCE_RPC_URL="${original_reference_rpc_url}"
      export REFERENCE_RPC_URL
    else
      skip "validate:getTransactionsForAddress" "TFA_REFERENCE_RPC_URL not set"
    fi
  fi

  run_k6 "validate:json-rpc batch protocol" \
    "tests/k6/scenarios/validation/superbank-rpc-validate-batch.js"
}

run_fuzz_suite() {
  run_k6 "fuzz:rpc params" \
    "tests/k6/scenarios/fuzz/fuzz-test-rpc-params.js"
  run_k6 "fuzz:official options" \
    "tests/k6/scenarios/fuzz/fuzz-options-rpc-methods.js"
}

run_replay_suite() {
  # LOG_FILE has priority over ADDRESS_FILE in the k6 helpers, so only set it for the replay test.
  if [[ -z "${LOG_FILE:-}" ]]; then
    LOG_FILE="./tests/k6/data/replay/synthetic-gsfa-replay.csv"
  fi
  export LOG_FILE
  run_k6 "replay:synthetic gsfa sample" \
    "tests/k6/scenarios/replay/replay-test.js"
  unset LOG_FILE || true
}

run_long_suite() {
  if [[ "${include_stress}" == "true" ]]; then
    for script in tests/k6/scenarios/stress/*.js; do
      if [[ "${script}" == "tests/k6/scenarios/stress/stress-test-get-inflation-reward.js" \
        && -z "${INFLATION_REWARD_EPOCH:-}" ]]; then
        skip "stress:$(basename "${script}")" "INFLATION_REWARD_EPOCH not set"
        continue
      fi
      run_k6 "stress:$(basename "${script}")" "${script}"
    done
  fi

  if [[ "${include_soak}" == "true" ]]; then
    run_k6 "soak:soak-test" "tests/k6/scenarios/soak/soak-test.js"
  fi

  if [[ "${include_spike}" == "true" ]]; then
    run_k6 "spike:spike-test" "tests/k6/scenarios/spike/spike-test.js"
  fi
}

run_basic_suite
run_validation_suite
run_fuzz_suite
run_replay_suite
run_long_suite

echo
echo "==== k6 suite complete ===="
echo "failures: ${failures}"
if [[ "${#failed[@]}" -gt 0 ]]; then
  echo "failed:"
  for name in "${failed[@]}"; do
    echo "  - ${name}"
  done
fi
if [[ "${#skipped[@]}" -gt 0 ]]; then
  echo "skipped:"
  for name in "${skipped[@]}"; do
    echo "  - ${name}"
  done
fi

if [[ "${failures}" -gt 0 ]]; then
  exit 1
fi
