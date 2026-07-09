#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Copyright 2025-2026 Triton One Limited. All rights reserved.
#

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

: "${SUPERBANK_NAMESPACE:=superbank-e2e}"
: "${SUPERBANK_KIND_CLUSTER:=superbank-e2e}"
: "${SUPERBANK_KIND_REGISTRY_PORT:=5001}"
: "${SUPERBANK_USE_LOCAL_REGISTRY:=1}"
: "${SUPERBANK_INGEST_MODE:=rpc}"
: "${SUPERBANK_INGEST_RPC_URL:=https://api.mainnet-beta.solana.com}"
: "${SUPERBANK_INGEST_RPC_FROM_SLOT:=350918000}"
: "${SUPERBANK_INGEST_SLOT_COUNT:=64}"
: "${SUPERBANK_INGEST_RPC_MAX_INFLIGHT:=2}"
: "${SUPERBANK_INGEST_RPC_RETRY_BACKOFF_MS:=1000}"
: "${SUPERBANK_CLICKHOUSE_USER:=default}"
: "${SUPERBANK_CLICKHOUSE_PASSWORD:=clickhouse-ci}"
: "${SUPERBANK_E2E_TILT_TIMEOUT:=65m}"
: "${SUPERBANK_E2E_TILT_READINESS_TIMEOUT:=35m}"
: "${SUPERBANK_E2E_INGEST_TIMEOUT:=20m}"
: "${SUPERBANK_E2E_RPC_PORT:=8899}"
: "${SUPERBANK_E2E_ARTIFACT_DIR:=artifacts/release-e2e}"
: "${SUPERBANK_E2E_K6_ARGS:=--stress --soak --spike}"
: "${SUPERBANK_E2E_REFERENCE_RPC_URL:=self}"
: "${SUPERBANK_E2E_CLEANUP:=1}"
: "${VALIDATE_LATEST_BLOCKHASH:=0}"

if [[ "${SUPERBANK_USE_LOCAL_REGISTRY}" == "1" ]]; then
  : "${LOCAL_REGISTRY_HOST:=localhost:${SUPERBANK_KIND_REGISTRY_PORT}}"
  export LOCAL_REGISTRY_HOST
fi

export SUPERBANK_NAMESPACE
export SUPERBANK_KIND_CLUSTER
export SUPERBANK_KIND_REGISTRY_PORT
export SUPERBANK_USE_LOCAL_REGISTRY
export SUPERBANK_INGEST_MODE
export SUPERBANK_INGEST_RPC_URL
export SUPERBANK_INGEST_RPC_FROM_SLOT
export SUPERBANK_INGEST_SLOT_COUNT
export SUPERBANK_INGEST_RPC_MAX_INFLIGHT
export SUPERBANK_INGEST_RPC_RETRY_BACKOFF_MS
export SUPERBANK_CLICKHOUSE_USER
export SUPERBANK_CLICKHOUSE_PASSWORD
export SUPERBANK_E2E_TILT_TIMEOUT
export SUPERBANK_E2E_TILT_READINESS_TIMEOUT

if [[ "${SUPERBANK_INGEST_MODE}" != "rpc" ]]; then
  echo "release E2E currently expects SUPERBANK_INGEST_MODE=rpc (got: ${SUPERBANK_INGEST_MODE})" >&2
  exit 2
fi

ARTIFACT_DIR="${ROOT_DIR}/${SUPERBANK_E2E_ARTIFACT_DIR}"
POOL_DIR="${ARTIFACT_DIR}/pools"
mkdir -p "${POOL_DIR}"

PORT_FORWARD_PID=""

log() {
  printf '[release-e2e] %s\n' "$*"
}

require_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "required command not found: ${cmd}" >&2
    exit 1
  fi
}

collect_diagnostics() {
  log "collecting diagnostics"
  mkdir -p "${ARTIFACT_DIR}/diagnostics"

  kubectl -n "${SUPERBANK_NAMESPACE}" get all -o wide \
    >"${ARTIFACT_DIR}/diagnostics/kubectl-get-all.txt" 2>&1 || true
  kubectl -n "${SUPERBANK_NAMESPACE}" describe pods \
    >"${ARTIFACT_DIR}/diagnostics/kubectl-describe-pods.txt" 2>&1 || true

  for resource in \
    job/clickhouse-ddl \
    job/superbank-ingest-rpc \
    deployment/superbank-rpc
  do
    local safe_name
    safe_name="${resource//\//-}"
    kubectl -n "${SUPERBANK_NAMESPACE}" logs "${resource}" --all-containers=true \
      >"${ARTIFACT_DIR}/diagnostics/${safe_name}.log" 2>&1 || true
  done
}

cleanup() {
  local status=$?

  if [[ "${status}" -ne 0 ]]; then
    collect_diagnostics || true
  fi

  if [[ -n "${PORT_FORWARD_PID}" ]]; then
    kill "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
    wait "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
  fi

  if [[ "${SUPERBANK_E2E_CLEANUP}" == "1" ]]; then
    tilt down >/dev/null 2>&1 || true
    kind delete cluster --name "${SUPERBANK_KIND_CLUSTER}" >/dev/null 2>&1 || true
  fi

  exit "${status}"
}
trap cleanup EXIT

for cmd in docker kind kubectl tilt k6 cargo curl python3; do
  require_cmd "${cmd}"
done

log "setting up Kind/Tilt prerequisites"
SUPERBANK_DEV_IN_NIX=1 scripts/dev/setup-tilt.sh \
  --cluster "${SUPERBANK_KIND_CLUSTER}" \
  --namespace "${SUPERBANK_NAMESPACE}"

log "running Tilt CI stack"
tilt ci \
  --timeout "${SUPERBANK_E2E_TILT_TIMEOUT}" \
  --output-snapshot-on-exit "${ARTIFACT_DIR}/tilt-snapshot.json"

log "verifying Kubernetes resources"
kubectl -n "${SUPERBANK_NAMESPACE}" wait --for=condition=Ready pod -l app=clickhouse --timeout=300s
kubectl -n "${SUPERBANK_NAMESPACE}" wait --for=condition=complete job/clickhouse-ddl --timeout=300s
kubectl -n "${SUPERBANK_NAMESPACE}" wait --for=condition=available deployment/superbank-rpc --timeout=300s
kubectl -n "${SUPERBANK_NAMESPACE}" wait --for=condition=complete job/superbank-ingest-rpc --timeout="${SUPERBANK_E2E_INGEST_TIMEOUT}"

CLICKHOUSE_POD="$(kubectl -n "${SUPERBANK_NAMESPACE}" get pod -l app=clickhouse -o jsonpath='{.items[0].metadata.name}')"

ch_query() {
  local query="$1"
  kubectl -n "${SUPERBANK_NAMESPACE}" exec "${CLICKHOUSE_POD}" -c clickhouse -- \
    clickhouse-client \
      --user "${SUPERBANK_CLICKHOUSE_USER}" \
      --password "${SUPERBANK_CLICKHOUSE_PASSWORD}" \
      --query "${query}"
}

TRANSACTION_COUNT="$(ch_query "SELECT count() FROM default.transactions FORMAT TabSeparated")"
BLOCK_COUNT="$(ch_query "SELECT count() FROM default.blocks_metadata FORMAT TabSeparated")"
GSFA_COUNT="$(ch_query "SELECT count() FROM default.gsfa FORMAT TabSeparated")"

cat >"${ARTIFACT_DIR}/clickhouse-counts.txt" <<EOF
transactions=${TRANSACTION_COUNT}
blocks_metadata=${BLOCK_COUNT}
gsfa=${GSFA_COUNT}
EOF

if [[ "${TRANSACTION_COUNT}" == "0" || "${BLOCK_COUNT}" == "0" ]]; then
  echo "Tilt ingestion completed without enough ClickHouse data for release E2E" >&2
  cat "${ARTIFACT_DIR}/clickhouse-counts.txt" >&2
  exit 1
fi

log "building k6 pools from ingested ClickHouse data"
ch_query "
SELECT base58Encode(address)
FROM
(
  SELECT address, max(slot) AS last_slot
  FROM default.gsfa
  GROUP BY address
  ORDER BY last_slot DESC
  LIMIT 200
)
FORMAT TabSeparated
" >"${POOL_DIR}/addresses.txt"

if [[ ! -s "${POOL_DIR}/addresses.txt" ]]; then
  ch_query "
SELECT base58Encode(address)
FROM
(
  SELECT arrayJoin(tx_account_keys) AS address, max(slot) AS last_slot
  FROM default.transactions
  GROUP BY address
  ORDER BY last_slot DESC
  LIMIT 200
)
FORMAT TabSeparated
" >"${POOL_DIR}/addresses.txt"
fi

ch_query "
SELECT base58Encode(signature)
FROM default.transactions
ORDER BY slot DESC, slot_idx DESC
LIMIT 200
FORMAT TabSeparated
" >"${POOL_DIR}/signatures.txt"

ch_query "
SELECT slot
FROM default.blocks_metadata
ORDER BY slot DESC
LIMIT 200
FORMAT TabSeparated
" >"${POOL_DIR}/slots.txt"

for pool in addresses signatures slots; do
  if [[ ! -s "${POOL_DIR}/${pool}.txt" ]]; then
    echo "generated k6 pool is empty: ${POOL_DIR}/${pool}.txt" >&2
    exit 1
  fi
done

log "starting superbank-rpc port-forward"
kubectl -n "${SUPERBANK_NAMESPACE}" port-forward "svc/superbank-rpc" \
  "${SUPERBANK_E2E_RPC_PORT}:8899" \
  >"${ARTIFACT_DIR}/port-forward.log" 2>&1 &
PORT_FORWARD_PID=$!

RPC_URL="http://127.0.0.1:${SUPERBANK_E2E_RPC_PORT}"
export RPC_URL

payload='{"jsonrpc":"2.0","id":1,"method":"getFirstAvailableBlock"}'
for _ in $(seq 1 120); do
  if curl -fsS --max-time 2 \
    -H 'content-type: application/json' \
    -d "${payload}" \
    "${RPC_URL}" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

curl -fsS --max-time 5 \
  -H 'content-type: application/json' \
  -d "${payload}" \
  "${RPC_URL}" \
  >"${ARTIFACT_DIR}/rpc-smoke-getFirstAvailableBlock.json"

export ADDRESS_FILE="${POOL_DIR}/addresses.txt"
export SIGNATURE_FILE="${POOL_DIR}/signatures.txt"
export SLOT_FILE="${POOL_DIR}/slots.txt"
export HOT_ADDRESS="${HOT_ADDRESS:-$(head -n 1 "${ADDRESS_FILE}")}"
export TFA_REFERENCE_RPC_URL="${TFA_REFERENCE_RPC_URL:-${RPC_URL}}"
export TBA_REFERENCE_RPC_URL="${TBA_REFERENCE_RPC_URL:-${RPC_URL}}"

if [[ "${SUPERBANK_E2E_REFERENCE_RPC_URL}" == "self" ]]; then
  export REFERENCE_RPC_URL="${RPC_URL}"
elif [[ -n "${SUPERBANK_E2E_REFERENCE_RPC_URL}" ]]; then
  export REFERENCE_RPC_URL="${SUPERBANK_E2E_REFERENCE_RPC_URL}"
fi

: "${VUS:=2}"
: "${DURATION:=15s}"
: "${STRESS_VUS:=2}"
: "${STRESS_DURATION:=15s}"
: "${STRESS_RAMP_UP:=2s}"
: "${STRESS_RAMP_DOWN:=2s}"
: "${SOAK_VUS:=2}"
: "${SPIKE_BASELINE_VUS:=1}"
: "${SPIKE_VUS:=2}"
: "${SPIKE_BASELINE_DURATION:=3s}"
: "${SPIKE_RAMP_UP:=2s}"
: "${SPIKE_DURATION:=10s}"
: "${SPIKE_RECOVERY_DURATION:=3s}"
: "${SPIKE_RAMP_DOWN:=2s}"
: "${REPLAY_VUS:=1}"
: "${HOT_MAX_PAGES:=1}"
: "${BLOCK_MAX_SUPPORTED_TX_VERSION:=0}"

export VUS
export DURATION
export STRESS_VUS
export STRESS_DURATION
export STRESS_RAMP_UP
export STRESS_RAMP_DOWN
export SOAK_VUS
export SPIKE_BASELINE_VUS
export SPIKE_VUS
export SPIKE_BASELINE_DURATION
export SPIKE_RAMP_UP
export SPIKE_DURATION
export SPIKE_RECOVERY_DURATION
export SPIKE_RAMP_DOWN
export REPLAY_VUS
export HOT_MAX_PAGES
export VALIDATE_LATEST_BLOCKHASH
export BLOCK_MAX_SUPPORTED_TX_VERSION

log "running k6 release suite"
# shellcheck disable=SC2086
scripts/test/run-k6.sh ${SUPERBANK_E2E_K6_ARGS} 2>&1 | tee "${ARTIFACT_DIR}/k6-release-suite.log"

log "release E2E completed"
