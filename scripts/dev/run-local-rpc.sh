#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Copyright 2025-2026 Triton One Limited. All rights reserved.
#

set -euo pipefail

if command -v cargo >/dev/null 2>&1; then
  CARGO=cargo
elif [[ -x "${HOME}/.cargo/bin/cargo" ]]; then
  CARGO="${HOME}/.cargo/bin/cargo"
else
  echo "cargo not found (install Rust or add it to PATH)." >&2
  exit 1
fi

is_truthy() {
  case "${1:-}" in
    1|true|TRUE|True|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

to_lower() {
  printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]'
}

derive_local_table() {
  local distributed="$1"
  if [[ -z "${distributed}" ]]; then
    return 1
  fi

  if [[ "${distributed}" == *_local ]]; then
    printf '%s' "${distributed}"
    return 0
  fi

  if [[ "${distributed}" == *.* ]]; then
    local db="${distributed%.*}"
    local table="${distributed##*.}"
    printf '%s.%s_local' "${db}" "${table}"
  else
    printf '%s_local' "${distributed}"
  fi
}

if [[ -n "${SUPERBANK_RPC_FEATURES-}" ]]; then
  ${CARGO} build --release -p superbank-rpc --features "${SUPERBANK_RPC_FEATURES}"
else
  ${CARGO} build --release -p superbank-rpc
fi

# Simple helper to run the superbank RPC against a local ClickHouse.
# Override any of these env vars as needed before invoking the script.
: "${RUST_LOG:=info,clickhouse_rs=warn}"
: "${LOG_FORMAT:=plain}"
: "${RPC_HOST:=0.0.0.0}"
: "${RPC_PORT:=8899}"
: "${METRICS_HOST:=0.0.0.0}"
: "${METRICS_PORT:=9900}"
: "${RPC_MAX_BODY_BYTES:=1048576}"
: "${RPC_REQUEST_TIMEOUT_MS:=10000}"
: "${RPC_CONCURRENCY_LIMIT:=512}"
: "${HYDRATION_CPU_CONCURRENCY:=8}"

: "${CLICKHOUSE_URL:=http://localhost:8123}"
: "${CLICKHOUSE_DATABASE:=default}"
: "${CLICKHOUSE_USER:=default}"
: "${CLICKHOUSE_PASSWORD:=}"
: "${CLICKHOUSE_QUERY_TIMEOUT_MS:=8000}"
: "${CLICKHOUSE_CLUSTER:={cluster}}"
: "${CLICKHOUSE_TOPOLOGY_CONFIG:=}"
: "${CLICKHOUSE_TRANSPORT:=http}"
: "${CLICKHOUSE_SCOPE:=distributed}"
: "${CLICKHOUSE_TCP_ACCESS_CHECK_TIMEOUT_MS:=2000}"
: "${CLICKHOUSE_SHARD_FANOUT_CONCURRENCY:=8}"
: "${CLICKHOUSE_IN_CLAUSE_CHUNK:=512}"
: "${CLICKHOUSE_STARTUP_TABLE_CHECK:=exists}"

: "${CLICKHOUSE_GSFA_TABLE:=default.gsfa}"
: "${CLICKHOUSE_SIGNATURE_STATUSES_TABLE:=}"
: "${CLICKHOUSE_TOKEN_OWNER_ACTIVITY_TABLE:=default.token_owner_activity}"
: "${CLICKHOUSE_GSFA_HOT_TABLE:=default.gsfa_hot}"
: "${CLICKHOUSE_GSFA_HOT_LOCAL_TABLE:=default.gsfa_hot_local}"
: "${CLICKHOUSE_GSFA_HOT_ADDRESSES:=}"
: "${CLICKHOUSE_TRANSACTION_TABLE:=default.transactions}"
: "${CLICKHOUSE_BLOCKS_METADATA_TABLE:=default.blocks_metadata}"

: "${MAX_SIGNATURES_LIMIT:=1000}"
: "${CLICKHOUSE_QUERY_CACHE_ENABLED:=true}"
: "${CLICKHOUSE_QUERY_CACHE_TTL_SECONDS:=1}"
: "${CLICKHOUSE_QUERY_CACHE_SHARE_BETWEEN_USERS:=false}"
: "${CLICKHOUSE_GSFA_LOCAL_TABLE:=}"
: "${CLICKHOUSE_SIGNATURES_LOCAL_TABLE:=}"
: "${CLICKHOUSE_TOKEN_OWNER_ACTIVITY_LOCAL_TABLE:=}"
: "${CLICKHOUSE_TRANSACTIONS_LOCAL_TABLE:=}"
: "${CLICKHOUSE_BLOCKS_METADATA_LOCAL_TABLE:=}"

# Optional gRPC head cache (Yellowstone DragonsMouth).
# Build with: SUPERBANK_RPC_FEATURES=grpc-head-cache
# The cache is disabled unless HEAD_CACHE_ENABLED=true and DRAGONSMOUTH_ENDPOINT is set.
: "${HEAD_CACHE_ENABLED:=false}"
: "${DRAGONSMOUTH_ENDPOINT:=}"
: "${DRAGONSMOUTH_X_TOKEN:=}"
: "${HEAD_CACHE_RETAIN_SLOTS:=32}"
: "${HEAD_CACHE_MIN_COMMITMENT:=processed}"
: "${GRPC_MAX_DECODING_BYTES:=67108864}"

if is_truthy "${HEAD_CACHE_ENABLED}" && [[ -z "${DRAGONSMOUTH_ENDPOINT}" ]]; then
  echo "HEAD_CACHE_ENABLED=true but DRAGONSMOUTH_ENDPOINT is empty; head cache will be disabled." >&2
  echo "Set DRAGONSMOUTH_ENDPOINT or set HEAD_CACHE_ENABLED=false." >&2
fi

if [[ -z "${CLICKHOUSE_SIGNATURE_STATUSES_TABLE:-}" ]]; then
  if [[ "${CLICKHOUSE_TRANSACTION_TABLE}" == *"transactions_local"* ]]; then
    CLICKHOUSE_SIGNATURE_STATUSES_TABLE="${CLICKHOUSE_TRANSACTION_TABLE/transactions_local/signatures_local}"
  elif [[ "${CLICKHOUSE_TRANSACTION_TABLE}" == *"transactions"* ]]; then
    CLICKHOUSE_SIGNATURE_STATUSES_TABLE="${CLICKHOUSE_TRANSACTION_TABLE/transactions/signatures}"
  else
    CLICKHOUSE_SIGNATURE_STATUSES_TABLE="default.signatures"
  fi
fi

if [[ -z "${CLICKHOUSE_GSFA_LOCAL_TABLE:-}" ]]; then
  CLICKHOUSE_GSFA_LOCAL_TABLE="$(derive_local_table "${CLICKHOUSE_GSFA_TABLE}")"
fi
if [[ -z "${CLICKHOUSE_SIGNATURES_LOCAL_TABLE:-}" ]]; then
  CLICKHOUSE_SIGNATURES_LOCAL_TABLE="$(derive_local_table "${CLICKHOUSE_SIGNATURE_STATUSES_TABLE}")"
fi
if [[ -z "${CLICKHOUSE_TOKEN_OWNER_ACTIVITY_LOCAL_TABLE:-}" ]]; then
  CLICKHOUSE_TOKEN_OWNER_ACTIVITY_LOCAL_TABLE="$(derive_local_table "${CLICKHOUSE_TOKEN_OWNER_ACTIVITY_TABLE}")"
fi
if [[ -z "${CLICKHOUSE_TRANSACTIONS_LOCAL_TABLE:-}" ]]; then
  CLICKHOUSE_TRANSACTIONS_LOCAL_TABLE="$(derive_local_table "${CLICKHOUSE_TRANSACTION_TABLE}")"
fi
if [[ -z "${CLICKHOUSE_BLOCKS_METADATA_LOCAL_TABLE:-}" ]]; then
  CLICKHOUSE_BLOCKS_METADATA_LOCAL_TABLE="$(derive_local_table "${CLICKHOUSE_BLOCKS_METADATA_TABLE}")"
fi

CLICKHOUSE_TRANSPORT="$(to_lower "${CLICKHOUSE_TRANSPORT}")"
CLICKHOUSE_SCOPE="$(to_lower "${CLICKHOUSE_SCOPE}")"

case "${CLICKHOUSE_TRANSPORT}" in
  http|tcp) ;;
  *)
    echo "Invalid CLICKHOUSE_TRANSPORT='${CLICKHOUSE_TRANSPORT}' (expected 'http' or 'tcp')." >&2
    exit 1
    ;;
esac

case "${CLICKHOUSE_SCOPE}" in
  distributed|shard-direct) ;;
  *)
    echo "Invalid CLICKHOUSE_SCOPE='${CLICKHOUSE_SCOPE}' (expected 'distributed' or 'shard-direct')." >&2
    exit 1
    ;;
esac

if [[ "${CLICKHOUSE_TRANSPORT}" == "tcp" && "${CLICKHOUSE_SCOPE}" != "shard-direct" ]]; then
  echo "CLICKHOUSE_TRANSPORT=tcp requires CLICKHOUSE_SCOPE=shard-direct." >&2
  exit 1
fi

export RUST_LOG
export LOG_FORMAT
export RPC_HOST
export RPC_PORT
export METRICS_HOST
export METRICS_PORT
export RPC_MAX_BODY_BYTES
export RPC_REQUEST_TIMEOUT_MS
export RPC_CONCURRENCY_LIMIT
export HYDRATION_CPU_CONCURRENCY
export CLICKHOUSE_URL
export CLICKHOUSE_DATABASE
export CLICKHOUSE_USER
export CLICKHOUSE_PASSWORD
export CLICKHOUSE_QUERY_TIMEOUT_MS
export CLICKHOUSE_GSFA_TABLE
export CLICKHOUSE_GSFA_HOT_TABLE
export CLICKHOUSE_GSFA_HOT_LOCAL_TABLE
export CLICKHOUSE_GSFA_HOT_ADDRESSES
export CLICKHOUSE_SIGNATURE_STATUSES_TABLE
export CLICKHOUSE_TOKEN_OWNER_ACTIVITY_TABLE
export CLICKHOUSE_TRANSACTION_TABLE
export CLICKHOUSE_BLOCKS_METADATA_TABLE
export MAX_SIGNATURES_LIMIT
export CLICKHOUSE_QUERY_CACHE_ENABLED
export CLICKHOUSE_QUERY_CACHE_TTL_SECONDS
export CLICKHOUSE_QUERY_CACHE_SHARE_BETWEEN_USERS
export CLICKHOUSE_TRANSPORT
export CLICKHOUSE_SCOPE
export CLICKHOUSE_TCP_ACCESS_CHECK_TIMEOUT_MS
export CLICKHOUSE_CLUSTER
export CLICKHOUSE_TOPOLOGY_CONFIG
export CLICKHOUSE_SHARD_FANOUT_CONCURRENCY
export CLICKHOUSE_IN_CLAUSE_CHUNK
export CLICKHOUSE_STARTUP_TABLE_CHECK
export CLICKHOUSE_GSFA_LOCAL_TABLE
export CLICKHOUSE_SIGNATURES_LOCAL_TABLE
export CLICKHOUSE_TOKEN_OWNER_ACTIVITY_LOCAL_TABLE
export CLICKHOUSE_TRANSACTIONS_LOCAL_TABLE
export CLICKHOUSE_BLOCKS_METADATA_LOCAL_TABLE
if [[ -n "${CLICKHOUSE_SHARD_HTTP_PORT:-}" ]]; then
  export CLICKHOUSE_SHARD_HTTP_PORT
fi

export HEAD_CACHE_ENABLED
export DRAGONSMOUTH_ENDPOINT
if [[ -n "${DRAGONSMOUTH_X_TOKEN}" ]]; then
  export DRAGONSMOUTH_X_TOKEN
fi
export HEAD_CACHE_RETAIN_SLOTS
export HEAD_CACHE_MIN_COMMITMENT
export GRPC_MAX_DECODING_BYTES

# Testing
export CLICKHOUSE_QUERY_ID_PREFIX=auto
export CLICKHOUSE_GSFA_STRICT_PAGINATION=1

./target/release/superbank-rpc \
  --rpc-max-body-bytes "${RPC_MAX_BODY_BYTES}" \
  --rpc-request-timeout-ms "${RPC_REQUEST_TIMEOUT_MS}" \
  --rpc-concurrency-limit "${RPC_CONCURRENCY_LIMIT}" \
  --host "${RPC_HOST}" \
  --port "${RPC_PORT}" \
  --metrics-host "${METRICS_HOST}" \
  --metrics-port "${METRICS_PORT}" \
  --clickhouse-query-timeout-ms "${CLICKHOUSE_QUERY_TIMEOUT_MS}" \
  --clickhouse-url "${CLICKHOUSE_URL}" \
  --clickhouse-database "${CLICKHOUSE_DATABASE}" \
  --clickhouse-user "${CLICKHOUSE_USER}" \
  --clickhouse-password "${CLICKHOUSE_PASSWORD}" \
  --clickhouse-transport "${CLICKHOUSE_TRANSPORT}" \
  --clickhouse-scope "${CLICKHOUSE_SCOPE}" \
  --clickhouse-tcp-access-check-timeout-ms "${CLICKHOUSE_TCP_ACCESS_CHECK_TIMEOUT_MS}" \
  --clickhouse-cluster "${CLICKHOUSE_CLUSTER}" \
  --clickhouse-shard-fanout-concurrency "${CLICKHOUSE_SHARD_FANOUT_CONCURRENCY}" \
  --clickhouse-in-clause-chunk "${CLICKHOUSE_IN_CLAUSE_CHUNK}" \
  --clickhouse-startup-table-check "${CLICKHOUSE_STARTUP_TABLE_CHECK}" \
  --hydration-cpu-concurrency "${HYDRATION_CPU_CONCURRENCY}" \
  --max-signatures-limit "${MAX_SIGNATURES_LIMIT}"
