#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Copyright 2025-2026 Triton One Limited. All rights reserved.
#

set -euo pipefail

# Backfill missing slots per epoch via Bigtable -> ClickHouse.
# Requires superbank built with BIGTABLE_SLOT_FILE support.

# Print an error message and exit non-zero.
die() {
  echo "error: $*" >&2
  exit 1
}

# Print a warning message to stderr.
warn() {
  echo "warn: $*" >&2
}

# Validate that a numeric argument is within an inclusive range.
require_int_in_range() {
  local name="$1"
  local value="$2"
  local min="$3"
  local max="$4"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    die "$name must be an integer"
  fi
  if (( value < min || value > max )); then
    die "$name must be within ${min}-${max}"
  fi
}

# Trim leading and trailing whitespace from a string.
trim_whitespace() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf '%s' "$s"
}

# Required settings
SUPERBANK_BIN=${SUPERBANK_BIN:-./superbank}
SUPERBANK_CONFIG=${SUPERBANK_CONFIG:-./superbank.yaml}
CLICKHOUSE_DATABASE=${CLICKHOUSE_DATABASE:-default}

# Optional settings
EPOCH_START=${EPOCH_START:-1}
EPOCH_END=${EPOCH_END:-10}
SLOTS_PER_EPOCH=${SLOTS_PER_EPOCH:-432000}
OUT_DIR=${OUT_DIR:-./missing-slots}

CLICKHOUSE_HOST=${CLICKHOUSE_HOST:-localhost}
CLICKHOUSE_PORT=${CLICKHOUSE_PORT:-9000}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-default}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-}
CLICKHOUSE_HTTP_PORT=${CLICKHOUSE_HTTP_PORT:-8123}
CLICKHOUSE_URL_SCHEME=${CLICKHOUSE_URL_SCHEME:-http}
CLICKHOUSE_URL_PATH=${CLICKHOUSE_URL_PATH:-}
CLICKHOUSE_URL=${CLICKHOUSE_URL:-}
CLICKHOUSE_CLUSTER=${CLICKHOUSE_CLUSTER:-default}
CLICKHOUSE_SHARDING_TABLE=${CLICKHOUSE_SHARDING_TABLE:-transactions}
CLICKHOUSE_SHARDING_KEY_EXPR=${CLICKHOUSE_SHARDING_KEY_EXPR:-}
CLICKHOUSE_HOST_BASE=${CLICKHOUSE_HOST_BASE:-127.0.0}
CLICKHOUSE_HOST_START=${CLICKHOUSE_HOST_START:-1}
CLICKHOUSE_HOST_END=${CLICKHOUSE_HOST_END:-1}
CLICKHOUSE_TRANSACTIONS_LOCAL_TABLE=${CLICKHOUSE_TRANSACTIONS_LOCAL_TABLE:-transactions_local}
CLICKHOUSE_BLOCKS_LOCAL_TABLE=${CLICKHOUSE_BLOCKS_LOCAL_TABLE:-blocks_metadata_local}

if [[ ! -x "$SUPERBANK_BIN" ]]; then
  echo "superbank binary not found at $SUPERBANK_BIN (build it or set SUPERBANK_BIN)" >&2
  exit 1
fi

if [[ ! -f "$SUPERBANK_CONFIG" ]]; then
  echo "superbank config not found at $SUPERBANK_CONFIG (set SUPERBANK_CONFIG)" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"

CH_ARGS=(--host "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_PORT" --user "$CLICKHOUSE_USER" --database "$CLICKHOUSE_DATABASE")
if [[ -n "$CLICKHOUSE_PASSWORD" ]]; then
  CH_ARGS+=(--password "$CLICKHOUSE_PASSWORD")
fi

require_int_in_range "CLICKHOUSE_HOST_START" "$CLICKHOUSE_HOST_START" 0 255
require_int_in_range "CLICKHOUSE_HOST_END" "$CLICKHOUSE_HOST_END" 0 255
if (( CLICKHOUSE_HOST_START > CLICKHOUSE_HOST_END )); then
  die "CLICKHOUSE_HOST_START must be <= CLICKHOUSE_HOST_END"
fi
CLICKHOUSE_HOST_COUNT=$((CLICKHOUSE_HOST_END - CLICKHOUSE_HOST_START + 1))

# Execute a ClickHouse query over TCP, returning output on success.
clickhouse_query() {
  local query="$1"
  clickhouse client "${CH_ARGS[@]}" --query "$query"
}

CLICKHOUSE_URL_PREFIX=""
CLICKHOUSE_URL_SUFFIX=""
if [[ -n "$CLICKHOUSE_URL" && "$CLICKHOUSE_URL" =~ ^([^:]+://)([^@/]+@)?([^:/]+)(.*)$ ]]; then
  CLICKHOUSE_URL_PREFIX="${BASH_REMATCH[1]}${BASH_REMATCH[2]}"
  CLICKHOUSE_URL_SUFFIX="${BASH_REMATCH[4]}"
fi
if [[ -z "$CLICKHOUSE_URL_PREFIX" ]]; then
  CLICKHOUSE_URL_PREFIX="${CLICKHOUSE_URL_SCHEME}://"
  CLICKHOUSE_URL_SUFFIX=":${CLICKHOUSE_HTTP_PORT}${CLICKHOUSE_URL_PATH}"
fi

declare -A CLUSTER_SHARD_HOSTS
CLUSTER_SHARD_COUNT=0
if cluster_rows="$(clickhouse_query "SELECT shard_num, if(host_address = '', host_name, host_address) AS host FROM system.clusters WHERE cluster='${CLICKHOUSE_CLUSTER}' AND replica_num=1 ORDER BY shard_num FORMAT TabSeparated")"; then
  while IFS=$'\t' read -r shard_num host; do
    if [[ -n "$shard_num" && -n "$host" ]]; then
      CLUSTER_SHARD_HOSTS["$shard_num"]="$host"
      CLUSTER_SHARD_COUNT=$((CLUSTER_SHARD_COUNT + 1))
    fi
  done <<<"$cluster_rows"
else
  warn "unable to query system.clusters for cluster '${CLICKHOUSE_CLUSTER}', falling back to host range rotation"
fi

if [[ -z "$CLICKHOUSE_SHARDING_KEY_EXPR" ]]; then
  if shard_expr="$(clickhouse_query "SELECT nullIf(replaceRegexpOne(engine_full, '^Distributed\\([^,]+,\\s*[^,]+,\\s*[^,]+,\\s*(.*)\\)$', '\\1'), engine_full) FROM system.tables WHERE database='${CLICKHOUSE_DATABASE}' AND name='${CLICKHOUSE_SHARDING_TABLE}' FORMAT TabSeparated")"; then
    shard_expr="$(trim_whitespace "$shard_expr")"
    if [[ "$shard_expr" != "\\N" && -n "$shard_expr" ]]; then
      CLICKHOUSE_SHARDING_KEY_EXPR="$shard_expr"
    fi
  else
    warn "unable to query sharding key for ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_SHARDING_TABLE}, falling back to epoch-based routing"
  fi
fi

if [[ -n "$CLICKHOUSE_SHARDING_KEY_EXPR" && "$CLICKHOUSE_SHARDING_KEY_EXPR" =~ rand[[:alnum:]_]*[[:space:]]*\( ]]; then
  warn "sharding key is random (${CLICKHOUSE_SHARDING_KEY_EXPR}); falling back to epoch-based routing"
  CLICKHOUSE_SHARDING_KEY_EXPR=""
fi

# Resolve the shard number for an epoch using a sharding key expression.
get_shard_for_epoch() {
  local epoch="$1"
  local shard_num=""
  if [[ -n "$CLICKHOUSE_SHARDING_KEY_EXPR" ]]; then
    if shard_num="$(clickhouse_query "WITH
      toUInt64(${SLOTS_PER_EPOCH}) AS slots_per_epoch,
      toUInt64(${epoch}) AS epoch_value,
      epoch_value * slots_per_epoch AS slot,
      toUInt32(0) AS slot_idx,
      ${CLICKHOUSE_SHARDING_KEY_EXPR} AS shard_key,
      (SELECT sum(shard_weight) FROM system.clusters WHERE cluster='${CLICKHOUSE_CLUSTER}' AND replica_num=1) AS total_weight,
      shard_key % total_weight AS shard_rem
    SELECT shard_num
    FROM (
      SELECT
        shard_num,
        sum(shard_weight) OVER (ORDER BY shard_num) AS cumulative_weight
      FROM system.clusters
      WHERE cluster='${CLICKHOUSE_CLUSTER}' AND replica_num=1
    )
    WHERE shard_rem < cumulative_weight
    ORDER BY shard_num
    LIMIT 1
    FORMAT TabSeparated")"; then
      shard_num="$(trim_whitespace "$shard_num")"
      if [[ "$shard_num" =~ ^[0-9]+$ ]]; then
        printf '%s' "$shard_num"
        return 0
      fi
    fi
  fi
  return 1
}

# Choose a fallback host using shard list or host range rotation.
fallback_host_for_epoch() {
  local epoch="$1"
  local epoch_num=$((10#$epoch))
  if (( CLUSTER_SHARD_COUNT > 0 )); then
    local shard_index=$((epoch_num % CLUSTER_SHARD_COUNT))
    local shard_num=$((shard_index + 1))
    local host="${CLUSTER_SHARD_HOSTS[$shard_num]}"
    if [[ -n "$host" ]]; then
      printf '%s' "$host"
      return 0
    fi
  fi
  if (( CLICKHOUSE_HOST_COUNT > 0 )); then
    local host_octet=$((CLICKHOUSE_HOST_START + (epoch_num % CLICKHOUSE_HOST_COUNT)))
    printf '%s' "${CLICKHOUSE_HOST_BASE}.${host_octet}"
    return 0
  fi
  printf '%s' "$CLICKHOUSE_HOST"
}

for epoch in $(seq "$EPOCH_START" "$EPOCH_END"); do
  outfile="$OUT_DIR/missing-slots-epoch-${epoch}.txt"
  echo "Epoch $epoch -> $outfile"

  clickhouse client "${CH_ARGS[@]}" --query "
WITH
  ${SLOTS_PER_EPOCH} AS slots_per_epoch,
  ${epoch} AS epoch,
  epoch * slots_per_epoch AS start_slot,
  (epoch + 1) * slots_per_epoch - 1 AS end_slot,
  tx AS (
    SELECT
      slot,
      countMerge(tx_count_state) AS tx_count
    FROM default.tx_counts_by_slot
    WHERE slot BETWEEN start_slot AND end_slot
    GROUP BY slot
  )
SELECT bm.slot
FROM default.blocks_metadata AS bm
LEFT JOIN tx USING (slot)
WHERE bm.slot BETWEEN start_slot AND end_slot
  AND bm.executed_transaction_count > 0
  AND ifNull(tx.tx_count, 0) = 0
ORDER BY bm.slot
SETTINGS distributed_product_mode = 'allow'
FORMAT TSV
" > "$outfile"

  if [[ ! -s "$outfile" ]]; then
    echo "  no missing slots; skipping superbank"
    rm -f "$outfile"
    continue
  fi

  echo "  ingesting $(wc -l < "$outfile") slots"
  shard_host=""
  shard_num=""
  if shard_num="$(get_shard_for_epoch "$epoch")"; then
    shard_host="${CLUSTER_SHARD_HOSTS[$shard_num]}"
  fi
  if [[ -z "$shard_host" ]]; then
    shard_host="$(fallback_host_for_epoch "$epoch")"
  fi
  if [[ -z "$shard_host" ]]; then
    die "unable to resolve shard host for epoch ${epoch}"
  fi
  shard_url="${CLICKHOUSE_URL_PREFIX}${shard_host}${CLICKHOUSE_URL_SUFFIX}"
  echo "  routing to shard ${shard_num:-unknown} (${shard_host})"
  
  RUST_LOG="info,solana_metrics::metrics=warn" \
    CLICKHOUSE_URL="$shard_url" \
    CLICKHOUSE_TRANSACTIONS_TABLE="$CLICKHOUSE_TRANSACTIONS_LOCAL_TABLE" \
    CLICKHOUSE_BLOCKS_TABLE="$CLICKHOUSE_BLOCKS_LOCAL_TABLE" \
    CLICKHOUSE_USER="$CLICKHOUSE_USER" \
    CLICKHOUSE_PASSWORD="$CLICKHOUSE_PASSWORD" \
    "$SUPERBANK_BIN" --config "$SUPERBANK_CONFIG" --bigtable-slot-file "$outfile"
done
