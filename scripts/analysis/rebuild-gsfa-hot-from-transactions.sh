#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Copyright 2025-2026 Triton One Limited. All rights reserved.
#

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/analysis/rebuild-gsfa-hot-from-transactions.sh <start_epoch> <end_epoch>

Rebuilds GSFA hot rows from transactions_local in epoch windows, where:
  epoch = intDiv(slot, 432000)
Runs one worker per primary shard host in parallel.

Environment:
  CH_HOST                         ClickHouse host used to query system.clusters (default: localhost)
  CH_PORT                         ClickHouse TCP port for shard hosts (default: 9000)
  CH_USER                         ClickHouse user (default: default)
  CH_PASS                         ClickHouse password (default: empty)
  SOURCE_CLUSTER                  Source cluster used to discover shard hosts (default: default)
  SOURCE_DATABASE                 Source database (default: default)
  SOURCE_TRANSACTIONS_LOCAL_TABLE Source local tx table (default: transactions_local)
  GSFA_HOT_TABLE                  Destination hot table (default: default.gsfa_hot)
  GSFA_HOT_ADDRESS                Hot address to rebuild (default: EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v)
EOF
}

is_uint() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

log() {
  printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

trim_whitespace() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf '%s' "$s"
}

build_query() {
  local epoch="$1"

  cat <<SQL
INSERT INTO ${GSFA_HOT_TABLE}
WITH
    [
        CAST(base58Decode('Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo') AS FixedString(32)),
        CAST(base58Decode('MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr') AS FixedString(32)),
        CAST(base58Decode('Memo4c2pN8afCj432Lb7RMVKi9PbQnnW7ewFFaV3oAH') AS FixedString(32))
    ] AS memo_program_ids,
    [
        CAST(base58Decode('11111111111111111111111111111111') AS FixedString(32)),
        CAST(base58Decode('Vote111111111111111111111111111111111111111') AS FixedString(32)),
        CAST(base58Decode('SysvarC1ock11111111111111111111111111111111') AS FixedString(32)),
        CAST(base58Decode('SysvarS1otHashes111111111111111111111111111') AS FixedString(32))
    ] AS gsfa_ignored_addresses,
    CAST(base58Decode('${GSFA_HOT_ADDRESS}') AS FixedString(32)) AS gsfa_hot_address
SELECT
    gsfa_hot_address AS address,
    signature,
    slot,
    slot_idx,
    memo,
    if(meta_status_ok = 1, NULL, meta_err) AS err,
    block_time
FROM
(
    WITH arrayConcat(tx_account_keys, meta_loaded_addresses_writable, meta_loaded_addresses_readonly) AS account_keys_all
    SELECT
        signature,
        slot,
        slot_idx,
        block_time,
        meta_status_ok,
        meta_err,
        tx_instructions_program_id_index,
        tx_instructions_data,
        nullIf(
            arrayStringConcat(
                arrayMap(
                    x -> x.2,
                    arrayFilter(
                        x -> has(memo_program_ids, x.1) AND isValidUTF8(x.2),
                        arrayZip(
                            arrayMap(idx -> arrayElement(account_keys_all, idx + 1), tx_instructions_program_id_index),
                            tx_instructions_data
                        )
                    )
                ),
                '; '
            ),
            ''
        ) AS memo
    FROM ${SOURCE_DATABASE}.${SOURCE_TRANSACTIONS_LOCAL_TABLE}
    WHERE intDiv(slot, 432000) = ${epoch}
      AND NOT has(gsfa_ignored_addresses, gsfa_hot_address)
      AND (
          has(tx_account_keys, gsfa_hot_address)
          OR has(meta_loaded_addresses_writable, gsfa_hot_address)
          OR has(meta_loaded_addresses_readonly, gsfa_hot_address)
      )
)
SETTINGS
    max_execution_time = 0,
    max_execution_time_leaf = 0,
    insert_distributed_sync = 1
SQL
}

clickhouse_query() {
  local query="$1"
  clickhouse-client "${CH_BASE_ARGS[@]}" -q "$query"
}

discover_source_hosts() {
  local rows shard_num host

  rows="$(clickhouse_query "SELECT shard_num, if(host_address = '', host_name, host_address) AS host FROM system.clusters WHERE cluster='${SOURCE_CLUSTER}' ORDER BY shard_num, replica_num LIMIT 1 BY shard_num FORMAT TabSeparated")" || {
    echo "error: unable to query system.clusters for cluster '${SOURCE_CLUSTER}'" >&2
    exit 1
  }

  declare -g -a SOURCE_SHARD_NUMS=()
  declare -g -a SOURCE_HOSTS=()

  while IFS=$'\t' read -r shard_num host; do
    shard_num="$(trim_whitespace "${shard_num:-}")"
    host="$(trim_whitespace "${host:-}")"

    if [[ -z "$shard_num" || -z "$host" ]]; then
      continue
    fi

    SOURCE_SHARD_NUMS+=("$shard_num")
    SOURCE_HOSTS+=("$host")
  done <<< "$rows"

  if (( ${#SOURCE_HOSTS[@]} == 0 )); then
    echo "error: cluster '${SOURCE_CLUSTER}' has no shard hosts" >&2
    exit 1
  fi
}

run_epoch_on_host() {
  local epoch="$1"
  local shard_num="$2"
  local host="$3"
  local host_args=(--host "$host" --port "$CH_PORT" --user "$CH_USER")
  if [[ -n "$CH_PASS" ]]; then
    host_args+=(--password "$CH_PASS")
  fi

  log "[epoch=${epoch} shard=${shard_num} host=${host}] start target=${GSFA_HOT_TABLE} hot_address=${GSFA_HOT_ADDRESS}"
  clickhouse-client "${host_args[@]}" -q "$(build_query "$epoch")"
  log "[epoch=${epoch} shard=${shard_num} host=${host}] ok"
}

run_host_worker() {
  local shard_num="$1"
  local host="$2"
  local epoch

  for epoch in $(seq "$START_EPOCH" "$END_EPOCH"); do
    run_epoch_on_host "$epoch" "$shard_num" "$host"
  done
}

if [[ $# -ne 2 ]]; then
  usage
  exit 1
fi

START_EPOCH="$1"
END_EPOCH="$2"

if ! is_uint "$START_EPOCH" || ! is_uint "$END_EPOCH"; then
  echo "error: start_epoch and end_epoch must be non-negative integers" >&2
  exit 1
fi
if (( START_EPOCH > END_EPOCH )); then
  echo "error: start_epoch must be <= end_epoch" >&2
  exit 1
fi

CH_HOST="${CH_HOST:-localhost}"
CH_PORT="${CH_PORT:-9000}"
CH_USER="${CH_USER:-default}"
CH_PASS="${CH_PASS:-}"
SOURCE_CLUSTER="${SOURCE_CLUSTER:-default}"
SOURCE_DATABASE="${SOURCE_DATABASE:-default}"
SOURCE_TRANSACTIONS_LOCAL_TABLE="${SOURCE_TRANSACTIONS_LOCAL_TABLE:-transactions_local}"
GSFA_HOT_TABLE="${GSFA_HOT_TABLE:-default.gsfa_hot}"
GSFA_HOT_ADDRESS="${GSFA_HOT_ADDRESS:-EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v}"

if [[ -z "$GSFA_HOT_ADDRESS" ]]; then
  echo "error: GSFA_HOT_ADDRESS must be non-empty" >&2
  exit 1
fi

CH_BASE_ARGS=(--host "$CH_HOST" --port "$CH_PORT" --user "$CH_USER")
if [[ -n "$CH_PASS" ]]; then
  CH_BASE_ARGS+=(--password "$CH_PASS")
fi

discover_source_hosts
log "discovered source hosts cluster=${SOURCE_CLUSTER} count=${#SOURCE_HOSTS[@]} hosts=${SOURCE_HOSTS[*]} hot_address=${GSFA_HOT_ADDRESS}"

cleanup() {
  jobs -pr | xargs -r kill || true
}
trap cleanup INT TERM

declare -a worker_pids=()
declare -a worker_labels=()
fail=0

for idx in "${!SOURCE_HOSTS[@]}"; do
  run_host_worker "${SOURCE_SHARD_NUMS[$idx]}" "${SOURCE_HOSTS[$idx]}" &
  worker_pids+=("$!")
  worker_labels+=("shard=${SOURCE_SHARD_NUMS[$idx]} host=${SOURCE_HOSTS[$idx]}")
done

for idx in "${!worker_pids[@]}"; do
  if ! wait "${worker_pids[$idx]}"; then
    log "[${worker_labels[$idx]}] failed"
    fail=1
    cleanup
  fi
done

if (( fail != 0 )); then
  exit 1
fi

log "complete start_epoch=${START_EPOCH} end_epoch=${END_EPOCH}"
