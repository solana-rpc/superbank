#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Copyright 2025-2026 Triton One Limited. All rights reserved.
#

set -euo pipefail

# Compare Bigtable slot inventory for epoch(s) against ClickHouse.
#
# Assumptions:
# - RPC_URL points to a Solana RPC node that can serve getBlocks for the requested epoch.
# - For historical epochs, getBlocks is backed by Bigtable (so slots reflect Bigtable contents).
# - ClickHouse tables follow ddl/cluster/blocks_metadata.sql (slot is UInt64, partitioned by intDiv(slot, 432000)).
# - CLICKHOUSE_CLUSTER identifies the cluster that owns CLICKHOUSE_BLOCKS_LOCAL_TABLE.
#
# Failure modes to consider:
# - RPC nodes may rate-limit or cap getBlocks responses; tune RPC_BLOCK_CHUNK_SLOTS and retries.
# - Incorrect epoch -> slot mapping if RPC is unavailable; use RPC_URL to resolve epoch schedule.
# - Missing or unavailable shards; the ClickHouse query fails instead of reporting a partial cluster as missing slots.

die() {
  echo "error: $*" >&2
  exit 1
}

warn() {
  echo "warn: $*" >&2
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

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

trim_whitespace() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf '%s' "$s"
}

resolve_table_name() {
  local table="$1"
  if [[ "$table" == *.* ]]; then
    printf '%s' "$table"
  else
    printf '%s.%s' "$CLICKHOUSE_DATABASE" "$table"
  fi
}

# Required settings
RPC_URL=${RPC_URL:-}

# Optional settings
EPOCH_START=${EPOCH_START:-1}
EPOCH_END=${EPOCH_END:-1}
OUT_DIR=${OUT_DIR:-./missing-clickhouse-slots}

RPC_TIMEOUT_SECS=${RPC_TIMEOUT_SECS:-30}
RPC_RETRIES=${RPC_RETRIES:-3}
RPC_BACKOFF_SECS=${RPC_BACKOFF_SECS:-0.5}
RPC_BLOCK_CHUNK_SLOTS=${RPC_BLOCK_CHUNK_SLOTS:-2000}
RPC_MIN_CHUNK_SLOTS=${RPC_MIN_CHUNK_SLOTS:-200}
RPC_CHUNK_SLEEP_MS=${RPC_CHUNK_SLEEP_MS:-0}
RPC_LOG_EVERY_CHUNKS=${RPC_LOG_EVERY_CHUNKS:-10}
RPC_REQUEST_ID_START=${RPC_REQUEST_ID_START:-1}
REFRESH_SLOTS=${REFRESH_SLOTS:-0}

CLICKHOUSE_HOST=${CLICKHOUSE_HOST:-localhost}
CLICKHOUSE_PORT=${CLICKHOUSE_PORT:-9000}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-default}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-}
CLICKHOUSE_DATABASE=${CLICKHOUSE_DATABASE:-default}
CLICKHOUSE_CLUSTER=${CLICKHOUSE_CLUSTER:-default}
CLICKHOUSE_BLOCKS_LOCAL_TABLE=${CLICKHOUSE_BLOCKS_LOCAL_TABLE:-blocks_metadata_local}

if [[ -z "$RPC_URL" ]]; then
  die "RPC_URL is required (used to query getBlocks and epoch schedule)"
fi

require_cmd python3
require_cmd clickhouse

require_int_in_range "EPOCH_START" "$EPOCH_START" 0 100000000
require_int_in_range "EPOCH_END" "$EPOCH_END" 0 100000000
if (( EPOCH_START > EPOCH_END )); then
  die "EPOCH_START must be <= EPOCH_END"
fi

require_int_in_range "RPC_TIMEOUT_SECS" "$RPC_TIMEOUT_SECS" 1 3600
require_int_in_range "RPC_RETRIES" "$RPC_RETRIES" 0 100
require_int_in_range "RPC_BLOCK_CHUNK_SLOTS" "$RPC_BLOCK_CHUNK_SLOTS" 1 1000000
require_int_in_range "RPC_MIN_CHUNK_SLOTS" "$RPC_MIN_CHUNK_SLOTS" 1 1000000
require_int_in_range "RPC_CHUNK_SLEEP_MS" "$RPC_CHUNK_SLEEP_MS" 0 600000
require_int_in_range "RPC_LOG_EVERY_CHUNKS" "$RPC_LOG_EVERY_CHUNKS" 0 1000000
require_int_in_range "RPC_REQUEST_ID_START" "$RPC_REQUEST_ID_START" 0 1000000000
if (( RPC_MIN_CHUNK_SLOTS > RPC_BLOCK_CHUNK_SLOTS )); then
  die "RPC_MIN_CHUNK_SLOTS must be <= RPC_BLOCK_CHUNK_SLOTS"
fi

mkdir -p "$OUT_DIR"

export RPC_URL RPC_TIMEOUT_SECS RPC_RETRIES RPC_BACKOFF_SECS RPC_REQUEST_ID_START RPC_BLOCK_CHUNK_SLOTS RPC_MIN_CHUNK_SLOTS RPC_CHUNK_SLEEP_MS RPC_LOG_EVERY_CHUNKS

CH_ARGS=(--host "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_PORT" --user "$CLICKHOUSE_USER" --database "$CLICKHOUSE_DATABASE")
if [[ -n "$CLICKHOUSE_PASSWORD" ]]; then
  CH_ARGS+=(--password "$CLICKHOUSE_PASSWORD")
fi

clickhouse_query() {
  local query="$1"
  clickhouse client "${CH_ARGS[@]}" --query "$query"
}

cluster_row_count="$(clickhouse_query "SELECT count() FROM system.clusters WHERE cluster='${CLICKHOUSE_CLUSTER}' FORMAT TabSeparated")" \
  || die "unable to query system.clusters for cluster '${CLICKHOUSE_CLUSTER}'"
cluster_row_count="$(trim_whitespace "$cluster_row_count")"
if [[ ! "$cluster_row_count" =~ ^[0-9]+$ ]] || (( cluster_row_count == 0 )); then
  die "cluster '${CLICKHOUSE_CLUSTER}' was not found on ${CLICKHOUSE_HOST}"
fi

blocks_table="$(resolve_table_name "$CLICKHOUSE_BLOCKS_LOCAL_TABLE")"

for epoch in $(seq "$EPOCH_START" "$EPOCH_END"); do
  slots_file="$OUT_DIR/bigtable-slots-epoch-${epoch}.txt"
  missing_file="$OUT_DIR/missing-clickhouse-slots-epoch-${epoch}.txt"

  if [[ -s "$slots_file" && "$REFRESH_SLOTS" == "0" ]]; then
    echo "Epoch $epoch -> reusing $slots_file"
    range_line="$(EPOCH="$epoch" python3 - <<'PY'
import json
import os
import sys
import time
import urllib.error
import urllib.request
import socket

def rpc_call(url, method, params, request_id, timeout_s, retries, backoff_s):
    payload = json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    last_err = None
    for attempt in range(retries + 1):
        if attempt > 0:
            time.sleep(backoff_s * attempt)
        try:
            req = urllib.request.Request(url, data=payload, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                raw = resp.read()
            body = json.loads(raw.decode("utf-8"))
            if isinstance(body, dict) and body.get("error"):
                raise RuntimeError(body["error"])
            return body.get("result")
        except (urllib.error.HTTPError, urllib.error.URLError, socket.timeout, json.JSONDecodeError, RuntimeError) as exc:
            last_err = exc
            continue
    raise RuntimeError(f"RPC call failed after {retries + 1} attempts: {last_err}")

def epoch_range(schedule, epoch):
    slots_per_epoch = int(schedule["slotsPerEpoch"])
    warmup = bool(schedule.get("warmup", False))
    if not warmup:
        start = epoch * slots_per_epoch
        end = start + slots_per_epoch - 1
        return start, end
    first_normal_epoch = int(schedule["firstNormalEpoch"])
    first_normal_slot = int(schedule["firstNormalSlot"])
    if epoch >= first_normal_epoch:
        start = first_normal_slot + (epoch - first_normal_epoch) * slots_per_epoch
        end = start + slots_per_epoch - 1
        return start, end
    denom = (1 << first_normal_epoch) - 1
    if denom <= 0:
        raise RuntimeError("invalid epoch schedule denominator")
    min_slots = first_normal_slot // denom
    start = min_slots * ((1 << epoch) - 1)
    slots_in_epoch = min_slots * (1 << epoch)
    end = start + slots_in_epoch - 1
    return start, end

rpc_url = os.environ["RPC_URL"]
epoch = int(os.environ["EPOCH"])
timeout_s = float(os.environ.get("RPC_TIMEOUT_SECS", "30"))
retries = int(os.environ.get("RPC_RETRIES", "3"))
backoff_s = float(os.environ.get("RPC_BACKOFF_SECS", "0.5"))
request_id = int(os.environ.get("RPC_REQUEST_ID_START", "1"))

schedule = rpc_call(rpc_url, "getEpochSchedule", [], request_id, timeout_s, retries, backoff_s)
start_slot, end_slot = epoch_range(schedule, epoch)
print(f"{start_slot} {end_slot}")
PY
)" || die "failed to resolve slot range for epoch ${epoch}"
  else
    echo "Epoch $epoch -> fetching slots via RPC"
    range_line="$(EPOCH="$epoch" OUTFILE="$slots_file" RPC_URL="$RPC_URL" RPC_TIMEOUT_SECS="$RPC_TIMEOUT_SECS" RPC_RETRIES="$RPC_RETRIES" RPC_BACKOFF_SECS="$RPC_BACKOFF_SECS" RPC_BLOCK_CHUNK_SLOTS="$RPC_BLOCK_CHUNK_SLOTS" RPC_MIN_CHUNK_SLOTS="$RPC_MIN_CHUNK_SLOTS" RPC_CHUNK_SLEEP_MS="$RPC_CHUNK_SLEEP_MS" RPC_LOG_EVERY_CHUNKS="$RPC_LOG_EVERY_CHUNKS" RPC_REQUEST_ID_START="$RPC_REQUEST_ID_START" python3 - <<'PY'
import json
import os
import sys
import time
import urllib.error
import urllib.request
import socket

def rpc_call(url, method, params, request_id, timeout_s, retries, backoff_s):
    payload = json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    last_err = None
    for attempt in range(retries + 1):
        if attempt > 0:
            time.sleep(backoff_s * attempt)
        try:
            req = urllib.request.Request(url, data=payload, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                raw = resp.read()
            body = json.loads(raw.decode("utf-8"))
            if isinstance(body, dict) and body.get("error"):
                raise RuntimeError(body["error"])
            return body.get("result")
        except (urllib.error.HTTPError, urllib.error.URLError, socket.timeout, json.JSONDecodeError, RuntimeError) as exc:
            last_err = exc
            continue
    raise RuntimeError(f"RPC call failed after {retries + 1} attempts: {last_err}")

def epoch_range(schedule, epoch):
    slots_per_epoch = int(schedule["slotsPerEpoch"])
    warmup = bool(schedule.get("warmup", False))
    if not warmup:
        start = epoch * slots_per_epoch
        end = start + slots_per_epoch - 1
        return start, end
    first_normal_epoch = int(schedule["firstNormalEpoch"])
    first_normal_slot = int(schedule["firstNormalSlot"])
    if epoch >= first_normal_epoch:
        start = first_normal_slot + (epoch - first_normal_epoch) * slots_per_epoch
        end = start + slots_per_epoch - 1
        return start, end
    denom = (1 << first_normal_epoch) - 1
    if denom <= 0:
        raise RuntimeError("invalid epoch schedule denominator")
    min_slots = first_normal_slot // denom
    start = min_slots * ((1 << epoch) - 1)
    slots_in_epoch = min_slots * (1 << epoch)
    end = start + slots_in_epoch - 1
    return start, end

def get_blocks(url, start_slot, end_slot, chunk_size, min_chunk, sleep_ms, log_every, request_id, timeout_s, retries, backoff_s, out):
    cursor = start_slot
    total = 0
    chunk_index = 0
    while cursor <= end_slot:
        chunk_end = min(end_slot, cursor + chunk_size - 1)
        chunk_index += 1
        if log_every and chunk_index % log_every == 0:
            print(f"  requesting slots {cursor}-{chunk_end} (chunk_size={chunk_size})", file=sys.stderr)
        try:
            slots = rpc_call(url, "getBlocks", [cursor, chunk_end], request_id, timeout_s, retries, backoff_s)
        except Exception as exc:
            if chunk_size > min_chunk:
                new_size = max(min_chunk, chunk_size // 2)
                print(
                    f"  warn: getBlocks {cursor}-{chunk_end} failed ({exc}); reducing chunk size to {new_size}",
                    file=sys.stderr,
                )
                chunk_size = new_size
                continue
            raise
        request_id += 1
        if not slots:
            cursor = chunk_end + 1
            continue
        for slot in slots:
            out.write(f"{int(slot)}\n")
            total += 1
        out.flush()
        cursor = chunk_end + 1
        if total and total % 100000 == 0:
            print(f"  fetched {total} slots...", file=sys.stderr)
        if sleep_ms:
            time.sleep(sleep_ms / 1000.0)
    return total

rpc_url = os.environ["RPC_URL"]
epoch = int(os.environ["EPOCH"])
outfile = os.environ["OUTFILE"]
timeout_s = float(os.environ.get("RPC_TIMEOUT_SECS", "30"))
retries = int(os.environ.get("RPC_RETRIES", "3"))
backoff_s = float(os.environ.get("RPC_BACKOFF_SECS", "0.5"))
chunk_size = int(os.environ.get("RPC_BLOCK_CHUNK_SLOTS", "2000"))
min_chunk = int(os.environ.get("RPC_MIN_CHUNK_SLOTS", "200"))
sleep_ms = int(os.environ.get("RPC_CHUNK_SLEEP_MS", "0"))
log_every = int(os.environ.get("RPC_LOG_EVERY_CHUNKS", "10"))
request_id = int(os.environ.get("RPC_REQUEST_ID_START", "1"))

schedule = rpc_call(rpc_url, "getEpochSchedule", [], request_id, timeout_s, retries, backoff_s)
request_id += 1
start_slot, end_slot = epoch_range(schedule, epoch)

with open(outfile, "w", encoding="utf-8") as fh:
    if min_chunk > chunk_size:
        min_chunk = chunk_size
    total = get_blocks(
        rpc_url,
        start_slot,
        end_slot,
        chunk_size,
        min_chunk,
        sleep_ms,
        log_every,
        request_id,
        timeout_s,
        retries,
        backoff_s,
        fh,
    )

print(f"{start_slot} {end_slot}")
print(f"  wrote {total} slots to {outfile}", file=sys.stderr)
PY
)" || die "failed to fetch slots for epoch ${epoch}"
  fi

  start_slot="$(trim_whitespace "${range_line%% *}")"
  end_slot="$(trim_whitespace "${range_line#* }")"
  if [[ -z "$start_slot" || -z "$end_slot" ]]; then
    die "unable to parse slot range for epoch ${epoch}"
  fi

  if [[ ! -s "$slots_file" ]]; then
    warn "no slots found for epoch ${epoch} (start=${start_slot} end=${end_slot}); skipping"
    rm -f "$slots_file"
    continue
  fi

  echo "  checking ClickHouse cluster ${CLICKHOUSE_CLUSTER} via ${CLICKHOUSE_HOST}"

  ch_slots_file="$OUT_DIR/clickhouse-slots-epoch-${epoch}.txt"
  clickhouse_query "
WITH
  toUInt64(${start_slot}) AS start_slot,
  toUInt64(${end_slot}) AS end_slot
SELECT slot
FROM cluster('${CLICKHOUSE_CLUSTER}', ${blocks_table})
WHERE slot BETWEEN start_slot AND end_slot
ORDER BY slot
SETTINGS skip_unavailable_shards = 0
FORMAT TSV
" > "$ch_slots_file"

  tmp_bigtable_sorted="${slots_file}.sorted"
  tmp_clickhouse_sorted="${ch_slots_file}.sorted"
  LC_ALL=C sort -n -u "$slots_file" > "$tmp_bigtable_sorted"
  LC_ALL=C sort -n -u "$ch_slots_file" > "$tmp_clickhouse_sorted"
  comm -23 "$tmp_bigtable_sorted" "$tmp_clickhouse_sorted" > "$missing_file"
  rm -f "$tmp_bigtable_sorted" "$tmp_clickhouse_sorted"

  if [[ ! -s "$missing_file" ]]; then
    echo "  no missing slots in ClickHouse"
    rm -f "$missing_file"
    continue
  fi

  echo "  missing $(wc -l < "$missing_file") slots -> $missing_file"
done
