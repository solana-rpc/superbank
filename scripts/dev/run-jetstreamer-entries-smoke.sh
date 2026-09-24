#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Copyright 2025-2026 Triton One Limited. All rights reserved.
#

set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."

if [[ $# -gt 1 || "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  echo "usage: scripts/dev/run-jetstreamer-entries-smoke.sh [epoch|start:end]" >&2
  exit 1
fi

command -v docker >/dev/null 2>&1 || {
  echo "docker not found" >&2
  exit 1
}

command -v cargo >/dev/null 2>&1 || {
  echo "cargo not found" >&2
  exit 1
}

range="${1:-358560000:358560099}"
genesis_slot="${JETSTREAMER_ALPENGLOW_GENESIS_SLOT:?set JETSTREAMER_ALPENGLOW_GENESIS_SLOT from a trusted genesis certificate}"
threads="${JETSTREAMER_THREADS:-4}"
container="clickhouse"
image="clickhouse/clickhouse-server:26.1.2.11"
dsn="http://localhost:8123"

wait_for_clickhouse() {
  for _ in {1..60}; do
    if docker exec "${container}" clickhouse-client -q "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "ClickHouse did not become ready" >&2
  exit 1
}

if docker inspect "${container}" >/dev/null 2>&1; then
  if [[ "$(docker inspect -f '{{.State.Running}}' "${container}")" != "true" ]]; then
    docker start "${container}" >/dev/null
  fi
else
  docker run -d \
    --name "${container}" \
    --ulimit nofile=262144:262144 \
    -p 8123:8123 \
    -p 9000:9000 \
    "${image}" >/dev/null
fi

wait_for_clickhouse

desired_default_user_config="$(cat <<'EOF'
<clickhouse>
  <users>
    <default>
      <networks>
        <ip>::/0</ip>
      </networks>
    </default>
  </users>
</clickhouse>
EOF
)"

current_default_user_config="$(docker exec "${container}" cat /etc/clickhouse-server/users.d/default-user.xml 2>/dev/null || true)"

if [[ "${current_default_user_config}" != "${desired_default_user_config}" ]]; then
  docker exec -i "${container}" sh -lc "cat > /etc/clickhouse-server/users.d/default-user.xml" <<< "${desired_default_user_config}"
  docker restart "${container}" >/dev/null
  wait_for_clickhouse
fi

for schema in ddl/local/transactions.sql ddl/local/blocks_metadata.sql ddl/local/entries.sql; do
  docker exec -i "${container}" clickhouse-client --multiquery < "${schema}"
done

docker exec -i "${container}" clickhouse-client --multiquery <<'SQL'
TRUNCATE TABLE default.transactions;
TRUNCATE TABLE default.blocks_metadata;
TRUNCATE TABLE default.entries;
SQL

(
  cd ingest/jetstreamer-clickhouse-plugin
  JETSTREAMER_CLICKHOUSE_MODE=remote \
  JETSTREAMER_CLICKHOUSE_DSN="${dsn}" \
  JETSTREAMER_THREADS="${threads}" \
  JETSTREAMER_ALPENGLOW_GENESIS_SLOT="${genesis_slot}" \
  RUST_LOG="${RUST_LOG:-info,clickhouse_rs=warn}" \
  cargo run --release --bin jetstreamer-clickhouse -- "${range}"
)

echo
echo "Entries summary"
docker exec -i "${container}" clickhouse-client -q \
  "SELECT count() AS rows, min(slot) AS min_slot, max(slot) AS max_slot FROM default.entries FORMAT Vertical"

echo
echo "Recent per-slot entry counts"
docker exec -i "${container}" clickhouse-client -q \
  "SELECT slot, count() AS entry_rows, sum(transaction_count) AS txs FROM default.entries GROUP BY slot ORDER BY slot DESC LIMIT 10"

echo
echo "Recent entry samples"
docker exec -i "${container}" clickhouse-client -q \
  "SELECT slot, entry_index, transaction_count, num_hashes, hex(hash) AS hash FROM default.entries ORDER BY slot DESC, entry_index DESC LIMIT 20"
