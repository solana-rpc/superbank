#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Copyright 2025-2026 Triton One Limited. All rights reserved.
#
# End-to-end smoke test for superbank-verify: ingest a small range of real
# mainnet slots (blocks, transactions, entries) with the Jetstreamer smoke
# helper, then run full PoH verification over that range.

set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."

if [[ $# -gt 1 || "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  echo "usage: scripts/dev/run-verify-smoke.sh [epoch|start:end]" >&2
  exit 1
fi

range="${1:-358560000:358560099}"

if [[ "${SKIP_INGEST:-}" != "1" ]]; then
  scripts/dev/run-jetstreamer-entries-smoke.sh "${range}"
fi

echo
echo "Running superbank-verify (mode=full) over ${range}"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}" \
RUST_LOG="${RUST_LOG:-info}" \
cargo run --release -p superbank-verify -- \
  --range "${range}" \
  --mode full \
  --allow-unverifiable
