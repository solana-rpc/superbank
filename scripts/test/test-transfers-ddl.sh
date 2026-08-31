#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Copyright 2025-2026 Triton One Limited. All rights reserved.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

if ! command -v clickhouse >/dev/null 2>&1; then
  echo "clickhouse is required; install ClickHouse 25.6+ or run this in the project dev shell" >&2
  exit 1
fi

# `clickhouse local` uses an ephemeral database here. The fixture never reaches a configured
# ClickHouse server and therefore validates the materialized view without mutating an environment.
clickhouse local --multiquery < <(
  sed -n '1,$p' ddl/local/transactions.sql
  sed -n '1,$p' ddl/local/transfers.sql
  sed -n '1,$p' tests/clickhouse/transfers-fixtures.sql
)
