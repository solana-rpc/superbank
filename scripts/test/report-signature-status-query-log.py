#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Summarize a scoped ClickHouse JSONEachRow query_log export without making requests.

Keep QueryStart and terminal rows from every coordinator and replica. Required fields:
node, query_id, initial_query_id, is_initial_query, type, query_duration_ms, ProfileEvents.
Cancellation verification additionally requires externally correlated cancellation records
and query started_at_ms timestamps. This tool never infers a disconnect from a timeout.
"""
import argparse
import json
import math
from collections import defaultdict
from pathlib import Path


TERMINAL = {"QueryFinish", "ExceptionBeforeStart", "ExceptionWhileProcessing"}


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def summarize(rows):
    durations = [float(row["query_duration_ms"]) for row in rows]
    cpu = sum(float(row["ProfileEvents"].get(name, 0)) for row in rows
              for name in ("UserTimeMicroseconds", "SystemTimeMicroseconds")) / 1_000_000
    return {"queries": len(rows), "cpu_seconds": cpu,
            "duration_ms": {"p50": percentile(durations, .5),
                            "p95": percentile(durations, .95),
                            "p99": percentile(durations, .99),
                            "max": max(durations, default=None)}}


def read_rows(path):
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    required = {"node", "query_id", "initial_query_id", "is_initial_query",
                "type", "query_duration_ms", "ProfileEvents"}
    for row in rows:
        if not required <= row.keys():
            raise ValueError(f"Missing required fields: {sorted(required - row.keys())}")
        if row["type"] not in TERMINAL | {"QueryStart"}:
            raise ValueError("Export query-log type as its string name")
        if not math.isfinite(float(row["query_duration_ms"])) or float(row["query_duration_ms"]) < 0:
            raise ValueError("query_duration_ms must be finite and nonnegative")
        if row["is_initial_query"] not in (0, 1, "0", "1"):
            raise ValueError("is_initial_query must be 0 or 1")
        if not isinstance(row["ProfileEvents"], dict):
            raise ValueError("ProfileEvents must be a JSON object")
    return rows


def audit(rows, limit_ms, require_leaves):
    groups = defaultdict(list)
    for row in rows:
        groups[(row["node"], row["query_id"])].append(row)
    terminal = []
    incomplete = []
    for key, events in groups.items():
        ends = [row for row in events if row["type"] in TERMINAL]
        if len(ends) != 1 or sum(row["type"] == "QueryStart" for row in events) != 1:
            incomplete.append({"node": key[0], "query_id": key[1]})
        terminal.extend(ends)
    coordinators = [row for row in terminal if int(row["is_initial_query"]) == 1]
    leaves = [row for row in terminal if int(row["is_initial_query"]) == 0]
    slow = [{"node": row["node"], "query_id": row["query_id"],
             "query_duration_ms": row["query_duration_ms"]}
            for row in terminal if float(row["query_duration_ms"]) > limit_ms]
    passed = bool(coordinators) and not incomplete and not slow and (bool(leaves) or not require_leaves)
    return {"observed_query_lifetimes_pass": passed, "limit_ms": limit_ms,
            "nodes_observed": sorted({row["node"] for row in rows}),
            "coordinator": summarize(coordinators), "leaf": summarize(leaves),
            "incomplete_or_duplicate_terminal": incomplete, "over_limit": slow,
            "client_disconnect_cancellation_verified": False}


def execution_deadline_failures(rows, deadline):
    failures = []
    for row in rows:
        if row["type"] not in TERMINAL:
            continue
        started = float(row["started_at_ms"])
        end = started + float(row["query_duration_ms"])
        if not math.isfinite(end) or started < 0 or end > deadline:
            failures.append({"node": row["node"], "query_id": row["query_id"],
                             "reason": "Execution exceeded the observed cancellation deadline"})
    return failures


def cancellation_deadline(event, grace_ms):
    root = event["initial_query_id"]
    if not isinstance(root, str) or not root:
        raise ValueError("Cancellation events require a nonempty initial_query_id")
    cancelled_at = float(event["cancelled_at_ms"])
    if not math.isfinite(cancelled_at) or cancelled_at < 0:
        raise ValueError("cancelled_at_ms must be finite and nonnegative")
    return root, cancelled_at + grace_ms


def cancellation_check(rows, events, observed_until_ms, grace_ms, require_leaves):
    failures = []
    if not events:
        return ["No externally correlated cancellation events were supplied"]
    for event in events:
        root, deadline = cancellation_deadline(event, grace_ms)
        if observed_until_ms < deadline:
            failures.append({"initial_query_id": root, "reason": "Incomplete observation interval"})
        related = [row for row in rows if row["initial_query_id"] == root or row["query_id"] == root]
        completeness = audit(related, float("inf"), require_leaves)
        if not completeness["observed_query_lifetimes_pass"]:
            failures.append({"initial_query_id": root, "reason": "Missing coordinator, leaf, or terminal evidence"})
        failures.extend(execution_deadline_failures(related, deadline))
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("export", type=Path)
    parser.add_argument("--max-query-ms", type=float, default=5000)
    parser.add_argument("--require-leaves", action="store_true")
    parser.add_argument("--cancellations", type=Path,
                        help="JSONEachRow: initial_query_id and actual cancelled_at_ms")
    parser.add_argument("--observed-until-ms", type=float, default=0,
                        help="End of complete query-log collection, Unix milliseconds")
    parser.add_argument("--cancellation-grace-ms", type=float, default=5000)
    args = parser.parse_args()
    if not math.isfinite(args.max_query_ms) or args.max_query_ms <= 0:
        parser.error("--max-query-ms must be finite and positive")
    if not math.isfinite(args.cancellation_grace_ms) or args.cancellation_grace_ms <= 0:
        parser.error("--cancellation-grace-ms must be finite and positive")
    if not math.isfinite(args.observed_until_ms):
        parser.error("--observed-until-ms must be finite")
    try:
        rows = read_rows(args.export)
        result = audit(rows, args.max_query_ms, args.require_leaves)
        events = [json.loads(line) for line in args.cancellations.read_text().splitlines()
                  if line.strip()] if args.cancellations else []
        failures = cancellation_check(rows, events, args.observed_until_ms,
                                      args.cancellation_grace_ms, args.require_leaves)
        result["cancellation_failures"] = failures
        result["client_disconnect_cancellation_verified"] = not failures
    except (OSError, ValueError, TypeError, KeyError) as error:
        parser.error(str(error))
    print(json.dumps(result, indent=2))
    raise SystemExit(not (result["observed_query_lifetimes_pass"]
                          and result["client_disconnect_cancellation_verified"]))


if __name__ == "__main__":
    main()
