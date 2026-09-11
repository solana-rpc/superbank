#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Audit a scoped ClickHouse JSONEachRow query_log export without making requests.

Required fields: node, query_id, initial_query_id, is_initial_query, type,
query_duration_ms, ProfileEvents. Verification additionally requires terminal
event_at_ms (actual event_time_microseconds / 1000, not start + duration),
exception_code, externally correlated cancellations, expected nodes, clock bounds,
and an independently collected count manifest after query-log flush on every node.
All timestamps are Unix milliseconds. Clock offsets are node time minus client time.
A timely QueryFinish proves observed termination, never disconnect cancellation.
"""
import argparse
import json
import math
from collections import Counter, defaultdict
from pathlib import Path


TERMINAL = {"QueryFinish", "ExceptionBeforeStart", "ExceptionWhileProcessing"}
EVENT_TYPES = TERMINAL | {"QueryStart"}
# ClickHouse v26.2.3.2-stable src/Common/ErrorCodes.cpp.
CANCELLED = {394, 735}
CANCELLED_BY_CLIENT = 735


def finite_number(value, name, minimum=None):
    number = float(value)
    if not math.isfinite(number) or (minimum is not None and number < minimum):
        raise ValueError(f"{name} must be finite and >= {minimum}")
    return number


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def summarize(rows):
    durations = [float(row["query_duration_ms"]) for row in rows]
    cpu = sum(float(row["ProfileEvents"].get(name, 0)) for row in rows
              for name in ("UserTimeMicroseconds", "SystemTimeMicroseconds")) / 1_000_000
    return {"queries": len(rows), "cpu_seconds": cpu,
            "terminal_types": dict(Counter(row["type"] for row in rows)),
            "duration_ms": {"p50": percentile(durations, .5),
                            "p95": percentile(durations, .95),
                            "p99": percentile(durations, .99),
                            "max": max(durations, default=None)}}


def read_jsonl(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def validate_row(row):
    required = {"node", "query_id", "initial_query_id", "is_initial_query",
                "type", "query_duration_ms", "ProfileEvents"}
    if not required <= row.keys():
        raise ValueError(f"Missing required fields: {sorted(required - row.keys())}")
    if row["type"] not in EVENT_TYPES:
        raise ValueError("Export query-log type as its string name")
    finite_number(row["query_duration_ms"], "query_duration_ms", 0)
    if row["is_initial_query"] not in (0, 1, "0", "1"):
        raise ValueError("is_initial_query must be 0 or 1")
    if not isinstance(row["ProfileEvents"], dict):
        raise ValueError("ProfileEvents must be a JSON object")
    for field in ("node", "query_id", "initial_query_id"):
        if not isinstance(row[field], str) or not row[field]:
            raise ValueError(f"{field} must be a nonempty string")


def read_rows(path):
    rows = read_jsonl(path)
    for row in rows:
        validate_row(row)
    return rows


def complete_execution(events, ends):
    if len(ends) != 1:
        return False
    expected_starts = int(ends[0]["type"] != "ExceptionBeforeStart")
    if sum(row["type"] == "QueryStart" for row in events) != expected_starts:
        return False
    identities = {(row["initial_query_id"], int(row["is_initial_query"])) for row in events}
    return len(identities) == 1


def audit(rows, limit_ms, require_leaves):
    groups = defaultdict(list)
    for row in rows:
        groups[(row["node"], row["query_id"])].append(row)
    terminal, incomplete = [], []
    for key, events in groups.items():
        ends = [row for row in events if row["type"] in TERMINAL]
        if not complete_execution(events, ends):
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
            "incomplete_or_duplicate_terminal": incomplete, "over_limit": slow}


def clock_bounds_for(node, clocks):
    bounds = clocks[node]
    lower = finite_number(bounds["min_offset_ms"], "min_offset_ms")
    upper = finite_number(bounds["max_offset_ms"], "max_offset_ms")
    if lower > upper:
        raise ValueError("min_offset_ms must not exceed max_offset_ms")
    return lower, upper


def manifest_node_failures(node, rows, manifest, observed_until_ms):
    record = manifest[node]
    counts = record["counts"]
    if set(counts) != EVENT_TYPES:
        raise ValueError("Manifest counts must name all four query-log event types, including zeros")
    if any(type(count) is not int or count < 0 for count in counts.values()):
        raise ValueError("Manifest counts must be nonnegative integers")
    actual = Counter(row["type"] for row in rows if row["node"] == node)
    failures = []
    if counts != {kind: actual[kind] for kind in EVENT_TYPES}:
        failures.append(f"Query-log count mismatch for {node}")
    collected_until = finite_number(record["observed_until_ms"], "manifest observed_until_ms", 0)
    if collected_until < observed_until_ms:
        failures.append(f"Incomplete query-log observation interval for {node}")
    return failures


def export_failures(rows, expected_nodes, clocks, manifest, observed_until_ms):
    if not expected_nodes:
        return ["Expected nodes were not supplied"]
    expected = set(expected_nodes)
    if not clocks or set(clocks) != expected:
        return ["Clock bounds must cover exactly the expected nodes"]
    if not manifest or set(manifest) != expected:
        return ["An independent query-log count manifest must cover exactly the expected nodes"]
    failures = []
    if {row["node"] for row in rows} - expected:
        failures.append("Export contains unexpected nodes")
    for node in sorted(expected):
        clock_bounds_for(node, clocks)
        failures.extend(manifest_node_failures(node, rows, manifest, observed_until_ms))
    return failures


def terminal_interval(row, clocks):
    ended = finite_number(row["event_at_ms"], "event_at_ms", 0)
    lower_offset, upper_offset = clock_bounds_for(row["node"], clocks)
    return ended - upper_offset, ended - lower_offset


def execution_deadline_failures(rows, deadline, clocks):
    failures = []
    for row in rows:
        if row["type"] not in TERMINAL:
            continue
        try:
            _, latest_end = terminal_interval(row, clocks)
            reason = "Execution exceeded the observed cancellation deadline" if latest_end > deadline else None
        except KeyError:
            reason = "Missing actual terminal event_at_ms or node clock bounds"
        if reason:
            failures.append({"node": row["node"], "query_id": row["query_id"], "reason": reason})
    return failures


def cancellation_deadline(event, grace_ms):
    root = event["initial_query_id"]
    if not isinstance(root, str) or not root:
        raise ValueError("Cancellation events require a nonempty initial_query_id")
    cancelled_at = finite_number(event["cancelled_at_ms"], "cancelled_at_ms", 0)
    return root, cancelled_at + grace_ms


def cancellation_evidence_failures(rows, cancelled_at, clocks):
    failures = []
    for row in rows:
        if row["type"] not in TERMINAL:
            continue
        initial = int(row["is_initial_query"]) == 1
        # A leaf can finish before the coordinator is cancelled; that is termination,
        # not cancellation evidence. Every initial execution must be cancelled.
        if not initial and row["type"] == "QueryFinish":
            continue
        allowed = CANCELLED
        if row.get("exception_code") not in allowed or row["type"] == "QueryFinish":
            failures.append({"node": row["node"], "query_id": row["query_id"],
                             "reason": "No client-disconnect cancellation evidence"})
            continue
        try:
            earliest_end, _ = terminal_interval(row, clocks)
            if earliest_end < cancelled_at:
                failures.append({"node": row["node"], "query_id": row["query_id"],
                                 "reason": "Cancellation timestamp precedes or overlaps client disconnect"})
        except KeyError:
            failures.append("Missing terminal timestamp or clock bounds for cancellation evidence")
    if not any(row["type"] in TERMINAL and row.get("exception_code") == CANCELLED_BY_CLIENT
               for row in rows):
        failures.append("No QUERY_WAS_CANCELLED_BY_CLIENT event in the correlated query family")
    return failures


def correlated_check(rows, event, observed_until_ms, grace_ms, require_leaves, clocks):
    root, deadline = cancellation_deadline(event, grace_ms)
    failures = []
    if observed_until_ms < deadline:
        failures.append({"initial_query_id": root, "reason": "Incomplete observation interval"})
    related = [row for row in rows if row["initial_query_id"] == root or row["query_id"] == root]
    if not audit(related, float("inf"), require_leaves)["observed_query_lifetimes_pass"]:
        failures.append({"initial_query_id": root, "reason": "Missing coordinator, leaf, or terminal evidence"})
    failures.extend(execution_deadline_failures(related, deadline, clocks))
    evidence = cancellation_evidence_failures(related, deadline - grace_ms, clocks)
    return failures, evidence


def cancellation_check(rows, events, observed_until_ms, grace_ms, require_leaves,
                       expected_nodes=None, clocks=None, manifest=None):
    clocks = clocks or {}
    failures = export_failures(rows, expected_nodes, clocks, manifest, observed_until_ms)
    evidence = []
    if not events:
        failures.append("No externally correlated cancellation events were supplied")
    seen = set()
    for event in events:
        root, _ = cancellation_deadline(event, grace_ms)
        if root in seen:
            failures.append(f"Duplicate cancellation correlation for {root}")
        seen.add(root)
        timing, cancellation = correlated_check(rows, event, observed_until_ms, grace_ms, require_leaves, clocks)
        failures.extend(timing)
        evidence.extend(cancellation)
    return {"termination_after_disconnect_verified": not failures,
            "client_disconnect_cancellation_verified": not failures and not evidence,
            "cancellation_failures": failures + evidence,
            "termination_failures": failures,
            "cancellation_attribution_note": "Requires a controlled test without competing cancellation sources; "
            "code 735 can describe a replica native-stream close, not only the original HTTP client."}


def argument_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("export", type=Path)
    parser.add_argument("--max-query-ms", type=float, default=5000)
    parser.add_argument("--require-leaves", action="store_true")
    parser.add_argument("--cancellations", type=Path,
                        help="JSONEachRow: initial_query_id and actual cancelled_at_ms")
    parser.add_argument("--observed-until-ms", type=float, default=0,
                        help="End of complete query-log collection in client-clock Unix milliseconds")
    parser.add_argument("--cancellation-grace-ms", type=float, default=5000)
    parser.add_argument("--expected-nodes", nargs="+", help="Every possible coordinator/replica, including idle nodes")
    parser.add_argument("--clock-bounds", type=Path,
                        help='JSON object: {node: {min_offset_ms: number, max_offset_ms: number}}')
    parser.add_argument("--count-manifest", type=Path,
                        help='JSON object: {node: {observed_until_ms: number, counts: {QueryStart: count, '
                             'QueryFinish: count, ExceptionBeforeStart: count, ExceptionWhileProcessing: count}}}; '
                             'independent counts of the same flushed export scope, including zero-count nodes')
    return parser


def optional_json(path):
    return json.loads(path.read_text()) if path else None


def main():
    parser = argument_parser()
    args = parser.parse_args()
    try:
        finite_number(args.max_query_ms, "--max-query-ms", 0.001)
        finite_number(args.cancellation_grace_ms, "--cancellation-grace-ms", 0.001)
        finite_number(args.observed_until_ms, "--observed-until-ms", 0)
        rows = read_rows(args.export)
        result = audit(rows, args.max_query_ms, args.require_leaves)
        events = read_jsonl(args.cancellations) if args.cancellations else []
        result.update(cancellation_check(rows, events, args.observed_until_ms,
                                         args.cancellation_grace_ms, args.require_leaves,
                                         args.expected_nodes, optional_json(args.clock_bounds),
                                         optional_json(args.count_manifest)))
    except (OSError, ValueError, TypeError, KeyError) as error:
        parser.error(str(error))
    print(json.dumps(result, indent=2))
    raise SystemExit(not (result["observed_query_lifetimes_pass"]
                          and result["client_disconnect_cancellation_verified"]))


if __name__ == "__main__":
    main()
