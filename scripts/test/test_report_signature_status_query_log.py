#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Offline regression tests for cancellation acceptance evidence."""
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path

SCRIPT = Path(__file__).with_name("report-signature-status-query-log.py")
SPEC = importlib.util.spec_from_file_location("report", SCRIPT)
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)


def row(kind="ExceptionWhileProcessing", node="coordinator", query="root", initial=1, **overrides):
    result = {"node": node, "query_id": query, "initial_query_id": "root",
              "is_initial_query": initial, "type": kind, "query_duration_ms": 100,
              "ProfileEvents": {}, "event_at_ms": 1200, "exception_code": 735}
    result.update(overrides)
    return result


def execution(**overrides):
    return [row("QueryStart", **overrides), row(**overrides)]


def inputs(rows, nodes=("coordinator", "replica")):
    clocks = {node: {"min_offset_ms": -20, "max_offset_ms": 20} for node in nodes}
    manifest = {}
    for node in nodes:
        counts = Counter(event["type"] for event in rows if event["node"] == node)
        manifest[node] = {"observed_until_ms": 7000,
                          "counts": {kind: counts[kind] for kind in REPORT.EVENT_TYPES}}
    return {"rows": rows, "events": [{"initial_query_id": "root", "cancelled_at_ms": 1000}],
            "observed_until_ms": 7000, "grace_ms": 5000, "require_leaves": False,
            "expected_nodes": list(nodes), "clocks": clocks, "manifest": manifest}


class QueryLogAuditTests(unittest.TestCase):
    def test_exception_before_start_needs_no_start(self):
        result = REPORT.audit([row("ExceptionBeforeStart")], 5000, False)
        self.assertTrue(result["observed_query_lifetimes_pass"])

    def test_other_terminals_require_start(self):
        for kind in ("QueryFinish", "ExceptionWhileProcessing"):
            with self.subTest(kind=kind):
                self.assertFalse(REPORT.audit([row(kind)], 5000, False)["observed_query_lifetimes_pass"])

    def test_duplicate_start_or_terminal_fails(self):
        for rows in (execution() + [row()], execution() + [row("QueryStart")]):
            self.assertFalse(REPORT.audit(rows, 5000, False)["observed_query_lifetimes_pass"])

    def test_start_and_terminal_must_share_initial_query(self):
        rows = execution()
        rows[0]["initial_query_id"] = "another-root"
        self.assertFalse(REPORT.audit(rows, 5000, False)["observed_query_lifetimes_pass"])

    def test_started_exception_before_start_is_inconsistent(self):
        rows = [row("QueryStart"), row("ExceptionBeforeStart")]
        self.assertFalse(REPORT.audit(rows, 5000, False)["observed_query_lifetimes_pass"])

    def test_missing_leaf_fails_when_required(self):
        self.assertFalse(REPORT.audit(execution(), 5000, True)["observed_query_lifetimes_pass"])


class CancellationEvidenceTests(unittest.TestCase):
    def test_complete_cancelled_execution_with_idle_replica_passes(self):
        result = REPORT.cancellation_check(**inputs(execution()))
        self.assertTrue(result["termination_after_disconnect_verified"])
        self.assertTrue(result["client_disconnect_cancellation_verified"])

    def test_replicas_may_finish_naturally_but_coordinator_must_be_cancelled(self):
        rows = execution() + [row("QueryStart", node="replica", query="leaf", initial=0),
                              row("QueryFinish", node="replica", query="leaf", initial=0, exception_code=0)]
        args = inputs(rows)
        args["require_leaves"] = True
        self.assertTrue(REPORT.cancellation_check(**args)["client_disconnect_cancellation_verified"])

    def test_cancelled_leaf_is_accepted(self):
        rows = execution() + execution(node="replica", query="leaf", initial=0, exception_code=394)
        self.assertTrue(REPORT.cancellation_check(**inputs(rows))["client_disconnect_cancellation_verified"])

    def test_natural_coordinator_finish_is_only_termination(self):
        rows = [row("QueryStart"), row("QueryFinish", exception_code=0)]
        result = REPORT.cancellation_check(**inputs(rows))
        self.assertTrue(result["termination_after_disconnect_verified"])
        self.assertFalse(result["client_disconnect_cancellation_verified"])

    def test_distributed_cancellation_may_have_394_coordinator_and_735_leaves(self):
        rows = execution(exception_code=394) + execution(node="replica", query="leaf", initial=0)
        result = REPORT.cancellation_check(**inputs(rows))
        self.assertTrue(result["termination_after_disconnect_verified"])
        self.assertTrue(result["client_disconnect_cancellation_verified"])

    def test_generic_kill_error_does_not_prove_disconnect_cause(self):
        result = REPORT.cancellation_check(**inputs(execution(exception_code=394)))
        self.assertTrue(result["termination_after_disconnect_verified"])
        self.assertFalse(result["client_disconnect_cancellation_verified"])

    def test_actual_event_timestamp_overrides_short_reported_duration(self):
        result = REPORT.cancellation_check(**inputs(execution(event_at_ms=7000, started_at_ms=1000)))
        self.assertFalse(result["termination_after_disconnect_verified"])

    def test_minimum_clock_offset_controls_latest_possible_finish(self):
        args = inputs(execution(event_at_ms=5990))
        result = REPORT.cancellation_check(**args)
        self.assertFalse(result["termination_after_disconnect_verified"])
        args["clocks"]["coordinator"] = {"min_offset_ms": 0, "max_offset_ms": 20}
        self.assertTrue(REPORT.cancellation_check(**args)["termination_after_disconnect_verified"])

    def test_cancellation_before_or_overlapping_disconnect_is_unverified(self):
        result = REPORT.cancellation_check(**inputs(execution(event_at_ms=1010)))
        self.assertTrue(result["termination_after_disconnect_verified"])
        self.assertFalse(result["client_disconnect_cancellation_verified"])

    def test_missing_event_timestamp_cannot_use_start_plus_duration(self):
        rows = execution(started_at_ms=1000)
        del rows[-1]["event_at_ms"]
        result = REPORT.cancellation_check(**inputs(rows))
        self.assertFalse(result["termination_after_disconnect_verified"])

    def test_each_external_evidence_input_is_required(self):
        for field in ("events", "expected_nodes", "clocks", "manifest"):
            args = inputs(execution())
            args[field] = None if field != "events" else []
            with self.subTest(field=field):
                self.assertFalse(REPORT.cancellation_check(**args)["client_disconnect_cancellation_verified"])

    def test_missing_expected_node_clock_or_manifest_is_unverified(self):
        for field in ("clocks", "manifest"):
            args = inputs(execution())
            del args[field]["replica"]
            with self.subTest(field=field):
                self.assertFalse(REPORT.cancellation_check(**args)["termination_after_disconnect_verified"])

    def test_count_mismatch_catches_truncated_export(self):
        args = inputs(execution())
        args["manifest"]["replica"]["counts"]["QueryStart"] = 1
        self.assertFalse(REPORT.cancellation_check(**args)["termination_after_disconnect_verified"])

    def test_manifest_observation_must_cover_requested_interval_on_every_node(self):
        args = inputs(execution())
        args["manifest"]["replica"]["observed_until_ms"] = 5999
        self.assertFalse(REPORT.cancellation_check(**args)["termination_after_disconnect_verified"])

    def test_observation_must_extend_to_disconnect_deadline(self):
        args = inputs(execution())
        args["observed_until_ms"] = 5999
        self.assertFalse(REPORT.cancellation_check(**args)["termination_after_disconnect_verified"])

    def test_duplicate_external_correlation_fails(self):
        args = inputs(execution())
        args["events"] *= 2
        self.assertFalse(REPORT.cancellation_check(**args)["termination_after_disconnect_verified"])

    def test_unmatched_cancellation_fails(self):
        args = inputs(execution())
        args["events"][0]["initial_query_id"] = "unknown"
        self.assertFalse(REPORT.cancellation_check(**args)["termination_after_disconnect_verified"])

    def test_unexpected_replica_fails(self):
        rows = execution() + execution(node="unexpected", query="leaf", initial=0)
        self.assertFalse(REPORT.cancellation_check(**inputs(rows))["termination_after_disconnect_verified"])

    def test_invalid_clock_bounds_rejected(self):
        for bounds in ({"min_offset_ms": 1, "max_offset_ms": 0},
                       {"min_offset_ms": float("nan"), "max_offset_ms": 0}):
            args = inputs(execution())
            args["clocks"]["coordinator"] = bounds
            with self.subTest(bounds=bounds), self.assertRaises(ValueError):
                REPORT.cancellation_check(**args)

    def test_before_start_client_cancellation_can_be_verified(self):
        result = REPORT.cancellation_check(**inputs([row("ExceptionBeforeStart")]))
        self.assertTrue(result["client_disconnect_cancellation_verified"])

    def test_non_cancellation_leaf_exception_does_not_pass(self):
        rows = execution() + execution(node="replica", query="leaf", initial=0, exception_code=159)
        result = REPORT.cancellation_check(**inputs(rows))
        self.assertTrue(result["termination_after_disconnect_verified"])
        self.assertFalse(result["client_disconnect_cancellation_verified"])


class CommandLineTests(unittest.TestCase):
    def test_legacy_inputs_still_report_but_cannot_verify_cancellation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queries.jsonl"
            path.write_text("\n".join(json.dumps(event) for event in execution()))
            result = subprocess.run([sys.executable, str(SCRIPT), str(path)],
                                    capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 1, result.stderr)
        report = json.loads(result.stdout)
        self.assertTrue(report["observed_query_lifetimes_pass"])
        self.assertFalse(report["client_disconnect_cancellation_verified"])

    def test_complete_inputs_exit_successfully(self):
        args = inputs(execution())
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            for name, value in (("clocks", args["clocks"]), ("manifest", args["manifest"])):
                (base / name).write_text(json.dumps(value))
            for name, value in (("rows", args["rows"]), ("events", args["events"])):
                (base / name).write_text("\n".join(json.dumps(event) for event in value))
            command = [sys.executable, str(SCRIPT), str(base / "rows"),
                       "--cancellations", str(base / "events"), "--observed-until-ms", "7000",
                       "--expected-nodes", "coordinator", "replica", "--clock-bounds", str(base / "clocks"),
                       "--count-manifest", str(base / "manifest")]
            result = subprocess.run(command, capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(json.loads(result.stdout)["client_disconnect_cancellation_verified"])


if __name__ == "__main__":
    unittest.main()
