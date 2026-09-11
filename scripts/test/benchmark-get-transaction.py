#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Local-only getTransaction SQL benchmarks; creates and removes its own cluster.

Uses repository payload/signature DDL and full transaction projection. All raw
measurements, query plans and terminal query logs are retained in --output.
This fixture does not measure deployed gateway behavior or production capacity.
"""
import argparse
import hashlib
import http.client
import importlib.util
import json
import math
import pathlib
import platform
import random
import re
import statistics
import struct
import socketserver
import threading
import time
import urllib.parse

ROOT = pathlib.Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("protocol", pathlib.Path(__file__).with_name("test-clickhouse-http-disconnect.py"))
PROTOCOL = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROTOCOL)
SETTINGS = ("max_threads=2,max_execution_time=10,max_execution_time_leaf=10,"
            "optimize_skip_unused_shards=1,use_query_cache=0,"
            "log_queries=1,log_profile_events=1,use_hedged_requests=0,max_parallel_replicas=1")
COLUMNS = re.search(r'TRANSACTION_SELECT_COLUMNS: &str = "(.*?)";',
                    (ROOT / "crates/superbank-rpc/src/clickhouse/queries.rs").read_text(), re.S)[1]


def request(port, query, query_id=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
    params = {"query_id": query_id} if query_id else {}
    started = time.perf_counter()
    try:
        connection.request("POST", "/?" + urllib.parse.urlencode(params), query.encode())
        response = connection.getresponse()
        body = response.read()
        if response.status != 200:
            raise RuntimeError(body.decode(errors="replace"))
        return body, (time.perf_counter() - started) * 1000
    finally:
        connection.close()


def sql(cluster, query, node=0):
    return request(cluster.ports[node], query)[0].decode(errors="replace")


def statements(text):
    # These two cluster DDL files have no embedded semicolons in literals.
    return [part for part in text.split(";") if part.strip()]


def position(number):
    return 432000 * (1 + number // 500000) + number % 500000 // 1000, number % 1000


def literal(number):
    return "unhex('" + hashlib.sha512(str(number).encode()).hexdigest() + "')"


def signature_query(number):
    key = literal(number)
    return ("SELECT slot,slot_idx FROM default.signatures "
            f"PREWHERE sig_bucket=cityHash64({key})%32 AND signature={key} "
            "ORDER BY slot DESC,slot_idx DESC LIMIT 1")


def payload_query(table, number, indexed=True, index_override=None, resolved=None):
    slot, idx = resolved if resolved is not None else position(number)
    if index_override is not None:
        idx = index_override
    pred = f" AND slot_idx={idx}" if indexed else ""
    order = "" if indexed else " ORDER BY slot_idx DESC"
    return (f"SELECT {COLUMNS} FROM default.{table} PREWHERE slot={slot}{pred} "
            f"AND signature={literal(number)}{order} LIMIT 1")


def combined_query(number):
    # An array explicitly distinguishes an absent signature from valid position (0,0).
    lookup = signature_query(number)
    return (f"WITH (SELECT groupArray(tuple(slot,slot_idx)) FROM ({lookup})) AS positions "
            f"SELECT {COLUMNS} FROM default.transactions "
            "PREWHERE slot=tupleElement(arrayElement(positions,1),1) "
            "AND slot_idx=tupleElement(arrayElement(positions,1),2) "
            f"AND signature={literal(number)} WHERE notEmpty(positions) LIMIT 1")


def snapshot(cluster):
    query = ("SELECT hostName() AS host,table,sumIf(rows,active) AS rows,sumIf(bytes_on_disk,active) AS disk_bytes,sum(bytes_on_disk) AS resident_parts_bytes,"
             "sumIf(data_compressed_bytes,active) AS compressed_bytes,sumIf(marks_bytes,active) AS marks_bytes,"
             "sumIf(primary_key_bytes_in_memory,active) AS primary_key_memory,countIf(active) AS parts "
             "FROM system.parts WHERE database='default' "
             "AND table IN ('transactions_local','transactions1024_local','signatures_local','signatures1024_local') "
             "GROUP BY host,table FORMAT JSONEachRow")
    return [json.loads(line) for i in range(3) for line in sql(cluster, query, i).splitlines()]


def prepare(cluster, rows):
    for table, granularity in [("transactions", 8192), ("transactions1024", 1024)]:
        ddl = (ROOT / "ddl/cluster/transactions.sql").read_text().replace("{cluster}", "fixture")
        ddl = ddl.replace("transactions", table).replace(
            "ORDER BY (slot, slot_idx, signature);",
            f"ORDER BY (slot, slot_idx, signature) SETTINGS index_granularity={granularity},index_granularity_bytes=10485760;")
        for statement in statements(ddl):
            sql(cluster, statement)
    for suffix in ["", "1024"]:
        ddl = (ROOT / "ddl/cluster/signatures.sql").read_text().replace("{cluster}", "fixture")
        ddl = re.sub(r"\bsignatures(?:_local)?\b", lambda match: match[0].replace("signatures", "signatures" + suffix), ddl)
        ddl = ddl.replace("transactions_local", "transactions" + suffix + "_local")
        for statement in statements(ddl):
            sql(cluster, statement)
    ingestion = []
    for table in ["transactions", "transactions1024"]:
        started = time.perf_counter()
        sampled_peak = 0
        for first in range(0, rows, 50000):
            count = min(50000, rows - first)
            query = (f"INSERT INTO default.{table} "
                     "(signature,slot,slot_idx,block_time,tx_version,tx_signatures,"
                     "tx_num_required_signatures,tx_account_keys,meta_status_ok,meta_log_messages_present,meta_log_messages) "
                     "SELECT SHA512(toString(number)),432000*(1+intDiv(number,500000))+intDiv(number%500000,1000),"
                     "number%1000,1700000000+intDiv(number,1000),"
                     "if(number%3=0,NULL,toNullable(toUInt8(number%3-1))),[SHA512(toString(number))],"
                     "1,[toFixedString('fixture-account',32)],1,1,"
                     "arrayMap(x->hex(SHA256(concat(toString(number),':',toString(x)))),"
                     "range(if(number%100=0,128,if(number%2=0,8,2)))) "
                     f"FROM numbers({first},{count}) SETTINGS distributed_foreground_insert=1,max_threads=2")
            sql(cluster, query)
            sampled_peak = max(sampled_peak, sum(row["resident_parts_bytes"] for row in snapshot(cluster)))
        ingestion.append({"table": table, "rows": rows, "seconds": time.perf_counter() - started,
                          "sampled_peak_all_table_parts_bytes": sampled_peak, "parts": snapshot(cluster)})
        print(f"loaded {table}: {rows} rows", flush=True)
    for suffix in ["", "1024"]:
        assert int(sql(cluster, "SELECT count() FROM default.signatures" + suffix)) == rows
    return ingestion


def run_lookup(port, number, variant, prefix, delay_ms):
    queries = []
    if variant == "two_step":
        queries.append(signature_query(number))
    table = "transactions1024" if variant == "granularity1024" else "transactions"
    queries.append(combined_query(number) if variant == "combined" else
                   payload_query(table, number, indexed=variant != "slot_only"))
    records, elapsed = [], 0
    for index, query in enumerate(queries):
        # Explicit simulated per-request latency, separate from measured HTTP time.
        if delay_ms:
            time.sleep(delay_ms / 1000)
        body, ms = request(port, query + " SETTINGS " + SETTINGS + " FORMAT RowBinary", f"{prefix}-{index}")
        elapsed += ms
        if variant == "two_step" and index == 0:
            if not body:
                records.append({"query_id": f"{prefix}-{index}", "http_ms": ms, "bytes": 0})
                break
            resolved = struct.unpack("<QI", body)
            queries[1] = payload_query(table, number, resolved=resolved)
        records.append({"query_id": f"{prefix}-{index}", "http_ms": ms, "bytes": len(body)})
    return {"http_ms": elapsed, "simulated_elapsed_ms": elapsed + delay_ms * len(records),
            "requests": records, "digest": hashlib.sha256(body).hexdigest()}


def query_logs(cluster):
    columns = ("hostName() AS host,type,query_id,initial_query_id,is_initial_query,query_duration_ms,"
               "read_rows,read_bytes,result_rows,memory_usage,exception_code,tables,query,ProfileEvents")
    result = []
    for i in range(3):
        sql(cluster, "SYSTEM FLUSH LOGS", i)
        query = (f"SELECT {columns} FROM system.query_log "
                 "WHERE startsWith(initial_query_id,'gettx-bench-') AND type!='QueryStart' FORMAT JSONEachRow")
        result.extend(json.loads(line) for line in sql(cluster, query, i).splitlines())
    return result


def quantiles(values):
    ordered = sorted(values)
    return {"p50": statistics.median(ordered), "p95": ordered[math.ceil(len(ordered)*.95)-1],
            "p99": ordered[math.ceil(len(ordered)*.99)-1]}


def run(args):
    args.output.mkdir(parents=True, exist_ok=False)
    cluster = PROTOCOL.Cluster(args.output, args.image, memory=args.node_memory)
    report = {"image": args.image, "node_memory": args.node_memory, "rows_per_payload_table": args.rows, "host": platform.platform(),
              "script_sha256": hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
              "settings": SETTINGS, "seed": 88, "runs": args.runs, "samples_per_run": args.samples,
              "filesystem_cache": "uncontrolled; cache-cleared means ClickHouse mark/uncompressed caches only",
              "measurements": [], "gateway_integration": "not accepted; all gates require explicit evidence"}
    proxy = None
    try:
        cluster.start()
        proxy = socketserver.ThreadingTCPServer(("127.0.0.1", 0), PROTOCOL.GatewayHandler)
        proxy.daemon_threads = True
        proxy.upstream_port = cluster.ports[0]
        proxy.stopping = threading.Event()
        proxy_thread = threading.Thread(target=proxy.serve_forever, daemon=True)
        proxy_thread.start()
        port = proxy.server_address[1]
        report["ingestion"] = prepare(cluster, args.rows)
        report["before_merge"] = snapshot(cluster)
        merge_start = time.perf_counter()
        for table in ["transactions_local", "transactions1024_local"]:
            for node in range(3):
                sql(cluster, f"OPTIMIZE TABLE default.{table} FINAL SETTINGS max_threads=2", node)
        report["merge_wall_seconds"] = time.perf_counter() - merge_start
        report["merges"] = []
        for node in range(3):
            sql(cluster, "SYSTEM FLUSH LOGS", node)
            query = ("SELECT hostName() AS host,table,count() AS merges,sum(duration_ms) AS duration_ms,"
                     "sum(read_rows) AS read_rows,sum(read_bytes) AS read_bytes,max(peak_memory_usage) AS peak_memory "
                     "FROM system.part_log WHERE database='default' AND event_type='MergeParts' AND error=0 "
                     "AND table IN ('transactions_local','transactions1024_local') GROUP BY host,table FORMAT JSONEachRow")
            report["merges"].extend(json.loads(line) for line in sql(cluster, query, node).splitlines())
        report["after_merge"] = snapshot(cluster)
        plans = {}
        for variant, query in {"combined": combined_query(1234), "position": payload_query("transactions", 1234),
                               "granularity1024": payload_query("transactions1024", 1234),
                               "slot_only": payload_query("transactions", 1234),
                               "local_position": payload_query("transactions_local", 1234),
                               "local_granularity1024": payload_query("transactions1024_local", 1234)}.items():
            try:
                nodes = range(3) if variant.startswith("local_") else [0]
                plans[variant] = "\n".join(
                    f"node {node}:\n" + sql(cluster, "EXPLAIN indexes=1 " + query + " SETTINGS " + SETTINGS, node)
                    for node in nodes)
            except RuntimeError as error:
                plans[variant] = str(error)
        report["plans"] = plans
        # Absent and stale position evidence is retained separately from steady-state reads.
        report["edge_cases"] = {}
        stale_slot, _ = position(1234)
        sql(cluster, "INSERT INTO default.signatures (signature,slot,slot_idx,err) "
            f"SELECT {literal(1234)},{stale_slot},9999,NULL SETTINGS distributed_foreground_insert=1")
        for label, query in [("absent", combined_query(args.rows + 1)),
                             ("stale_combined", combined_query(1234)),
                             ("stale_position", payload_query("transactions", 1234, index_override=9999)),
                             ("stale_fallback", payload_query("transactions", 1234, indexed=False))]:
            try:
                body, ms = request(port, query + " SETTINGS " + SETTINGS + " FORMAT RowBinary", "gettx-bench-edge-" + label)
                report["edge_cases"][label] = {"bytes": len(body), "http_ms": ms}
            except RuntimeError as error:
                report["edge_cases"][label] = {"error": str(error)}
        report["gateway_integration"] = (
            "rejected: combined query returns indistinguishable empty responses for an absent signature "
            "and a stale position whose payload exists; it cannot preserve fallback without resolving the signature again"
            if report["edge_cases"].get("stale_combined", {}).get("bytes") == 0
            and report["edge_cases"].get("stale_fallback", {}).get("bytes", 0) > 0
            else "not accepted: remaining correctness, pruning, performance and cancellation gates require evidence")
        corpus = random.Random(88).sample(range(args.rows), args.samples)
        corpus = [number if number != 1234 else 1235 for number in corpus]
        variants = ["position", "slot_only", "granularity1024", "two_step", "combined"]
        expected = {}
        for run in range(args.runs):
            for cold in [False, True]:
                for variant in variants[::1 if run % 2 == 0 else -1]:
                    for sample, number in enumerate(corpus):
                        if cold:
                            for node in range(3):
                                sql(cluster, "SYSTEM DROP MARK CACHE", node)
                                sql(cluster, "SYSTEM DROP UNCOMPRESSED CACHE", node)
                        prefix = f"gettx-bench-{run}-{int(cold)}-{variant}-{sample}"
                        try:
                            row = run_lookup(port, number, variant, prefix, args.delay_ms)
                            if number in expected:
                                assert row["digest"] == expected[number], (variant, number)
                            expected[number] = row["digest"]
                        except RuntimeError as error:
                            row = {"error": str(error)}
                        row.update(run=run, cold=cold, variant=variant, sample=sample)
                        report["measurements"].append(row)
                    print(f"run {run+1}/{args.runs} cold={cold} {variant}", flush=True)
        report["query_logs"] = query_logs(cluster)
        report["final_parts"] = snapshot(cluster)
        terminals = {row["query_id"]: row for row in report["query_logs"]
                     if row["is_initial_query"] and row["type"] == "QueryFinish"}
        for row in report["measurements"]:
            events = [terminals.get(request["query_id"]) for request in row.get("requests", [])]
            if not events or any(event is None for event in events):
                continue
            row["server_ms"] = sum(int(event["query_duration_ms"]) for event in events)
            row["read_rows"] = sum(int(event["read_rows"]) for event in events)
            row["read_bytes"] = sum(int(event["read_bytes"]) for event in events)
            row["cpu_us"] = sum(int(event["ProfileEvents"].get(key, 0)) for event in events
                                for key in ["UserTimeMicroseconds", "SystemTimeMicroseconds"])
            row["modeled_100ms_rtt"] = row["http_ms"] + 100 * len(events)
        report["summary"] = {}
        for variant in variants:
            for cold in [False, True]:
                rows = [r for r in report["measurements"] if r["variant"] == variant and r["cold"] == cold]
                good = [r["http_ms"] for r in rows if "http_ms" in r]
                report["summary"][f"{variant}-cold={cold}"] = {"errors": len(rows)-len(good),
                    "http_ms": quantiles(good) if good else None,
                    "means": {key: statistics.mean(r[key] for r in rows if key in r)
                              for key in ["server_ms", "read_rows", "read_bytes", "cpu_us"]
                              if any(key in r for r in rows)}}
    finally:
        (args.output / "report.json").write_text(json.dumps(report, indent=2))
        if proxy:
            proxy.stopping.set()
            proxy.shutdown()
            proxy.server_close()
            proxy_thread.join()
        cluster.close()
    print(json.dumps(report.get("summary", {}), indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--image", default=PROTOCOL.IMAGE)
    parser.add_argument("--node-memory", default="8g")
    parser.add_argument("--rows", type=int, default=3000000)
    parser.add_argument("--samples", type=int, default=50)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--delay-ms", type=float, default=0)
    args = parser.parse_args()
    if args.rows < 2000 or args.samples < 1 or args.samples > args.rows or args.runs < 1 or args.delay_ms < 0:
        parser.error("require rows>=2000, 1<=samples<=rows, runs>=1, delay-ms>=0")
    run(args)
