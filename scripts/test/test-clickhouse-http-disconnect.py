#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Exercise HTTP disconnect cancellation on an isolated three-node ClickHouse cluster.

Requires Docker and the Rust build prerequisites. Never connects to an existing ClickHouse endpoint. Example:
  python3 scripts/test/test-clickhouse-http-disconnect.py --output /tmp/ch-disconnect

The proxy is deliberately small and transparent; this proves server/proxy mechanics,
not the deployed gateway's behavior. Four positive cases must cancel; two negative
gateway controls must demonstrate incompatibility for the suite to pass. A gateway
must discard queued requests when its downstream closes. All containers and the private network are removed
on exit. JSON evidence and generated configuration remain in --output.
"""
import argparse
import contextlib
import http.client
import http.server
import os
import json
import pathlib
import select
import socket
import socketserver
import subprocess
import threading
import time
import urllib.parse
import uuid


IMAGE = "clickhouse/clickhouse-server:26.2.3.2"
CANCELLED = {394, 735}  # QUERY_WAS_CANCELLED / QUERY_WAS_CANCELLED_BY_CLIENT


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True, stderr=subprocess.STDOUT).strip()


def wait_for(check, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.1)
    raise AssertionError("Timed out waiting for fixture condition")


class Cluster:
    def __init__(self, output, image):
        self.output, self.image = output, image
        self.name = "ch-disconnect-" + uuid.uuid4().hex[:10]
        self.nodes = [self.name + f"-{i}" for i in range(3)]
        self.created = []
        self.ports = []

    def configuration(self):
        shards = "".join(f"<shard><replica><host>{node}</host><port>9000</port>"
                         "</replica></shard>" for node in self.nodes)
        return f"""<clickhouse>
<listen_host>0.0.0.0</listen_host><max_thread_pool_size>256</max_thread_pool_size>
<background_pool_size>4</background_pool_size><background_schedule_pool_size>4</background_schedule_pool_size>
<background_merges_mutations_concurrency_ratio>8</background_merges_mutations_concurrency_ratio>
<remote_servers><fixture>{shards}</fixture></remote_servers>
<macros><cluster>fixture</cluster></macros>
<zookeeper><node><host>{self.nodes[0]}</host><port>9181</port></node></zookeeper>
<distributed_ddl><path>/fixture/ddl</path><pool_size>1</pool_size></distributed_ddl>
<query_log><flush_interval_milliseconds>100</flush_interval_milliseconds></query_log>
</clickhouse>"""

    def start(self):
        docker("network", "create", self.name)
        common = self.output / "cluster.xml"
        common.write_text(self.configuration())
        keeper = self.output / "keeper.xml"
        keeper.write_text("""<clickhouse><keeper_server><tcp_port>9181</tcp_port>
<server_id>1</server_id><log_storage_path>/var/lib/clickhouse/keeper/log</log_storage_path>
<snapshot_storage_path>/var/lib/clickhouse/keeper/snapshots</snapshot_storage_path>
<raft_configuration><server><id>1</id><hostname>localhost</hostname><port>9234</port>
</server></raft_configuration></keeper_server></clickhouse>""")
        for i, name in enumerate(self.nodes):
            args = ["run", "-d", "--name", name, "--hostname", name, "--network", self.name,
                    "--cpus", "2", "--memory", "2g", "-p", "127.0.0.1::8123",
                    "-e", "CLICKHOUSE_SKIP_USER_SETUP=1", "-v",
                    f"{common}:/etc/clickhouse-server/config.d/fixture.xml:ro"]
            if i == 0:
                args += ["-v", f"{keeper}:/etc/clickhouse-server/config.d/keeper.xml:ro"]
            self.created.append(name)
            docker(*args, self.image)
            self.ports.append(int(docker("port", name, "8123/tcp").rsplit(":", 1)[1]))
        for i in range(3):
            wait_for(lambda i=i: self.ready(i), 90)
        versions = [self.sql("SELECT version()", i).strip() for i in range(3)]
        assert len(set(versions)) == 1, versions
        self.version = versions[0]
        for i in range(3):
            self.sql("CREATE VIEW slow AS "
                     "SELECT number, sleepEachRow(0.05) AS delay FROM numbers(600)", i)
        for i in range(3):
            self.sql("CREATE TABLE fixture_signatures_local (signature FixedString(64), "
                     "sig_bucket UInt64 MATERIALIZED cityHash64(signature)%32,slot UInt64, "
                     "slot_idx UInt32,err Nullable(String)) ENGINE=MergeTree "
                     "ORDER BY (sig_bucket,signature,slot,slot_idx)", i)
        self.sql("CREATE TABLE fixture_signatures AS fixture_signatures_local "
                 "ENGINE=Distributed(fixture,default,fixture_signatures_local)")
        self.sql("INSERT INTO fixture_signatures_local (signature,slot,slot_idx,err) VALUES "
                 f"(unhex('{('11' * 64)}'),17,1,NULL),"
                 f"(unhex('{('33' * 64)}'),19,2,'\"BlockhashNotFound\"')")
        self.sql("CREATE TABLE slow_all (number UInt64, delay UInt8) "
                 "ENGINE=Distributed(fixture,default,slow)")

    def sql(self, query, node=0):
        connection = http.client.HTTPConnection("127.0.0.1", self.ports[node], timeout=5)
        try:
            connection.request("POST", "/", query.encode())
            response = connection.getresponse()
            body = response.read().decode()
            if response.status != 200:
                raise RuntimeError(body)
            return body
        finally:
            connection.close()

    def ready(self, node):
        try:
            return self.sql("SELECT 1", node).strip() == "1"
        except (OSError, RuntimeError, http.client.HTTPException):
            return False

    def active(self, query_id):
        query = ("SELECT count() FROM system.processes WHERE initial_query_id='"
                 + query_id + "' OR query_id='" + query_id + "'")
        return [int(self.sql(query, i).strip()) for i in range(3)]

    def terminals(self, query_id):
        rows = []
        for i in range(3):
            query = ("SELECT type,query_id,initial_query_id,is_initial_query,exception_code,"
                     "query_duration_ms,Settings FROM system.query_log WHERE "
                     f"initial_query_id='{query_id}' AND type!='QueryStart' FORMAT JSONEachRow")
            rows.extend(dict(json.loads(line), node=i) for line in self.sql(query, i).splitlines())
        return rows

    def close(self):
        for name in reversed(self.created):
            with contextlib.suppress(subprocess.CalledProcessError):
                docker("cp", f"{name}:/var/log/clickhouse-server/clickhouse-server.err.log",
                       str(self.output / f"{name}.err.log"))
            with contextlib.suppress(subprocess.CalledProcessError):
                logs = docker("logs", name)
                (self.output / f"{name}.log").write_text(logs)
                docker("rm", "-f", name)
        with contextlib.suppress(subprocess.CalledProcessError):
            docker("network", "rm", self.name)


class Proxy:
    """One connection; propagate EOF, or deliberately ignore it for the control."""
    def __init__(self, upstream_port, propagate, forward_delay=0):
        self.upstream_port, self.propagate = upstream_port, propagate
        self.forward_delay = forward_delay
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.port = self.listener.getsockname()[1]
        self.stop = threading.Event()
        self.disconnected = threading.Event()
        self.error = None
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def relay(self, client, upstream):
        downstream = True
        buffered = bytearray()
        forward_at = time.monotonic() + self.forward_delay
        while not self.stop.is_set():
            if buffered and time.monotonic() >= forward_at:
                upstream.sendall(buffered)
                buffered.clear()
            ready, _, _ = select.select([upstream] + ([client] if downstream else []), [], [], 0.1)
            for source in ready:
                data = source.recv(65536)
                if not data and source is client:
                    self.disconnected.set()
                    downstream = False
                    if self.propagate:
                        return
                elif not data:
                    return
                elif source is client:
                    buffered.extend(data)
                elif downstream:
                    client.sendall(data)

    def run(self):
        try:
            client, _ = self.listener.accept()
            with client, socket.create_connection(("127.0.0.1", self.upstream_port)) as upstream:
                self.relay(client, upstream)
        except OSError as error:
            if not self.stop.is_set():
                self.error = repr(error)
        finally:
            self.listener.close()

    def close(self):
        self.stop.set()
        self.thread.join(timeout=2)
        assert not self.thread.is_alive(), "Proxy did not stop"
        assert self.error is None, self.error


class GatewayHandler(socketserver.BaseRequestHandler):
    """Transparent persistent connections, including production binary compression."""
    def handle(self):
        try:
            with socket.create_connection(("127.0.0.1", self.server.upstream_port)) as upstream:
                sockets = [self.request, upstream]
                while not self.server.stopping.is_set():
                    ready, _, _ = select.select(sockets, [], [], 0.1)
                    for source in ready:
                        data = source.recv(65536)
                        if not data:
                            return
                        target = upstream if source is self.request else self.request
                        target.sendall(data)
        except OSError:
            return


class ControlHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_POST(self):
        cluster = self.server.cluster
        actions = {
            "/pause-replica": lambda: docker("pause", cluster.nodes[2]),
            "/resume-replica": lambda: docker("unpause", cluster.nodes[2]),
            "/block-ddl": lambda: block_ddl(cluster),
            "/assert-ddl-blocked": lambda: assert_ddl_blocked(cluster),
        }
        try:
            actions[self.path]()
            self.send_response(200)
        except (KeyError, AssertionError, RuntimeError, subprocess.CalledProcessError):
            self.send_response(500)
        self.send_header("Content-Length", "0")
        self.end_headers()


def rust_test_binary(output, explicit):
    if explicit:
        return explicit.resolve()
    root = pathlib.Path(__file__).resolve().parents[2]
    command = ["cargo", "test", "-p", "superbank-rpc", "--lib", "--all-features", "--locked",
               "--no-run", "--message-format=json"]
    with (output / "rust-build.log").open("w") as log:
        result = subprocess.run(command, cwd=root, stdout=subprocess.PIPE, stderr=log, text=True)
    artifacts = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
    with (output / "rust-build.log").open("a") as log:
        for row in artifacts:
            if row.get("reason") == "compiler-message":
                log.write(row.get("message", {}).get("rendered") or "")
    result.check_returncode()
    executables = {row["executable"] for row in artifacts if row.get("reason") == "compiler-artifact"
                   and row.get("executable") and row.get("profile", {}).get("test")
                   and row.get("target", {}).get("name") == "superbank_rpc"
                   and row.get("target", {}).get("kind") == ["lib"]}
    assert len(executables) == 1, f"Expected one RPC test executable: {executables}"
    return pathlib.Path(executables.pop())


def validate_rust_test_binary(executable):
    test_filter = "clickhouse::disconnect::integration_tests::"
    listed = subprocess.check_output([str(executable), test_filter, "--ignored", "--list"], text=True)
    assert listed.count(": test") == 6, "All six real protocol tests must be present in the library test binary"


def run_rust_integration(cluster, executable):
    gateway = socketserver.ThreadingTCPServer(("127.0.0.1", 0), GatewayHandler)
    gateway.daemon_threads = True
    gateway.upstream_port = cluster.ports[0]
    gateway.stopping = threading.Event()
    control = http.server.ThreadingHTTPServer(("127.0.0.1", 0), ControlHandler)
    control.cluster = cluster
    workers = [threading.Thread(target=server.serve_forever, daemon=True) for server in (gateway, control)]
    for worker in workers:
        worker.start()
    env = os.environ.copy()
    env["SUPERBANK_DISCONNECT_TEST_URL"] = f"http://127.0.0.1:{gateway.server_address[1]}"
    env["SUPERBANK_DISCONNECT_TEST_CONTROL_URL"] = f"http://127.0.0.1:{control.server_address[1]}"
    test_filter = "clickhouse::disconnect::integration_tests::"
    command = [str(executable), test_filter, "--ignored", "--nocapture", "--test-threads=1"]
    try:
        result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=300)
        (cluster.output / "rust-integration.log").write_text(result.stdout + result.stderr)
        result.check_returncode()
        assert "6 passed; 0 failed" in result.stdout, "No silently skipped protocol tests"
        return {"passed": True, "tests": 6, "production_validation_and_compression": True}
    finally:
        with contextlib.suppress(subprocess.CalledProcessError):
            docker("unpause", cluster.nodes[2])
        gateway.stopping.set()
        for server in (gateway, control):
            server.shutdown()
            server.server_close()
        for worker in workers:
            worker.join(timeout=2)


def request_bytes(query_id, streaming):
    params = {"query_id": query_id, "readonly": 2,
              "cancel_http_readonly_queries_on_client_close": 1,
              "max_threads": 1, "max_block_size": 1, "max_execution_time": 35,
              "max_execution_time_leaf": 35, "use_hedged_requests": 0,
              "max_parallel_replicas": 1, "log_query_settings": 1,
              "wait_end_of_query": 0, "buffer_size": 1}
    query = "SELECT " + ("number,delay" if streaming else "sum(delay)") + " FROM slow_all FORMAT JSONEachRow"
    body = query.encode()
    return (f"POST /?{urllib.parse.urlencode(params)} HTTP/1.1\r\nHost: localhost\r\n"
            f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode() + body


def receive_stream(client):
    client.settimeout(5)
    received = b""
    while b"\r\n\r\n" not in received or not received.split(b"\r\n\r\n", 1)[1]:
        chunk = client.recv(65536)
        assert chunk, "Query finished before streaming abandonment"
        received += chunk
    assert b"200 OK" in received.split(b"\r\n", 1)[0], received[:400]
    return received


def validate_terminals(terminals):
    assert len(terminals) == 3 and {r['node'] for r in terminals} == {0, 1, 2}, terminals
    assert sum(r['is_initial_query'] for r in terminals) == 1, terminals
    assert all(r['Settings'].get('readonly') == '2' and
               r['Settings'].get('cancel_http_readonly_queries_on_client_close') == '1'
               for r in terminals), terminals
    assert all(r['type'] != 'QueryFinish' and r['exception_code'] in CANCELLED for r in terminals), terminals


def run_case(cluster, name, streaming=False, propagate=True):
    query_id = "http_disconnect_" + name + "_" + uuid.uuid4().hex[:8]
    proxy = Proxy(cluster.ports[0], propagate)
    client = socket.create_connection(("127.0.0.1", proxy.port))
    received = b""
    try:
        client.sendall(request_bytes(query_id, streaming))
        wait_for(lambda: all(cluster.active(query_id)), 10)
        if streaming:
            received = receive_stream(client)
        else:
            assert not select.select([client], [], [], 0)[0], "Headers already received"
        abandoned = time.monotonic()
        client.close()
        assert proxy.disconnected.wait(1), "Proxy did not observe actual client close"
        if propagate:
            wait_for(lambda: not any(cluster.active(query_id)), 5)
            elapsed = time.monotonic() - abandoned
            assert elapsed <= 5, "Observed termination exceeded five seconds"
            terminals = wait_for(lambda: cluster.terminals(query_id), 5)
            wait_for(lambda: len(cluster.terminals(query_id)) >= 3, 5)
            terminals = cluster.terminals(query_id)
            validate_terminals(terminals)
        else:
            time.sleep(5.2)
            assert all(cluster.active(query_id)), "Negative control unexpectedly stopped"
            elapsed, terminals = None, []
        return {"name": name, "query_id": query_id, "streaming": streaming,
                "proxy_propagates_disconnect": propagate,
                "cancel_gate_pass": propagate, "stop_after_disconnect_seconds": elapsed,
                "received_bytes": len(received), "terminals": terminals}
    finally:
        client.close()
        proxy.close()
        wait_for(lambda: not any(cluster.active(query_id)), 5)


def delayed_forwarding_control(cluster):
    """Expose the unsupported contract: queued HTTP work forwarded after EOF."""
    query_id = "http_disconnect_delayed_" + uuid.uuid4().hex[:8]
    proxy = Proxy(cluster.ports[0], False, forward_delay=1.5)
    client = socket.create_connection(("127.0.0.1", proxy.port))
    try:
        client.sendall(request_bytes(query_id, False))
        client.close()
        assert proxy.disconnected.wait(1)
        abandoned = time.monotonic()
        quiet = []
        for _ in range(2):
            assert not any(cluster.active(query_id))
            quiet.append(time.monotonic() - abandoned)
            time.sleep(0.25)
        wait_for(lambda: all(cluster.active(query_id)), 10)
        return {"name": "negative_delayed_forwarding", "query_id": query_id,
                "cancel_gate_pass": False, "expected_incompatible_gateway": True,
                "quiet_observations_seconds": quiet,
                "appeared_after_disconnect_seconds": time.monotonic() - abandoned,
                "limitation": "Two quiet probes cannot prove a gateway will not submit work later."}
    finally:
        client.close()
        proxy.close()
        wait_for(lambda: not any(cluster.active(query_id)), 5)


def assert_ddl_blocked(cluster):
    count = int(cluster.sql("SELECT count() FROM system.distributed_ddl_queue "
                            "WHERE query LIKE '%ddl_queued%' AND status='Inactive'").strip())
    assert count == 3, "DDL blocker finished before cancellation verification"
    return count


def block_ddl(cluster):
    cluster.sql("CREATE TABLE ddl_blocker ON CLUSTER fixture ENGINE=Memory AS SELECT "
                "number,sleepEachRow(0.05) delay FROM numbers(800) "
                "SETTINGS max_block_size=1,distributed_ddl_task_timeout=0")
    wait_for(lambda: int(cluster.sql("SELECT count() FROM system.distributed_ddl_queue "
                                    "WHERE query LIKE 'CREATE TABLE default.ddl_blocker%' "
                                    "AND status='Active'").strip()) == 3, 10)
    cluster.sql("CREATE TABLE ddl_queued ON CLUSTER fixture (x UInt8) ENGINE=Memory "
                "SETTINGS distributed_ddl_task_timeout=0")
    return wait_for(lambda: int(cluster.sql("SELECT count() FROM system.distributed_ddl_queue "
                                          "WHERE query LIKE '%ddl_queued%' AND status='Inactive'").strip()) == 3)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--image", default=IMAGE)
    parser.add_argument("--rust-test-binary", type=pathlib.Path, help="Use a prebuilt RPC test executable")
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    executable = rust_test_binary(args.output, args.rust_test_binary)
    validate_rust_test_binary(executable)
    cluster = Cluster(args.output, args.image)
    report = {"image": args.image, "cases": [], "passed": False}
    try:
        cluster.start()
        report["version"] = cluster.version
        report["image_digest"] = docker("image", "inspect", args.image, "--format", "{{json .RepoDigests}}")
        for name, stream, propagate in [("before_headers", False, True), ("streaming", True, True),
                                         ("negative_gateway", False, False)]:
            result = run_case(cluster, name, stream, propagate)
            report["cases"].append(result)
            print(json.dumps(result), flush=True)
        delayed = delayed_forwarding_control(cluster)
        report["cases"].append(delayed)
        print(json.dumps(delayed), flush=True)
        block_ddl(cluster)
        for name, stream in [("ddl_blocked_before_headers", False), ("ddl_blocked_streaming", True)]:
            result = run_case(cluster, name, stream)
            report["cases"].append(result)
            print(json.dumps(result), flush=True)
        report["queued_ddl_still_inactive"] = int(cluster.sql(
            "SELECT count() FROM system.distributed_ddl_queue WHERE query LIKE '%ddl_queued%' "
            "AND status='Inactive'").strip())
        assert report["queued_ddl_still_inactive"] == 3, "DDL blocker finished before gate completed"
        report["kill_queries"] = [int(cluster.sql(
            "SELECT count() FROM system.query_log WHERE startsWith(query, 'KILL QUERY')", i).strip())
            for i in range(3)]
        assert report["kill_queries"] == [0, 0, 0]
        wait_for(lambda: int(cluster.sql("SELECT count() FROM system.distributed_ddl_queue "
                                        "WHERE status IN ('Active','Inactive')").strip()) == 0, 50)
        cluster.sql("DROP TABLE ddl_blocker ON CLUSTER fixture SETTINGS distributed_ddl_task_timeout=10")
        cluster.sql("DROP TABLE ddl_queued ON CLUSTER fixture SETTINGS distributed_ddl_task_timeout=10")
        report["rust_integration"] = run_rust_integration(cluster, executable)
        report["kill_queries"] = [int(cluster.sql(
            "SELECT count() FROM system.query_log WHERE startsWith(query, 'KILL QUERY')", i).strip())
            for i in range(3)]
        assert report["kill_queries"] == [0, 0, 0]
        report["passed"] = True
    finally:
        (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        cluster.close()


if __name__ == "__main__":
    main()
