#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Exercise production local archive/restore and RPC hydration on disposable ClickHouse.

Build superbank, superbank-solparq and superbank-rpc first. Run with
DISK_CACHE_TEST_URL=http://127.0.0.1:18196 python3 scripts/test/agave43-archive-roundtrip.py
Only uniquely named test databases are created/dropped. No external RPC is used.
"""
import contextlib
import http.server
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]
URL = os.environ['DISK_CACHE_TEST_URL']
PARSED = urllib.parse.urlparse(URL)
if PARSED.scheme != 'http' or PARSED.hostname != '127.0.0.1' or not PARSED.port:
    raise ValueError('DISK_CACHE_TEST_URL must be an explicit loopback HTTP endpoint')
ENV = {key: os.environ[key] for key in ('PATH', 'HOME', 'TMPDIR') if key in os.environ}


def query(sql):
    request = urllib.request.Request(URL, data=sql.encode())
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read()


def statements(text):
    text = '\n'.join(line for line in text.splitlines() if not line.lstrip().startswith('--'))
    quoted, escaped, start = False, False, 0
    for index, char in enumerate(text):
        if escaped:
            escaped = False
        elif char == '\\' and quoted:
            escaped = True
        elif char == "'":
            quoted = not quoted
        elif char == ';' and not quoted:
            yield text[start:index]
            start = index + 1
    if text[start:].strip():
        yield text[start:]


def schema(database):
    query(f'CREATE DATABASE {database}')
    for table in ('transactions', 'blocks_metadata', 'entries', 'signatures', 'gsfa',
                  'gsfa_hot', 'gsfa_nohot', 'token_owner_activity'):
        ddl = (ROOT / 'ddl/local' / (table + '.sql')).read_text()
        for sql in statements(ddl.replace('default.', database + '.')):
            query(sql)


def seed(database):
    for slot, spelling in ((10, 'VATDebit'), (11, 'validator-admission-ticket-debit'), (12, 'VATDebit')):
        # Slot 12 represents historical rewards without commissionBps columns populated.
        bps = '[]' if slot == 12 else '[NULL,725]'
        rewards = (f"[toFixedString('vat',32),toFixedString('stake',32)],[-10,100],"
                   f"[90,200],['{spelling}','Staking'],[NULL,7],{bps}")
        query(f"INSERT INTO {database}.blocks_metadata (slot,parent_slot,blockhash,parent_blockhash,block_height,executed_transaction_count,rewards_present,rewards_pubkey,rewards_lamports,rewards_post_balance,rewards_type,rewards_commission,rewards_commission_bps) VALUES ({slot},{slot-1},toFixedString('hash',32),toFixedString('hash',32),{slot},1,1,{rewards})")
        query(f"INSERT INTO {database}.transactions (signature,slot,slot_idx,tx_signatures,tx_account_keys,tx_num_required_signatures,meta_status_ok,meta_pre_balances,meta_post_balances,meta_rewards_present,meta_reward_pubkey,meta_reward_lamports,meta_reward_post_balance,meta_reward_type,meta_reward_commission,meta_reward_commission_bps) VALUES (toFixedString('sig-{slot}',64),{slot},0,[toFixedString('sig-{slot}',64)],[toFixedString('key',32)],1,1,[1],[1],1,{rewards})")


class Reference(http.server.BaseHTTPRequestHandler):
    """Deterministic produced-slot reference for archive completeness checks only."""
    def log_message(self, *_args):
        pass

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        body = {'jsonrpc': '2.0', 'id': request['id']}
        if request['method'] == 'getSlot':
            body['result'] = 12
        elif request['method'] == 'getBlocks':
            start, end = request['params'][:2]
            body['result'] = [slot for slot in (10, 11, 12) if start <= slot <= end]
        else:
            body['error'] = {'code': -32601, 'message': 'Unsupported fixture method'}
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())


def run(binary, args, directory):
    with (directory / (binary + '.log')).open('w') as log:
        result = subprocess.run([str(ROOT / 'target/debug' / binary)] + args,
                                env=ENV, stdout=log, stderr=log, timeout=120)
    if result.returncode:
        raise RuntimeError((directory / (binary + '.log')).read_text())


def rpc(url, method, params):
    payload = json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params}).encode()
    request = urllib.request.Request(url, data=payload, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=5) as response:
        body = json.load(response)
    assert 'error' not in body, body
    return body['result']


@contextlib.contextmanager
def rpc_server(database, directory):
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    env = dict(ENV, RPC_HOST='127.0.0.1', RPC_PORT=str(port), METRICS_HOST='127.0.0.1',
               METRICS_PORT='0', CLICKHOUSE_URL=URL, CLICKHOUSE_DATABASE=database,
               CLICKHOUSE_CLUSTER='', RUST_LOG='warn')
    for key, table in {'TRANSACTION': 'transactions', 'BLOCKS_METADATA': 'blocks_metadata',
                       'GSFA': 'gsfa', 'GSFA_HOT': 'gsfa_hot', 'SIGNATURE_STATUSES': 'signatures',
                       'TOKEN_OWNER_ACTIVITY': 'token_owner_activity'}.items():
        env['CLICKHOUSE_' + key + '_TABLE'] = database + '.' + table
    with (directory / (database + '-rpc.log')).open('w') as log:
        process = subprocess.Popen([str(ROOT / 'target/debug/superbank-rpc')], env=env,
                                   stdout=log, stderr=log)
        endpoint = f'http://127.0.0.1:{port}'
        try:
            for _ in range(100):
                if process.poll() is not None:
                    raise RuntimeError('RPC exited; inspect ' + str(directory))
                try:
                    rpc(endpoint, 'getSlot', [])
                    break
                except OSError:
                    time.sleep(.1)
            else:
                raise RuntimeError('RPC startup timed out')
            yield endpoint
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def responses(database, directory):
    result = []
    with rpc_server(database, directory) as endpoint:
        for slot in (10, 11, 12):
            block = rpc(endpoint, 'getBlock', [slot, {'encoding': 'json', 'maxSupportedTransactionVersion': 1}])
            for rewards in (block['rewards'], block['transactions'][0]['meta']['rewards']):
                assert rewards[0]['rewardType'] == 'VATDebit', rewards
                assert rewards[0]['lamports'] == -10 and rewards[0]['postBalance'] == 90, rewards
                assert rewards[1]['commission'] == 7, rewards
                assert rewards[1].get('commissionBps') == (None if slot == 12 else 725), rewards
            result.append(block)
    return result


def main():
    prefix = 'test_agave43_archive_' + uuid.uuid4().hex
    source, restored = prefix + '_source', prefix + '_restored'
    directory = Path(tempfile.mkdtemp(prefix='agave43-archive-'))
    reference = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Reference)
    threading.Thread(target=reference.serve_forever, daemon=True).start()
    try:
        schema(source)
        schema(restored)
        seed(source)
        expected = responses(source, directory)
        run('superbank-solparq', ['--db-server', '127.0.0.1', '--db-server-port', str(PARSED.port),
            '--db-database', source, '--db-user', 'default', '--db-password', '',
            '--archive-range-type', 'custom', '--archive-slot-range', '10-12',
            '--archive-file-output-location', str(directory / 'bundles'),
            '--solana-rpc-url', f'http://127.0.0.1:{reference.server_port}'], directory)
        manifests = list((directory / 'bundles').rglob('manifest.json'))
        assert len(manifests) == 1, manifests
        run('superbank', ['--source', 'solparq', '--solparq-archive-location', 'local',
            '--solparq-archive-path', str(manifests[0].parent), '--clickhouse-url', URL,
            '--clickhouse-database', restored, '--clickhouse-user', 'default',
            '--clickhouse-password', '', '--metrics-host', '127.0.0.1', '--metrics-port', '0'], directory)
        for table in ('transactions', 'blocks_metadata'):
            columns = 'meta_reward_' if table == 'transactions' else 'rewards_'
            fields = ','.join(columns + name for name in ('pubkey','lamports','post_balance','type','commission','commission_bps'))
            before = query(f'SELECT {fields} FROM {source}.{table} ORDER BY slot FORMAT JSONEachRow')
            after = query(f'SELECT {fields} FROM {restored}.{table} ORDER BY slot FORMAT JSONEachRow')
            assert after == before, table
        assert responses(restored, directory) == expected
        print('PASS: production archive/restore preserves reward columns and hydrated blocks')
        print('Artifacts:', directory)
    finally:
        reference.shutdown()
        reference.server_close()
        query(f'DROP DATABASE IF EXISTS {restored} SYNC')
        query(f'DROP DATABASE IF EXISTS {source} SYNC')


if __name__ == '__main__':
    main()
