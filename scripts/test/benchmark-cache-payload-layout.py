#!/usr/bin/env python3
"""Bounded loopback-only ClickHouse payload layout experiment; never uses live services."""
import argparse
import hashlib
import json
import os
import pathlib
import random
import socket
import subprocess
import threading
import time
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
BASE = 500_000_000
ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'


def signature(number):
    raw = hashlib.sha512(str(number).encode()).digest()
    n = int.from_bytes(raw, 'big')
    value = ''
    while n:
        n, digit = divmod(n, 58)
        value = ALPHABET[digit] + value
    return '1' * (len(raw) - len(raw.lstrip(b'\0'))) + value


def fixture_insert(database, start, count):
    # SHA-derived payload chunks prevent repeated padding from dominating compression.
    fields = {
        'signature': 'sig', 'slot': f'{BASE}+intDiv(number,1000)', 'slot_idx': 'number%1000',
        'block_time': 'toInt64(1700000000+intDiv(number,1000))', 'message_hash': "SHA256(concat('message',toString(number)))",
        'is_vote': '0', 'tx_version': 'if(number%2=0,NULL,toUInt8(0))', 'tx_signatures': '[sig]',
        'tx_num_required_signatures': '1', 'tx_num_readonly_unsigned_accounts': '1',
        'tx_account_keys': "arrayMap(i -> SHA256(concat('key',toString(number),':',toString(i))),range(10))",
        'tx_recent_blockhash': "SHA256(concat('block',toString(number)))",
        'tx_instructions_program_id_index': '[9,9]', 'tx_instructions_accounts': '[[0,1,2],[3,4,5]]',
        'tx_instructions_data': "arrayMap(i -> arrayStringConcat(arrayMap(j -> SHA256(concat('instruction',toString(number),':',toString(i),':',toString(j))),range(6))),range(2))",
        'tx_address_table_lookups_present': 'number%2',
        'tx_address_table_lookup_account_key': "if(number%2=1,[SHA256(concat('lookup',toString(number)))],[])",
        'tx_address_table_lookup_writable_indexes': 'if(number%2=1,[[0]],[])',
        'tx_address_table_lookup_readonly_indexes': 'if(number%2=1,[[1]],[])',
        'meta_status_ok': '1', 'meta_fee': '5000', 'meta_pre_balances': 'arrayMap(i -> toUInt64(1000000+i),range(if(number%2=1,12,10)))',
        'meta_post_balances': 'arrayMap(i -> toUInt64(if(i=0,995000,1000000+i)),range(if(number%2=1,12,10)))',
        'meta_inner_instructions_present': '1', 'meta_inner_instructions_index': '[0]',
        'meta_inner_instructions_program_id_index': '[[9]]', 'meta_inner_instructions_accounts': '[[[0,1]]]',
        'meta_inner_instructions_data': "[[SHA256(concat('inner',toString(number)))]]", 'meta_inner_instructions_stack_height': '[[toUInt32(2)]]',
        'meta_log_messages_present': '1',
        'meta_log_messages': "arrayMap(i -> concat('Program log: ',base64Encode(SHA256(concat('log',toString(number),':',toString(i))))),range(if(number%100=0,576,27)))",
        'meta_loaded_addresses_writable': "if(number%2=1,[SHA256(concat('loadedw',toString(number)))],[])",
        'meta_loaded_addresses_readonly': "if(number%2=1,[SHA256(concat('loadedr',toString(number)))],[])",
        'meta_return_data_present': '1', 'meta_return_data_program_id': "SHA256(concat('key',toString(number),':9'))",
        'meta_return_data_data': "arrayStringConcat(arrayMap(i -> SHA256(concat('return',toString(number),':',toString(i))),range(4)))",
        'meta_compute_units_consumed': '10000', 'meta_cost_units': '12000',
    }
    return f"INSERT INTO {database}.transactions ({','.join(fields)}) WITH SHA512(toString(number)) AS sig SELECT {','.join(fields.values())} FROM numbers({start},{count}) SETTINGS max_threads=2"


def table_ddl(database, table, granularity, compact):
    source = (ROOT / f'ddl/replicated/{table}.sql').read_text()
    columns = source[source.index('\n(') + 2:source.index('\n)\nENGINE')]
    if table == 'transactions':
        settings = f'index_granularity={granularity},index_granularity_bytes=10485760'
        if compact:
            settings += ',min_compress_block_size=16384,max_compress_block_size=65536'
        keys = '(slot,slot_idx,signature)'
    else:
        settings = 'allow_experimental_reverse_key=1,index_granularity=512,index_granularity_bytes=67108864,min_bytes_for_wide_part=10485760,compress_primary_key=1,compress_marks=1'
        keys = '(sig_bucket,signature,slot DESC,slot_idx)'
    return f'CREATE TABLE {database}.{table} ({columns}) ENGINE=ReplacingMergeTree(slot) PARTITION BY intDiv(slot,10000) ORDER BY {keys} SETTINGS {settings}'


class Experiment:
    def __init__(self, args):
        self.args = args
        self.path = args.output.resolve()
        self.process = None
        self.database = None
        self.stop = threading.Event()
        self.resource_error = None
        self.reference_digests = None

    def query(self, sql, json_rows=False):
        data = (sql + (' FORMAT JSONEachRow' if json_rows else '')).encode()
        req = urllib.request.Request('http://127.0.0.1:18195/', data=data)
        try:
            result = urllib.request.urlopen(req, timeout=600).read().decode()
        except Exception as exc:
            if hasattr(exc, 'read'):
                raise RuntimeError(exc.read().decode()) from exc
            raise
        return [json.loads(line) for line in result.splitlines()] if json_rows else result

    def guard_resources(self):
        while not self.stop.wait(3):
            size = 0
            try:
                for path in self.path.rglob('*'):
                    try:
                        if path.is_file():
                            size += path.stat().st_size
                    except FileNotFoundError:
                        pass
            except Exception as error:
                self.resource_error = f'disk watchdog failed: {error}'
                self.process.terminate()
                return
            if size > self.args.disk_cap_gib * 1024**3:
                self.resource_error = f'experiment disk cap exceeded: {size}'
                self.process.terminate()
                return

    def start(self):
        for port in (18195,19095):
            with socket.socket() as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind(('127.0.0.1',port))
        self.path.mkdir(parents=True, exist_ok=True)
        if (self.path/'server-data').exists():
            raise RuntimeError('owned server-data already exists; choose a new artifact directory')
        for directory in ('server-data','tmp','user_files'):
            (self.path/directory).mkdir()
        config = f'''<clickhouse><logger><level>warning</level><log>{self.path}/server.log</log><errorlog>{self.path}/error.log</errorlog><size>10M</size><count>2</count></logger><listen_host>127.0.0.1</listen_host><http_port>18195</http_port><tcp_port>19095</tcp_port><path>{self.path}/server-data/</path><tmp_path>{self.path}/tmp/</tmp_path><user_files_path>{self.path}/user_files/</user_files_path><max_server_memory_usage>17179869184</max_server_memory_usage><uncompressed_cache_size>536870912</uncompressed_cache_size><mark_cache_size>536870912</mark_cache_size><max_thread_pool_size>512</max_thread_pool_size><background_pool_size>16</background_pool_size><background_schedule_pool_size>4</background_schedule_pool_size><profiles><default><max_memory_usage>8589934592</max_memory_usage><max_threads>2</max_threads><log_queries>1</log_queries><log_profile_events>1</log_profile_events></default></profiles><users><default><password></password><networks><ip>127.0.0.1</ip></networks><profile>default</profile><quota>default</quota></default></users><quotas><default><interval><duration>3600</duration><queries>0</queries><errors>0</errors><result_rows>0</result_rows><read_rows>0</read_rows><execution_time>0</execution_time></interval></default></quotas><query_log><database>system</database><table>query_log</table><flush_interval_milliseconds>500</flush_interval_milliseconds></query_log><part_log><database>system</database><table>part_log</table><flush_interval_milliseconds>500</flush_interval_milliseconds></part_log><merge_tree><number_of_free_entries_in_pool_to_execute_mutation>2</number_of_free_entries_in_pool_to_execute_mutation><number_of_free_entries_in_pool_to_lower_max_size_of_merge>2</number_of_free_entries_in_pool_to_lower_max_size_of_merge></merge_tree></clickhouse>'''
        (self.path/'config.xml').write_text(config)
        self.process = subprocess.Popen([str(self.args.clickhouse),'server','--config-file='+str(self.path/'config.xml')],stdout=(self.path/'stdout.log').open('w'),stderr=(self.path/'stderr.log').open('w'))
        for _ in range(120):
            if self.process.poll() is not None:
                raise RuntimeError('owned ClickHouse exited; inspect stderr/error logs')
            try:
                version = self.query('SELECT version()').strip()
                assert version == '26.1.2.11', version
                break
            except OSError:
                time.sleep(.5)
        else:
            raise RuntimeError('ClickHouse did not become ready')
        threading.Thread(target=self.guard_resources, daemon=True).start()

    def samples(self):
        rng = random.Random(91489)
        ids = rng.sample(range(self.args.rows),1200)
        result = []
        for index,mode in enumerate(('payload','two_step')):
            chosen = ids[index*600:(index+1)*600]
            for phase in ('first_touch','repeated'):
                for batch in range(3):
                    current = chosen[batch*200:(batch+1)*200].copy()
                    rng.shuffle(current)
                    for number in current:
                        result.append(dict(signature=signature(number),slot=BASE+number//1000,slot_idx=number%1000,batch=batch+1,phase=phase,mode=mode))
        result.sort(key=lambda sample: sample['phase'] != 'first_touch')
        return result

    def layout(self, granularity, compact, samples):
        name = f'g{granularity}-'+('blocks64k' if compact else 'default')
        path = self.path/name
        path.mkdir()
        self.database = 'payload_layout_'+name.replace('-','_')
        database = self.database
        self.query(f'CREATE DATABASE {database}')
        for table in ('transactions','signatures'):
            self.query(table_ddl(database,table,granularity,compact))
            (path/f'{table}-create.sql').write_text(self.query(f'SHOW CREATE TABLE {database}.{table}'))
        # Inspect effective column overrides as well as table settings before any insertion.
        (path/'columns.json').write_text(json.dumps(self.query(f"SELECT table,name,type,compression_codec FROM system.columns WHERE database='{database}' ORDER BY table,position",True),indent=2))
        effective = dict(profile=self.query("SELECT name,value FROM system.settings WHERE name IN ('min_compress_block_size','max_compress_block_size','max_threads')",True), merge_tree=self.query("SELECT name,value FROM system.merge_tree_settings WHERE name IN ('min_compress_block_size','max_compress_block_size','index_granularity','index_granularity_bytes')",True))
        (path/'effective-settings.json').write_text(json.dumps(effective,indent=2))
        started=time.monotonic()
        for offset in range(0,self.args.rows,self.args.insert_rows):
            self.query(fixture_insert(database,offset,min(self.args.insert_rows,self.args.rows-offset)))
            if self.resource_error:
                raise RuntimeError(self.resource_error)
        insert_seconds=time.monotonic()-started
        self.query(f'INSERT INTO {database}.signatures (signature,slot,slot_idx,err) SELECT signature,slot,slot_idx,meta_err FROM {database}.transactions')
        started=time.monotonic()
        for table in ('transactions','signatures'):
            self.query(f'OPTIMIZE TABLE {database}.{table} FINAL SETTINGS max_threads=2')
        merge_seconds=time.monotonic()-started
        for table in ('transactions','signatures'):
            counts=self.query(f'SELECT count() AS n FROM {database}.{table}',True)
            assert counts[0]['n']==self.args.rows,counts
        storage=self.query(f"SELECT table,count() AS parts,sum(rows) AS rows,sum(bytes_on_disk) AS bytes_on_disk,sum(data_uncompressed_bytes) AS uncompressed_bytes,sum(marks) AS marks,sum(marks_bytes) AS marks_bytes,sum(primary_key_bytes_in_memory) AS primary_key_memory FROM system.parts WHERE database='{database}' AND active GROUP BY table",True)
        (path/'storage.json').write_text(json.dumps(dict(insert_seconds=insert_seconds,insert_rows_per_second=self.args.rows/insert_seconds,settle_seconds=merge_seconds,tables=storage),indent=2))
        manifest=dict(database=database,partition_slots=10000,query_max_threads=2,layout=name,samples=samples)
        (path/'manifest.json').write_text(json.dumps(manifest))
        env=dict(os.environ,DISK_CACHE_TEST_URL='http://127.0.0.1:18195',PAYLOAD_LAYOUT_MANIFEST=str(path/'manifest.json'),PAYLOAD_LAYOUT_OUTPUT=str(path/'results.json'))
        with (path/'harness.log').open('w') as log:
            subprocess.run([str(self.args.harness),'payload_layout_benchmark','--ignored','--nocapture'],env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=900)
        output=json.loads((path/'results.json').read_text())
        assert len(output['samples']) == 2400, 'missing benchmark samples'
        digests={}
        for row in output['samples']:
            digest=row['measurements']['payload_digest']
            assert row['signature'] not in digests or digests[row['signature']]==digest, 'first/repeated payload differs'
            digests[row['signature']]=digest
        assert len(digests)==1200, 'unexpected unique signature count'
        if self.reference_digests is None:
            self.reference_digests=digests
        assert digests==self.reference_digests, 'mapped payload differs between layouts'
        (path/'parity.json').write_text(json.dumps(dict(unique_signatures=len(digests),samples=len(output['samples']),matched=True)))
        time.sleep(1)
        self.query('SYSTEM FLUSH LOGS')
        logs=self.query(f"SELECT query_id,toString(type) AS type,query_duration_ms,read_rows,read_bytes,result_rows,result_bytes,exception_code,ProfileEvents FROM system.query_log WHERE has(databases,'{database}') AND type != 'QueryStart'",True)
        (path/'query-log.json').write_text(json.dumps(logs))
        parts=self.query(f"SELECT event_type,count() AS n,sum(duration_ms) AS duration_ms,sum(rows) AS rows,sum(size_in_bytes) AS bytes,sum(ProfileEvents['UserTimeMicroseconds']) AS user_us,sum(ProfileEvents['SystemTimeMicroseconds']) AS system_us FROM system.part_log WHERE database='{database}' GROUP BY event_type",True)
        (path/'part-log.json').write_text(json.dumps(parts))
        self.query(f'DROP DATABASE {database} SYNC')
        self.database=None
        payload=next(table for table in storage if table['table']=='transactions')
        print(name,'complete',round(insert_seconds,2),'s insert',round(merge_seconds,2),'s settle',payload['bytes_on_disk'],'disk bytes',round(payload['uncompressed_bytes']/self.args.rows,1),'uncompressed bytes/row',flush=True)

    def close(self):
        try:
            if self.database and self.process and self.process.poll() is None:
                self.query(f'DROP DATABASE {self.database} SYNC')
        finally:
            self.stop.set()
            if self.process and self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--clickhouse',type=pathlib.Path,required=True)
    parser.add_argument('--harness',type=pathlib.Path,required=True)
    parser.add_argument('--output',type=pathlib.Path,required=True)
    parser.add_argument('--rows',type=int,default=1_000_000)
    parser.add_argument('--insert-rows',type=int,default=25_000)
    parser.add_argument('--disk-cap-gib',type=int,default=100)
    parser.add_argument('--smoke',action='store_true',help='Run baseline and smallest-block layout to validate fixture, settings and parity')
    args=parser.parse_args()
    if args.insert_rows <= 0:
        parser.error('insert-rows must be positive')
    if args.rows<1200 or args.rows>1_000_000 or not 1<=args.disk_cap_gib<=100:
        parser.error('rows must be1200..1000000 and disk cap1..100GiB')
    experiment=Experiment(args)
    try:
        experiment.start()
        samples=experiment.samples()
        layouts=[(g,c) for g in (8192,1024,256,64) for c in (False,True)]
        random.Random(91489).shuffle(layouts)
        if args.smoke:
            layouts=[(8192,False),(64,True)]
        (experiment.path/'layout-order.json').write_text(json.dumps(layouts))
        for granularity,compact in layouts:
            experiment.layout(granularity,compact,samples)
    finally:
        experiment.close()


if __name__=='__main__':
    main()
