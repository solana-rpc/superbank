# Local testnet with native ClickHouse

Run three native processes: ClickHouse, the `superbank` ingestor, and
`superbank-rpc`. No validator or container runtime is required. The gRPC endpoint
selects the Solana network; there is no `--testnet` flag.

## ClickHouse

Install the ClickHouse binary and the Rust/native build prerequisites listed in
the root Dockerfile. The native setup has been exercised with ClickHouse
26.8.1.2041. Use a dedicated data directory so testnet rows cannot mix with another
network's rows. Choose a directory on local disk (for example, ext4), not a
network/FUSE mount. The following commands run from the repository root:

```bash
export SUPERBANK_LOCAL_DIR="/workspaces/superbank-testnet-local"
mkdir -p "$SUPERBANK_LOCAL_DIR/clickhouse" "$SUPERBANK_LOCAL_DIR/genesis"
cat > "$SUPERBANK_LOCAL_DIR/clickhouse/config.xml" <<EOF
<clickhouse>
  <logger><level>information</level><log>$SUPERBANK_LOCAL_DIR/clickhouse/server.log</log><errorlog>$SUPERBANK_LOCAL_DIR/clickhouse/error.log</errorlog><size>20M</size><count>3</count></logger>
  <listen_host>127.0.0.1</listen_host><http_port>8123</http_port><tcp_port>9000</tcp_port>
  <path>$SUPERBANK_LOCAL_DIR/clickhouse/data/</path><tmp_path>$SUPERBANK_LOCAL_DIR/clickhouse/tmp/</tmp_path><user_files_path>$SUPERBANK_LOCAL_DIR/clickhouse/user_files/</user_files_path>
  <max_server_memory_usage>17179869184</max_server_memory_usage>
  <profiles><default><max_memory_usage>4294967296</max_memory_usage></default></profiles>
  <users><default><password></password><networks><ip>127.0.0.1</ip></networks><profile>default</profile><quota>default</quota></default></users>
  <quotas><default><interval><duration>3600</duration><queries>0</queries><errors>0</errors><result_rows>0</result_rows><read_rows>0</read_rows><execution_time>0</execution_time></interval></default></quotas>
</clickhouse>
EOF
clickhouse server --config-file="$SUPERBANK_LOCAL_DIR/clickhouse/config.xml" \
  --pid-file="$SUPERBANK_LOCAL_DIR/clickhouse/server.pid" --daemon
clickhouse client --query 'SELECT version()'
for schema in transactions blocks_metadata entries gsfa signatures token_owner_activity; do
  clickhouse client --multiquery < "ddl/local/$schema.sql" || break
done
```

Wait until the client connects before applying schemas. The example caps server
memory at 16 GiB and individual queries at 4 GiB; these are local limits, not
minimum hardware requirements. It allows the passwordless default user only over
loopback. Data persists across process restarts; the schemas have no automatic TTL.

## Ingest live finalized blocks

Create git-ignored `superbank.yaml` with:

```yaml
source: grpc
endpoint: "http://YOUR_TESTNET_GRPC_HOST:10000"
commitment: finalized
clickhouse-url: "http://127.0.0.1:8123"
clickhouse-database: default
clickhouse-user: default
clickhouse-password: ""
transactions-table: default.transactions
blocks-table: default.blocks_metadata
entries-table: default.entries
metrics-host: "127.0.0.1"
metrics-port: 9901
flush-interval-secs: 5
```

Use `http://` for plaintext gRPC or `https://` for TLS, as required by your
provider. Supply `DRAGONSMOUTH_X_TOKEN` or `x-token` only if authentication is
required. The endpoint must allow unfiltered full blocks with transactions and
entries. Leave gRPC health watching enabled unless the provider does not expose
the health service; in that case set `grpc-health-watch-enabled: false`.

Omitting `dragonsmouth-from-slot` starts live. The root example's value `0`
instead requests the earliest replayable slot. For restart recovery, use
`dragonsmouth-from-slot: "*"` to request the latest stored slot, subject to the
provider's replay retention. It cannot repair older gaps automatically.

```bash
cargo build --release --locked -p superbank -p superbank-rpc
RUST_LOG=info target/release/superbank --config superbank.yaml
```

## Genesis and RPC

Download the current testnet genesis from the official testnet RPC service, or
obtain it from your testnet operator:

```bash
curl -fSs https://api.testnet.solana.com/genesis.tar.bz2 \
  -o "$SUPERBANK_LOCAL_DIR/genesis/genesis.tar.bz2"
tar -xjf "$SUPERBANK_LOCAL_DIR/genesis/genesis.tar.bz2" \
  -C "$SUPERBANK_LOCAL_DIR/genesis" genesis.bin
```

Verify that the SHA-256 hash of `genesis.bin`, encoded as base58, matches
`getGenesisHash` from the same testnet. Use this exact file for `GENESIS_PATH`.
Testnet uses warmup epochs; the default no-warmup schedule gives incorrect
epoch boundaries. `GENESIS_PATH` controls both `getEpochSchedule` and
`getInflationReward` epoch calculations.

In a separate terminal, set `SUPERBANK_LOCAL_DIR` to the same directory and run:

```bash
RPC_HOST=127.0.0.1 RPC_PORT=8899 METRICS_HOST=127.0.0.1 \
CLICKHOUSE_URL=http://127.0.0.1:8123 CLICKHOUSE_DATABASE=default \
GENESIS_PATH="$SUPERBANK_LOCAL_DIR/genesis/genesis.bin" \
RUST_LOG=info target/release/superbank-rpc
```

CLI flags and environment variables override ingestor YAML. RPC connection
settings are separate: passing the ingestor YAML to RPC does not configure its
ClickHouse connection.

## Verify and stop

```bash
curl -fSs http://127.0.0.1:9901/health
clickhouse client --query 'SELECT count(), min(slot), max(slot) FROM blocks_metadata'
curl -sS http://127.0.0.1:8899 -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getEpochSchedule"}'
curl -sS http://127.0.0.1:8899 -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getSlot"}'
```

Stored slots should advance, and `getEpochSchedule` should match testnet.
Block/transaction queries cover only ingested history. Reward queries additionally
need the payout boundary and relevant partition blocks; a correct schedule alone
does not make past rewards available. Use the bounded RPC backfill described in
the ingestor README if those blocks are needed, with a testnet RPC URL.

Stop foreground Superbank processes with Ctrl-C. Stop this dedicated ClickHouse
instance with `kill -TERM "$(cat "$SUPERBANK_LOCAL_DIR/clickhouse/server.pid")"`.
Retain its data directory for the next run. Following a testnet ledger reset,
use a fresh data directory and matching genesis file.

## Switching to devnet

Stop the testnet processes and preserve their data directory. Repeat the native
setup with a fresh `SUPERBANK_LOCAL_DIR`, such as
`/workspaces/superbank-devnet-local`, and a devnet gRPC endpoint. Set
`superbank.yaml`'s `endpoint` to that endpoint (including `http://` for plaintext).
Download genesis from `https://api.devnet.solana.com/genesis.tar.bz2` and verify
its hash against `getGenesisHash` at `https://api.devnet.solana.com`. Point RPC's
`GENESIS_PATH` to the devnet file. Reuse the same localhost ports only after the
testnet processes have stopped.

Do not reuse the testnet tables: rows are keyed by slot/signature, without a
network identifier. Compare local block hashes and `getEpochSchedule` with
public devnet after ingestion begins. Devnet currently uses no warmup epochs;
using its actual genesis file makes that setting explicit.
