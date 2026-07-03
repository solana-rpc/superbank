# Self-Hosting Superbank

Superbank is an open-source Solana transaction indexer built by Triton. It ingests the full ledger into ClickHouse and exposes a Solana-compatible JSON-RPC endpoint — giving you your own queryable copy of the chain, with no rate limits and sub-millisecond lookups on historical data.

This guide walks through running Superbank on a single node from scratch: ClickHouse setup, DDL, ingestion configuration, and the RPC server. It also covers how to filter the index to only your app's transactions.

> **Scope — live and recent ingestion.** This guide connects Superbank to Dragon's Mouth or Fumarole to index Solana from the current tip forward. For historical backfill from genesis through any past slot, see the companion guide: **[Self-hosting Solana history: step-by-step walkthrough for backfill](guide-self-hosting-history-backfill.md)** — it walks through streaming Old Faithful CAR archives into this same Superbank schema at up to 2.7M TPS using Anza's Jetstreamer.

---

## Self-hosting vs. Triton managed

**Self-host when:**
- You need full control over what's indexed (e.g., filter to only your program's transactions)
- You have compliance or data-residency requirements
- Your query volume is high enough that operating your own node is cheaper than per-request pricing
- You want to run custom queries directly against ClickHouse

**Use Triton's managed endpoints when:**
- You want the fastest path to production — no ClickHouse operations, no infra
- You don't need to index historical data beyond what Triton retains
- You'd rather not maintain the ingestor and RPC layer yourself

Both options expose the same RPC interface, so you can start managed and migrate to self-hosted later without changing your client code.

---

## How the components fit together

Superbank has three parts, each running as a separate process:

```
[Dragon's Mouth / Fumarole / RPC]
          ↓
  superbank (ingestor)      ← Rust binary; runs on your host or in a container
          ↓
     ClickHouse             ← runs in Docker (this guide) or native install
          ↑
  superbank-rpc             ← Rust binary; runs on your host or in a container
          ↓
  Your app / JSON-RPC clients
```

- **ClickHouse** is the storage layer. The two binaries connect to it over HTTP (port 8123).
- **`superbank`** reads from your ingestion source and writes rows into ClickHouse.
- **`superbank-rpc`** reads from ClickHouse and serves the Solana-compatible RPC API.

None of the three components need to run on the same machine — they only need to be network-reachable to each other. For a single-node setup, running all three on one host is the simplest approach.

---

## What Superbank provides

Once running, Superbank serves these RPC methods against your local ClickHouse:

| Method | Notes |
|---|---|
| `getSignaturesForAddress` | Standard + `beforeSlot`/`untilSlot` extensions |
| `getSignatureStatuses` | |
| `getTransaction` | Standard + `slot` hint extension |
| `getBlock` | |
| `getTransactionsForAddress` | Triton extension — pagination + token account filtering |
| `getBlockHeight`, `getSlot`, `getLatestBlockhash` | Chain tip queries |
| `getBlockTime`, `getBlocks`, `getBlocksWithLimit` | |
| `getHealth`, `getFirstAvailableBlock`, `minimumLedgerSlot` | |
| `getInflationReward`, `getTransactionCount` | |

Adding `processed` commitment (in addition to `confirmed`/`finalized`) requires the optional gRPC head-cache feature — covered at the end of this guide.

---

## Prerequisites

- **ClickHouse** — 26.x or later. Docker is the fastest way to get one running.
- **Rust** — stable toolchain, 1.80+. Install via [rustup.rs](https://rustup.rs).
- **A Dragon's Mouth gRPC endpoint or Fumarole subscription** — for live ingestion. Get one at [customers.triton.one](https://customers.triton.one). For backfill only, the JSON-RPC source works with any public endpoint.
- Git and standard build tools (`gcc`, `pkg-config`, `libssl-dev` on Linux / Xcode Command Line Tools on macOS).

---

## Step 1: Clone and build

```bash
git clone --recurse-submodules https://github.com/solana-rpc/superbank.git
cd superbank

# Build both binaries
cargo build --release -p superbank -p superbank-rpc
```

The build produces `./target/release/superbank` (ingestor) and `./target/release/superbank-rpc` (RPC server).

---

## Step 2: Start ClickHouse

You have three options:

**Option A — Docker (recommended for getting started)**

The fastest path — no installation required beyond Docker itself:

```bash
docker run -d \
  --name superbank-clickhouse \
  -p 8123:8123 \
  -p 9000:9000 \
  --ulimit nofile=262144:262144 \
  -e CLICKHOUSE_DB=default \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD="" \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  clickhouse/clickhouse-server:26.1.2.11
```

**Option B — Native install (recommended for production)**

For production servers, install ClickHouse directly on the host for better performance and easier disk/memory tuning. Follow the [official install guide](https://clickhouse.com/docs/en/install) for your OS. Use version 26.x or later.

**Option C — ClickHouse Cloud or Altinity**

If you'd rather not manage ClickHouse infrastructure yourself, both [ClickHouse Cloud](https://clickhouse.cloud) and [Altinity.Cloud](https://altinity.com/cloud-database/) offer hosted ClickHouse. Point `clickhouse-url` at your cloud endpoint and set credentials accordingly.

---

Check it's up:

```bash
curl -s http://localhost:8123/ping
# → Ok.
```

---

## Step 3: Apply the DDL

Superbank uses three base tables and several materialized views. Apply the local (single-node) DDL:

```bash
# Base tables
cat ddl/local/transactions.sql    | docker exec -i superbank-clickhouse clickhouse-client --multiquery
cat ddl/local/blocks_metadata.sql | docker exec -i superbank-clickhouse clickhouse-client --multiquery
cat ddl/local/entries.sql         | docker exec -i superbank-clickhouse clickhouse-client --multiquery  # optional — only needed for PoH entry data

# Materialized views (derived by ClickHouse at insert time — not by the ingestor)
cat ddl/local/gsfa.sql                | docker exec -i superbank-clickhouse clickhouse-client --multiquery
cat ddl/local/signatures.sql          | docker exec -i superbank-clickhouse clickhouse-client --multiquery
cat ddl/local/token_owner_activity.sql | docker exec -i superbank-clickhouse clickhouse-client --multiquery
```

**What each table is for:**

| Table/View | Type | Contents |
|---|---|---|
| `transactions` | Base table | Every transaction — 66 columns, partitioned by epoch |
| `blocks_metadata` | Base table | Block-level data: blockhash, height, rewards |
| `entries` | Base table | PoH entry metadata (Fumarole and gRPC sources only) |
| `gsfa` | Materialized view | Powers `getSignaturesForAddress` — 32-bucket address partitioning |
| `signatures` | Materialized view | Powers `getSignatureStatuses` — 32-bucket signature partitioning |
| `token_owner_activity` | Materialized view | Powers token-filtered `getTransactionsForAddress` |

**No filtering happens at the ingest layer.** The ingestor writes every transaction from the source as-is into the `transactions` base table. The materialized views are computed by ClickHouse at insert time. This means:

- If an account doesn't appear in a transaction, it won't appear in `gsfa`.
- gSFA exclusions (below) are a DDL concern, not an ingestor concern.
- The base `transactions` table is always the full unfiltered ledger.

### gSFA default exclusions

By default, four system-level addresses are excluded from the `gsfa` materialized view. They appear in almost every transaction so they'd dominate the index without adding value:

- **System Program** — `11111111111111111111111111111111`
- **Vote Program** — `Vote111111111111111111111111111111111111111`
- **SysvarClock** — `SysvarC1ock11111111111111111111111111111111`
- **SysvarSlotHashes** — `SysvarS1otHashes111111111111111111111111111`

This exclusion is in `ddl/local/gsfa.sql` as the `gsfa_ignored_addresses` array. If you need any of these addresses indexed, remove them from the array before running the DDL:

```sql
-- In ddl/local/gsfa.sql, find this block and remove any address you want to keep:
[
    CAST(base58Decode('11111111111111111111111111111111') AS FixedString(32)),
    CAST(base58Decode('Vote111111111111111111111111111111111111111') AS FixedString(32)),
    CAST(base58Decode('SysvarC1ock11111111111111111111111111111111') AS FixedString(32)),
    CAST(base58Decode('SysvarS1otHashes111111111111111111111111111') AS FixedString(32))
] AS gsfa_ignored_addresses
```

---

## Step 4: Configure the ingestor

Copy the example config and fill in your endpoint:

```bash
cp superbank.example.yaml superbank.yaml
```

For live ingestion via Dragon's Mouth (gRPC):

```yaml
source: "grpc"

endpoint: "https://your-endpoint.rpcpool.com:443"
x-token: "your-token"
commitment: "finalized"

clickhouse-url: "http://localhost:8123"
clickhouse-database: "default"
clickhouse-user: "default"
clickhouse-password: ""

transactions-table: "default.transactions"
blocks-table: "default.blocks_metadata"
entries-table: "default.entries"
```

For live ingestion via **Fumarole** (persistent consumer group — resumes from where it left off):

```yaml
source: "fumarole"

fumarole-endpoint: "https://your-endpoint.rpcpool.com:443"
fumarole-x-token: "your-token"
fumarole-consumer-group: "superbank-mainnet"
fumarole-create-consumer-group: true  # set to false after first run
fumarole-data-plane-tcp-connections: 4       # parallel TCP connections for download; max 20
fumarole-concurrent-download-limit-per-tcp: 2
# fumarole-memory-soft-limit-bytes: 25769803776  # 24 GiB soft limit; set 0 to disable

clickhouse-url: "http://localhost:8123"
clickhouse-database: "default"
clickhouse-user: "default"
clickhouse-password: ""

transactions-table: "default.transactions"
blocks-table: "default.blocks_metadata"
entries-table: "default.entries"
```

On first run you'll see the consumer group created, then data flowing:

```
INFO superbank::ingest::fumarole: starting superbank fumarole ingest source="fumarole" endpoint=https://your-endpoint.rpcpool.com:443 consumer_group=superbank-mainnet
INFO superbank::ingest::fumarole: created Fumarole consumer group consumer_group=superbank-mainnet
INFO superbank::clickhouse: clickhouse insert committed table="default.transactions" rows=4051 slot_min=424497433 slot_max=424497435
INFO superbank::clickhouse: clickhouse insert committed table="default.blocks_metadata" rows=3 slot_min=424497433 slot_max=424497435
INFO superbank::clickhouse: clickhouse insert committed table="default.entries" rows=3412 slot_min=424497433 slot_max=424497435
```

After the first run, set `fumarole-create-consumer-group: false` — the group persists on the server and resumes from its last committed position on restart.

Fumarole is generally preferred for production: it uses a persistent consumer group that survives restarts, has at-least-once delivery guarantees, and also populates the `entries` table like the gRPC source. Dragon's Mouth gRPC is simpler for local dev.

For **historical backfill**, there are two approaches depending on scale:

**Large-scale backfill (recommended): Jetstreamer + Old Faithful**

For ingesting months or years of history, use the [Jetstreamer adapter](ingest/jetstreamer-clickhouse-plugin/) pointed at Triton's [Old Faithful](https://docs.triton.one/project-yellowstone/old-faithful-historical-archive) archival backend. Old Faithful has full history back to genesis and serves data at wire speed — far faster than polling `getBlock` over HTTP. Bound the range with the epoch or slot arguments; see the [Filtering section](#filtering-to-your-own-transactions) for trade-offs if you also want a program filter.

**Small-to-medium backfill: JSON-RPC source**

For shorter ranges (e.g., a few thousand slots), the `rpc` source works well:

```yaml
source: "rpc"

rpc-url: "https://your-endpoint.rpcpool.com/your-token"
rpc-from-slot: 424000000
rpc-slot-count: 10000    # or use rpc-to-slot for an explicit end slot
rpc-max-inflight: 64

clickhouse-url: "http://localhost:8123"
# ... rest same as above
```

Use a Triton HTTP RPC endpoint (available from your [customers.triton.one](https://customers.triton.one) dashboard) — Dragon's Mouth is gRPC, so the `rpc` source needs your regular HTTP JSON-RPC endpoint. Triton's shared pool allows up to 120 RPS, so `rpc-max-inflight: 64` runs without throttling. Triton also automatically routes `getBlock` for older slots through Old Faithful, so you get full history regardless of slot age. The public `api.mainnet-beta.solana.com` endpoint rate-limits `getBlock` aggressively and lacks full history — avoid it for any bulk backfill.

### ClickHouse insert retry

For gRPC, RPC, and Bigtable sources, the ingestor retries failed ClickHouse inserts with exponential backoff. Fumarole is excluded — it manages durability at the consumer-group level.

| Config key | Env var | Default | Description |
|---|---|---|---|
| `insert-max-retries` | `CLICKHOUSE_INSERT_MAX_RETRIES` | `5` | Maximum retry attempts per batch |
| `insert-retry-base-ms` | `CLICKHOUSE_INSERT_RETRY_BASE_MS` | `1000` | Initial retry delay (ms) |
| `insert-retry-max-ms` | `CLICKHOUSE_INSERT_RETRY_MAX_MS` | `30000` | Maximum retry delay after backoff (ms) |

---

## Step 5: Start the ingestor

```bash
./target/release/superbank --config superbank.yaml
```

You'll see log lines as blocks are ingested. The ingestor writes to `transactions`, `blocks_metadata`, and `entries`. ClickHouse materializes the views (`gsfa`, `signatures`, `token_owner_activity`) in the background as rows arrive.

Example output (RPC backfill source):

```
INFO superbank: starting name="superbank" version="0.3.0"
INFO superbank::ingest::rpc: starting superbank ingest source="rpc" from_slot=424274200 to_slot=424274209
INFO superbank::clickhouse: clickhouse insert committed table="default.transactions" rows=1582 slot=424274200 progress_percent=100.0
INFO superbank::clickhouse: clickhouse insert committed table="default.blocks_metadata" rows=1 slot=424274200
```

Example output (gRPC / Dragon's Mouth live source):

```
INFO superbank: starting name="superbank" version="0.3.0"
INFO superbank::ingest::grpc: starting superbank ingest source="grpc" endpoint=https://your-endpoint.rpcpool.com:443
INFO superbank::ingest::grpc: gRPC health check passed status="serving"
INFO superbank::ingest::grpc: subscribed to gRPC stream from_slot=424432530
INFO superbank::clickhouse: clickhouse insert committed table="default.transactions" rows=2387 slot_min=424432637 slot_max=424432638
INFO superbank::clickhouse: clickhouse insert committed table="default.entries" rows=1221 slot_min=424432637 slot_max=424432638
```

The gRPC source also populates the `entries` table (PoH entry metadata) — the RPC backfill source does not.

Check that data is landing:

```bash
docker exec superbank-clickhouse clickhouse-client --query "SELECT count() FROM default.transactions"
# → 13950

docker exec superbank-clickhouse clickhouse-client --query "SELECT max(slot) FROM default.transactions"
# → 424274209
```

---

## Step 6: Start the RPC server

```bash
CLICKHOUSE_URL=http://localhost:8123 \
CLICKHOUSE_DATABASE=default \
CLICKHOUSE_USER=default \
CLICKHOUSE_PASSWORD="" \
CLICKHOUSE_GSFA_TABLE=default.gsfa \
CLICKHOUSE_SIGNATURE_STATUSES_TABLE=default.signatures \
CLICKHOUSE_TOKEN_OWNER_ACTIVITY_TABLE=default.token_owner_activity \
CLICKHOUSE_TRANSACTION_TABLE=default.transactions \
CLICKHOUSE_BLOCKS_METADATA_TABLE=default.blocks_metadata \
./target/release/superbank-rpc
```

By default, all JSON-RPC responses use HTTP `200 OK` — including error bodies. To return HTTP `503 Service Unavailable` on server-side failures (useful for load balancers that route on HTTP status), add `--emit-http-errors` or set `SUPERBANK_RPC_EMIT_HTTP_ERRORS=true`. Only internal errors (`-32603`), timeouts (`-32000`), node unhealthy (`-32005`), and storage unreachable (`-32019`) are promoted to HTTP 503 — client and data-condition errors remain 200 OK.

Test it:

```bash
curl -s http://localhost:8899 \
  -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}' | jq .
# → {"jsonrpc":"2.0","result":"ok","id":1}

curl -s http://localhost:8899 \
  -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getFirstAvailableBlock"}' | jq .
# → {"jsonrpc":"2.0","result":424274200,"id":1}

curl -s http://localhost:8899 \
  -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getTransaction","params":["<signature>",{"encoding":"json","maxSupportedTransactionVersion":0}]}' | jq '{slot: .result.slot, blockTime: .result.blockTime, fee: .result.meta.fee}'
# → {"slot": 424274200, "blockTime": 1780588286, "fee": 5000}

curl -s http://localhost:8899 \
  -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getBlock","params":[424274200,{"encoding":"json","maxSupportedTransactionVersion":0,"transactionDetails":"signatures"}]}' | jq '{blockhash: .result.blockhash, blockHeight: .result.blockHeight, numSignatures: (.result.signatures | length)}'
# → {"blockhash": "Hpk2nu2vYpfMVNuW363vtc1JgMWkuCkgkjWVYyKfU14A", "blockHeight": 402357051, "numSignatures": 1582}

# Triton extension — paginated transaction history with full tx data
curl -s http://localhost:8899 \
  -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getTransactionsForAddress","params":["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",{"limit":3,"maxSupportedTransactionVersion":0}]}' | jq '{count: (.result.data | length), firstSlot: .result.data[0].slot}'
# → {"count": 3, "firstSlot": 424497440}
```

Each item in `getTransactionsForAddress` now includes a `version` field (`"legacy"` or `0` for versioned transactions). Pass `maxSupportedTransactionVersion: 0` in the params to receive versioned transactions.

Your Superbank instance is now serving Solana-compatible RPC at `:8899`.

---

## Filtering to your own transactions

By default, Superbank ingests every transaction on the network. If you want a targeted history — say, only transactions involving your program — there are two approaches with very different trade-offs.

### Recommended: filter by time range, ingest everything

The safest approach is to pick the time window you care about and ingest every transaction in it, without any program filter. This keeps your derived tables (`gsfa`, `token_owner_activity`) and synthesized methods (`getBlock`, `getTransactionsForAddress`, `getSignaturesForAddress`) fully accurate — they depend on having the complete transaction set for every block in the indexed range.

Use the `rpc-from-slot` / `rpc-to-slot` parameters (or the Jetstreamer epoch argument) to bound the range:

```yaml
# superbank.yaml — ingest a specific slot range, no program filter
source: "rpc"
rpc-url: "https://your-endpoint.rpcpool.com/your-token"
rpc-from-slot: 420000000
rpc-to-slot:   425000000
rpc-max-inflight: 64
clickhouse-url: "http://localhost:8123"
```

This is the approach to use for any production instance where downstream consumers will call gSFA, gTFA, or `getBlock`.

### Advanced: filter by program (breaks derived methods)

> **Warning:** Filtering by program produces an incomplete transaction set. The following RPC methods will return incorrect or empty results:
> - `getSignaturesForAddress` — misses signatures for addresses that appeared in filtered-out transactions
> - `getTransactionsForAddress` — same
> - `getBlock` — blocks will appear to have fewer transactions than they actually did
>
> Use this only if you are building a purpose-specific index where you control the query surface and explicitly do not need those methods.

The Jetstreamer adapter is in `ingest/jetstreamer-clickhouse-plugin/src/lib.rs`. The filter is a 1–2 line addition to the `on_transaction` method:

**Before (current `on_transaction`, line ~409):**

```rust
fn on_transaction<'a>(
    &'a self,
    thread_id: usize,
    db: Option<Arc<Client>>,
    transaction: &'a TransactionData,
) -> PluginFuture<'a> {
    async move {
        let Some(db) = self.resolve_db(db) else {
            return Ok(());
        };
        let row = TransactionRow::from_transaction(transaction);
        // ... buffers the row for this slot
```

**After — add a program filter at the top of the async block:**

The plugin already has a `parse_pubkey_bytes` helper that decodes a base58 address to `[u8; 32]`. Use that to avoid adding a new dependency:

```rust
// Add near the bottom of lib.rs (outside the impl block, before parse_pubkey_bytes):
fn my_program_id() -> [u8; 32] {
    parse_pubkey_bytes("YourProgramPubkeyHere")
        .expect("valid program pubkey")
}

// In on_transaction, add three lines at the top of the async block:
fn on_transaction<'a>(
    &'a self,
    thread_id: usize,
    db: Option<Arc<Client>>,
    transaction: &'a TransactionData,
) -> PluginFuture<'a> {
    async move {
        // Filter: only ingest transactions that involve our program
        let program = my_program_id();
        if !transaction.transaction.message.static_account_keys().iter().any(|k| k.to_bytes() == program) {
            return Ok(());  // skip this transaction
        }

        let Some(db) = self.resolve_db(db) else {
            return Ok(());
        };
        // ... rest of the function unchanged
```

The `static_account_keys()` slice contains every static account referenced in the transaction — fee payer, program IDs, and all account operands. If your program ID appears anywhere, the transaction passes the filter.

**To filter by multiple programs** (e.g., your program and a token program it interacts with):

```rust
fn is_relevant(transaction: &TransactionData) -> bool {
    const PROGRAMS: &[&str] = &[
        "YourProgramPubkeyHere",
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    ];
    let keys = transaction.transaction.message.static_account_keys();
    PROGRAMS.iter().any(|p| {
        if let Some(bytes) = parse_pubkey_bytes(p) {
            keys.iter().any(|k| k.to_bytes() == bytes)
        } else {
            false
        }
    })
}

// In on_transaction:
if !is_relevant(transaction) {
    return Ok(());
}
```

After adding the filter, rebuild and run:

```bash
cd ingest/jetstreamer-clickhouse-plugin

# Rebuild
cargo build --release

# Run for a specific epoch
./target/release/jetstreamer-clickhouse 800

# Or for a specific slot range
./target/release/jetstreamer-clickhouse 424000000:424100000
```

---

## Optional: processed commitment with the gRPC head cache

By default, the RPC server supports `confirmed` and `finalized` commitment. To also serve `processed` commitment (latest tip, sub-slot latency), enable the head cache:

```bash
# Build with the feature enabled
cargo build --release -p superbank-rpc --features grpc-head-cache
```

Then start the RPC server with the head cache pointed at a Dragon's Mouth endpoint (add these env vars alongside the ones from Step 6):

```bash
HEAD_CACHE_ENABLED=true \
DRAGONSMOUTH_ENDPOINT=https://your-endpoint.rpcpool.com:443 \
DRAGONSMOUTH_X_TOKEN=your-token \
HEAD_CACHE_RETAIN_SLOTS=32 \
HEAD_CACHE_MIN_COMMITMENT=processed \
CLICKHOUSE_URL=http://localhost:8123 \
# ... rest of env vars from Step 6
./target/release/superbank-rpc
```

On startup you'll see the head cache subscribe to Dragon's Mouth:

```
INFO superbank_rpc: starting name="superbank-rpc" version="0.3.0"
INFO superbank_rpc::server: RPC server listening on http://0.0.0.0:8899
INFO superbank_rpc::head_cache::dragonsmouth: head cache: subscribed to DragonsMouth block-meta stream endpoint="https://your-endpoint.rpcpool.com:443" min_commitment=Processed
INFO superbank_rpc::head_cache::dragonsmouth: head cache: subscribed to DragonsMouth endpoint="https://your-endpoint.rpcpool.com:443" min_commitment=Processed
```

Test all three commitment levels:

```bash
curl -s http://localhost:8899 \
  -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{"commitment":"processed"}]}' | jq .result
# → 424467184   (latest tip)

curl -s http://localhost:8899 \
  -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{"commitment":"confirmed"}]}' | jq .result
# → 424467183   (1 slot behind processed)

curl -s http://localhost:8899 \
  -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{"commitment":"finalized"}]}' | jq .result
# → 424432638   (from ClickHouse)
```

The cache holds the last `HEAD_CACHE_RETAIN_SLOTS` slots in memory and merges them with ClickHouse results for `getTransaction` and `getSignatureStatuses` queries.

**RAM requirement:** The head cache itself is lightweight — at the default of 32 slots, it holds roughly 32 × ~1,500 mainnet transactions worth of metadata in memory, typically under 500 MB. The dominant memory cost is ClickHouse: for a full mainnet history, plan for at least 32 GB RAM on the ClickHouse host (64 GB+ for comfortable production operation). The `superbank` and `superbank-rpc` binaries themselves each use well under 2 GB.

Note: the `grpc-head-cache` feature pulls in AGPL-licensed dependencies via the Yellowstone gRPC crate. This is why it's an opt-in compile feature rather than the default.

---

## Optional: gRPC streaming (`grpc-streaming`)

When compiled with `--features grpc-streaming` and enabled at runtime, the RPC server exposes a tonic gRPC endpoint alongside JSON-RPC. The current implementation supports historical `StreamBlocks` and `StreamTransactions` over bounded inclusive slot ranges from ClickHouse.

`StreamBlocks` returns one message per matching block with block metadata, rewards, and transaction payloads. `StreamTransactions` returns one message per matching transaction. Both support account include/exclude/required filters, vote filtering, and failed/success filtering.

Build with the feature:

```bash
cargo build --release -p superbank-rpc --features grpc-streaming
```

Then start with gRPC enabled (add alongside the env vars from Step 6):

```bash
SUPERBANK_GRPC_ENABLED=true \
SUPERBANK_GRPC_PORT=10000 \
./target/release/superbank-rpc
```

Configuration:

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--superbank-grpc-enabled` | `SUPERBANK_GRPC_ENABLED` | `false` | Enable gRPC endpoint at runtime |
| `--superbank-grpc-host` | `SUPERBANK_GRPC_HOST` | `0.0.0.0` | Bind host |
| `--superbank-grpc-port` | `SUPERBANK_GRPC_PORT` | `10000` | Bind port |
| `--superbank-grpc-max-slot-range` | `SUPERBANK_GRPC_MAX_SLOT_RANGE` | `100` | Maximum inclusive slots per stream request |
| `--superbank-grpc-query-timeout-ms` | `SUPERBANK_GRPC_QUERY_TIMEOUT_MS` | `30000` | Per-chunk ClickHouse range-query timeout (ms) |
| `--superbank-grpc-chunk-slots` | `SUPERBANK_GRPC_CHUNK_SLOTS` | `8` | Slots fetched from ClickHouse per chunk |
| `--superbank-grpc-max-send-bytes` | `SUPERBANK_GRPC_MAX_SEND_BYTES` | `104857600` | Max encoded gRPC response message size (100 MB) |
| `--superbank-grpc-max-concurrent-streams` | `SUPERBANK_GRPC_MAX_CONCURRENT_STREAMS` | `20` | HTTP/2 concurrent stream limit |

---

## Optional: disk cache (`disk-cache`)

When compiled with `--features disk-cache` (which implies `grpc-head-cache`) and enabled at runtime, superbank-rpc keeps a RocksDB-backed cache of recent **finalized** slots on local disk. The read tiering becomes:

```
head cache (memory, unfinalized tip) → disk cache (finalized, recent slots) → ClickHouse (full history)
```

The cache is hydrated from ClickHouse on startup, kept current as the Dragon's Mouth stream finalizes slots, and self-repairs gaps from ClickHouse. It never writes to ClickHouse. Any miss, hole, or decode failure silently falls back to a ClickHouse read.

Methods served from disk when covered: `getBlock`, `getBlocks`, `getBlocksWithLimit`, `getBlockTime`, `getTransaction`, `getSignatureStatuses`, `getSignaturesForAddress`, and `getTransactionsForAddress` (including `tokenAccounts` filters).

> **Sizing:** at mainnet volume the default 10-epoch window (4,320,000 slots) needs on the order of **15–20 TB** of NVMe even with compression (~1.5–2 TB per epoch). Set `DISK_CACHE_MAX_BYTES` to bound disk usage — the retention window shrinks to fit.

Requires `HEAD_CACHE_ENABLED=true` with a `DRAGONSMOUTH_ENDPOINT`. Set `HEAD_CACHE_RETAIN_SLOTS` to at least `150` when the disk cache is enabled (the default `32` is too small — finalized slots are evicted before the disk snapshot hook fires).

```bash
# Build with disk-cache (implies grpc-head-cache)
cargo build --release -p superbank-rpc --features disk-cache

# Run
RPC_HOST=0.0.0.0 RPC_PORT=8899 \
CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_DATABASE=default \
HEAD_CACHE_ENABLED=true HEAD_CACHE_RETAIN_SLOTS=150 \
DRAGONSMOUTH_ENDPOINT=https://your-endpoint.rpcpool.com:443 \
DRAGONSMOUTH_X_TOKEN=your-token \
DISK_CACHE_ENABLED=true \
DISK_CACHE_PATH=/var/lib/superbank/disk-cache \
DISK_CACHE_MAX_BYTES=2199023255552 \
./target/release/superbank-rpc
```

Key configuration options:

| Option | Environment | Default | Notes |
|---|---|---|---|
| `--disk-cache-enabled` | `DISK_CACHE_ENABLED` | `false` | Enable at runtime |
| `--disk-cache-path` | `DISK_CACHE_PATH` | — | RocksDB directory; required when enabled |
| `--disk-cache-retain-slots` | `DISK_CACHE_RETAIN_SLOTS` | `4320000` | Finalized slots to retain (~10 epochs) |
| `--disk-cache-max-bytes` | `DISK_CACHE_MAX_BYTES` | `0` | Disk byte budget; `0` = unlimited |
| `--disk-cache-block-cache-bytes` | `DISK_CACHE_BLOCK_CACHE_BYTES` | `4294967296` | RocksDB block cache (4 GB default) |
| `--disk-cache-read-concurrency` | `DISK_CACHE_READ_CONCURRENCY` | `64` | Max concurrent blocking disk reads |
| `--disk-cache-backfill-enabled` | `DISK_CACHE_BACKFILL_ENABLED` | `true` | ClickHouse→disk backfill on startup |

Observability: `superbank_disk_cache_*` metrics cover coverage span, hit/miss per operation, and write/backfill/repair/eviction activity. The `X-Superbank-Sources` response header reports `disk-cache` when a response was served from disk.

Note: the `disk-cache` feature implies `grpc-head-cache` and therefore pulls in AGPL-licensed dependencies via the Yellowstone gRPC crate.

---

## Going to production

### Use replicated DDL for HA

For a production multi-node setup, use `ddl/replicated/` instead of `ddl/local/`. This requires ClickHouse Keeper or ZooKeeper and uses `ReplicatedReplacingMergeTree`. The DDL commands follow the same pattern as the local setup:

```bash
cat ddl/replicated/transactions.sql    | docker exec -i superbank-clickhouse clickhouse-client --multiquery
cat ddl/replicated/blocks_metadata.sql | docker exec -i superbank-clickhouse clickhouse-client --multiquery
cat ddl/replicated/gsfa.sql            | docker exec -i superbank-clickhouse clickhouse-client --multiquery
# ... etc for other tables
```

Note: the replicated tables will fail to create if ClickHouse Keeper or ZooKeeper is not configured and reachable. Set up your Keeper cluster before applying this DDL. The `ddl/local/` variant used in this guide does not have this requirement.

### Use Fumarole as the ingestion source

Fumarole is the recommended source for production:
- Persistent consumer groups survive ingestor restarts
- At-least-once delivery with commit checkpoints
- Lower reconnect risk than raw gRPC streaming

Set `source: fumarole` in your YAML and configure `fumarole-consumer-group`. After the first run, set `fumarole-create-consumer-group: false`.

### Metrics

Both binaries expose Prometheus metrics at `/metrics`:
- Ingestor: `http://<host>:9901/metrics`
- RPC server: `http://<host>:9900/metrics`

**Key RPC server metrics:**

| Metric | What it tells you |
|---|---|
| `superbank_rpc_requests_total{method,status}` | Request count per method — track error rate by filtering `status!="200"` |
| `superbank_rpc_response_time_seconds{method}` | Full end-to-end latency histogram — use for p50/p95/p99 |
| `superbank_rpc_clickhouse_duration_seconds{method}` | Time spent in ClickHouse per method — isolates DB vs. overhead |
| `superbank_rpc_inflight_requests{method}` | Concurrent in-flight requests — spike indicates backpressure |
| `superbank_rpc_route_total{method,outcome}` | Routing outcomes — watch for `outcome="backend_error"` or `outcome="timeout"` |
| `superbank_head_cache_active` | `1` = head cache connected to Dragon's Mouth; `0` = disconnected |
| `superbank_head_cache_latest_slot` | Most recent slot in cache — compare to chain tip to measure lag |
| `superbank_process_uptime_seconds_total` | Process uptime |

**Key ingestor metrics (`:9901`):**

The ingestor tracks insert throughput, batch sizes, and ClickHouse write latency. Watch for any sustained gap between the max ingested slot and the chain tip — this indicates the ingestor is falling behind.

The ingestor exposes a liveness health check at `http://<host>:9901/health`. It returns `200 OK` while flushing normally, and `503 Service Unavailable` when no successful ClickHouse flush has occurred within `health-stale-secs` seconds (default: 120). Set `health-stale-secs: 0` in your config to disable the staleness check. Add `METRICS_CLUSTER_LABEL` to attach a static `cluster="..."` label to all ingestor metrics — useful for multi-cluster Prometheus setups.

When running the Fumarole source, the following additional gauges and counters are emitted:

| Metric | Description |
|---|---|
| `superbank_ingest_fumarole_memory_soft_limit_bytes` | Configured memory soft limit |
| `superbank_ingest_fumarole_buffered_bytes` | Estimated assembler bytes in memory |
| `superbank_ingest_fumarole_pending_slots` | Slots waiting to flush |
| `superbank_ingest_fumarole_rss_bytes` | Process RSS sampled by the memory guard |
| `superbank_ingest_fumarole_pressure_flushes_total` | Backpressure-triggered flushes (rising counter indicates memory pressure) |

**Example Prometheus queries:**

```promql
# p99 RPC latency per method
histogram_quantile(0.99, rate(superbank_rpc_response_time_seconds_bucket[5m]))

# Error rate across all methods
sum(rate(superbank_rpc_requests_total{status!="200"}[5m]))
  / sum(rate(superbank_rpc_requests_total[5m]))

# ClickHouse query time p95
histogram_quantile(0.95, rate(superbank_rpc_clickhouse_duration_seconds_bucket[5m]))
```

---

## What's next

- **Kubernetes** — `deploy/k8s/` has ready-made manifests for ClickHouse, the DDL init job, superbank-rpc, and both ingestion modes.
- **Load testing** — `tests/k6/` has k6 scenarios for all RPC methods, including validation tests that compare your Superbank instance against a reference RPC.
- **Triton hosted** — if you'd rather not operate ClickHouse and the ingestor yourself, Triton runs Superbank as a managed service. Same RPC interface, no infra.
