# Jetstreamer ClickHouse Plugin

High-throughput ingestion plugin for Jetstreamer 0.7 (Agave 4) that writes Solana blocks,
transactions, and PoH entries into the `blocks_metadata`, `transactions`, and `entries` tables.
Jetstreamer's block callback has no bank ID or Alpenglow footer, so this plugin stops after
the trusted Alpenglow genesis slot. Use a qualified source for later blocks.

## Usage

```rust
use std::sync::Arc;

use jetstreamer_firehose::epochs;
use jetstreamer_plugin::PluginRunner;
use jetstreamer_clickhouse_plugin::{ClickhouseIngestConfig, ClickhouseIngestPlugin};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let threads = 4;
    let dsn = "http://localhost:8123";

    let config = ClickhouseIngestConfig {
        single_node: false,
        alpenglow_genesis_slot: Some(0), // replace with the trusted genesis slot
        ..Default::default()
    };
    let plugin = ClickhouseIngestPlugin::new(config, threads);

    let mut runner = PluginRunner::new(dsn, threads);
    runner.register(Box::new(plugin));

    let (start, _) = epochs::epoch_to_slot_range(800);
    let (_, end_inclusive) = epochs::epoch_to_slot_range(805);
    runner
        .run(start..(end_inclusive + 1), true)
        .await?;

    Ok(())
}
```

## Notes

- Transactions and entries are buffered per slot and flushed when the corresponding `on_block`
  arrives so `block_time` is populated from the block metadata.
- Jetstreamer already parses PoH entries while reconstructing blockhashes; this plugin consumes
  those entry notifications to populate the `entries` table alongside blocks and transactions.
- The default configuration writes to `default.entries`, so apply the matching schema set under
  `../../ddl/` and include `entries.sql` before running the plugin.
- `alpenglow_genesis_slot` or `JETSTREAMER_ALPENGLOW_GENESIS_SLOT` is required.
  The plugin preserves v1 transaction configuration and basis-point reward commission,
  but rejects later blocks because upstream Jetstreamer does not expose their bank/footer data.
- The `single_node` toggle defaults to clustered mode. In single-node deployments, keep it
  enabled for clarity when using `../../ddl/local/*.sql`.
- Backpressure tuning can be overridden with environment variables (defaults shown):
  - `JETSTREAMER_CLICKHOUSE_FLUSH_MAX_ROWS` (100000)
  - `JETSTREAMER_CLICKHOUSE_FLUSH_MAX_BYTES` (67108864)
  - `JETSTREAMER_CLICKHOUSE_FLUSH_INTERVAL_MS` (10000)
  - `JETSTREAMER_CLICKHOUSE_MAX_INFLIGHT_BATCHES` (8) (max concurrent insert workers per thread)
  - `JETSTREAMER_CLICKHOUSE_PENDING_TX_CAPACITY` (4096)
  - `JETSTREAMER_CLICKHOUSE_RETRY_MAX` (5)
  - `JETSTREAMER_CLICKHOUSE_RETRY_BACKOFF_MS` (50)
  - `JETSTREAMER_CLICKHOUSE_ASYNC_INSERT` (true)
  - `JETSTREAMER_CLICKHOUSE_WAIT_FOR_ASYNC_INSERT` (false)
  - `JETSTREAMER_CLICKHOUSE_INSERT_SEND_TIMEOUT_MS` (10000)
  - `JETSTREAMER_CLICKHOUSE_INSERT_END_TIMEOUT_MS` (60000)
- If `JETSTREAMER_INGEST_CLICKHOUSE_DSN` is set, the plugin will write to that DSN instead of the
  ClickHouse client provided by the runner (which can be pointed at a different Jetstreamer
  helper or left disabled).

## Standalone runner

> **Note:** `ingest/jetstreamer-clickhouse-plugin` is its own Cargo workspace, separate from the
> root workspace. Run all `cargo` commands from within this directory. Running
> `cargo build --release -p jetstreamer-clickhouse-plugin` from the repo root will silently do
> nothing because the crate is not a member of the root workspace.
>
> Also: `ingest/jetstreamer` (a dependency) is a git submodule. If the build fails with missing
> source files, populate it first:
> ```bash
> git submodule update --init
> ```

This crate ships a minimal runner binary so you can copy just this folder and run the plugin:

```bash
JETSTREAMER_ALPENGLOW_GENESIS_SLOT=<trusted-genesis-slot> cargo run --release --bin jetstreamer-clickhouse -- 800
# or a slot range:
JETSTREAMER_ALPENGLOW_GENESIS_SLOT=<trusted-genesis-slot> cargo run --release --bin jetstreamer-clickhouse -- 358560000:367631999
```

From the Superbank repo root, you can also run the end-to-end local smoke test helper:

```bash
JETSTREAMER_ALPENGLOW_GENESIS_SLOT=<trusted-genesis-slot> scripts/dev/run-jetstreamer-entries-smoke.sh
# or override the default range:
JETSTREAMER_ALPENGLOW_GENESIS_SLOT=<trusted-genesis-slot> scripts/dev/run-jetstreamer-entries-smoke.sh 358560000:358560099
```

For the stock local Docker ClickHouse setup, that helper also updates the container's
`default-user.xml` so the host-side Jetstreamer HTTP client can reach `localhost:8123`.
