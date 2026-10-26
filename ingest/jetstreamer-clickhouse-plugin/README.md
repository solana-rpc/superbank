# Jetstreamer ClickHouse Plugin

High-throughput ingestion plugin for Jetstreamer 0.7 (Agave 4) that writes Solana blocks, transactions, and PoH entries into the `blocks_metadata`, `transactions`, and `entries` tables. Jetstreamer's block callback has no bank ID or Alpenglow footer, so this plugin rejects data after its trusted historical bound. The standalone runner clamps its range before starting. Use a qualified source for later blocks.

## Usage

```rust
use jetstreamer_firehose::epochs;
use jetstreamer::JetstreamerRunner;
use jetstreamer_clickhouse_plugin::{ClickhouseIngestConfig, ClickhouseIngestPlugin};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let recorded_finalized_slot: u64 = std::env::var("JETSTREAMER_PREACTIVATION_THROUGH_SLOT")?.parse()?;
    let commission_activation: u64 = std::env::var("JETSTREAMER_BLOCK_REWARD_COMMISSION_BPS_FROM_SLOT")?.parse()?;
    let threads = 4;
    let config = ClickhouseIngestConfig {
        single_node: false,
        block_reward_commission_bps_from_slot: Some(commission_activation),
        preactivation_through_slot: Some(recorded_finalized_slot), // offline attestation described below
        ..Default::default()
    };
    let plugin = ClickhouseIngestPlugin::new(config, threads);
    let (start, _) = epochs::epoch_to_slot_range(800);
    let (_, end_inclusive) = epochs::epoch_to_slot_range(805);

    let range = plugin.historical_slot_range(start, end_inclusive)?;

    JetstreamerRunner::default()
        .with_threads(threads)
        .with_slot_range(range)
        .with_plugin(Box::new(plugin))
        .run()?;

    Ok(())
}
```

## Notes

- Transactions and entries are buffered per slot and flushed when the corresponding `on_block`
  arrives so `block_time` is populated from the block metadata.
- Jetstreamer 0.7 / Agave 4.2 transaction-v1 messages populate the transaction version and
  transaction config columns. Transaction reward commission fields are preserved as supplied. Block rewards require separate SIMD-0291 era qualification below.
- Jetstreamer already parses PoH entries while reconstructing blockhashes; this plugin consumes
  those entry notifications to populate the `entries` table alongside blocks and transactions.
- The default configuration writes to `default.entries`, so apply the matching schema set under
  `../../ddl/` and include `entries.sql` before running the plugin.
- Exactly one of `alpenglow_genesis_slot` / `JETSTREAMER_ALPENGLOW_GENESIS_SLOT` or `preactivation_through_slot` / `JETSTREAMER_PREACTIVATION_THROUGH_SLOT` is required. The plugin preserves v1 transaction configuration and basis-point reward commission, but rejects later blocks because upstream Jetstreamer does not expose their bank/footer data.
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
JETSTREAMER_BLOCK_REWARD_COMMISSION_BPS_FROM_SLOT=<trusted-SIMD-0291-slot> \
JETSTREAMER_ALPENGLOW_GENESIS_SLOT=<trusted-genesis-slot> cargo run --release --bin jetstreamer-clickhouse -- 800
# or a slot range:
JETSTREAMER_BLOCK_REWARD_COMMISSION_BPS_FROM_SLOT=<trusted-SIMD-0291-slot> \
JETSTREAMER_ALPENGLOW_GENESIS_SLOT=<trusted-genesis-slot> cargo run --release --bin jetstreamer-clickhouse -- 358560000:367631999
```

From the Superbank repo root, you can also run the end-to-end local smoke test helper:

```bash
JETSTREAMER_BLOCK_REWARD_COMMISSION_BPS_FROM_SLOT=<trusted-SIMD-0291-slot> \
JETSTREAMER_ALPENGLOW_GENESIS_SLOT=<trusted-genesis-slot> scripts/dev/run-jetstreamer-entries-smoke.sh
# or override the default range:
JETSTREAMER_BLOCK_REWARD_COMMISSION_BPS_FROM_SLOT=<trusted-SIMD-0291-slot> \
JETSTREAMER_ALPENGLOW_GENESIS_SLOT=<trusted-genesis-slot> scripts/dev/run-jetstreamer-entries-smoke.sh 358560000:358560099
```

For the stock local Docker ClickHouse setup, that helper also updates the container's
`default-user.xml` so the host-side Jetstreamer HTTP client can reach `localhost:8123`.

### Preactivation evidence

For a preactivation backfill, record trusted same-cluster `getSlot` at finalized commitment **before** an authoritative successful null `getAgGenesisCert` response. Retain both responses and attest a slot at or below that finalized tip with `preactivation_through_slot` / `JETSTREAMER_PREACTIVATION_THROUGH_SLOT`. The plugin trusts this explicit offline attestation and does not discover evidence itself. Missing, unsupported, failed or malformed RPC responses cannot qualify this mode. The finite bound never advances automatically; requalify evidence for a later run. When a certificate exists, use its trusted slot instead, and do not set both bounds.

```sh
JETSTREAMER_BLOCK_REWARD_COMMISSION_BPS_FROM_SLOT=<trusted-SIMD-0291-slot> \
JETSTREAMER_PREACTIVATION_THROUGH_SLOT=<recorded-finalized-slot> \
cargo run --release --bin jetstreamer-clickhouse -- <historical-start>:<historical-end>
```

### Block reward commission era

The pinned Jetstreamer firehose normalizes a legacy percent commission to runtime basis points (`percent * 100`) before emitting `BlockData`. The block callback also reports `commission_rate_in_basis_points` (whether the block's stored rewards used basis points), but this plugin does not read it yet. Configure `block_reward_commission_bps_from_slot` / `JETSTREAMER_BLOCK_REWARD_COMMISSION_BPS_FROM_SLOT` with an evidenced same-cluster SIMD-0291 (`commission_rate_in_basis_points`) activation slot. Earlier block rewards restore the original percent in `rewards_commission` and leave bps NULL; later rewards retain actual bps and leave percent NULL. Alternatively attest that the entire bounded range predates that feature with `block_reward_commission_percent` / `JETSTREAMER_BLOCK_REWARD_COMMISSION_PERCENT=true`. Missing or conflicting era settings reject commission-bearing block rewards. Qualify from the cluster's feature activation ledger or archived source-field evidence; values, divisibility, dates, binary versions and the Alpenglow genesis slot cannot determine this era. For example, historical 7% remains 7 with NULL bps, while actual 725 bps remains 725 with NULL percent. Transaction rewards retain upstream's explicit source fields.

### Runner bounds and callback errors

Jetstreamer logs plugin callback errors and continues its runner. A rejected `on_block` does **not** stop upstream, so embedding callers must apply `plugin.historical_slot_range(start, end_inclusive)?` before `run()` as above. The standalone binary does this automatically: a crossing range is clamped to the inclusive trusted historical bound, a start beyond it is rejected before running, and an unrepresentable exclusive end is rejected. Transactions and entries after the bound are rejected before allocating rows or starting writers, so continued upstream callbacks cannot accumulate rejected payloads. Other callback failures are still logged by upstream; inspect logs and verify completeness after any run, including missing commission-era qualification.

The standalone runner also requires exactly one qualified block reward commission era before starting, even if its requested blocks might have no commission. Set `JETSTREAMER_BLOCK_REWARD_COMMISSION_BPS_FROM_SLOT=<trusted-SIMD-0291-slot>` or attest the entire bounded range with `JETSTREAMER_BLOCK_REWARD_COMMISSION_PERCENT=true`. This prevents missing era configuration from merely logging errors and skipping commission-bearing blocks after the runner has started.
