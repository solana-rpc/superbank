//! The default pre-archive PoH gate run against ClickHouse over a range that spans mainnet's
//! Alpenglow genesis slot. Needs a disposable server: see `poh_boundary_clickhouse_url`.

use std::path::Path;

use sha2::{Digest, Sha256};
use superbank_solparq::{config::Config, poh::verify_archive_range};

const G: u64 = 459_652_999;
const HISTORICAL_TICKS_PER_SLOT: u64 = 64;
const HASHES_PER_TICK: u64 = 62_500;

/// Only an explicit loopback HTTP URL is accepted, because each test creates and drops a database.
fn poh_boundary_clickhouse_url() -> String {
    let url = std::env::var("SOLPARQ_TEST_CLICKHOUSE_URL")
        .expect("set SOLPARQ_TEST_CLICKHOUSE_URL=http://127.0.0.1:<port> (disposable ClickHouse)");
    assert!(
        url.starts_with("http://127.0.0.1:"),
        "SOLPARQ_TEST_CLICKHOUSE_URL must be a loopback HTTP endpoint"
    );
    url.trim_end_matches('/').to_string()
}

struct TestDb {
    url: String,
    name: String,
    http: reqwest::Client,
}

impl TestDb {
    async fn create(tag: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let db = Self {
            url: poh_boundary_clickhouse_url(),
            name: format!("solparq_poh_{tag}_{nanos}"),
            http: reqwest::Client::new(),
        };
        db.exec(&format!("CREATE DATABASE {}", db.name)).await;
        let ddl_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../ddl/local");
        for table in ["blocks_metadata", "entries", "transactions"] {
            let ddl = std::fs::read_to_string(ddl_dir.join(format!("{table}.sql"))).unwrap();
            for statement in ddl_statements(&ddl.replace("default.", &format!("{}.", db.name))) {
                db.exec(&statement).await;
            }
            // Keep conflicting duplicate rows in separate parts until the gate reads them.
            db.exec(&format!("SYSTEM STOP MERGES {}.{table}", db.name))
                .await;
        }
        db
    }

    async fn exec(&self, sql: &str) {
        let response = self
            .http
            .post(&self.url)
            .body(sql.to_string())
            .send()
            .await
            .expect("ClickHouse request");
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        assert!(status.is_success(), "{sql}\n-> {status}: {body}");
    }

    fn config(&self, extra: &[&str]) -> Config {
        let mut args = vec![
            "superbank-solparq",
            "--db-server",
            &self.url,
            "--db-database",
            &self.name,
            "--db-user",
            "default",
            "--db-password",
            "",
            "--archive-range-type",
            "custom",
        ];
        args.extend_from_slice(extra);
        Config::try_parse_from(args).expect("config")
    }

    async fn insert_block(&self, block: &Block) {
        self.exec(&format!(
            "INSERT INTO {}.blocks_metadata (slot, parent_slot, blockhash, parent_blockhash, \
             executed_transaction_count, entry_count) VALUES ({}, {}, unhex('{}'), unhex('{}'), 0, {})",
            self.name,
            block.slot,
            block.parent_slot,
            hex(&block.blockhash()),
            hex(&block.parent_blockhash),
            block.entries.len()
        ))
        .await;
        let rows: Vec<String> = block
            .entries
            .iter()
            .enumerate()
            .map(|(index, (num_hashes, hash))| {
                format!(
                    "({}, {index}, 0, 0, {num_hashes}, unhex('{}'))",
                    block.slot,
                    hex(hash)
                )
            })
            .collect();
        self.insert_entry_rows(&rows).await;
    }

    async fn insert_entry_rows(&self, rows: &[String]) {
        self.exec(&format!(
            "INSERT INTO {}.entries (slot, entry_index, starting_transaction_index, \
             transaction_count, num_hashes, hash) VALUES {}",
            self.name,
            rows.join(", ")
        ))
        .await;
    }

    async fn drop(self) {
        self.exec(&format!("DROP DATABASE IF EXISTS {}", self.name))
            .await;
    }
}

/// A block of tick entries only: `(num_hashes, hash)` per entry, the last hash being the blockhash.
struct Block {
    slot: u64,
    parent_slot: u64,
    parent_blockhash: [u8; 32],
    entries: Vec<(u64, [u8; 32])>,
}

impl Block {
    fn blockhash(&self) -> [u8; 32] {
        self.entries.last().unwrap().1
    }

    /// Historical layout: 64 ticks per covered slot of `HASHES_PER_TICK` each. Hashes are labels;
    /// structural mode checks counts and linkage, not hash values.
    fn historical(slot: u64, parent: &Block) -> Self {
        let ticks = (slot - parent.slot) * HISTORICAL_TICKS_PER_SLOT;
        Self {
            slot,
            parent_slot: parent.slot,
            parent_blockhash: parent.blockhash(),
            entries: (0..ticks)
                .map(|tick| (HASHES_PER_TICK, label(slot, tick)))
                .collect(),
        }
    }

    /// Alpenglow layout: one Alpentick with `num_hashes = 1`, a real PoH hash of the parent.
    fn alpenglow(slot: u64, parent: &Block) -> Self {
        let parent_blockhash = parent.blockhash();
        Self {
            slot,
            parent_slot: parent.slot,
            parent_blockhash,
            entries: vec![(1, Sha256::digest(parent_blockhash).into())],
        }
    }
}

fn label(slot: u64, tick: u64) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(slot.to_le_bytes());
    hasher.update(tick.to_le_bytes());
    hasher.finalize().into()
}

fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn ddl_statements(ddl: &str) -> Vec<String> {
    let text: String = ddl
        .lines()
        .map(|line| line.split_once("--").map_or(line, |(code, _)| code))
        .collect::<Vec<_>>()
        .join("\n");
    text.split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .map(str::to_string)
        .collect()
}

/// Seed, then G-2, G (skips G-1), G+1, G+3 (skips G+2), then a tail block.
/// `alpenglow_at` picks which slots get the Alpenglow layout.
fn chain(alpenglow_at: impl Fn(u64) -> bool) -> Vec<Block> {
    chain_at(G, alpenglow_at)
}

fn chain_at(g: u64, alpenglow_at: impl Fn(u64) -> bool) -> Vec<Block> {
    let seed_parent = Block {
        slot: g - 4,
        parent_slot: g - 5,
        parent_blockhash: [0; 32],
        entries: vec![(HASHES_PER_TICK, label(g - 4, 0))],
    };
    let mut blocks: Vec<Block> = Vec::new();
    for slot in [g - 3, g - 2, g, g + 1, g + 3, g + 4] {
        let parent = blocks.last().unwrap_or(&seed_parent);
        let block = if alpenglow_at(slot) {
            Block::alpenglow(slot, parent)
        } else {
            Block::historical(slot, parent)
        };
        blocks.push(block);
    }
    blocks
}

const RANGE: (u64, u64) = (G - 2, G + 3);

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires disposable ClickHouse: SOLPARQ_TEST_CLICKHOUSE_URL=http://127.0.0.1:8123"]
async fn default_gate_passes_a_range_spanning_the_alpenglow_genesis_slot() {
    let db = TestDb::create("pass").await;
    for block in chain(|slot| slot > G) {
        db.insert_block(&block).await;
    }

    // No PoH or Alpenglow flags: the defaults are mainnet's G and `structural`.
    let config = db.config(&[]);
    assert_eq!(config.alpenglow_genesis_slot, Some(G));
    let poh = verify_archive_range(&config, RANGE.0, RANGE.1)
        .await
        .unwrap()
        .expect("PoH check runs by default");
    assert_eq!(poh.mode, "structural");
    assert_eq!(
        (poh.slots_ok, poh.slots_skipped, poh.slots_failed),
        (4, 2, 0),
        "{poh:?}"
    );
    assert!(!poh.blocks_archive(), "{poh:?}");

    // `full` recomputes the Alpenglow hashes; the parent seed at G is only linked, not hashed.
    let config = db.config(&["--poh-verify-mode", "full"]);
    let poh = verify_archive_range(&config, G + 1, G + 3)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((poh.slots_ok, poh.slots_failed), (2, 0), "{poh:?}");
    assert!(!poh.blocks_archive(), "{poh:?}");

    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires disposable ClickHouse: SOLPARQ_TEST_CLICKHOUSE_URL=http://127.0.0.1:8123"]
async fn rules_switch_exactly_after_the_genesis_slot() {
    let db = TestDb::create("shift").await;
    // Off by one in each direction: G uses the Alpenglow layout and G+1 the historical one.
    for block in chain(|slot| slot >= G && slot != G + 1) {
        db.insert_block(&block).await;
    }

    let poh = verify_archive_range(&db.config(&[]), RANGE.0, RANGE.1)
        .await
        .unwrap()
        .unwrap();
    assert!(poh.blocks_archive(), "{poh:?}");
    let failed: Vec<(u64, u64)> = poh
        .failed_ranges
        .iter()
        .map(|range| (range.start_slot, range.end_slot))
        .collect();
    assert_eq!(failed, vec![(G, G + 1)], "{poh:?}");
    assert_eq!(poh.findings.get("tick_count_mismatch"), Some(&2), "{poh:?}");

    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires disposable ClickHouse: SOLPARQ_TEST_CLICKHOUSE_URL=http://127.0.0.1:8123"]
async fn conflicting_duplicate_rows_block_the_archive() {
    let db = TestDb::create("dup").await;
    for block in chain(|slot| slot > G) {
        db.insert_block(&block).await;
    }
    // A second, different entry row for G+3: the gate may read either copy, so the slot fails.
    db.insert_entry_rows(&[format!(
        "({}, 0, 0, 0, 1, unhex('{}'))",
        G + 3,
        hex(&[7; 32])
    )])
    .await;

    let poh = verify_archive_range(&db.config(&[]), RANGE.0, RANGE.1)
        .await
        .unwrap()
        .unwrap();
    assert!(poh.blocks_archive(), "{poh:?}");
    assert_eq!(poh.findings.get("duplicate_conflict"), Some(&1), "{poh:?}");
    assert!(
        poh.failed_ranges
            .iter()
            .any(|range| range.start_slot <= G + 3 && G + 3 <= range.end_slot),
        "{poh:?}"
    );

    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires disposable ClickHouse: SOLPARQ_TEST_CLICKHOUSE_URL=http://127.0.0.1:8123"]
async fn devnet_and_testnet_defaults_use_their_own_genesis_slot() {
    for (cluster, g) in [("devnet", 504_148_999), ("testnet", 444_625_255)] {
        let db = TestDb::create(cluster).await;
        for block in chain_at(g, |slot| slot > g) {
            db.insert_block(&block).await;
        }

        let config = db.config(&["--solana-cluster", cluster]);
        assert_eq!(config.alpenglow_genesis_slot, Some(g), "{cluster}");
        let poh = verify_archive_range(&config, g - 2, g + 3)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (poh.slots_ok, poh.slots_skipped, poh.slots_failed),
            (4, 2, 0),
            "{cluster}: {poh:?}"
        );

        // The mainnet boundary puts this cluster's first Alpenglow blocks under historical rules.
        let config = db.config(&[
            "--solana-cluster",
            cluster,
            "--alpenglow-genesis-slot",
            "459652999",
        ]);
        let poh = verify_archive_range(&config, g - 2, g + 3)
            .await
            .unwrap()
            .unwrap();
        assert!(poh.blocks_archive(), "{cluster}: {poh:?}");

        db.drop().await;
    }
}
