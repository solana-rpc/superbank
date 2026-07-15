// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Read-only ClickHouse access. All range queries deduplicate
//! ReplacingMergeTree rows with `LIMIT 1 BY <sorting key>`; because the tables
//! use `ReplacingMergeTree(slot)` with the version column inside the sorting
//! key, duplicate rows for a key carry no deterministic winner — an explicit
//! `uniqExact` probe reports keys whose duplicates actually differ.

use std::collections::BTreeMap;
use std::time::Instant;

use anyhow::{Context, Result};
use clickhouse::{Client as ClickHouseClient, Row};
use serde::Deserialize;
use serde_big_array::Array;

use crate::cli::Args;
use crate::metrics;
use crate::report::{Finding, FindingCode};
use crate::verify::poh::Signature64;
use crate::verify::{BlockInfo, EntryInfo};

/// slot -> slot_idx -> all signatures of that transaction, in order.
pub(crate) type TxSignaturesBySlot = BTreeMap<u64, BTreeMap<u32, Vec<Signature64>>>;

#[derive(Clone)]
pub(crate) struct ChDb {
    client: ClickHouseClient,
    pub(crate) blocks_table: String,
    pub(crate) entries_table: String,
    pub(crate) transactions_table: String,
}

#[derive(Debug, Row, Deserialize)]
struct BlockRow {
    slot: u64,
    parent_slot: u64,
    blockhash: Array<u8, 32>,
    parent_blockhash: Array<u8, 32>,
    executed_transaction_count: u64,
    entry_count: u64,
}

impl From<BlockRow> for BlockInfo {
    fn from(row: BlockRow) -> Self {
        BlockInfo {
            slot: row.slot,
            parent_slot: row.parent_slot,
            blockhash: *row.blockhash,
            parent_blockhash: *row.parent_blockhash,
            executed_transaction_count: row.executed_transaction_count,
            entry_count: row.entry_count,
        }
    }
}

#[derive(Debug, Row, Deserialize)]
struct EntryRow {
    slot: u64,
    entry_index: u32,
    starting_transaction_index: u32,
    transaction_count: u32,
    num_hashes: u64,
    hash: Array<u8, 32>,
}

#[derive(Debug, Row, Deserialize)]
struct TxSignaturesRow {
    slot: u64,
    slot_idx: u32,
    tx_signatures: Vec<Array<u8, 64>>,
}

impl ChDb {
    pub(crate) fn new(args: &Args) -> Self {
        let mut client = ClickHouseClient::default()
            .with_url(&args.clickhouse_url)
            .with_database(&args.clickhouse_database);
        if !args.clickhouse_user.is_empty() {
            client = client.with_user(&args.clickhouse_user);
        }
        if !args.clickhouse_password.is_empty() {
            client = client.with_password(&args.clickhouse_password);
        }
        Self {
            client,
            blocks_table: args.blocks_table.clone(),
            entries_table: args.entries_table.clone(),
            transactions_table: args.transactions_table.clone(),
        }
    }

    /// Lowest and highest slot present in blocks_metadata.
    pub(crate) async fn fetch_bounds(&self) -> Result<Option<(u64, u64)>> {
        #[derive(Debug, Row, Deserialize)]
        struct BoundsRow {
            min_slot: Option<u64>,
            max_slot: Option<u64>,
        }

        let query = format!(
            "SELECT minOrNull(slot) AS min_slot, maxOrNull(slot) AS max_slot FROM {}",
            self.blocks_table
        );
        let row = self
            .client
            .query(&query)
            .fetch_one::<BoundsRow>()
            .await
            .with_context(|| format!("query slot bounds from {}", self.blocks_table))?;
        Ok(row.min_slot.zip(row.max_slot))
    }

    pub(crate) async fn fetch_blocks(&self, start: u64, end: u64) -> Result<Vec<BlockInfo>> {
        let query = format!(
            "SELECT slot, parent_slot, blockhash, parent_blockhash, \
             executed_transaction_count, entry_count \
             FROM {} WHERE slot BETWEEN {start} AND {end} \
             ORDER BY slot LIMIT 1 BY slot",
            self.blocks_table
        );
        let started = Instant::now();
        let rows = self
            .client
            .query(&query)
            .fetch_all::<BlockRow>()
            .await
            .with_context(|| format!("query blocks in [{start}, {end}]"))?;
        metrics::observe_fetch_duration(&self.blocks_table, started.elapsed().as_secs_f64());
        Ok(rows.into_iter().map(BlockInfo::from).collect())
    }

    pub(crate) async fn fetch_block_at(&self, slot: u64) -> Result<Option<BlockInfo>> {
        let query = format!(
            "SELECT slot, parent_slot, blockhash, parent_blockhash, \
             executed_transaction_count, entry_count \
             FROM {} WHERE slot = {slot} ORDER BY slot LIMIT 1 BY slot",
            self.blocks_table
        );
        let row = self
            .client
            .query(&query)
            .fetch_optional::<BlockRow>()
            .await
            .with_context(|| format!("query block at slot {slot}"))?;
        Ok(row.map(BlockInfo::from))
    }

    pub(crate) async fn fetch_first_block_after(&self, slot: u64) -> Result<Option<BlockInfo>> {
        let query = format!(
            "SELECT slot, parent_slot, blockhash, parent_blockhash, \
             executed_transaction_count, entry_count \
             FROM {} WHERE slot > {slot} ORDER BY slot LIMIT 1 BY slot LIMIT 1",
            self.blocks_table
        );
        let row = self
            .client
            .query(&query)
            .fetch_optional::<BlockRow>()
            .await
            .with_context(|| format!("query first block after slot {slot}"))?;
        Ok(row.map(BlockInfo::from))
    }

    pub(crate) async fn fetch_entries(
        &self,
        start: u64,
        end: u64,
    ) -> Result<BTreeMap<u64, Vec<EntryInfo>>> {
        let query = format!(
            "SELECT slot, entry_index, starting_transaction_index, \
             transaction_count, num_hashes, hash \
             FROM {} WHERE slot BETWEEN {start} AND {end} \
             ORDER BY slot, entry_index LIMIT 1 BY slot, entry_index",
            self.entries_table
        );
        let started = Instant::now();
        let rows = self
            .client
            .query(&query)
            .fetch_all::<EntryRow>()
            .await
            .with_context(|| format!("query entries in [{start}, {end}]"))?;
        metrics::observe_fetch_duration(&self.entries_table, started.elapsed().as_secs_f64());

        let mut by_slot: BTreeMap<u64, Vec<EntryInfo>> = BTreeMap::new();
        for row in rows {
            by_slot.entry(row.slot).or_default().push(EntryInfo {
                entry_index: row.entry_index,
                num_hashes: row.num_hashes,
                hash: *row.hash,
                starting_transaction_index: row.starting_transaction_index,
                transaction_count: row.transaction_count,
            });
        }
        Ok(by_slot)
    }

    pub(crate) async fn fetch_tx_signatures(
        &self,
        start: u64,
        end: u64,
    ) -> Result<TxSignaturesBySlot> {
        let query = format!(
            "SELECT slot, slot_idx, tx_signatures \
             FROM {} WHERE slot BETWEEN {start} AND {end} \
             ORDER BY slot, slot_idx LIMIT 1 BY slot, slot_idx",
            self.transactions_table
        );
        let started = Instant::now();
        let rows = self
            .client
            .query(&query)
            .fetch_all::<TxSignaturesRow>()
            .await
            .with_context(|| format!("query transaction signatures in [{start}, {end}]"))?;
        metrics::observe_fetch_duration(&self.transactions_table, started.elapsed().as_secs_f64());

        let mut by_slot: TxSignaturesBySlot = BTreeMap::new();
        for row in rows {
            by_slot.entry(row.slot).or_default().insert(
                row.slot_idx,
                row.tx_signatures
                    .into_iter()
                    .map(|signature| *signature)
                    .collect(),
            );
        }
        Ok(by_slot)
    }

    /// Detect ReplacingMergeTree duplicates whose contents actually differ.
    /// `LIMIT 1 BY` picks an arbitrary variant, so conflicting duplicates
    /// could otherwise shadow good data non-deterministically.
    pub(crate) async fn fetch_duplicate_conflicts(
        &self,
        start: u64,
        end: u64,
        include_transactions: bool,
    ) -> Result<Vec<Finding>> {
        #[derive(Debug, Row, Deserialize)]
        struct SlotConflict {
            slot: u64,
        }

        #[derive(Debug, Row, Deserialize)]
        struct SlotIndexConflict {
            slot: u64,
            index: u32,
        }

        let mut findings = Vec::new();

        let query = format!(
            "SELECT slot FROM {} WHERE slot BETWEEN {start} AND {end} GROUP BY slot \
             HAVING uniqExact((parent_slot, blockhash, parent_blockhash, \
             executed_transaction_count, entry_count)) > 1",
            self.blocks_table
        );
        for row in self
            .client
            .query(&query)
            .fetch_all::<SlotConflict>()
            .await
            .with_context(|| format!("probe duplicate blocks in [{start}, {end}]"))?
        {
            findings.push(Finding::new(
                row.slot,
                FindingCode::DuplicateConflict,
                format!("conflicting duplicate rows in {}", self.blocks_table),
            ));
        }

        let query = format!(
            "SELECT slot, entry_index AS index FROM {} \
             WHERE slot BETWEEN {start} AND {end} GROUP BY slot, entry_index \
             HAVING uniqExact((starting_transaction_index, transaction_count, \
             num_hashes, hash)) > 1",
            self.entries_table
        );
        for row in self
            .client
            .query(&query)
            .fetch_all::<SlotIndexConflict>()
            .await
            .with_context(|| format!("probe duplicate entries in [{start}, {end}]"))?
        {
            findings.push(
                Finding::new(
                    row.slot,
                    FindingCode::DuplicateConflict,
                    format!("conflicting duplicate rows in {}", self.entries_table),
                )
                .with_entry_index(row.index),
            );
        }

        if include_transactions {
            let query = format!(
                "SELECT slot, slot_idx AS index FROM {} \
                 WHERE slot BETWEEN {start} AND {end} GROUP BY slot, slot_idx \
                 HAVING uniqExact((signature, tx_signatures)) > 1",
                self.transactions_table
            );
            for row in self
                .client
                .query(&query)
                .fetch_all::<SlotIndexConflict>()
                .await
                .with_context(|| format!("probe duplicate transactions in [{start}, {end}]"))?
            {
                findings.push(Finding::new(
                    row.slot,
                    FindingCode::DuplicateConflict,
                    format!(
                        "conflicting duplicate rows in {} at slot_idx {}",
                        self.transactions_table, row.index
                    ),
                ));
            }
        }

        Ok(findings)
    }
}
