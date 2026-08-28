// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::solana_sdk::{hash::Hash, pubkey::Pubkey};
use serde::Deserialize;
use serde_big_array::Array;
use solana_clock::DEFAULT_SLOTS_PER_EPOCH;
use solana_epoch_rewards_hasher::EpochRewardsHasher;

use crate::processing::{ProcessingError, ProcessingResult};

use super::QueryFreshnessClass;
use super::client::ClickHouseClient;
use super::constants::SLOT_SHARD_DIVISOR;
#[cfg(feature = "grpc-streaming")]
use super::queries::build_transactions_by_slot_range_query;
use super::queries::{
    BLOCK_ACCOUNTS_BASE_COLUMNS, BLOCK_FULL_BASE_COLUMNS, BLOCK_METADATA_BASE_COLUMNS,
    BLOCK_METADATA_REWARD_COLUMNS, BLOCK_SIGNATURE_COLUMNS, BLOCK_TRANSACTION_REWARD_COLUMNS,
    format_select_columns,
};
use super::rows::{
    BlockAccountsTransactionRow, BlockFullTransactionRow, BlockMetadataBaseRow, BlockMetadataRow,
    BlockSignatureRow, fetch_blockhash_height_row, map_block_accounts_transaction_row,
    map_block_full_transaction_row, map_block_metadata_base_row, map_block_metadata_row,
    map_block_signature_row,
};
#[cfg(feature = "grpc-streaming")]
use super::rows::{TransactionRow, map_transaction_row};
use super::types::{
    BlockMetadataRecord, InflationRewardLookupOutcome, InflationRewardRecord, QueryTimings,
    StoredAccountsTransactionRecord, StoredTransactionRecord,
};
use super::util::{annotate_required_query, http_query_with_id};

fn inflation_epoch_slot_bounds(epoch: u64) -> Option<(u64, u64)> {
    let next_epoch = epoch.checked_add(1)?;
    let following_epoch = next_epoch.checked_add(1)?;
    let start_slot = next_epoch.checked_mul(DEFAULT_SLOTS_PER_EPOCH)?;
    let end_slot_exclusive = following_epoch.checked_mul(DEFAULT_SLOTS_PER_EPOCH)?;
    Some((start_slot, end_slot_exclusive))
}

const MAX_INFLATION_REWARD_PARTITIONS: usize = 4_096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InflationRewardSelection {
    VoteOnly,
    VoteAndStake,
    StakeOnly,
}

impl InflationRewardSelection {
    fn sql_predicate(self) -> &'static str {
        match self {
            Self::VoteOnly => "lowerUTF8(ifNull(reward.4, '')) = 'voting'",
            Self::VoteAndStake => "lowerUTF8(ifNull(reward.4, '')) IN ('staking', 'voting')",
            Self::StakeOnly => "lowerUTF8(ifNull(reward.4, '')) = 'staking'",
        }
    }
}

#[derive(Deserialize, clickhouse::Row)]
struct InflationBoundaryRow {
    slot: u64,
    parent_slot: u64,
    parent_blockhash: Array<u8, 32>,
    block_height: Option<u64>,
    rewards_num_partitions: Option<u64>,
}

#[derive(Deserialize, clickhouse::Row)]
struct InflationSlotRow {
    slot: u64,
    block_height: Option<u64>,
}

#[derive(Deserialize, clickhouse::Row)]
struct InflationRewardRow {
    pubkey: Array<u8, 32>,
    effective_slot: u64,
    lamports: i64,
    post_balance: u64,
    commission: Option<u8>,
    commission_bps: Option<u16>,
}

fn build_inflation_boundary_query(
    table: &str,
    start_slot: u64,
    end_slot_exclusive: u64,
    settings_clause: &str,
) -> String {
    let payout_epoch = start_slot / SLOT_SHARD_DIVISOR;
    format!(
        "SELECT
            slot,
            parent_slot,
            parent_blockhash,
            block_height,
            rewards_num_partitions
         FROM {table}
         PREWHERE
            intDiv(slot, {slot_shard_divisor}) = {payout_epoch}
            AND slot >= {start_slot}
            AND slot < {end_slot_exclusive}
         ORDER BY slot ASC
         LIMIT 1
         {settings_clause}",
        slot_shard_divisor = SLOT_SHARD_DIVISOR,
    )
}

fn build_inflation_partition_slots_query(
    table: &str,
    boundary_slot: u64,
    end_slot_exclusive: u64,
    required_block_heights: &[u64],
    settings_clause: &str,
) -> String {
    let payout_epoch = boundary_slot / SLOT_SHARD_DIVISOR;
    let block_height_literals = required_block_heights
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT slot, block_height
         FROM {table}
         PREWHERE
            intDiv(slot, {slot_shard_divisor}) = {payout_epoch}
            AND slot > {boundary_slot}
            AND slot < {end_slot_exclusive}
         WHERE block_height IN ({block_height_literals})
         ORDER BY block_height ASC, slot ASC
         {settings_clause}",
        slot_shard_divisor = SLOT_SHARD_DIVISOR,
    )
}

fn build_inflation_progress_query(
    table: &str,
    boundary_slot: u64,
    end_slot_exclusive: u64,
    settings_clause: &str,
) -> String {
    let payout_epoch = boundary_slot / SLOT_SHARD_DIVISOR;
    format!(
        "SELECT slot, block_height
         FROM {table}
         PREWHERE
            intDiv(slot, {slot_shard_divisor}) = {payout_epoch}
            AND slot >= {boundary_slot}
            AND slot < {end_slot_exclusive}
         ORDER BY slot DESC
         LIMIT 1
         {settings_clause}",
        slot_shard_divisor = SLOT_SHARD_DIVISOR,
    )
}

fn build_inflation_rewards_query(
    table: &str,
    addresses: &[[u8; 32]],
    slots: &[u64],
    selection: InflationRewardSelection,
    settings_clause: &str,
) -> String {
    let address_literals = addresses
        .iter()
        .map(|address| {
            format!(
                "toFixedString(unhex('{}'), 32)",
                hex::encode(address).to_uppercase()
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let slot_literals = slots
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let reward_type_predicate = selection.sql_predicate();

    format!(
        "WITH [{address_literals}] AS target_pubkeys
         SELECT
            pubkey,
            tupleElement(latest, 1) AS effective_slot,
            tupleElement(latest, 2) AS lamports,
            tupleElement(latest, 3) AS post_balance,
            tupleElement(latest, 4) AS commission,
            tupleElement(latest, 5) AS commission_bps
         FROM (
            SELECT
                reward.1 AS pubkey,
                argMax(tuple(slot, reward.2, reward.3, reward.5, reward.6), slot) AS latest
            FROM (
                SELECT
                    slot,
                    rewards_pubkey,
                    rewards_lamports,
                    rewards_post_balance,
                    rewards_type,
                    rewards_commission,
                    if(
                        empty(rewards_commission_bps),
                        arrayMap(_ -> CAST(NULL, 'Nullable(UInt16)'), rewards_pubkey),
                        rewards_commission_bps
                    ) AS rewards_commission_bps
                FROM {table}
                PREWHERE slot IN ({slot_literals})
                WHERE rewards_present = 1 AND hasAny(rewards_pubkey, target_pubkeys)
            ) AS selected_blocks
            ARRAY JOIN arrayZip(
                rewards_pubkey,
                rewards_lamports,
                rewards_post_balance,
                rewards_type,
                rewards_commission,
                rewards_commission_bps
            ) AS reward
            WHERE has(target_pubkeys, reward.1) AND {reward_type_predicate}
            GROUP BY pubkey
         )
         {settings_clause}"
    )
}

fn inflation_reward_partition(address: &[u8; 32], num_partitions: usize, seed: &[u8; 32]) -> usize {
    EpochRewardsHasher::new(num_partitions, &Hash::new_from_array(*seed))
        .hash_address_to_partition(&Pubkey::new_from_array(*address))
}

#[derive(Debug, PartialEq, Eq)]
struct InflationRewardPartitionPlan {
    block_height_by_pubkey: HashMap<[u8; 32], u64>,
    required_block_heights: Vec<u64>,
}

fn plan_inflation_reward_partitions(
    addresses: &[[u8; 32]],
    num_partitions: usize,
    seed: &[u8; 32],
    boundary_block_height: u64,
) -> Option<InflationRewardPartitionPlan> {
    let mut block_height_by_pubkey = HashMap::with_capacity(addresses.len());
    let mut required_block_height_set = HashSet::with_capacity(addresses.len());
    for address in addresses {
        let partition_index = inflation_reward_partition(address, num_partitions, seed);
        let required_block_height = boundary_block_height
            .checked_add(partition_index as u64)?
            .checked_add(1)?;
        block_height_by_pubkey.insert(*address, required_block_height);
        required_block_height_set.insert(required_block_height);
    }
    let mut required_block_heights = required_block_height_set.into_iter().collect::<Vec<_>>();
    required_block_heights.sort_unstable();

    Some(InflationRewardPartitionPlan {
        block_height_by_pubkey,
        required_block_heights,
    })
}

fn inflation_rewards_period_is_active(
    current_block_height: u64,
    rewards_complete_block_height: u64,
) -> bool {
    current_block_height < rewards_complete_block_height
}

#[derive(Deserialize, clickhouse::Row)]
struct SlotArrayRow {
    slots: Vec<u64>,
}

fn shard_indices_for_slot_range<F>(
    start_slot: u64,
    end_slot: u64,
    mut shard_index_for_bucket: F,
) -> Vec<usize>
where
    F: FnMut(u64) -> usize,
{
    if end_slot < start_slot {
        return Vec::new();
    }

    let start_bucket = start_slot / SLOT_SHARD_DIVISOR;
    let end_bucket = end_slot / SLOT_SHARD_DIVISOR;
    let mut seen = HashSet::new();
    let mut shard_indices = Vec::new();
    for bucket in start_bucket..=end_bucket {
        let shard_index = shard_index_for_bucket(bucket);
        if seen.insert(shard_index) {
            shard_indices.push(shard_index);
        }
    }
    shard_indices
}

#[cfg(any(feature = "disk-cache", feature = "grpc-streaming"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ShardSlotRange {
    shard_index: usize,
    start_slot: u64,
    end_slot: u64,
}

#[cfg(any(feature = "disk-cache", feature = "grpc-streaming"))]
fn shard_slot_ranges_for_slot_range<F>(
    start_slot: u64,
    end_slot: u64,
    mut shard_index_for_bucket: F,
) -> Vec<ShardSlotRange>
where
    F: FnMut(u64) -> usize,
{
    if end_slot < start_slot {
        return Vec::new();
    }

    let mut ranges = Vec::new();
    let mut segment_start = start_slot;
    loop {
        let bucket = segment_start / SLOT_SHARD_DIVISOR;
        let next_bucket_start = bucket
            .checked_add(1)
            .and_then(|next_bucket| next_bucket.checked_mul(SLOT_SHARD_DIVISOR))
            .unwrap_or(u64::MAX);
        let segment_end = next_bucket_start.saturating_sub(1).min(end_slot);
        ranges.push(ShardSlotRange {
            shard_index: shard_index_for_bucket(bucket),
            start_slot: segment_start,
            end_slot: segment_end,
        });

        if segment_end == end_slot {
            break;
        }
        segment_start = segment_end + 1;
    }

    ranges
}

fn normalize_slots(mut slots: Vec<u64>) -> Vec<u64> {
    if slots.len() <= 1 {
        return slots;
    }

    slots.sort_unstable();
    slots.dedup();
    slots
}

async fn fetch_slot_rows(
    client: &clickhouse::Client,
    query: &str,
) -> ProcessingResult<(Vec<u64>, QueryTimings)> {
    let start = Instant::now();
    let mut cursor = client
        .query(query)
        .fetch::<SlotArrayRow>()
        .map_err(|e| ProcessingError::database(e.to_string(), e))?;

    let row_opt = cursor
        .next()
        .await
        .map_err(|e| ProcessingError::database(e.to_string(), e))?;
    let row_present = row_opt.is_some();
    let slots = row_opt.map_or_else(Vec::new, |row| row.slots);

    let timings = QueryTimings {
        elapsed_ms: start.elapsed().as_millis() as u64,
        received_bytes: cursor.received_bytes(),
        decoded_bytes: cursor.decoded_bytes(),
        rows_read: Some(0),
        rows_read_unknown: true,
        rows_returned: u64::from(row_present),
    };

    Ok((slots, timings))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BlockTransactionProjection {
    Signatures,
    Accounts,
    Full,
}

fn build_block_metadata_query(
    table: &str,
    slot: u64,
    include_rewards: bool,
    settings_clause: &str,
    supports_prewhere: bool,
) -> String {
    let mut columns = BLOCK_METADATA_BASE_COLUMNS.to_vec();
    if include_rewards {
        columns.extend_from_slice(BLOCK_METADATA_REWARD_COLUMNS);
    }

    let filter_clause = if supports_prewhere {
        "PREWHERE"
    } else {
        "WHERE"
    };
    format!(
        "SELECT
                {columns}
             FROM {blocks_metadata_table}
             {filter_clause} slot = {slot}
             LIMIT 1
             {settings_clause}",
        columns = format_select_columns(&columns),
        blocks_metadata_table = table,
        filter_clause = filter_clause,
        slot = slot,
        settings_clause = settings_clause
    )
}

fn build_block_time_query(
    table: &str,
    slot: u64,
    settings_clause: &str,
    supports_prewhere: bool,
) -> String {
    let filter_clause = if supports_prewhere {
        "PREWHERE"
    } else {
        "WHERE"
    };
    format!(
        "SELECT
                block_time
             FROM {table}
             {filter_clause} slot = {slot}
             LIMIT 1
             {settings_clause}"
    )
}

fn build_block_transactions_query(
    table: &str,
    slot: u64,
    projection: BlockTransactionProjection,
    settings_clause: &str,
) -> String {
    let columns = match projection {
        BlockTransactionProjection::Signatures => BLOCK_SIGNATURE_COLUMNS.to_vec(),
        BlockTransactionProjection::Accounts => {
            let mut columns = BLOCK_ACCOUNTS_BASE_COLUMNS.to_vec();
            columns.extend_from_slice(BLOCK_TRANSACTION_REWARD_COLUMNS);
            columns
        }
        BlockTransactionProjection::Full => {
            let mut columns = BLOCK_FULL_BASE_COLUMNS.to_vec();
            columns.extend_from_slice(BLOCK_TRANSACTION_REWARD_COLUMNS);
            columns
        }
    };

    format!(
        "SELECT
                {columns}
             FROM {transaction_table}
             PREWHERE slot = {slot}
             ORDER BY slot_idx ASC, signature ASC
             LIMIT 1 BY signature
             {settings_clause}",
        columns = format_select_columns(&columns),
        transaction_table = table,
        slot = slot,
        settings_clause = settings_clause
    )
}

// Efficiency bound for the isBlockhashValid lookup so the query hits the slot primary
// key, not the unindexed blockhash. It must still exceed MAX_PROCESSING_AGE (150), since
// skipped slots make slots >= block heights, or valid rows fall outside the scan.
const BLOCKHASH_VALID_SLOT_WINDOW: u64 = 512;

fn build_blockhash_valid_query(
    table: &str,
    blockhash_literal: &str,
    start_slot: u64,
    end_slot: u64,
    min_block_height: u64,
    settings_clause: &str,
) -> String {
    let start_bucket = start_slot / SLOT_SHARD_DIVISOR;
    let end_bucket = end_slot / SLOT_SHARD_DIVISOR;
    format!(
        "SELECT 1 AS present
             FROM {blocks_metadata_table}
             PREWHERE
                intDiv(slot, {slot_shard_divisor}) BETWEEN {start_bucket} AND {end_bucket}
                AND slot BETWEEN {start_slot} AND {end_slot}
             WHERE blockhash = {blockhash_literal} AND block_height >= {min_block_height}
             LIMIT 1
             {settings_clause}",
        blocks_metadata_table = table,
        slot_shard_divisor = SLOT_SHARD_DIVISOR,
        start_bucket = start_bucket,
        end_bucket = end_bucket,
        start_slot = start_slot,
        end_slot = end_slot,
        blockhash_literal = blockhash_literal,
        min_block_height = min_block_height,
        settings_clause = settings_clause,
    )
}

/// Range variant for streaming/cache backfill: every block in `[start, end]`,
/// rewards included, ascending. Always routed via the distributed table because
/// slot ranges can straddle shard buckets.
#[cfg(any(feature = "disk-cache", feature = "grpc-streaming"))]
fn build_block_metadata_range_query(
    table: &str,
    start_slot: u64,
    end_slot: u64,
    settings_clause: &str,
) -> String {
    let mut columns = BLOCK_METADATA_BASE_COLUMNS.to_vec();
    columns.extend_from_slice(BLOCK_METADATA_REWARD_COLUMNS);
    let start_bucket = start_slot / SLOT_SHARD_DIVISOR;
    let end_bucket = end_slot / SLOT_SHARD_DIVISOR;

    format!(
        "SELECT
                {columns}
             FROM {blocks_metadata_table}
             PREWHERE
                intDiv(slot, {slot_shard_divisor}) BETWEEN {start_bucket} AND {end_bucket}
                AND slot BETWEEN {start_slot} AND {end_slot}
             ORDER BY slot ASC
             LIMIT 1 BY slot
             {settings_clause}",
        columns = format_select_columns(&columns),
        blocks_metadata_table = table,
        slot_shard_divisor = SLOT_SHARD_DIVISOR,
        start_bucket = start_bucket,
        end_bucket = end_bucket,
        start_slot = start_slot,
        end_slot = end_slot,
        settings_clause = settings_clause
    )
}

fn build_transaction_count_query(
    table: &str,
    slot: u64,
    inclusive: bool,
    settings_clause: &str,
) -> String {
    let operator = if inclusive { "<=" } else { "<" };
    format!(
        "SELECT
                count() AS transaction_count
             FROM {transaction_table}
             PREWHERE slot {operator} {slot}
             {settings_clause}",
        transaction_table = table,
        operator = operator,
        slot = slot,
        settings_clause = settings_clause
    )
}

async fn fetch_block_metadata_projection(
    client: &clickhouse::Client,
    query: &str,
    include_rewards: bool,
) -> ProcessingResult<(Option<BlockMetadataRecord>, QueryTimings)> {
    if include_rewards {
        let start = Instant::now();
        let mut cursor = client
            .query(query)
            .fetch::<BlockMetadataRow>()
            .map_err(|e| ProcessingError::database(e.to_string(), e))?;
        let row_opt = cursor
            .next()
            .await
            .map_err(|e| ProcessingError::database(e.to_string(), e))?;
        let timings = QueryTimings {
            elapsed_ms: start.elapsed().as_millis() as u64,
            received_bytes: cursor.received_bytes(),
            decoded_bytes: cursor.decoded_bytes(),
            rows_read: Some(0),
            rows_read_unknown: true,
            rows_returned: u64::from(row_opt.is_some()),
        };
        Ok((row_opt.map(map_block_metadata_row), timings))
    } else {
        let start = Instant::now();
        let mut cursor = client
            .query(query)
            .fetch::<BlockMetadataBaseRow>()
            .map_err(|e| ProcessingError::database(e.to_string(), e))?;
        let row_opt = cursor
            .next()
            .await
            .map_err(|e| ProcessingError::database(e.to_string(), e))?;
        let timings = QueryTimings {
            elapsed_ms: start.elapsed().as_millis() as u64,
            received_bytes: cursor.received_bytes(),
            decoded_bytes: cursor.decoded_bytes(),
            rows_read: Some(0),
            rows_read_unknown: true,
            rows_returned: u64::from(row_opt.is_some()),
        };
        Ok((row_opt.map(map_block_metadata_base_row), timings))
    }
}

#[cfg(any(feature = "disk-cache", feature = "grpc-streaming"))]
async fn fetch_block_metadata_range(
    client: &clickhouse::Client,
    query: &str,
) -> ProcessingResult<(Vec<BlockMetadataRecord>, QueryTimings)> {
    let start = Instant::now();
    let mut cursor = client
        .query(query)
        .fetch::<BlockMetadataRow>()
        .map_err(|e| ProcessingError::database(e.to_string(), e))?;
    let mut records = Vec::new();
    while let Some(row) = cursor
        .next()
        .await
        .map_err(|e| ProcessingError::database(e.to_string(), e))?
    {
        records.push(map_block_metadata_row(row));
    }
    let timings = QueryTimings {
        elapsed_ms: start.elapsed().as_millis() as u64,
        received_bytes: cursor.received_bytes(),
        decoded_bytes: cursor.decoded_bytes(),
        rows_read: Some(0),
        rows_read_unknown: true,
        rows_returned: records.len() as u64,
    };
    Ok((records, timings))
}

#[cfg(feature = "grpc-streaming")]
async fn fetch_transaction_range(
    client: &clickhouse::Client,
    query: &str,
) -> ProcessingResult<(Vec<StoredTransactionRecord>, QueryTimings)> {
    let start = Instant::now();
    let mut cursor = client
        .query(query)
        .fetch::<TransactionRow>()
        .map_err(|e| ProcessingError::database(e.to_string(), e))?;
    let mut records = Vec::new();
    while let Some(row) = cursor
        .next()
        .await
        .map_err(|e| ProcessingError::database(e.to_string(), e))?
    {
        records.push(map_transaction_row(row));
    }
    let timings = QueryTimings {
        elapsed_ms: start.elapsed().as_millis() as u64,
        received_bytes: cursor.received_bytes(),
        decoded_bytes: cursor.decoded_bytes(),
        rows_read: Some(0),
        rows_read_unknown: true,
        rows_returned: records.len() as u64,
    };
    Ok((records, timings))
}

async fn fetch_block_signatures_projection(
    client: &clickhouse::Client,
    query: &str,
) -> ProcessingResult<(Vec<String>, QueryTimings)> {
    let start = Instant::now();
    let mut cursor = client
        .query(query)
        .fetch::<BlockSignatureRow>()
        .map_err(|e| ProcessingError::database(e.to_string(), e))?;
    let mut rows = Vec::new();
    while let Some(row) = cursor
        .next()
        .await
        .map_err(|e| ProcessingError::database(e.to_string(), e))?
    {
        rows.push(row);
    }
    let timings = QueryTimings {
        elapsed_ms: start.elapsed().as_millis() as u64,
        received_bytes: cursor.received_bytes(),
        decoded_bytes: cursor.decoded_bytes(),
        rows_read: Some(0),
        rows_read_unknown: true,
        rows_returned: rows.len() as u64,
    };
    Ok((
        rows.into_iter().map(map_block_signature_row).collect(),
        timings,
    ))
}

async fn fetch_block_accounts_projection(
    client: &clickhouse::Client,
    query: &str,
) -> ProcessingResult<(Vec<StoredAccountsTransactionRecord>, QueryTimings)> {
    let start = Instant::now();
    let mut cursor = client
        .query(query)
        .fetch::<BlockAccountsTransactionRow>()
        .map_err(|e| ProcessingError::database(e.to_string(), e))?;
    let mut rows = Vec::new();
    while let Some(row) = cursor
        .next()
        .await
        .map_err(|e| ProcessingError::database(e.to_string(), e))?
    {
        rows.push(row);
    }
    let timings = QueryTimings {
        elapsed_ms: start.elapsed().as_millis() as u64,
        received_bytes: cursor.received_bytes(),
        decoded_bytes: cursor.decoded_bytes(),
        rows_read: Some(0),
        rows_read_unknown: true,
        rows_returned: rows.len() as u64,
    };
    Ok((
        rows.into_iter()
            .map(map_block_accounts_transaction_row)
            .collect(),
        timings,
    ))
}

async fn fetch_block_full_projection(
    client: &clickhouse::Client,
    query: &str,
    slot: u64,
) -> ProcessingResult<(Vec<StoredTransactionRecord>, QueryTimings)> {
    let start = Instant::now();
    let mut cursor = client
        .query(query)
        .fetch::<BlockFullTransactionRow>()
        .map_err(|e| ProcessingError::database(e.to_string(), e))?;
    let mut rows = Vec::new();
    while let Some(row) = cursor
        .next()
        .await
        .map_err(|e| ProcessingError::database(e.to_string(), e))?
    {
        rows.push(row);
    }
    let timings = QueryTimings {
        elapsed_ms: start.elapsed().as_millis() as u64,
        received_bytes: cursor.received_bytes(),
        decoded_bytes: cursor.decoded_bytes(),
        rows_read: Some(0),
        rows_read_unknown: true,
        rows_returned: rows.len() as u64,
    };
    let records = rows
        .into_iter()
        .map(|row| map_block_full_transaction_row(row, slot))
        .collect::<ProcessingResult<Vec<_>>>()?;
    Ok((records, timings))
}

impl ClickHouseClient {
    pub async fn get_latest_finalized_slot(&self) -> ProcessingResult<Option<u64>> {
        #[cfg(test)]
        if let Some(latest_slot) = self.latest_finalized_slot_for_tests {
            return Ok(latest_slot);
        }

        self.with_http_query_timeout("get_latest_finalized_slot", async {
            #[derive(Deserialize, clickhouse::Row)]
            struct MaxSlotRow {
                max_slot: Option<u64>,
            }

            let blocks_metadata_table = &self.blocks_metadata_table;
            let settings_clause = self.select_settings_clause(
                "get_latest_finalized_slot",
                QueryFreshnessClass::TipSensitive,
            );
            let query = format!(
                "SELECT maxOrNull(slot) AS max_slot FROM {blocks_metadata_table} {settings_clause}",
                blocks_metadata_table = blocks_metadata_table,
                settings_clause = settings_clause
            );

            let row = self
                .client
                .query(&query)
                .fetch_one::<MaxSlotRow>()
                .await
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;

            Ok(row.max_slot)
        })
        .await
    }

    pub async fn is_blockhash_valid_in_window(
        &self,
        blockhash: &[u8; 32],
        context_slot: u64,
        min_block_height: u64,
    ) -> ProcessingResult<(bool, QueryTimings)> {
        self.with_http_query_timeout("is_blockhash_valid", async {
            #[derive(Deserialize, clickhouse::Row)]
            struct PresentRow {
                present: u8,
            }

            let blocks_metadata_table = &self.blocks_metadata_table;
            let settings_clause = self
                .select_settings_clause("is_blockhash_valid", QueryFreshnessClass::TipSensitive);
            let start_slot = context_slot.saturating_sub(BLOCKHASH_VALID_SLOT_WINDOW);
            let blockhash_literal = format!(
                "toFixedString(unhex('{}'), 32)",
                hex::encode(blockhash).to_uppercase()
            );
            let query = build_blockhash_valid_query(
                blocks_metadata_table,
                &blockhash_literal,
                start_slot,
                context_slot,
                min_block_height,
                &settings_clause,
            );
            let start = Instant::now();
            let mut cursor = self
                .client
                .query(&query)
                .fetch::<PresentRow>()
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;

            let row_opt = cursor
                .next()
                .await
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;
            let exists = row_opt.is_some_and(|row| row.present == 1);

            let timings = QueryTimings {
                elapsed_ms: start.elapsed().as_millis() as u64,
                received_bytes: cursor.received_bytes(),
                decoded_bytes: cursor.decoded_bytes(),
                rows_read: Some(0),
                rows_read_unknown: true,
                rows_returned: u64::from(exists),
            };

            Ok((exists, timings))
        })
        .await
    }

    pub async fn get_first_available_block(&self) -> ProcessingResult<(Option<u64>, QueryTimings)> {
        self.with_http_query_timeout("get_first_available_block", async {
            #[derive(Deserialize, clickhouse::Row)]
            struct MinSlotRow {
                min_slot: Option<u64>,
            }

            let blocks_metadata_table = &self.blocks_metadata_table;
            let settings_clause = self.select_settings_clause(
                "get_first_available_block",
                QueryFreshnessClass::Historical,
            );
            let query = format!(
                "SELECT minOrNull(slot) AS min_slot FROM {blocks_metadata_table} {settings_clause}",
                blocks_metadata_table = blocks_metadata_table,
                settings_clause = settings_clause
            );

            let start = Instant::now();
            let mut cursor = self
                .client
                .query(&query)
                .fetch::<MinSlotRow>()
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;

            let row_opt = cursor
                .next()
                .await
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;

            let timings = QueryTimings {
                elapsed_ms: start.elapsed().as_millis() as u64,
                received_bytes: cursor.received_bytes(),
                decoded_bytes: cursor.decoded_bytes(),
                rows_read: Some(0),
                rows_read_unknown: true,
                rows_returned: u64::from(row_opt.is_some()),
            };

            Ok((row_opt.and_then(|row| row.min_slot), timings))
        })
        .await
    }

    pub async fn minimum_ledger_slot(&self) -> ProcessingResult<(Option<u64>, QueryTimings)> {
        self.with_http_query_timeout("minimum_ledger_slot", async {
            #[derive(Deserialize, clickhouse::Row)]
            struct MinSlotRow {
                min_slot: Option<u64>,
            }

            let blocks_metadata_table = &self.blocks_metadata_table;
            let settings_clause =
                self.select_settings_clause("minimum_ledger_slot", QueryFreshnessClass::Historical);
            let query = format!(
                "SELECT minOrNull(slot) AS min_slot FROM {blocks_metadata_table} {settings_clause}",
                blocks_metadata_table = blocks_metadata_table,
                settings_clause = settings_clause
            );

            let start = Instant::now();
            let mut cursor = self
                .client
                .query(&query)
                .fetch::<MinSlotRow>()
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;

            let row_opt = cursor
                .next()
                .await
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;

            let timings = QueryTimings {
                elapsed_ms: start.elapsed().as_millis() as u64,
                received_bytes: cursor.received_bytes(),
                decoded_bytes: cursor.decoded_bytes(),
                rows_read: Some(0),
                rows_read_unknown: true,
                rows_returned: u64::from(row_opt.is_some()),
            };

            Ok((row_opt.and_then(|row| row.min_slot), timings))
        })
        .await
    }

    async fn execute_transaction_count_query(
        &self,
        operation: &'static str,
        query: String,
    ) -> ProcessingResult<(u64, QueryTimings)> {
        self.with_http_query_timeout(operation, async move {
            #[derive(Deserialize, clickhouse::Row)]
            struct TransactionCountRow {
                transaction_count: u64,
            }

            let start = Instant::now();
            let mut cursor = self
                .client
                .query(&query)
                .fetch::<TransactionCountRow>()
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;

            let row_opt = cursor
                .next()
                .await
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;

            let timings = QueryTimings {
                elapsed_ms: start.elapsed().as_millis() as u64,
                received_bytes: cursor.received_bytes(),
                decoded_bytes: cursor.decoded_bytes(),
                rows_read: Some(0),
                rows_read_unknown: true,
                rows_returned: u64::from(row_opt.is_some()),
            };

            Ok((
                row_opt.map(|row| row.transaction_count).unwrap_or_default(),
                timings,
            ))
        })
        .await
    }

    pub async fn get_transaction_count_by_slot(
        &self,
        slot: u64,
    ) -> ProcessingResult<(u64, QueryTimings)> {
        let settings_clause = self.select_settings_clause(
            "get_transaction_count_by_slot",
            QueryFreshnessClass::TipSensitive,
        );
        let query =
            build_transaction_count_query(&self.transaction_table, slot, true, &settings_clause);
        self.execute_transaction_count_query("get_transaction_count_by_slot", query)
            .await
    }

    #[cfg(feature = "grpc-head-cache")]
    pub async fn get_transaction_count_before_slot(
        &self,
        slot: u64,
    ) -> ProcessingResult<(u64, QueryTimings)> {
        let settings_clause = self.select_settings_clause(
            "get_transaction_count_before_slot",
            QueryFreshnessClass::TipSensitive,
        );
        let query =
            build_transaction_count_query(&self.transaction_table, slot, false, &settings_clause);
        self.execute_transaction_count_query("get_transaction_count_before_slot", query)
            .await
    }

    pub async fn get_block_slots_by_range(
        &self,
        start_slot: u64,
        end_slot: u64,
    ) -> ProcessingResult<(Vec<u64>, QueryTimings)> {
        self.with_http_query_timeout("get_block_slots_by_range", async {
            let start_bucket = start_slot / SLOT_SHARD_DIVISOR;
            let end_bucket = end_slot / SLOT_SHARD_DIVISOR;
            let settings_clause = self.select_settings_clause(
                "get_block_slots_by_range",
                QueryFreshnessClass::Historical,
            );
            let distributed_query = format!(
                "SELECT
                    groupArray(slot) AS slots
                 FROM {blocks_metadata_table}
                 PREWHERE
                    intDiv(slot, {slot_shard_divisor}) BETWEEN {start_bucket} AND {end_bucket}
                    AND slot BETWEEN {start_slot} AND {end_slot}
                 {settings_clause}",
                blocks_metadata_table = self.blocks_metadata_table,
                slot_shard_divisor = SLOT_SHARD_DIVISOR,
                start_bucket = start_bucket,
                end_bucket = end_bucket,
                start_slot = start_slot,
                end_slot = end_slot,
                settings_clause = settings_clause
            );

            if self.scope_shard_direct()
                && self.transport_http()
                && let (Some(topology), Some(local_table)) =
                    (&self.shard_topology, &self.blocks_metadata_local_table)
            {
                let target_shards = shard_indices_for_slot_range(start_slot, end_slot, |bucket| {
                    topology.shard_index_for_hash(bucket)
                });
                if target_shards.is_empty() {
                    return Ok((Vec::new(), QueryTimings::zero()));
                }

                tracing::debug!(
                    start_slot = start_slot,
                    end_slot = end_slot,
                    range_width = end_slot.saturating_sub(start_slot),
                    target_shards_count = target_shards.len(),
                    local_table = local_table.as_str(),
                    "Forcing distributed query path for get_block_slots_by_range in shard-direct HTTP mode"
                );
                let (slots, timings) = fetch_slot_rows(&self.client, &distributed_query).await?;
                return Ok((normalize_slots(slots), timings));
            }

            let (slots, timings) = fetch_slot_rows(&self.client, &distributed_query).await?;
            Ok((normalize_slots(slots), timings))
        })
        .await
    }

    pub async fn get_inflation_rewards_for_epoch(
        &self,
        addresses: &[[u8; 32]],
        epoch: u64,
    ) -> ProcessingResult<(InflationRewardLookupOutcome, QueryTimings)> {
        const OPERATION: &str = "get_inflation_rewards_for_epoch";

        if addresses.is_empty() {
            return Ok((
                InflationRewardLookupOutcome::Complete(Vec::new()),
                QueryTimings::zero(),
            ));
        }

        let (start_slot, end_slot_exclusive) =
            inflation_epoch_slot_bounds(epoch).ok_or_else(|| {
                ProcessingError::deserialization_msg(format!("epoch {epoch} is out of range"))
            })?;

        let mut deduped_addresses = Vec::with_capacity(addresses.len());
        let mut seen = HashSet::with_capacity(addresses.len());
        for address in addresses {
            if seen.insert(*address) {
                deduped_addresses.push(*address);
            }
        }

        let settings_clause = self.inflation_reward_settings_clause(OPERATION);
        let (query_client, blocks_metadata_table, cleanup_cluster, query_scope) = if self
            .scope_shard_direct()
            && let (Some(topology), Some(local_table)) =
                (&self.shard_topology, &self.blocks_metadata_local_table)
        {
            let shard = topology.shard_for_hash(start_slot / SLOT_SHARD_DIVISOR);
            (
                shard.http_client.clone(),
                local_table.clone(),
                None,
                "shard_local",
            )
        } else {
            (
                self.client.clone(),
                self.blocks_metadata_table.clone(),
                self.shard_routing
                    .as_ref()
                    .map(|config| config.cluster.clone()),
                "distributed",
            )
        };
        let query_timeout = self.inflation_reward_limits.query_timeout;
        let workflow_started = Instant::now();
        let deadline = tokio::time::Instant::now() + query_timeout;

        // Keep one global HTTP permit for the complete multi-query workflow. This prevents each
        // step from releasing and reacquiring capacity while an admitted reward request is active.
        // Admission and execution share one bounded deadline so saturation sheds work rather than
        // waiting indefinitely before the operation timeout starts.
        let _http_permit =
            match tokio::time::timeout_at(deadline, self.acquire_http_query_permit()).await {
                Ok(Ok(permit)) => permit,
                Ok(Err(err)) => return Err(err),
                Err(_) => {
                    crate::metrics::clickhouse_timeout(OPERATION);
                    crate::metrics::inflation_reward_lookup("unknown", "timeout");
                    return Err(ProcessingError::timeout_msg(format!(
                        "ClickHouse operation '{OPERATION}' timed out after {query_timeout:?}"
                    )));
                }
            };
        let execute = async {
            let mut timings = QueryTimings::zero();

            let boundary_query = build_inflation_boundary_query(
                &blocks_metadata_table,
                start_slot,
                end_slot_exclusive,
                &settings_clause,
            );
            let (boundary_query, boundary_query_id) =
                annotate_required_query(boundary_query, "get_inflation_reward_boundary");
            let mut boundary_cleanup = self.http_query_cleanup_for_client(
                query_client.clone(),
                cleanup_cluster.clone(),
                OPERATION,
                boundary_query_id.clone(),
            );
            let boundary_started = Instant::now();
            let mut boundary_cursor =
                http_query_with_id(&query_client, &boundary_query, Some(boundary_query_id))
                    .fetch::<InflationBoundaryRow>()
                    .map_err(|e| ProcessingError::database(e.to_string(), e))?;
            let boundary = match boundary_cursor.next().await {
                Ok(row) => row,
                Err(err) => {
                    boundary_cleanup.spawn_cleanup("error");
                    return Err(ProcessingError::database(err.to_string(), err));
                }
            };
            timings.add(QueryTimings {
                elapsed_ms: boundary_started.elapsed().as_millis() as u64,
                received_bytes: boundary_cursor.received_bytes(),
                decoded_bytes: boundary_cursor.decoded_bytes(),
                rows_read: Some(0),
                rows_read_unknown: true,
                rows_returned: u64::from(boundary.is_some()),
            });
            boundary_cleanup.disarm();

            let Some(boundary) = boundary else {
                crate::metrics::inflation_reward_lookup("unknown", "boundary_missing");
                return Ok((
                    InflationRewardLookupOutcome::BoundaryUnavailable { slot: start_slot },
                    timings,
                ));
            };
            if boundary.parent_slot >= start_slot {
                crate::metrics::inflation_reward_lookup("unknown", "invalid_boundary");
                return Err(ProcessingError::database_msg(format!(
                    "slot {} is not a valid epoch boundary for inflation epoch {epoch}",
                    boundary.slot
                )));
            }

            let boundary_block_height = boundary.block_height.ok_or_else(|| {
                crate::metrics::inflation_reward_lookup("unknown", "invalid_boundary");
                ProcessingError::database_msg(format!(
                    "inflation reward boundary slot {} has no block height",
                    boundary.slot
                ))
            })?;

            let num_partitions = match boundary.rewards_num_partitions {
                Some(num_partitions) => {
                    let num_partitions = usize::try_from(num_partitions).map_err(|_| {
                        ProcessingError::database_msg(format!(
                            "inflation epoch {epoch} declares too many reward partitions"
                        ))
                    })?;
                    if num_partitions == 0 || num_partitions > MAX_INFLATION_REWARD_PARTITIONS {
                        crate::metrics::inflation_reward_lookup(
                            "partitioned",
                            "invalid_partitions",
                        );
                        return Err(ProcessingError::database_msg(format!(
                            "inflation epoch {epoch} declares unsupported reward partition count {num_partitions}"
                        )));
                    }
                    Some(num_partitions)
                }
                None => None,
            };
            let partitioned = num_partitions.is_some();
            let boundary_selection = if partitioned {
                InflationRewardSelection::VoteOnly
            } else {
                InflationRewardSelection::VoteAndStake
            };
            let boundary_rewards_query = build_inflation_rewards_query(
                &blocks_metadata_table,
                &deduped_addresses,
                &[boundary.slot],
                boundary_selection,
                &settings_clause,
            );
            let (boundary_rewards_query, boundary_rewards_query_id) = annotate_required_query(
                boundary_rewards_query,
                "get_inflation_reward_boundary_rewards",
            );
            let mut boundary_rewards_cleanup = self.http_query_cleanup_for_client(
                query_client.clone(),
                cleanup_cluster.clone(),
                OPERATION,
                boundary_rewards_query_id.clone(),
            );
            let boundary_rewards_started = Instant::now();
            let mut boundary_rewards_cursor = http_query_with_id(
                &query_client,
                &boundary_rewards_query,
                Some(boundary_rewards_query_id),
            )
            .fetch::<InflationRewardRow>()
            .map_err(|e| ProcessingError::database(e.to_string(), e))?;
            let mut rewards_by_pubkey = HashMap::with_capacity(deduped_addresses.len());
            loop {
                let next = match boundary_rewards_cursor.next().await {
                    Ok(next) => next,
                    Err(err) => {
                        boundary_rewards_cleanup.spawn_cleanup("error");
                        return Err(ProcessingError::database(err.to_string(), err));
                    }
                };
                let Some(row) = next else {
                    break;
                };
                let pubkey = row.pubkey.0;
                if !seen.contains(&pubkey) {
                    return Err(ProcessingError::database_msg(
                        "boundary reward query returned an unrequested address".to_string(),
                    ));
                }
                if row.effective_slot != boundary.slot {
                    return Err(ProcessingError::database_msg(format!(
                        "boundary reward for requested address came from slot {}, expected {}",
                        row.effective_slot, boundary.slot
                    )));
                }
                let previous = rewards_by_pubkey.insert(
                    pubkey,
                    InflationRewardRecord {
                        pubkey,
                        effective_slot: row.effective_slot,
                        lamports: row.lamports,
                        post_balance: row.post_balance,
                        commission: row.commission,
                        commission_bps: row.commission_bps,
                    },
                );
                if previous.is_some() {
                    return Err(ProcessingError::database_msg(
                        "boundary reward query returned a duplicate address".to_string(),
                    ));
                }
            }
            timings.add(QueryTimings {
                elapsed_ms: boundary_rewards_started.elapsed().as_millis() as u64,
                received_bytes: boundary_rewards_cursor.received_bytes(),
                decoded_bytes: boundary_rewards_cursor.decoded_bytes(),
                rows_read: Some(0),
                rows_read_unknown: true,
                rows_returned: rewards_by_pubkey.len() as u64,
            });
            boundary_rewards_cleanup.disarm();

            if !partitioned {
                crate::metrics::inflation_reward_selected_blocks(1);
                crate::metrics::inflation_reward_lookup("non_partitioned", "success");
                tracing::info!(
                    epoch,
                    input_addresses = addresses.len(),
                    unique_addresses = deduped_addresses.len(),
                    declared_partitions = 0,
                    selected_blocks = 1,
                    elapsed_ms = workflow_started.elapsed().as_millis() as u64,
                    received_bytes = timings.received_bytes,
                    query_scope,
                    "Completed targeted getInflationReward lookup"
                );
                return Ok((
                    InflationRewardLookupOutcome::Complete(
                        rewards_by_pubkey.into_values().collect(),
                    ),
                    timings,
                ));
            }

            let num_partitions =
                num_partitions.expect("partitioned boundary has a partition count");
            let rewards_complete_block_height = boundary_block_height
                .checked_add(num_partitions as u64)
                .and_then(|height| height.checked_add(1))
                .ok_or_else(|| {
                    ProcessingError::database_msg(format!(
                        "inflation epoch {epoch} rewards-complete block height overflowed"
                    ))
                })?;

            let unresolved_addresses = deduped_addresses
                .iter()
                .copied()
                .filter(|address| !rewards_by_pubkey.contains_key(address))
                .collect::<Vec<_>>();
            if unresolved_addresses.is_empty() {
                crate::metrics::inflation_reward_selected_blocks(1);
                crate::metrics::inflation_reward_lookup("partitioned", "success");
                tracing::info!(
                    epoch,
                    input_addresses = addresses.len(),
                    unique_addresses = deduped_addresses.len(),
                    declared_partitions = num_partitions,
                    selected_blocks = 1,
                    boundary_block_height = boundary.block_height,
                    elapsed_ms = workflow_started.elapsed().as_millis() as u64,
                    received_bytes = timings.received_bytes,
                    query_scope,
                    "Completed targeted getInflationReward lookup"
                );
                return Ok((
                    InflationRewardLookupOutcome::Complete(
                        rewards_by_pubkey.into_values().collect(),
                    ),
                    timings,
                ));
            }

            let partition_plan = plan_inflation_reward_partitions(
                &unresolved_addresses,
                num_partitions,
                &boundary.parent_blockhash.0,
                boundary_block_height,
            )
            .ok_or_else(|| {
                ProcessingError::database_msg(format!(
                    "inflation epoch {epoch} partition block height overflowed"
                ))
            })?;
            let required_block_height_set = partition_plan
                .required_block_heights
                .iter()
                .copied()
                .collect::<HashSet<_>>();

            let partition_slots_query = build_inflation_partition_slots_query(
                &blocks_metadata_table,
                boundary.slot,
                end_slot_exclusive,
                &partition_plan.required_block_heights,
                &settings_clause,
            );
            let (partition_slots_query, partition_slots_query_id) = annotate_required_query(
                partition_slots_query,
                "get_inflation_reward_partition_slots",
            );
            let mut partition_slots_cleanup = self.http_query_cleanup_for_client(
                query_client.clone(),
                cleanup_cluster.clone(),
                OPERATION,
                partition_slots_query_id.clone(),
            );
            let partition_slots_started = Instant::now();
            let mut partition_slots_cursor = http_query_with_id(
                &query_client,
                &partition_slots_query,
                Some(partition_slots_query_id),
            )
            .fetch::<InflationSlotRow>()
            .map_err(|e| ProcessingError::database(e.to_string(), e))?;
            let mut slot_by_block_height =
                HashMap::with_capacity(partition_plan.required_block_heights.len());
            let mut block_height_by_slot =
                HashMap::with_capacity(partition_plan.required_block_heights.len());
            let mut partition_slot_rows = 0u64;
            loop {
                let next = match partition_slots_cursor.next().await {
                    Ok(next) => next,
                    Err(err) => {
                        partition_slots_cleanup.spawn_cleanup("error");
                        return Err(ProcessingError::database(err.to_string(), err));
                    }
                };
                let Some(row) = next else {
                    break;
                };
                partition_slot_rows = partition_slot_rows.saturating_add(1);
                let block_height = row.block_height.ok_or_else(|| {
                    ProcessingError::database_msg(format!(
                        "inflation reward partition slot {} has no block height",
                        row.slot
                    ))
                })?;
                if !required_block_height_set.contains(&block_height) {
                    return Err(ProcessingError::database_msg(format!(
                        "partition lookup returned unexpected block height {block_height}"
                    )));
                }
                if row.slot <= boundary.slot || row.slot >= end_slot_exclusive {
                    return Err(ProcessingError::database_msg(format!(
                        "partition lookup returned unexpected slot {}",
                        row.slot
                    )));
                }
                if let Some(previous_slot) = slot_by_block_height.insert(block_height, row.slot)
                    && previous_slot != row.slot
                {
                    return Err(ProcessingError::database_msg(format!(
                        "partition block height {block_height} maps to multiple slots"
                    )));
                }
                if let Some(previous_height) = block_height_by_slot.insert(row.slot, block_height)
                    && previous_height != block_height
                {
                    return Err(ProcessingError::database_msg(format!(
                        "partition slot {} maps to multiple block heights",
                        row.slot
                    )));
                }
            }
            timings.add(QueryTimings {
                elapsed_ms: partition_slots_started.elapsed().as_millis() as u64,
                received_bytes: partition_slots_cursor.received_bytes(),
                decoded_bytes: partition_slots_cursor.decoded_bytes(),
                rows_read: Some(0),
                rows_read_unknown: true,
                rows_returned: partition_slot_rows,
            });
            partition_slots_cleanup.disarm();

            let missing_block_heights = partition_plan
                .required_block_heights
                .iter()
                .copied()
                .filter(|block_height| !slot_by_block_height.contains_key(block_height))
                .collect::<Vec<_>>();
            if !missing_block_heights.is_empty() {
                let progress_query = build_inflation_progress_query(
                    &blocks_metadata_table,
                    boundary.slot,
                    end_slot_exclusive,
                    &settings_clause,
                );
                let (progress_query, progress_query_id) =
                    annotate_required_query(progress_query, "get_inflation_reward_payout_progress");
                let mut progress_cleanup = self.http_query_cleanup_for_client(
                    query_client.clone(),
                    cleanup_cluster.clone(),
                    OPERATION,
                    progress_query_id.clone(),
                );
                let progress_started = Instant::now();
                let mut progress_cursor =
                    http_query_with_id(&query_client, &progress_query, Some(progress_query_id))
                        .fetch::<InflationSlotRow>()
                        .map_err(|e| ProcessingError::database(e.to_string(), e))?;
                let progress = match progress_cursor.next().await {
                    Ok(row) => row,
                    Err(err) => {
                        progress_cleanup.spawn_cleanup("error");
                        return Err(ProcessingError::database(err.to_string(), err));
                    }
                };
                timings.add(QueryTimings {
                    elapsed_ms: progress_started.elapsed().as_millis() as u64,
                    received_bytes: progress_cursor.received_bytes(),
                    decoded_bytes: progress_cursor.decoded_bytes(),
                    rows_read: Some(0),
                    rows_read_unknown: true,
                    rows_returned: u64::from(progress.is_some()),
                });
                progress_cleanup.disarm();

                let progress = progress.ok_or_else(|| {
                    ProcessingError::database_msg(format!(
                        "payout epoch progress disappeared after boundary slot {} was read",
                        boundary.slot
                    ))
                })?;
                let current_block_height = progress.block_height.ok_or_else(|| {
                    ProcessingError::database_msg(format!(
                        "payout epoch progress slot {} has no block height",
                        progress.slot
                    ))
                })?;
                if progress.slot < boundary.slot
                    || progress.slot >= end_slot_exclusive
                    || current_block_height < boundary_block_height
                {
                    return Err(ProcessingError::database_msg(format!(
                        "payout epoch progress returned unexpected slot {} at block height {}",
                        progress.slot, current_block_height
                    )));
                }
                if inflation_rewards_period_is_active(
                    current_block_height,
                    rewards_complete_block_height,
                ) {
                    crate::metrics::inflation_reward_lookup("partitioned", "active");
                    return Ok((
                        InflationRewardLookupOutcome::RewardsPeriodActive {
                            slot: progress.slot,
                            current_block_height,
                            rewards_complete_block_height,
                        },
                        timings,
                    ));
                }

                return Err(ProcessingError::database_msg(format!(
                    "inflation rewards for epoch {epoch} reached completion block height {rewards_complete_block_height}, but required partition block heights {missing_block_heights:?} are missing"
                )));
            }

            let mut expected_slot_by_pubkey = HashMap::with_capacity(unresolved_addresses.len());
            let mut selected_slots = Vec::with_capacity(unresolved_addresses.len());
            let mut selected_slot_set = HashSet::with_capacity(unresolved_addresses.len());
            for address in &unresolved_addresses {
                let required_block_height = partition_plan.block_height_by_pubkey[address];
                let expected_slot = slot_by_block_height[&required_block_height];
                expected_slot_by_pubkey.insert(*address, expected_slot);
                if selected_slot_set.insert(expected_slot) {
                    selected_slots.push(expected_slot);
                }
            }
            selected_slots.sort_unstable();

            let partition_rewards_query = build_inflation_rewards_query(
                &blocks_metadata_table,
                &unresolved_addresses,
                &selected_slots,
                InflationRewardSelection::StakeOnly,
                &settings_clause,
            );
            let (partition_rewards_query, partition_rewards_query_id) = annotate_required_query(
                partition_rewards_query,
                "get_inflation_reward_partition_rewards",
            );
            let mut partition_rewards_cleanup = self.http_query_cleanup_for_client(
                query_client.clone(),
                cleanup_cluster.clone(),
                OPERATION,
                partition_rewards_query_id.clone(),
            );
            let partition_rewards_started = Instant::now();
            let mut partition_rewards_cursor = http_query_with_id(
                &query_client,
                &partition_rewards_query,
                Some(partition_rewards_query_id),
            )
            .fetch::<InflationRewardRow>()
            .map_err(|e| ProcessingError::database(e.to_string(), e))?;
            let mut partition_reward_count = 0u64;
            loop {
                let next = match partition_rewards_cursor.next().await {
                    Ok(next) => next,
                    Err(err) => {
                        partition_rewards_cleanup.spawn_cleanup("error");
                        return Err(ProcessingError::database(err.to_string(), err));
                    }
                };
                let Some(row) = next else {
                    break;
                };
                let pubkey = row.pubkey.0;
                let Some(expected_slot) = expected_slot_by_pubkey.get(&pubkey) else {
                    return Err(ProcessingError::database_msg(
                        "partition reward query returned an unrequested address".to_string(),
                    ));
                };
                if row.effective_slot != *expected_slot {
                    return Err(ProcessingError::database_msg(format!(
                        "partition reward for requested address came from slot {}, expected {}",
                        row.effective_slot, expected_slot
                    )));
                }
                let previous = rewards_by_pubkey.insert(
                    pubkey,
                    InflationRewardRecord {
                        pubkey,
                        effective_slot: row.effective_slot,
                        lamports: row.lamports,
                        post_balance: row.post_balance,
                        commission: row.commission,
                        commission_bps: row.commission_bps,
                    },
                );
                if previous.is_some() {
                    return Err(ProcessingError::database_msg(
                        "partition reward query returned a duplicate address".to_string(),
                    ));
                }
                partition_reward_count = partition_reward_count.saturating_add(1);
            }
            timings.add(QueryTimings {
                elapsed_ms: partition_rewards_started.elapsed().as_millis() as u64,
                received_bytes: partition_rewards_cursor.received_bytes(),
                decoded_bytes: partition_rewards_cursor.decoded_bytes(),
                rows_read: Some(0),
                rows_read_unknown: true,
                rows_returned: partition_reward_count,
            });
            partition_rewards_cleanup.disarm();

            let selected_block_count = selected_slots.len().saturating_add(1);
            crate::metrics::inflation_reward_selected_blocks(selected_block_count);
            crate::metrics::inflation_reward_lookup("partitioned", "success");
            tracing::info!(
                epoch,
                input_addresses = addresses.len(),
                unique_addresses = deduped_addresses.len(),
                declared_partitions = num_partitions,
                selected_blocks = selected_block_count,
                boundary_block_height = boundary.block_height,
                elapsed_ms = workflow_started.elapsed().as_millis() as u64,
                received_bytes = timings.received_bytes,
                query_scope,
                "Completed targeted getInflationReward lookup"
            );

            Ok((
                InflationRewardLookupOutcome::Complete(rewards_by_pubkey.into_values().collect()),
                timings,
            ))
        };

        let execute = Box::pin(execute);
        match tokio::time::timeout_at(deadline, execute).await {
            Ok(result) => {
                if let Err(err) = &result {
                    let message = err.to_string();
                    if message.contains("MEMORY_LIMIT_EXCEEDED")
                        || message.contains("TOO_MANY_ROWS_OR_BYTES")
                        || message.contains("max_bytes_to_read")
                    {
                        crate::metrics::inflation_reward_lookup("unknown", "resource_limit");
                    }
                }
                result
            }
            Err(_) => {
                crate::metrics::clickhouse_timeout(OPERATION);
                crate::metrics::inflation_reward_lookup("unknown", "timeout");
                Err(ProcessingError::timeout_msg(format!(
                    "ClickHouse operation '{OPERATION}' timed out after {query_timeout:?}"
                )))
            }
        }
    }

    pub async fn get_block_time_by_slot(
        &self,
        slot: u64,
    ) -> ProcessingResult<(Option<Option<i64>>, QueryTimings)> {
        self.with_http_query_timeout("get_block_time_by_slot", async {
            #[derive(Deserialize, clickhouse::Row)]
            struct BlockTimeRow {
                block_time: Option<i64>,
            }

            let (mut row_opt, timings, used_local) = if self.scope_shard_direct()
                && self.transport_http()
                && let (Some(topology), Some(local_table)) =
                    (&self.shard_topology, &self.blocks_metadata_local_table)
            {
                let shard = topology.shard_for_hash(slot / SLOT_SHARD_DIVISOR);
                let settings_clause = topology.settings_clause(
                    "get_block_time_by_slot_local_http",
                    QueryFreshnessClass::Historical,
                );
                let query = build_block_time_query(
                    local_table,
                    slot,
                    &settings_clause,
                    self.blocks_metadata_supports_prewhere,
                );

                let start = Instant::now();
                match shard.http_client.query(&query).fetch::<BlockTimeRow>() {
                    Ok(mut cursor) => {
                        let row_opt = cursor
                            .next()
                            .await
                            .map_err(|e| ProcessingError::database(e.to_string(), e))?;
                        let timings = QueryTimings {
                            elapsed_ms: start.elapsed().as_millis() as u64,
                            received_bytes: cursor.received_bytes(),
                            decoded_bytes: cursor.decoded_bytes(),
                            rows_read: Some(0),
                            rows_read_unknown: true,
                            rows_returned: u64::from(row_opt.is_some()),
                        };
                        (row_opt, timings, true)
                    }
                    Err(err) => {
                        tracing::warn!(
                            "Shard {}:{} HTTP query failed; falling back to distributed table: {}",
                            shard.host,
                            shard.tcp_port,
                            err
                        );
                        let settings_clause = self.select_settings_clause(
                            "get_block_time_by_slot_fallback_distributed",
                            QueryFreshnessClass::Historical,
                        );
                        let query = build_block_time_query(
                            &self.blocks_metadata_table,
                            slot,
                            &settings_clause,
                            self.blocks_metadata_supports_prewhere,
                        );
                        let start = Instant::now();
                        let mut cursor = self
                            .client
                            .query(&query)
                            .fetch::<BlockTimeRow>()
                            .map_err(|e| ProcessingError::database(e.to_string(), e))?;
                        let row_opt = cursor
                            .next()
                            .await
                            .map_err(|e| ProcessingError::database(e.to_string(), e))?;
                        let timings = QueryTimings {
                            elapsed_ms: start.elapsed().as_millis() as u64,
                            received_bytes: cursor.received_bytes(),
                            decoded_bytes: cursor.decoded_bytes(),
                            rows_read: Some(0),
                            rows_read_unknown: true,
                            rows_returned: u64::from(row_opt.is_some()),
                        };
                        (row_opt, timings, false)
                    }
                }
            } else {
                let settings_clause = self.select_settings_clause(
                    "get_block_time_by_slot_distributed",
                    QueryFreshnessClass::Historical,
                );
                let query = build_block_time_query(
                    &self.blocks_metadata_table,
                    slot,
                    &settings_clause,
                    self.blocks_metadata_supports_prewhere,
                );
                let start = Instant::now();
                let mut cursor = self
                    .client
                    .query(&query)
                    .fetch::<BlockTimeRow>()
                    .map_err(|e| ProcessingError::database(e.to_string(), e))?;
                let row_opt = cursor
                    .next()
                    .await
                    .map_err(|e| ProcessingError::database(e.to_string(), e))?;
                let timings = QueryTimings {
                    elapsed_ms: start.elapsed().as_millis() as u64,
                    received_bytes: cursor.received_bytes(),
                    decoded_bytes: cursor.decoded_bytes(),
                    rows_read: Some(0),
                    rows_read_unknown: true,
                    rows_returned: u64::from(row_opt.is_some()),
                };
                (row_opt, timings, false)
            };

            let mut timings = timings;
            if used_local && row_opt.is_none() {
                let settings_clause = self.select_settings_clause(
                    "get_block_time_by_slot_distributed_retry",
                    QueryFreshnessClass::Historical,
                );
                let query = build_block_time_query(
                    &self.blocks_metadata_table,
                    slot,
                    &settings_clause,
                    self.blocks_metadata_supports_prewhere,
                );
                let start = Instant::now();
                let mut cursor = self
                    .client
                    .query(&query)
                    .fetch::<BlockTimeRow>()
                    .map_err(|e| ProcessingError::database(e.to_string(), e))?;
                let fallback_opt = cursor
                    .next()
                    .await
                    .map_err(|e| ProcessingError::database(e.to_string(), e))?;
                let fallback_timings = QueryTimings {
                    elapsed_ms: start.elapsed().as_millis() as u64,
                    received_bytes: cursor.received_bytes(),
                    decoded_bytes: cursor.decoded_bytes(),
                    rows_read: Some(0),
                    rows_read_unknown: true,
                    rows_returned: u64::from(fallback_opt.is_some()),
                };
                timings.add(fallback_timings);
                row_opt = fallback_opt;
            }

            Ok((row_opt.map(|row| row.block_time), timings))
        })
        .await
    }

    pub async fn get_blockhash_height_by_slot(
        &self,
        slot: u64,
    ) -> ProcessingResult<(Option<([u8; 32], Option<u64>)>, QueryTimings)> {
        self.with_http_query_timeout("get_blockhash_height_by_slot", async {
            let (mut row_opt, timings, used_local) = if self.scope_shard_direct()
                && self.transport_http()
                && let (Some(topology), Some(local_table)) =
                    (&self.shard_topology, &self.blocks_metadata_local_table)
            {
                let shard = topology.shard_for_hash(slot / SLOT_SHARD_DIVISOR);
                let settings_clause = topology.settings_clause(
                    "get_blockhash_height_by_slot_local_http",
                    QueryFreshnessClass::Historical,
                );
                let query = format!(
                    "SELECT
                            blockhash,
                            block_height
                         FROM {blocks_metadata_table}
                         PREWHERE slot = {slot}
                         LIMIT 1
                         {settings_clause}",
                    blocks_metadata_table = local_table,
                    slot = slot,
                    settings_clause = settings_clause
                );

                match fetch_blockhash_height_row(&shard.http_client, &query).await {
                    Ok(result) => (result.0, result.1, true),
                    Err(err) => {
                        tracing::warn!(
                            "Shard {}:{} HTTP query failed; falling back to distributed table: {}",
                            shard.host,
                            shard.tcp_port,
                            err
                        );
                        let settings_clause = self.select_settings_clause(
                            "get_blockhash_height_by_slot_fallback_distributed",
                            QueryFreshnessClass::Historical,
                        );
                        let query = format!(
                            "SELECT
                                    blockhash,
                                    block_height
                                 FROM {blocks_metadata_table}
                                 PREWHERE slot = {slot}
                                 LIMIT 1
                                 {settings_clause}",
                            blocks_metadata_table = self.blocks_metadata_table,
                            slot = slot,
                            settings_clause = settings_clause
                        );
                        let result = fetch_blockhash_height_row(&self.client, &query).await?;
                        (result.0, result.1, false)
                    }
                }
            } else {
                let settings_clause = self.select_settings_clause(
                    "get_blockhash_height_by_slot_distributed",
                    QueryFreshnessClass::Historical,
                );
                let query = format!(
                    "SELECT
                            blockhash,
                            block_height
                         FROM {blocks_metadata_table}
                         PREWHERE slot = {slot}
                         LIMIT 1
                         {settings_clause}",
                    blocks_metadata_table = self.blocks_metadata_table,
                    slot = slot,
                    settings_clause = settings_clause
                );
                let result = fetch_blockhash_height_row(&self.client, &query).await?;
                (result.0, result.1, false)
            };

            let mut timings = timings;
            if used_local && row_opt.is_none() {
                let settings_clause = self.select_settings_clause(
                    "get_blockhash_height_by_slot_distributed_retry",
                    QueryFreshnessClass::Historical,
                );
                let query = format!(
                    "SELECT
                        blockhash,
                        block_height
                     FROM {blocks_metadata_table}
                     PREWHERE slot = {slot}
                     LIMIT 1
                     {settings_clause}",
                    blocks_metadata_table = self.blocks_metadata_table,
                    slot = slot,
                    settings_clause = settings_clause
                );
                let (fallback_opt, fallback_timings) =
                    fetch_blockhash_height_row(&self.client, &query).await?;
                timings.add(fallback_timings);
                row_opt = fallback_opt;
            }

            let Some(row) = row_opt else {
                return Ok((None, timings));
            };

            Ok((Some((row.blockhash.0, row.block_height)), timings))
        })
        .await
    }

    pub async fn get_block_metadata_by_slot(
        &self,
        slot: u64,
        include_rewards: bool,
    ) -> ProcessingResult<(Option<BlockMetadataRecord>, QueryTimings)> {
        self.with_http_query_timeout("get_block_metadata_by_slot", async {
            let (mut metadata_opt, mut timings, used_local) = if self.scope_shard_direct()
                && let (Some(topology), Some(local_table)) =
                    (&self.shard_topology, &self.blocks_metadata_local_table)
            {
                let shard = topology.shard_for_hash(slot / SLOT_SHARD_DIVISOR);
                let settings_clause = topology.settings_clause(
                    "get_block_metadata_by_slot_local_http",
                    QueryFreshnessClass::Historical,
                );
                let query = build_block_metadata_query(
                    local_table,
                    slot,
                    include_rewards,
                    &settings_clause,
                    self.blocks_metadata_supports_prewhere,
                );

                match fetch_block_metadata_projection(&shard.http_client, &query, include_rewards)
                    .await
                {
                    Ok(result) => (result.0, result.1, true),
                    Err(err) => {
                        tracing::warn!(
                            "Shard {}:{} HTTP query failed; falling back to distributed table: {}",
                            shard.host,
                            shard.tcp_port,
                            err
                        );
                        let settings_clause = self.select_settings_clause(
                            "get_block_metadata_by_slot_fallback_distributed",
                            QueryFreshnessClass::Historical,
                        );
                        let query = build_block_metadata_query(
                            &self.blocks_metadata_table,
                            slot,
                            include_rewards,
                            &settings_clause,
                            self.blocks_metadata_supports_prewhere,
                        );
                        let result =
                            fetch_block_metadata_projection(&self.client, &query, include_rewards)
                                .await?;
                        (result.0, result.1, false)
                    }
                }
            } else {
                let settings_clause = self.select_settings_clause(
                    "get_block_metadata_by_slot_distributed",
                    QueryFreshnessClass::Historical,
                );
                let query = build_block_metadata_query(
                    &self.blocks_metadata_table,
                    slot,
                    include_rewards,
                    &settings_clause,
                    self.blocks_metadata_supports_prewhere,
                );
                let result =
                    fetch_block_metadata_projection(&self.client, &query, include_rewards).await?;
                (result.0, result.1, false)
            };

            if used_local && metadata_opt.is_none() {
                let settings_clause = self.select_settings_clause(
                    "get_block_metadata_by_slot_distributed_retry",
                    QueryFreshnessClass::Historical,
                );
                let query = build_block_metadata_query(
                    &self.blocks_metadata_table,
                    slot,
                    include_rewards,
                    &settings_clause,
                    self.blocks_metadata_supports_prewhere,
                );
                let (fallback_opt, fallback_timings) =
                    fetch_block_metadata_projection(&self.client, &query, include_rewards).await?;
                timings.add(fallback_timings);
                metadata_opt = fallback_opt;
            }

            Ok((metadata_opt, timings))
        })
        .await
    }

    pub async fn get_block_signatures_by_slot(
        &self,
        slot: u64,
    ) -> ProcessingResult<(Vec<String>, QueryTimings)> {
        self.with_http_query_timeout("get_block_signatures_by_slot", async {
            let projection = BlockTransactionProjection::Signatures;
            let (mut signatures, mut timings, used_local) = if self.scope_shard_direct()
                && let (Some(topology), Some(local_table)) =
                    (&self.shard_topology, &self.transactions_local_table)
            {
                let shard = topology.shard_for_hash(slot / SLOT_SHARD_DIVISOR);
                let settings_clause = topology.settings_clause(
                    "get_block_signatures_by_slot_local_http",
                    QueryFreshnessClass::Historical,
                );
                let query =
                    build_block_transactions_query(local_table, slot, projection, &settings_clause);

                match fetch_block_signatures_projection(&shard.http_client, &query).await {
                    Ok(result) => (result.0, result.1, true),
                    Err(err) => {
                        tracing::warn!(
                            "Shard {}:{} HTTP query failed; falling back to distributed table: {}",
                            shard.host,
                            shard.tcp_port,
                            err
                        );
                        let settings_clause = self.select_settings_clause(
                            "get_block_signatures_by_slot_fallback_distributed",
                            QueryFreshnessClass::Historical,
                        );
                        let query = build_block_transactions_query(
                            &self.transaction_table,
                            slot,
                            projection,
                            &settings_clause,
                        );
                        let result =
                            fetch_block_signatures_projection(&self.client, &query).await?;
                        (result.0, result.1, false)
                    }
                }
            } else {
                let settings_clause = self.select_settings_clause(
                    "get_block_signatures_by_slot_distributed",
                    QueryFreshnessClass::Historical,
                );
                let query = build_block_transactions_query(
                    &self.transaction_table,
                    slot,
                    projection,
                    &settings_clause,
                );
                let result = fetch_block_signatures_projection(&self.client, &query).await?;
                (result.0, result.1, false)
            };

            if used_local && signatures.is_empty() {
                let settings_clause = self.select_settings_clause(
                    "get_block_signatures_by_slot_distributed_retry",
                    QueryFreshnessClass::Historical,
                );
                let query = build_block_transactions_query(
                    &self.transaction_table,
                    slot,
                    projection,
                    &settings_clause,
                );
                let (fallback_signatures, fallback_timings) =
                    fetch_block_signatures_projection(&self.client, &query).await?;
                timings.add(fallback_timings);
                signatures = fallback_signatures;
            }

            Ok((signatures, timings))
        })
        .await
    }

    pub async fn get_block_accounts_by_slot(
        &self,
        slot: u64,
    ) -> ProcessingResult<(Vec<StoredAccountsTransactionRecord>, QueryTimings)> {
        self.with_http_query_timeout("get_block_accounts_by_slot", async {
            let projection = BlockTransactionProjection::Accounts;
            let (mut records, mut timings, used_local) = if self.scope_shard_direct()
                && let (Some(topology), Some(local_table)) =
                    (&self.shard_topology, &self.transactions_local_table)
            {
                let shard = topology.shard_for_hash(slot / SLOT_SHARD_DIVISOR);
                let settings_clause = topology.settings_clause(
                    "get_block_accounts_by_slot_local_http",
                    QueryFreshnessClass::Historical,
                );
                let query =
                    build_block_transactions_query(local_table, slot, projection, &settings_clause);

                match fetch_block_accounts_projection(&shard.http_client, &query).await {
                    Ok(result) => (result.0, result.1, true),
                    Err(err) => {
                        tracing::warn!(
                            "Shard {}:{} HTTP query failed; falling back to distributed table: {}",
                            shard.host,
                            shard.tcp_port,
                            err
                        );
                        let settings_clause = self.select_settings_clause(
                            "get_block_accounts_by_slot_fallback_distributed",
                            QueryFreshnessClass::Historical,
                        );
                        let query = build_block_transactions_query(
                            &self.transaction_table,
                            slot,
                            projection,
                            &settings_clause,
                        );
                        let result = fetch_block_accounts_projection(&self.client, &query).await?;
                        (result.0, result.1, false)
                    }
                }
            } else {
                let settings_clause = self.select_settings_clause(
                    "get_block_accounts_by_slot_distributed",
                    QueryFreshnessClass::Historical,
                );
                let query = build_block_transactions_query(
                    &self.transaction_table,
                    slot,
                    projection,
                    &settings_clause,
                );
                let result = fetch_block_accounts_projection(&self.client, &query).await?;
                (result.0, result.1, false)
            };

            if used_local && records.is_empty() {
                let settings_clause = self.select_settings_clause(
                    "get_block_accounts_by_slot_distributed_retry",
                    QueryFreshnessClass::Historical,
                );
                let query = build_block_transactions_query(
                    &self.transaction_table,
                    slot,
                    projection,
                    &settings_clause,
                );
                let (fallback_records, fallback_timings) =
                    fetch_block_accounts_projection(&self.client, &query).await?;
                timings.add(fallback_timings);
                records = fallback_records;
            }

            Ok((records, timings))
        })
        .await
    }

    pub async fn get_block_full_transactions_by_slot(
        &self,
        slot: u64,
    ) -> ProcessingResult<(Vec<StoredTransactionRecord>, QueryTimings)> {
        self.with_http_query_timeout("get_block_full_transactions_by_slot", async {
            let projection = BlockTransactionProjection::Full;
            let (mut records, mut timings, used_local) = if self.scope_shard_direct()
                && let (Some(topology), Some(local_table)) =
                    (&self.shard_topology, &self.transactions_local_table)
            {
                let shard = topology.shard_for_hash(slot / SLOT_SHARD_DIVISOR);
                let settings_clause = topology.settings_clause(
                    "get_block_full_transactions_by_slot_local_http",
                    QueryFreshnessClass::Historical,
                );
                let query =
                    build_block_transactions_query(local_table, slot, projection, &settings_clause);

                match fetch_block_full_projection(&shard.http_client, &query, slot).await {
                    Ok(result) => (result.0, result.1, true),
                    Err(err) => {
                        tracing::warn!(
                            "Shard {}:{} HTTP query failed; falling back to distributed table: {}",
                            shard.host,
                            shard.tcp_port,
                            err
                        );
                        let settings_clause = self.select_settings_clause(
                            "get_block_full_transactions_by_slot_fallback_distributed",
                            QueryFreshnessClass::Historical,
                        );
                        let query = build_block_transactions_query(
                            &self.transaction_table,
                            slot,
                            projection,
                            &settings_clause,
                        );
                        let result =
                            fetch_block_full_projection(&self.client, &query, slot).await?;
                        (result.0, result.1, false)
                    }
                }
            } else {
                let settings_clause = self.select_settings_clause(
                    "get_block_full_transactions_by_slot_distributed",
                    QueryFreshnessClass::Historical,
                );
                let query = build_block_transactions_query(
                    &self.transaction_table,
                    slot,
                    projection,
                    &settings_clause,
                );
                let result = fetch_block_full_projection(&self.client, &query, slot).await?;
                (result.0, result.1, false)
            };

            if used_local && records.is_empty() {
                let settings_clause = self.select_settings_clause(
                    "get_block_full_transactions_by_slot_distributed_retry",
                    QueryFreshnessClass::Historical,
                );
                let query = build_block_transactions_query(
                    &self.transaction_table,
                    slot,
                    projection,
                    &settings_clause,
                );
                let (fallback_records, fallback_timings) =
                    fetch_block_full_projection(&self.client, &query, slot).await?;
                timings.add(fallback_timings);
                records = fallback_records;
            }

            Ok((records, timings))
        })
        .await
    }

    /// All block metadata in `[start_slot, end_slot]`, ascending, rewards
    /// included. Streaming/cache backfill: shard-direct deployments use the
    /// shard-local table for each covered slot bucket; distributed deployments
    /// use the distributed table. The caller-provided timeout is sized for
    /// range scans rather than interactive reads.
    #[cfg(any(feature = "disk-cache", feature = "grpc-streaming"))]
    pub(crate) async fn get_block_metadata_by_slot_range(
        &self,
        start_slot: u64,
        end_slot: u64,
        timeout: std::time::Duration,
    ) -> ProcessingResult<(Vec<BlockMetadataRecord>, QueryTimings)> {
        self.with_http_query_timeout_duration("get_block_metadata_by_slot_range", timeout, async {
            if self.scope_shard_direct()
                && let (Some(topology), Some(local_table)) =
                    (&self.shard_topology, &self.blocks_metadata_local_table)
            {
                let settings_clause = self.select_settings_clause_with_timeout(
                    "get_block_metadata_by_slot_range_local_http",
                    QueryFreshnessClass::Historical,
                    timeout,
                );
                let ranges = shard_slot_ranges_for_slot_range(start_slot, end_slot, |bucket| {
                    topology.shard_index_for_hash(bucket)
                });
                let mut records = Vec::new();
                let mut timings = QueryTimings::zero();
                let mut local_failed = false;
                for range in ranges {
                    let Some(shard) = topology.shard_at(range.shard_index) else {
                        local_failed = true;
                        continue;
                    };
                    let query = build_block_metadata_range_query(
                        local_table,
                        range.start_slot,
                        range.end_slot,
                        &settings_clause,
                    );
                    match fetch_block_metadata_range(&shard.http_client, &query).await {
                        Ok((mut local_records, local_timings)) => {
                            timings.add(local_timings);
                            records.append(&mut local_records);
                        }
                        Err(err) => {
                            tracing::warn!(
                                "Shard {}:{} HTTP range query failed; falling back to distributed table: {}",
                                shard.host,
                                shard.tcp_port,
                                err
                            );
                            records.clear();
                            timings = QueryTimings::zero();
                            local_failed = true;
                            break;
                        }
                    }
                }

                if !local_failed {
                    return Ok((records, timings));
                }
            }

            let settings_clause = self.select_settings_clause_with_timeout(
                "get_block_metadata_by_slot_range",
                QueryFreshnessClass::Historical,
                timeout,
            );
            let query = build_block_metadata_range_query(
                &self.blocks_metadata_table,
                start_slot,
                end_slot,
                &settings_clause,
            );

            fetch_block_metadata_range(&self.client, &query).await
        })
        .await
    }

    /// All transactions in `[start_slot, end_slot]`, ordered by
    /// `(slot, slot_idx)` ascending. See
    /// [`Self::get_block_metadata_by_slot_range`] for the routing rationale.
    #[cfg(feature = "grpc-streaming")]
    pub(crate) async fn get_block_full_transactions_by_slot_range(
        &self,
        start_slot: u64,
        end_slot: u64,
        timeout: std::time::Duration,
    ) -> ProcessingResult<(Vec<StoredTransactionRecord>, QueryTimings)> {
        self.with_http_query_timeout_duration(
            "get_block_full_transactions_by_slot_range",
            timeout,
            async {
                if self.scope_shard_direct()
                    && let (Some(topology), Some(local_table)) =
                        (&self.shard_topology, &self.transactions_local_table)
                {
                    let settings_clause = self.select_settings_clause_with_timeout(
                        "get_block_full_transactions_by_slot_range_local_http",
                        QueryFreshnessClass::Historical,
                        timeout,
                    );
                    let ranges = shard_slot_ranges_for_slot_range(start_slot, end_slot, |bucket| {
                        topology.shard_index_for_hash(bucket)
                    });
                    let mut records = Vec::new();
                    let mut timings = QueryTimings::zero();
                    let mut local_failed = false;
                    for range in ranges {
                        let Some(shard) = topology.shard_at(range.shard_index) else {
                            local_failed = true;
                            continue;
                        };
                        let query = build_transactions_by_slot_range_query(
                            local_table,
                            range.start_slot,
                            range.end_slot,
                            &settings_clause,
                        );
                        match fetch_transaction_range(&shard.http_client, &query).await {
                            Ok((mut local_records, local_timings)) => {
                                timings.add(local_timings);
                                records.append(&mut local_records);
                            }
                            Err(err) => {
                                tracing::warn!(
                                    "Shard {}:{} HTTP transaction range query failed; falling back to distributed table: {}",
                                    shard.host,
                                    shard.tcp_port,
                                    err
                                );
                                records.clear();
                                timings = QueryTimings::zero();
                                local_failed = true;
                                break;
                            }
                        }
                    }

                    if !local_failed {
                        return Ok((records, timings));
                    }
                }

                let settings_clause = self.select_settings_clause_with_timeout(
                    "get_block_full_transactions_by_slot_range",
                    QueryFreshnessClass::Historical,
                    timeout,
                );
                let query = build_transactions_by_slot_range_query(
                    &self.transaction_table,
                    start_slot,
                    end_slot,
                    &settings_clause,
                );

                fetch_transaction_range(&self.client, &query).await
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        BlockTransactionProjection, InflationRewardSelection, SLOT_SHARD_DIVISOR,
        build_block_metadata_query, build_block_time_query, build_block_transactions_query,
        build_blockhash_valid_query, build_inflation_boundary_query,
        build_inflation_partition_slots_query, build_inflation_progress_query,
        build_inflation_rewards_query, build_transaction_count_query, inflation_epoch_slot_bounds,
        inflation_reward_partition, inflation_rewards_period_is_active, normalize_slots,
        plan_inflation_reward_partitions, shard_indices_for_slot_range,
    };
    #[cfg(any(feature = "disk-cache", feature = "grpc-streaming"))]
    use super::{
        ShardSlotRange, build_block_metadata_range_query, shard_slot_ranges_for_slot_range,
    };

    #[test]
    fn shard_indices_for_slot_range_returns_single_shard_for_single_bucket() {
        let shards = shard_indices_for_slot_range(10, 100, |bucket| (bucket % 4) as usize);
        assert_eq!(shards, vec![0]);
    }

    #[test]
    fn shard_indices_for_slot_range_handles_bucket_boundaries() {
        let start_slot = SLOT_SHARD_DIVISOR - 1;
        let end_slot = SLOT_SHARD_DIVISOR + 1;
        let shards = shard_indices_for_slot_range(start_slot, end_slot, |bucket| bucket as usize);
        assert_eq!(shards, vec![0, 1]);
    }

    #[test]
    fn shard_indices_for_slot_range_deduplicates_shards() {
        let end_slot = SLOT_SHARD_DIVISOR * 5;
        let shards = shard_indices_for_slot_range(0, end_slot, |bucket| (bucket % 2) as usize);
        assert_eq!(shards, vec![0, 1]);
    }

    #[test]
    fn shard_indices_for_slot_range_returns_empty_when_range_inverted() {
        let shards = shard_indices_for_slot_range(10, 9, |_| 0);
        assert!(shards.is_empty());
    }

    #[test]
    #[cfg(any(feature = "disk-cache", feature = "grpc-streaming"))]
    fn shard_slot_ranges_for_slot_range_keeps_single_bucket_together() {
        let ranges = shard_slot_ranges_for_slot_range(10, 100, |bucket| (bucket % 3) as usize);
        assert_eq!(
            ranges,
            vec![ShardSlotRange {
                shard_index: 0,
                start_slot: 10,
                end_slot: 100,
            }]
        );
    }

    #[test]
    #[cfg(any(feature = "disk-cache", feature = "grpc-streaming"))]
    fn shard_slot_ranges_for_slot_range_splits_at_bucket_boundaries() {
        let start_slot = SLOT_SHARD_DIVISOR - 2;
        let end_slot = SLOT_SHARD_DIVISOR + 2;
        let ranges =
            shard_slot_ranges_for_slot_range(start_slot, end_slot, |bucket| bucket as usize);
        assert_eq!(
            ranges,
            vec![
                ShardSlotRange {
                    shard_index: 0,
                    start_slot,
                    end_slot: SLOT_SHARD_DIVISOR - 1,
                },
                ShardSlotRange {
                    shard_index: 1,
                    start_slot: SLOT_SHARD_DIVISOR,
                    end_slot,
                },
            ]
        );
    }

    #[test]
    fn normalize_slots_handles_empty_and_singleton() {
        assert_eq!(normalize_slots(Vec::new()), Vec::<u64>::new());
        assert_eq!(normalize_slots(vec![42]), vec![42]);
    }

    #[test]
    fn normalize_slots_sorts_and_deduplicates() {
        let slots = normalize_slots(vec![5, 2, 5, 3, 2, 9, 9, 1]);
        assert_eq!(slots, vec![1, 2, 3, 5, 9]);
    }

    #[test]
    fn inflation_boundary_query_reads_only_lightweight_metadata() {
        let query = build_inflation_boundary_query(
            "default.blocks_metadata",
            121_392_000,
            121_824_000,
            "SETTINGS max_threads=2",
        );

        assert!(query.contains("intDiv(slot, 432000) = 281"));
        assert!(query.contains("slot >= 121392000"));
        assert!(query.contains("slot < 121824000"));
        assert!(query.contains("rewards_num_partitions"));
        assert!(!query.contains("rewards_pubkey"));
        assert!(query.contains("ORDER BY slot ASC"));
        assert!(query.contains("LIMIT 1"));
        assert!(query.contains("SETTINGS max_threads=2"));
    }

    #[test]
    fn inflation_epoch_1018_payout_starts_at_foundation_error_slot() {
        assert_eq!(
            inflation_epoch_slot_bounds(1_018),
            Some((440_208_000, 440_640_000))
        );
    }

    #[test]
    fn inflation_partition_slots_query_targets_exact_block_heights() {
        let query = build_inflation_partition_slots_query(
            "default.blocks_metadata",
            121_392_000,
            121_824_000,
            &[270_000_001, 270_000_017],
            "",
        );

        assert!(query.contains("SELECT slot, block_height"));
        assert!(query.contains("slot > 121392000"));
        assert!(query.contains("slot < 121824000"));
        assert!(query.contains("WHERE block_height IN (270000001, 270000017)"));
        assert!(query.contains("ORDER BY block_height ASC, slot ASC"));
        assert!(!query.contains("LIMIT 1 BY slot"));
        assert!(!query.contains("LIMIT 293"));
    }

    #[test]
    fn inflation_progress_query_reads_latest_landed_payout_block() {
        let query = build_inflation_progress_query(
            "default.blocks_metadata",
            121_392_000,
            121_824_000,
            "SETTINGS max_threads=2",
        );

        assert!(query.contains("SELECT slot, block_height"));
        assert!(query.contains("slot >= 121392000"));
        assert!(query.contains("slot < 121824000"));
        assert!(query.contains("ORDER BY slot DESC"));
        assert!(query.contains("LIMIT 1"));
        assert!(query.contains("SETTINGS max_threads=2"));
    }

    #[test]
    fn inflation_rewards_query_expands_only_exact_slots() {
        let addresses = [[1u8; 32], [2u8; 32]];
        let query = build_inflation_rewards_query(
            "default.blocks_metadata",
            &addresses,
            &[121_392_000, 121_392_017],
            InflationRewardSelection::StakeOnly,
            "SETTINGS max_threads=2, max_memory_usage=536870912",
        );

        assert!(query.contains("PREWHERE slot IN (121392000, 121392017)"));
        assert!(!query.contains("PREWHERE slot >="));
        assert!(query.contains("ARRAY JOIN arrayZip"));
        assert!(query.contains("AS rewards_commission_bps"));
        assert!(query.contains("CAST(NULL, 'Nullable(UInt16)')"));
        assert!(query.contains("tupleElement(latest, 5) AS commission_bps"));
        assert!(query.contains("= 'staking'"));
        assert!(query.contains("max_threads=2"));
        assert!(query.contains("max_memory_usage=536870912"));
    }

    #[test]
    fn inflation_reward_partition_is_deterministic_and_bounded() {
        let seed = [9u8; 32];
        let first = inflation_reward_partition(&[1u8; 32], 293, &seed);
        let repeated = inflation_reward_partition(&[1u8; 32], 293, &seed);
        let second = inflation_reward_partition(&[2u8; 32], 293, &seed);

        assert_eq!(first, repeated);
        assert!(first < 293);
        assert!(second < 293);
        assert_ne!(first, second);
    }

    #[test]
    fn inflation_partition_plan_deduplicates_shared_partitions_and_targets_distinct_ones() {
        let seed = [9u8; 32];
        let num_partitions = 4;
        let boundary_block_height = 270_000_000;
        let candidates = (0..=u8::MAX).map(|value| [value; 32]).collect::<Vec<_>>();
        let mut addresses_by_partition = HashMap::<usize, Vec<[u8; 32]>>::new();
        for address in candidates {
            addresses_by_partition
                .entry(inflation_reward_partition(&address, num_partitions, &seed))
                .or_default()
                .push(address);
        }

        let (shared_partition, shared_addresses) = addresses_by_partition
            .iter()
            .find(|(_, addresses)| addresses.len() >= 2)
            .expect("test inputs should share a partition");
        let shared_plan = plan_inflation_reward_partitions(
            &shared_addresses[..2],
            num_partitions,
            &seed,
            boundary_block_height,
        )
        .expect("partition heights should not overflow");
        assert_eq!(
            shared_plan.required_block_heights,
            vec![boundary_block_height + *shared_partition as u64 + 1]
        );

        let mut distinct_partitions = addresses_by_partition.iter();
        let (first_partition, first_addresses) = distinct_partitions
            .next()
            .expect("at least two partitions should be represented");
        let (second_partition, second_addresses) = distinct_partitions
            .find(|(partition, _)| partition != &first_partition)
            .expect("at least two partitions should be represented");
        let distinct_addresses = [first_addresses[0], second_addresses[0]];
        let distinct_plan = plan_inflation_reward_partitions(
            &distinct_addresses,
            num_partitions,
            &seed,
            boundary_block_height,
        )
        .expect("partition heights should not overflow");
        let mut expected_heights = vec![
            boundary_block_height + *first_partition as u64 + 1,
            boundary_block_height + *second_partition as u64 + 1,
        ];
        expected_heights.sort_unstable();
        assert_eq!(distinct_plan.required_block_heights, expected_heights);
    }

    #[test]
    fn inflation_partition_plan_rejects_block_height_overflow() {
        assert!(
            plan_inflation_reward_partitions(&[[1u8; 32]], 293, &[9u8; 32], u64::MAX).is_none()
        );
    }

    #[test]
    fn inflation_reward_period_stays_active_until_completion_height() {
        assert!(inflation_rewards_period_is_active(1_292, 1_293));
        assert!(!inflation_rewards_period_is_active(1_293, 1_293));
        assert!(!inflation_rewards_period_is_active(1_294, 1_293));
    }

    #[test]
    fn block_metadata_query_omits_reward_columns_when_not_requested() {
        let query = build_block_metadata_query("default.blocks_metadata", 42, false, "", true);

        assert!(query.contains("executed_transaction_count"));
        assert!(!query.contains("rewards_present"));
        assert!(!query.contains("rewards_pubkey"));
    }

    #[test]
    fn memory_block_queries_use_where_instead_of_prewhere() {
        let metadata = build_block_metadata_query("cache.blocks_metadata", 42, true, "", false);
        let block_time = build_block_time_query("cache.blocks_metadata", 42, "", false);

        assert!(metadata.contains("WHERE slot = 42"));
        assert!(!metadata.contains("PREWHERE"));
        assert!(block_time.contains("WHERE slot = 42"));
        assert!(!block_time.contains("PREWHERE"));
    }

    #[test]
    #[cfg(any(feature = "disk-cache", feature = "grpc-streaming"))]
    fn block_metadata_range_query_includes_bucket_predicate_for_shard_pruning() {
        let query = build_block_metadata_range_query(
            "default.blocks_metadata",
            SLOT_SHARD_DIVISOR + 1,
            SLOT_SHARD_DIVISOR + 10,
            "",
        );

        assert!(query.contains("intDiv(slot, 432000) BETWEEN 1 AND 1"));
        assert!(query.contains("AND slot BETWEEN 432001 AND 432010"));
    }

    #[test]
    fn blockhash_valid_query_uses_slot_bounds_and_inclusive_height() {
        let query = build_blockhash_valid_query(
            "default.blocks_metadata",
            "toFixedString(unhex('AB'), 32)",
            744,
            1000,
            850,
            "",
        );

        assert!(query.contains("intDiv(slot, 432000) BETWEEN 0 AND 0"));
        assert!(query.contains("AND slot BETWEEN 744 AND 1000"));
        assert!(query.contains("blockhash = toFixedString(unhex('AB'), 32)"));
        assert!(query.contains("block_height >= 850"));
        assert!(query.contains("LIMIT 1"));
    }

    #[test]
    fn block_signatures_query_reads_only_signature_column() {
        let query = build_block_transactions_query(
            "default.transactions",
            42,
            BlockTransactionProjection::Signatures,
            "",
        );

        assert!(query.contains("SELECT\n                signature\n"));
        assert!(!query.contains("tx_account_keys"));
        assert!(!query.contains("meta_fee"));
    }

    #[test]
    fn block_signatures_query_orders_by_slot_index_before_signature_dedup() {
        let query = build_block_transactions_query(
            "default.transactions",
            42,
            BlockTransactionProjection::Signatures,
            "",
        );

        assert!(query.contains("ORDER BY slot_idx ASC, signature ASC"));
        assert!(query.contains("LIMIT 1 BY signature"));
    }

    #[test]
    fn transaction_count_query_reads_transaction_table_with_prewhere() {
        let query = build_transaction_count_query("default.transactions", 42, false, "");

        assert!(query.contains("count() AS transaction_count"));
        assert!(query.contains("FROM default.transactions"));
        assert!(query.contains("PREWHERE slot < 42"));
        assert!(!query.contains("executed_transaction_count"));
    }

    #[test]
    fn block_accounts_query_excludes_heavy_fields() {
        let query = build_block_transactions_query(
            "default.transactions",
            42,
            BlockTransactionProjection::Accounts,
            "",
        );

        assert!(query.contains("tx_account_keys"));
        assert!(query.contains("tx_instructions_program_id_index"));
        assert!(query.contains("meta_loaded_addresses_writable"));
        assert!(!query.contains("tx_config_priority_fee"));
        assert!(!query.contains("meta_log_messages"));
        assert!(!query.contains("meta_return_data_data"));
        assert!(!query.contains("meta_compute_units_consumed"));
        assert!(query.contains("meta_reward_pubkey"));
    }

    #[test]
    fn block_full_query_omits_dead_per_row_fields_and_optional_rewards() {
        let query = build_block_transactions_query(
            "default.transactions",
            42,
            BlockTransactionProjection::Full,
            "",
        );

        assert!(query.contains("tx_recent_blockhash"));
        assert!(query.contains("tx_config_priority_fee"));
        assert!(query.contains("tx_config_compute_unit_limit"));
        assert!(query.contains("tx_config_loaded_accounts_data_size_limit"));
        assert!(query.contains("tx_config_heap_size"));
        assert!(query.contains("meta_return_data_data"));
        assert!(!query.contains("\n                slot,"));
        assert!(!query.contains("\n                block_time,"));
        assert!(query.contains("meta_reward_pubkey"));
    }
}
