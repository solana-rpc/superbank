// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::time::Instant;

use clickhouse::Client as HttpClient;
use serde::Deserialize;
use serde_big_array::Array;
use serde_bytes::ByteBuf;

use crate::processing::{ProcessingError, ProcessingResult};

use super::types::{
    BlockMetadataRecord, QueryTimings, StoredAccountsTransactionRecord, StoredTransactionRecord,
};

#[derive(Deserialize, clickhouse::Row)]
pub(crate) struct TransactionRow {
    pub(crate) signature: Array<u8, 64>,
    pub(crate) slot: u64,
    pub(crate) slot_idx: u32,
    pub(crate) block_time: Option<i64>,
    pub(crate) is_vote: bool,
    pub(crate) tx_version: Option<u8>,
    pub(crate) tx_signatures: Vec<Array<u8, 64>>,
    pub(crate) tx_num_required_signatures: u8,
    pub(crate) tx_num_readonly_signed_accounts: u8,
    pub(crate) tx_num_readonly_unsigned_accounts: u8,
    pub(crate) tx_account_keys: Vec<Array<u8, 32>>,
    pub(crate) tx_recent_blockhash: Array<u8, 32>,
    pub(crate) tx_instructions_program_id_index: Vec<u8>,
    pub(crate) tx_instructions_accounts: Vec<Vec<u8>>,
    pub(crate) tx_instructions_data: Vec<ByteBuf>,
    pub(crate) tx_address_table_lookups_present: bool,
    pub(crate) tx_address_table_lookup_account_key: Vec<Array<u8, 32>>,
    pub(crate) tx_address_table_lookup_writable_indexes: Vec<Vec<u8>>,
    pub(crate) tx_address_table_lookup_readonly_indexes: Vec<Vec<u8>>,
    pub(crate) meta_status_ok: bool,
    pub(crate) meta_err: Option<String>,
    pub(crate) meta_fee: u64,
    pub(crate) meta_pre_balances: Vec<u64>,
    pub(crate) meta_post_balances: Vec<u64>,
    pub(crate) meta_inner_instructions_present: bool,
    pub(crate) meta_inner_instructions_index: Vec<u8>,
    pub(crate) meta_inner_instructions_program_id_index: Vec<Vec<u8>>,
    pub(crate) meta_inner_instructions_accounts: Vec<Vec<Vec<u8>>>,
    pub(crate) meta_inner_instructions_data: Vec<Vec<ByteBuf>>,
    pub(crate) meta_inner_instructions_stack_height: Vec<Vec<Option<u32>>>,
    pub(crate) meta_log_messages_present: bool,
    pub(crate) meta_log_messages: Vec<String>,
    pub(crate) meta_pre_token_balances_present: bool,
    pub(crate) meta_pre_token_account_index: Vec<u8>,
    pub(crate) meta_pre_token_mint: Vec<Array<u8, 32>>,
    pub(crate) meta_pre_token_owner: Vec<Option<Array<u8, 32>>>,
    pub(crate) meta_pre_token_program_id: Vec<Option<Array<u8, 32>>>,
    pub(crate) meta_pre_token_amount: Vec<String>,
    pub(crate) meta_pre_token_decimals: Vec<u8>,
    pub(crate) meta_pre_token_ui_amount: Vec<Option<f64>>,
    pub(crate) meta_pre_token_ui_amount_string: Vec<String>,
    pub(crate) meta_post_token_balances_present: bool,
    pub(crate) meta_post_token_account_index: Vec<u8>,
    pub(crate) meta_post_token_mint: Vec<Array<u8, 32>>,
    pub(crate) meta_post_token_owner: Vec<Option<Array<u8, 32>>>,
    pub(crate) meta_post_token_program_id: Vec<Option<Array<u8, 32>>>,
    pub(crate) meta_post_token_amount: Vec<String>,
    pub(crate) meta_post_token_decimals: Vec<u8>,
    pub(crate) meta_post_token_ui_amount: Vec<Option<f64>>,
    pub(crate) meta_post_token_ui_amount_string: Vec<String>,
    pub(crate) meta_rewards_present: bool,
    pub(crate) meta_reward_pubkey: Vec<String>,
    pub(crate) meta_reward_lamports: Vec<i64>,
    pub(crate) meta_reward_post_balance: Vec<u64>,
    pub(crate) meta_reward_type: Vec<Option<String>>,
    pub(crate) meta_reward_commission: Vec<Option<u8>>,
    pub(crate) meta_loaded_addresses_writable: Vec<Array<u8, 32>>,
    pub(crate) meta_loaded_addresses_readonly: Vec<Array<u8, 32>>,
    pub(crate) meta_return_data_present: bool,
    pub(crate) meta_return_data_program_id: Option<Array<u8, 32>>,
    pub(crate) meta_return_data_data: Option<ByteBuf>,
    pub(crate) meta_compute_units_consumed: Option<u64>,
    pub(crate) meta_cost_units: Option<u64>,
}

#[derive(Deserialize, clickhouse::Row)]
pub(crate) struct BlockMetadataRow {
    pub(crate) slot: u64,
    pub(crate) parent_slot: u64,
    pub(crate) blockhash: Array<u8, 32>,
    pub(crate) parent_blockhash: Array<u8, 32>,
    pub(crate) block_time: Option<i64>,
    pub(crate) block_height: Option<u64>,
    pub(crate) executed_transaction_count: u64,
    pub(crate) entry_count: u64,
    pub(crate) rewards_present: bool,
    pub(crate) rewards_pubkey: Vec<Array<u8, 32>>,
    pub(crate) rewards_lamports: Vec<i64>,
    pub(crate) rewards_post_balance: Vec<u64>,
    pub(crate) rewards_type: Vec<Option<String>>,
    pub(crate) rewards_commission: Vec<Option<u8>>,
    pub(crate) rewards_num_partitions: Option<u64>,
}

#[derive(Deserialize, clickhouse::Row)]
pub(crate) struct BlockMetadataBaseRow {
    pub(crate) slot: u64,
    pub(crate) parent_slot: u64,
    pub(crate) blockhash: Array<u8, 32>,
    pub(crate) parent_blockhash: Array<u8, 32>,
    pub(crate) block_time: Option<i64>,
    pub(crate) block_height: Option<u64>,
    pub(crate) executed_transaction_count: u64,
    pub(crate) entry_count: u64,
    pub(crate) rewards_num_partitions: Option<u64>,
}

#[derive(Deserialize, clickhouse::Row)]
pub(crate) struct BlockSignatureRow {
    pub(crate) signature: Array<u8, 64>,
}

#[derive(Deserialize, clickhouse::Row)]
pub(crate) struct BlockAccountsTransactionRow {
    pub(crate) tx_version: Option<u8>,
    pub(crate) tx_signatures: Vec<Array<u8, 64>>,
    pub(crate) tx_num_required_signatures: u8,
    pub(crate) tx_num_readonly_signed_accounts: u8,
    pub(crate) tx_num_readonly_unsigned_accounts: u8,
    pub(crate) tx_account_keys: Vec<Array<u8, 32>>,
    pub(crate) tx_instructions_program_id_index: Vec<u8>,
    pub(crate) meta_status_ok: bool,
    pub(crate) meta_err: Option<String>,
    pub(crate) meta_fee: u64,
    pub(crate) meta_pre_balances: Vec<u64>,
    pub(crate) meta_post_balances: Vec<u64>,
    pub(crate) meta_pre_token_balances_present: bool,
    pub(crate) meta_pre_token_account_index: Vec<u8>,
    pub(crate) meta_pre_token_mint: Vec<Array<u8, 32>>,
    pub(crate) meta_pre_token_owner: Vec<Option<Array<u8, 32>>>,
    pub(crate) meta_pre_token_program_id: Vec<Option<Array<u8, 32>>>,
    pub(crate) meta_pre_token_amount: Vec<String>,
    pub(crate) meta_pre_token_decimals: Vec<u8>,
    pub(crate) meta_pre_token_ui_amount: Vec<Option<f64>>,
    pub(crate) meta_pre_token_ui_amount_string: Vec<String>,
    pub(crate) meta_post_token_balances_present: bool,
    pub(crate) meta_post_token_account_index: Vec<u8>,
    pub(crate) meta_post_token_mint: Vec<Array<u8, 32>>,
    pub(crate) meta_post_token_owner: Vec<Option<Array<u8, 32>>>,
    pub(crate) meta_post_token_program_id: Vec<Option<Array<u8, 32>>>,
    pub(crate) meta_post_token_amount: Vec<String>,
    pub(crate) meta_post_token_decimals: Vec<u8>,
    pub(crate) meta_post_token_ui_amount: Vec<Option<f64>>,
    pub(crate) meta_post_token_ui_amount_string: Vec<String>,
    pub(crate) meta_rewards_present: bool,
    pub(crate) meta_reward_pubkey: Vec<String>,
    pub(crate) meta_reward_lamports: Vec<i64>,
    pub(crate) meta_reward_post_balance: Vec<u64>,
    pub(crate) meta_reward_type: Vec<Option<String>>,
    pub(crate) meta_reward_commission: Vec<Option<u8>>,
    pub(crate) meta_loaded_addresses_writable: Vec<Array<u8, 32>>,
    pub(crate) meta_loaded_addresses_readonly: Vec<Array<u8, 32>>,
}

#[derive(Deserialize, clickhouse::Row)]
pub(crate) struct BlockFullTransactionRow {
    pub(crate) slot_idx: u32,
    pub(crate) is_vote: bool,
    pub(crate) tx_version: Option<u8>,
    pub(crate) tx_signatures: Vec<Array<u8, 64>>,
    pub(crate) tx_num_required_signatures: u8,
    pub(crate) tx_num_readonly_signed_accounts: u8,
    pub(crate) tx_num_readonly_unsigned_accounts: u8,
    pub(crate) tx_account_keys: Vec<Array<u8, 32>>,
    pub(crate) tx_recent_blockhash: Array<u8, 32>,
    pub(crate) tx_instructions_program_id_index: Vec<u8>,
    pub(crate) tx_instructions_accounts: Vec<Vec<u8>>,
    pub(crate) tx_instructions_data: Vec<ByteBuf>,
    pub(crate) tx_address_table_lookups_present: bool,
    pub(crate) tx_address_table_lookup_account_key: Vec<Array<u8, 32>>,
    pub(crate) tx_address_table_lookup_writable_indexes: Vec<Vec<u8>>,
    pub(crate) tx_address_table_lookup_readonly_indexes: Vec<Vec<u8>>,
    pub(crate) meta_status_ok: bool,
    pub(crate) meta_err: Option<String>,
    pub(crate) meta_fee: u64,
    pub(crate) meta_pre_balances: Vec<u64>,
    pub(crate) meta_post_balances: Vec<u64>,
    pub(crate) meta_inner_instructions_present: bool,
    pub(crate) meta_inner_instructions_index: Vec<u8>,
    pub(crate) meta_inner_instructions_program_id_index: Vec<Vec<u8>>,
    pub(crate) meta_inner_instructions_accounts: Vec<Vec<Vec<u8>>>,
    pub(crate) meta_inner_instructions_data: Vec<Vec<ByteBuf>>,
    pub(crate) meta_inner_instructions_stack_height: Vec<Vec<Option<u32>>>,
    pub(crate) meta_log_messages_present: bool,
    pub(crate) meta_log_messages: Vec<String>,
    pub(crate) meta_pre_token_balances_present: bool,
    pub(crate) meta_pre_token_account_index: Vec<u8>,
    pub(crate) meta_pre_token_mint: Vec<Array<u8, 32>>,
    pub(crate) meta_pre_token_owner: Vec<Option<Array<u8, 32>>>,
    pub(crate) meta_pre_token_program_id: Vec<Option<Array<u8, 32>>>,
    pub(crate) meta_pre_token_amount: Vec<String>,
    pub(crate) meta_pre_token_decimals: Vec<u8>,
    pub(crate) meta_pre_token_ui_amount: Vec<Option<f64>>,
    pub(crate) meta_pre_token_ui_amount_string: Vec<String>,
    pub(crate) meta_post_token_balances_present: bool,
    pub(crate) meta_post_token_account_index: Vec<u8>,
    pub(crate) meta_post_token_mint: Vec<Array<u8, 32>>,
    pub(crate) meta_post_token_owner: Vec<Option<Array<u8, 32>>>,
    pub(crate) meta_post_token_program_id: Vec<Option<Array<u8, 32>>>,
    pub(crate) meta_post_token_amount: Vec<String>,
    pub(crate) meta_post_token_decimals: Vec<u8>,
    pub(crate) meta_post_token_ui_amount: Vec<Option<f64>>,
    pub(crate) meta_post_token_ui_amount_string: Vec<String>,
    pub(crate) meta_rewards_present: bool,
    pub(crate) meta_reward_pubkey: Vec<String>,
    pub(crate) meta_reward_lamports: Vec<i64>,
    pub(crate) meta_reward_post_balance: Vec<u64>,
    pub(crate) meta_reward_type: Vec<Option<String>>,
    pub(crate) meta_reward_commission: Vec<Option<u8>>,
    pub(crate) meta_loaded_addresses_writable: Vec<Array<u8, 32>>,
    pub(crate) meta_loaded_addresses_readonly: Vec<Array<u8, 32>>,
    pub(crate) meta_return_data_present: bool,
    pub(crate) meta_return_data_program_id: Option<Array<u8, 32>>,
    pub(crate) meta_return_data_data: Option<ByteBuf>,
    pub(crate) meta_compute_units_consumed: Option<u64>,
    pub(crate) meta_cost_units: Option<u64>,
}

#[derive(Deserialize, clickhouse::Row)]
pub(crate) struct BlockhashHeightRow {
    pub(crate) blockhash: Array<u8, 32>,
    pub(crate) block_height: Option<u64>,
}

pub(crate) fn map_transaction_row(row: TransactionRow) -> StoredTransactionRecord {
    StoredTransactionRecord {
        signature: row.signature.0,
        slot: row.slot,
        slot_idx: row.slot_idx,
        block_time: row.block_time,
        is_vote: row.is_vote,
        tx_version: row.tx_version,
        tx_signatures: row.tx_signatures.into_iter().map(|sig| sig.0).collect(),
        tx_num_required_signatures: row.tx_num_required_signatures,
        tx_num_readonly_signed_accounts: row.tx_num_readonly_signed_accounts,
        tx_num_readonly_unsigned_accounts: row.tx_num_readonly_unsigned_accounts,
        tx_account_keys: row.tx_account_keys.into_iter().map(|key| key.0).collect(),
        tx_recent_blockhash: row.tx_recent_blockhash.0,
        tx_instructions_program_id_index: row.tx_instructions_program_id_index,
        tx_instructions_accounts: row.tx_instructions_accounts,
        tx_instructions_data: row
            .tx_instructions_data
            .into_iter()
            .map(|data| data.into_vec())
            .collect(),
        tx_address_table_lookups_present: row.tx_address_table_lookups_present,
        tx_address_table_lookup_account_key: row
            .tx_address_table_lookup_account_key
            .into_iter()
            .map(|key| key.0)
            .collect(),
        tx_address_table_lookup_writable_indexes: row.tx_address_table_lookup_writable_indexes,
        tx_address_table_lookup_readonly_indexes: row.tx_address_table_lookup_readonly_indexes,
        meta_status_ok: row.meta_status_ok,
        meta_err: row.meta_err,
        meta_fee: row.meta_fee,
        meta_pre_balances: row.meta_pre_balances,
        meta_post_balances: row.meta_post_balances,
        meta_inner_instructions_present: row.meta_inner_instructions_present,
        meta_inner_instructions_index: row.meta_inner_instructions_index,
        meta_inner_instructions_program_id_index: row.meta_inner_instructions_program_id_index,
        meta_inner_instructions_accounts: row.meta_inner_instructions_accounts,
        meta_inner_instructions_data: row
            .meta_inner_instructions_data
            .into_iter()
            .map(|group| group.into_iter().map(|data| data.into_vec()).collect())
            .collect(),
        meta_inner_instructions_stack_height: row.meta_inner_instructions_stack_height,
        meta_log_messages_present: row.meta_log_messages_present,
        meta_log_messages: row.meta_log_messages,
        meta_pre_token_balances_present: row.meta_pre_token_balances_present,
        meta_pre_token_account_index: row.meta_pre_token_account_index,
        meta_pre_token_mint: row
            .meta_pre_token_mint
            .into_iter()
            .map(|mint| mint.0)
            .collect(),
        meta_pre_token_owner: row
            .meta_pre_token_owner
            .into_iter()
            .map(|owner| owner.map(|value| value.0))
            .collect(),
        meta_pre_token_program_id: row
            .meta_pre_token_program_id
            .into_iter()
            .map(|program_id| program_id.map(|value| value.0))
            .collect(),
        meta_pre_token_amount: row.meta_pre_token_amount,
        meta_pre_token_decimals: row.meta_pre_token_decimals,
        meta_pre_token_ui_amount: row.meta_pre_token_ui_amount,
        meta_pre_token_ui_amount_string: row.meta_pre_token_ui_amount_string,
        meta_post_token_balances_present: row.meta_post_token_balances_present,
        meta_post_token_account_index: row.meta_post_token_account_index,
        meta_post_token_mint: row
            .meta_post_token_mint
            .into_iter()
            .map(|mint| mint.0)
            .collect(),
        meta_post_token_owner: row
            .meta_post_token_owner
            .into_iter()
            .map(|owner| owner.map(|value| value.0))
            .collect(),
        meta_post_token_program_id: row
            .meta_post_token_program_id
            .into_iter()
            .map(|program_id| program_id.map(|value| value.0))
            .collect(),
        meta_post_token_amount: row.meta_post_token_amount,
        meta_post_token_decimals: row.meta_post_token_decimals,
        meta_post_token_ui_amount: row.meta_post_token_ui_amount,
        meta_post_token_ui_amount_string: row.meta_post_token_ui_amount_string,
        meta_rewards_present: row.meta_rewards_present,
        meta_reward_pubkey: row.meta_reward_pubkey,
        meta_reward_lamports: row.meta_reward_lamports,
        meta_reward_post_balance: row.meta_reward_post_balance,
        meta_reward_type: row.meta_reward_type,
        meta_reward_commission: row.meta_reward_commission,
        meta_loaded_addresses_writable: row
            .meta_loaded_addresses_writable
            .into_iter()
            .map(|addr| addr.0)
            .collect(),
        meta_loaded_addresses_readonly: row
            .meta_loaded_addresses_readonly
            .into_iter()
            .map(|addr| addr.0)
            .collect(),
        meta_return_data_present: row.meta_return_data_present,
        meta_return_data_program_id: row.meta_return_data_program_id.map(|value| value.0),
        meta_return_data_data: row.meta_return_data_data.map(|data| data.into_vec()),
        meta_compute_units_consumed: row.meta_compute_units_consumed,
        meta_cost_units: row.meta_cost_units,
    }
}

pub(crate) fn map_block_metadata_base_row(row: BlockMetadataBaseRow) -> BlockMetadataRecord {
    BlockMetadataRecord {
        slot: row.slot,
        parent_slot: row.parent_slot,
        blockhash: row.blockhash.0,
        parent_blockhash: row.parent_blockhash.0,
        block_time: row.block_time,
        block_height: row.block_height,
        executed_transaction_count: row.executed_transaction_count,
        entry_count: row.entry_count,
        rewards_present: false,
        rewards_pubkey: Vec::new(),
        rewards_lamports: Vec::new(),
        rewards_post_balance: Vec::new(),
        rewards_type: Vec::new(),
        rewards_commission: Vec::new(),
        rewards_num_partitions: row.rewards_num_partitions,
    }
}

pub(crate) fn map_block_signature_row(row: BlockSignatureRow) -> String {
    bs58::encode(row.signature.0).into_string()
}

pub(crate) fn map_block_accounts_transaction_row(
    row: BlockAccountsTransactionRow,
) -> StoredAccountsTransactionRecord {
    StoredAccountsTransactionRecord {
        tx_version: row.tx_version,
        tx_signatures: row.tx_signatures.into_iter().map(|sig| sig.0).collect(),
        tx_num_required_signatures: row.tx_num_required_signatures,
        tx_num_readonly_signed_accounts: row.tx_num_readonly_signed_accounts,
        tx_num_readonly_unsigned_accounts: row.tx_num_readonly_unsigned_accounts,
        tx_account_keys: row.tx_account_keys.into_iter().map(|key| key.0).collect(),
        tx_instructions_program_id_index: row.tx_instructions_program_id_index,
        meta_status_ok: row.meta_status_ok,
        meta_err: row.meta_err,
        meta_fee: row.meta_fee,
        meta_pre_balances: row.meta_pre_balances,
        meta_post_balances: row.meta_post_balances,
        meta_pre_token_balances_present: row.meta_pre_token_balances_present,
        meta_pre_token_account_index: row.meta_pre_token_account_index,
        meta_pre_token_mint: row
            .meta_pre_token_mint
            .into_iter()
            .map(|mint| mint.0)
            .collect(),
        meta_pre_token_owner: row
            .meta_pre_token_owner
            .into_iter()
            .map(|owner| owner.map(|value| value.0))
            .collect(),
        meta_pre_token_program_id: row
            .meta_pre_token_program_id
            .into_iter()
            .map(|program_id| program_id.map(|value| value.0))
            .collect(),
        meta_pre_token_amount: row.meta_pre_token_amount,
        meta_pre_token_decimals: row.meta_pre_token_decimals,
        meta_pre_token_ui_amount: row.meta_pre_token_ui_amount,
        meta_pre_token_ui_amount_string: row.meta_pre_token_ui_amount_string,
        meta_post_token_balances_present: row.meta_post_token_balances_present,
        meta_post_token_account_index: row.meta_post_token_account_index,
        meta_post_token_mint: row
            .meta_post_token_mint
            .into_iter()
            .map(|mint| mint.0)
            .collect(),
        meta_post_token_owner: row
            .meta_post_token_owner
            .into_iter()
            .map(|owner| owner.map(|value| value.0))
            .collect(),
        meta_post_token_program_id: row
            .meta_post_token_program_id
            .into_iter()
            .map(|program_id| program_id.map(|value| value.0))
            .collect(),
        meta_post_token_amount: row.meta_post_token_amount,
        meta_post_token_decimals: row.meta_post_token_decimals,
        meta_post_token_ui_amount: row.meta_post_token_ui_amount,
        meta_post_token_ui_amount_string: row.meta_post_token_ui_amount_string,
        meta_rewards_present: row.meta_rewards_present,
        meta_reward_pubkey: row.meta_reward_pubkey,
        meta_reward_lamports: row.meta_reward_lamports,
        meta_reward_post_balance: row.meta_reward_post_balance,
        meta_reward_type: row.meta_reward_type,
        meta_reward_commission: row.meta_reward_commission,
        meta_loaded_addresses_writable: row
            .meta_loaded_addresses_writable
            .into_iter()
            .map(|addr| addr.0)
            .collect(),
        meta_loaded_addresses_readonly: row
            .meta_loaded_addresses_readonly
            .into_iter()
            .map(|addr| addr.0)
            .collect(),
    }
}

pub(crate) fn map_block_full_transaction_row(
    row: BlockFullTransactionRow,
    slot: u64,
) -> ProcessingResult<StoredTransactionRecord> {
    let tx_signatures = row
        .tx_signatures
        .into_iter()
        .map(|sig| sig.0)
        .collect::<Vec<_>>();
    let signature = primary_block_signature(&tx_signatures, slot)?;

    Ok(StoredTransactionRecord {
        signature,
        slot,
        slot_idx: row.slot_idx,
        block_time: None,
        is_vote: row.is_vote,
        tx_version: row.tx_version,
        tx_signatures,
        tx_num_required_signatures: row.tx_num_required_signatures,
        tx_num_readonly_signed_accounts: row.tx_num_readonly_signed_accounts,
        tx_num_readonly_unsigned_accounts: row.tx_num_readonly_unsigned_accounts,
        tx_account_keys: row.tx_account_keys.into_iter().map(|key| key.0).collect(),
        tx_recent_blockhash: row.tx_recent_blockhash.0,
        tx_instructions_program_id_index: row.tx_instructions_program_id_index,
        tx_instructions_accounts: row.tx_instructions_accounts,
        tx_instructions_data: row
            .tx_instructions_data
            .into_iter()
            .map(|data| data.into_vec())
            .collect(),
        tx_address_table_lookups_present: row.tx_address_table_lookups_present,
        tx_address_table_lookup_account_key: row
            .tx_address_table_lookup_account_key
            .into_iter()
            .map(|key| key.0)
            .collect(),
        tx_address_table_lookup_writable_indexes: row.tx_address_table_lookup_writable_indexes,
        tx_address_table_lookup_readonly_indexes: row.tx_address_table_lookup_readonly_indexes,
        meta_status_ok: row.meta_status_ok,
        meta_err: row.meta_err,
        meta_fee: row.meta_fee,
        meta_pre_balances: row.meta_pre_balances,
        meta_post_balances: row.meta_post_balances,
        meta_inner_instructions_present: row.meta_inner_instructions_present,
        meta_inner_instructions_index: row.meta_inner_instructions_index,
        meta_inner_instructions_program_id_index: row.meta_inner_instructions_program_id_index,
        meta_inner_instructions_accounts: row.meta_inner_instructions_accounts,
        meta_inner_instructions_data: row
            .meta_inner_instructions_data
            .into_iter()
            .map(|group| group.into_iter().map(|data| data.into_vec()).collect())
            .collect(),
        meta_inner_instructions_stack_height: row.meta_inner_instructions_stack_height,
        meta_log_messages_present: row.meta_log_messages_present,
        meta_log_messages: row.meta_log_messages,
        meta_pre_token_balances_present: row.meta_pre_token_balances_present,
        meta_pre_token_account_index: row.meta_pre_token_account_index,
        meta_pre_token_mint: row
            .meta_pre_token_mint
            .into_iter()
            .map(|mint| mint.0)
            .collect(),
        meta_pre_token_owner: row
            .meta_pre_token_owner
            .into_iter()
            .map(|owner| owner.map(|value| value.0))
            .collect(),
        meta_pre_token_program_id: row
            .meta_pre_token_program_id
            .into_iter()
            .map(|program_id| program_id.map(|value| value.0))
            .collect(),
        meta_pre_token_amount: row.meta_pre_token_amount,
        meta_pre_token_decimals: row.meta_pre_token_decimals,
        meta_pre_token_ui_amount: row.meta_pre_token_ui_amount,
        meta_pre_token_ui_amount_string: row.meta_pre_token_ui_amount_string,
        meta_post_token_balances_present: row.meta_post_token_balances_present,
        meta_post_token_account_index: row.meta_post_token_account_index,
        meta_post_token_mint: row
            .meta_post_token_mint
            .into_iter()
            .map(|mint| mint.0)
            .collect(),
        meta_post_token_owner: row
            .meta_post_token_owner
            .into_iter()
            .map(|owner| owner.map(|value| value.0))
            .collect(),
        meta_post_token_program_id: row
            .meta_post_token_program_id
            .into_iter()
            .map(|program_id| program_id.map(|value| value.0))
            .collect(),
        meta_post_token_amount: row.meta_post_token_amount,
        meta_post_token_decimals: row.meta_post_token_decimals,
        meta_post_token_ui_amount: row.meta_post_token_ui_amount,
        meta_post_token_ui_amount_string: row.meta_post_token_ui_amount_string,
        meta_rewards_present: row.meta_rewards_present,
        meta_reward_pubkey: row.meta_reward_pubkey,
        meta_reward_lamports: row.meta_reward_lamports,
        meta_reward_post_balance: row.meta_reward_post_balance,
        meta_reward_type: row.meta_reward_type,
        meta_reward_commission: row.meta_reward_commission,
        meta_loaded_addresses_writable: row
            .meta_loaded_addresses_writable
            .into_iter()
            .map(|addr| addr.0)
            .collect(),
        meta_loaded_addresses_readonly: row
            .meta_loaded_addresses_readonly
            .into_iter()
            .map(|addr| addr.0)
            .collect(),
        meta_return_data_present: row.meta_return_data_present,
        meta_return_data_program_id: row.meta_return_data_program_id.map(|value| value.0),
        meta_return_data_data: row.meta_return_data_data.map(|data| data.into_vec()),
        meta_compute_units_consumed: row.meta_compute_units_consumed,
        meta_cost_units: row.meta_cost_units,
    })
}

fn primary_block_signature(tx_signatures: &[[u8; 64]], slot: u64) -> ProcessingResult<[u8; 64]> {
    tx_signatures.first().copied().ok_or_else(|| {
        ProcessingError::database_msg(format!(
            "invalid full block transaction row for slot {slot}: transaction is missing primary signature"
        ))
    })
}

pub(crate) async fn fetch_single_transaction_row(
    client: &HttpClient,
    query: &str,
) -> ProcessingResult<(Option<TransactionRow>, QueryTimings)> {
    let start = Instant::now();
    let mut cursor = client
        .query(query)
        .fetch::<TransactionRow>()
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

    Ok((row_opt, timings))
}

pub(crate) async fn fetch_blockhash_height_row(
    client: &HttpClient,
    query: &str,
) -> ProcessingResult<(Option<BlockhashHeightRow>, QueryTimings)> {
    let start = Instant::now();
    let mut cursor = client
        .query(query)
        .fetch::<BlockhashHeightRow>()
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

    Ok((row_opt, timings))
}

pub(crate) fn map_block_metadata_row(row: BlockMetadataRow) -> BlockMetadataRecord {
    BlockMetadataRecord {
        slot: row.slot,
        parent_slot: row.parent_slot,
        blockhash: row.blockhash.0,
        parent_blockhash: row.parent_blockhash.0,
        block_time: row.block_time,
        block_height: row.block_height,
        executed_transaction_count: row.executed_transaction_count,
        entry_count: row.entry_count,
        rewards_present: row.rewards_present,
        rewards_pubkey: row.rewards_pubkey.into_iter().map(|key| key.0).collect(),
        rewards_lamports: row.rewards_lamports,
        rewards_post_balance: row.rewards_post_balance,
        rewards_type: row.rewards_type,
        rewards_commission: row.rewards_commission,
        rewards_num_partitions: row.rewards_num_partitions,
    }
}

#[cfg(test)]
mod tests {
    use super::primary_block_signature;
    use crate::processing::ProcessingError;

    #[test]
    fn primary_block_signature_rejects_empty_signature_list() {
        let err = primary_block_signature(&[], 42).expect_err("expected error");

        match err {
            ProcessingError::Database { context, source } => {
                assert!(context.contains("slot 42"));
                assert!(context.contains("missing primary signature"));
                assert!(source.is_none());
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn primary_block_signature_returns_first_signature() {
        let signature = [9u8; 64];
        let resolved =
            primary_block_signature(&[signature, [8u8; 64]], 42).expect("primary signature");

        assert_eq!(resolved, signature);
    }
}
