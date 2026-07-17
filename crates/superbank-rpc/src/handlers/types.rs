// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use serde::{Deserialize, Serialize};
use serde_json::Value;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::transaction::TransactionVersion;
use solana_transaction_status::{EncodedTransaction, UiTransactionStatusMeta};

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetSignaturesForAddressOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) limit: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) before: Option<String>,
    #[serde(rename = "beforeSlot")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) before_slot: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) until: Option<String>,
    #[serde(rename = "untilSlot")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) until_slot: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[allow(dead_code)]
    pub(crate) commitment: Option<String>,
    #[serde(rename = "minContextSlot")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) min_context_slot: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GetBlocksConfig {
    #[serde(flatten)]
    pub(crate) commitment: Option<CommitmentConfig>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GetBlockHeightConfig {
    #[serde(flatten)]
    pub(crate) commitment: Option<CommitmentConfig>,
    #[serde(rename = "minContextSlot")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) min_context_slot: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GetSlotConfig {
    #[serde(flatten)]
    pub(crate) commitment: Option<CommitmentConfig>,
    #[serde(rename = "minContextSlot")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) min_context_slot: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GetLatestBlockhashConfig {
    #[serde(flatten)]
    pub(crate) commitment: Option<CommitmentConfig>,
    #[serde(rename = "minContextSlot")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) min_context_slot: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GetInflationRewardConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) epoch: Option<u64>,
    #[serde(flatten)]
    pub(crate) commitment: Option<CommitmentConfig>,
    #[serde(rename = "minContextSlot")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) min_context_slot: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct ComparisonFilter<T> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) gte: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) gt: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) lte: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) lt: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) eq: Option<T>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct GetTransactionsForAddressFilters {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) slot: Option<ComparisonFilter<u64>>,
    #[serde(rename = "blockTime")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) block_time: Option<ComparisonFilter<i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) signature: Option<ComparisonFilter<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) status: Option<String>,
    #[serde(rename = "tokenAccounts")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) token_accounts: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct GetTransactionsForAddressOptions {
    #[serde(rename = "transactionDetails")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) transaction_details: Option<String>,
    #[serde(rename = "sortOrder")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) sort_order: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) limit: Option<u64>,
    #[serde(rename = "paginationToken")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pagination_token: Option<String>,
    #[serde(rename = "beforeSlot")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) before_slot: Option<u64>,
    #[serde(rename = "untilSlot")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) until_slot: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) commitment: Option<String>,
    #[serde(rename = "minContextSlot")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) min_context_slot: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) encoding: Option<String>,
    #[serde(rename = "maxSupportedTransactionVersion")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_supported_transaction_version: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) filters: Option<GetTransactionsForAddressFilters>,
}

pub(crate) const MAX_GET_BLOCKS_RANGE: u64 = 500_000;

#[derive(Debug, Deserialize)]
pub(crate) struct GetSignatureStatusesConfig {
    #[serde(rename = "searchTransactionHistory")]
    pub(crate) search_transaction_history: Option<bool>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SignatureInfo {
    pub(crate) signature: String,
    pub(crate) slot: u64,
    pub(crate) err: Option<Value>,
    pub(crate) memo: Option<String>,
    #[serde(rename = "blockTime")]
    pub(crate) block_time: Option<i64>,
    #[serde(rename = "confirmationStatus")]
    pub(crate) confirmation_status: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct TransactionsForAddressSignaturesInfo {
    pub(crate) signature: String,
    pub(crate) slot: u64,
    #[serde(rename = "transactionIndex")]
    pub(crate) transaction_index: u32,
    pub(crate) err: Option<Value>,
    pub(crate) memo: Option<String>,
    #[serde(rename = "blockTime")]
    pub(crate) block_time: Option<i64>,
    #[serde(rename = "confirmationStatus")]
    pub(crate) confirmation_status: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct TransactionsForAddressFullInfo {
    pub(crate) slot: u64,
    #[serde(rename = "transactionIndex")]
    pub(crate) transaction_index: u32,
    #[serde(rename = "blockTime")]
    pub(crate) block_time: Option<i64>,
    pub(crate) transaction: EncodedTransaction,
    pub(crate) meta: Option<UiTransactionStatusMeta>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) version: Option<TransactionVersion>,
}

#[derive(Debug, Serialize)]
pub(crate) struct TransactionsForAddressResult<T> {
    pub(crate) data: Vec<T>,
    #[serde(rename = "paginationToken")]
    pub(crate) pagination_token: Option<String>,
}

pub(crate) fn reject_unknown_fields(value: &Value, allowed: &[&str]) -> Result<(), String> {
    let Some(object) = value.as_object() else {
        return Ok(());
    };
    let mut invalid = Vec::new();
    for key in object.keys() {
        if !allowed.iter().any(|allowed_key| allowed_key == key) {
            invalid.push(key.clone());
        }
    }
    if invalid.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "Invalid params: unknown field(s) {}",
            invalid.join(", ")
        ))
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct SignatureStatusInfo {
    pub(crate) slot: u64,
    pub(crate) confirmations: Option<u64>,
    pub(crate) err: Option<Value>,
    pub(crate) status: Value,
    #[serde(rename = "confirmationStatus")]
    pub(crate) confirmation_status: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct RpcContextSlot {
    pub(crate) slot: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GetLatestBlockhashValue {
    pub(crate) blockhash: String,
    pub(crate) last_valid_block_height: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InflationRewardInfo {
    pub(crate) epoch: u64,
    pub(crate) effective_slot: u64,
    pub(crate) amount: u64,
    pub(crate) post_balance: u64,
    pub(crate) commission: Option<u8>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EpochSchedule {
    pub(crate) slots_per_epoch: u64,
    pub(crate) leader_schedule_slot_offset: u64,
    pub(crate) warmup: bool,
    pub(crate) first_normal_epoch: u64,
    pub(crate) first_normal_slot: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct GetLatestBlockhashResult {
    pub(crate) context: RpcContextSlot,
    pub(crate) value: GetLatestBlockhashValue,
}
#[derive(Debug, Serialize)]
pub(crate) struct IsBlockhashValidResult {
    pub(crate) context: RpcContextSlot,
    pub(crate) value: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct SignatureStatusesResult {
    pub(crate) context: RpcContextSlot,
    pub(crate) value: Vec<Option<SignatureStatusInfo>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransactionsForAddressDetails {
    Signatures,
    Full,
}
