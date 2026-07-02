// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

#[derive(Debug, Clone)]
pub struct QueryTimings {
    pub elapsed_ms: u64,
    pub received_bytes: u64,
    pub decoded_bytes: u64,
    pub rows_read: Option<u64>,
    pub rows_read_unknown: bool,
    pub rows_returned: u64,
}

impl QueryTimings {
    fn merge_rows_read(
        lhs_rows_read: Option<u64>,
        lhs_rows_read_unknown: bool,
        rhs_rows_read: Option<u64>,
        rhs_rows_read_unknown: bool,
    ) -> (Option<u64>, bool) {
        let known_rows_read = lhs_rows_read
            .unwrap_or(0)
            .saturating_add(rhs_rows_read.unwrap_or(0));
        let rows_read_unknown = lhs_rows_read_unknown
            || rhs_rows_read_unknown
            || lhs_rows_read.is_none()
            || rhs_rows_read.is_none();
        let rows_read = if rows_read_unknown && known_rows_read == 0 {
            None
        } else {
            Some(known_rows_read)
        };

        (rows_read, rows_read_unknown)
    }

    pub fn add(&mut self, other: QueryTimings) {
        self.elapsed_ms = self.elapsed_ms.saturating_add(other.elapsed_ms);
        self.received_bytes = self.received_bytes.saturating_add(other.received_bytes);
        self.decoded_bytes = self.decoded_bytes.saturating_add(other.decoded_bytes);
        (self.rows_read, self.rows_read_unknown) = Self::merge_rows_read(
            self.rows_read,
            self.rows_read_unknown,
            other.rows_read,
            other.rows_read_unknown,
        );
        self.rows_returned = self.rows_returned.saturating_add(other.rows_returned);
    }

    /// Combine per-shard timings for concurrent fanout queries.
    ///
    /// For parallel execution, wall-clock latency is the *max* shard latency, while
    /// byte counts are additive across shards.
    pub fn merge_parallel(&mut self, other: QueryTimings) {
        self.elapsed_ms = self.elapsed_ms.max(other.elapsed_ms);
        self.received_bytes = self.received_bytes.saturating_add(other.received_bytes);
        self.decoded_bytes = self.decoded_bytes.saturating_add(other.decoded_bytes);
        (self.rows_read, self.rows_read_unknown) = Self::merge_rows_read(
            self.rows_read,
            self.rows_read_unknown,
            other.rows_read,
            other.rows_read_unknown,
        );
        self.rows_returned = self.rows_returned.saturating_add(other.rows_returned);
    }

    pub fn zero() -> Self {
        Self {
            elapsed_ms: 0,
            received_bytes: 0,
            decoded_bytes: 0,
            rows_read: Some(0),
            rows_read_unknown: false,
            rows_returned: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::QueryTimings;

    #[test]
    fn add_preserves_known_rows_read_when_other_is_unknown() {
        let mut lhs = QueryTimings {
            elapsed_ms: 5,
            received_bytes: 10,
            decoded_bytes: 20,
            rows_read: Some(100),
            rows_read_unknown: false,
            rows_returned: 2,
        };
        let rhs = QueryTimings {
            elapsed_ms: 6,
            received_bytes: 11,
            decoded_bytes: 22,
            rows_read: Some(0),
            rows_read_unknown: true,
            rows_returned: 3,
        };

        lhs.add(rhs);

        assert_eq!(lhs.rows_read, Some(100));
        assert!(lhs.rows_read_unknown);
        assert_eq!(lhs.rows_returned, 5);
    }

    #[test]
    fn merge_parallel_preserves_known_rows_read_when_other_is_unknown() {
        let mut lhs = QueryTimings {
            elapsed_ms: 5,
            received_bytes: 10,
            decoded_bytes: 20,
            rows_read: Some(100),
            rows_read_unknown: false,
            rows_returned: 2,
        };
        let rhs = QueryTimings {
            elapsed_ms: 6,
            received_bytes: 11,
            decoded_bytes: 22,
            rows_read: Some(0),
            rows_read_unknown: true,
            rows_returned: 3,
        };

        lhs.merge_parallel(rhs);

        assert_eq!(lhs.rows_read, Some(100));
        assert!(lhs.rows_read_unknown);
        assert_eq!(lhs.rows_returned, 5);
        assert_eq!(lhs.elapsed_ms, 6);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenAccountsFilter {
    None,
    BalanceChanged,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatusFilter {
    Any,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Default)]
pub struct NumericFilter<T> {
    pub gte: Option<T>,
    pub gt: Option<T>,
    pub lte: Option<T>,
    pub lt: Option<T>,
    pub eq: Option<T>,
}

#[derive(Debug, Clone, Default)]
pub struct SignatureFilter {
    pub gte: Option<String>,
    pub gt: Option<String>,
    pub lte: Option<String>,
    pub lt: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ResolvedSignatureFilter {
    pub gte: Option<SignatureSlot>,
    pub gt: Option<SignatureSlot>,
    pub lte: Option<SignatureSlot>,
    pub lt: Option<SignatureSlot>,
}

#[derive(Debug, Clone)]
pub enum PaginationToken {
    Signature(String),
    SlotIndex { slot: u64, idx: u32 },
}

#[derive(Debug, Clone)]
pub struct TransactionsForAddressQuery {
    pub address: String,
    pub limit: u64,
    pub sort_order: SortOrder,
    pub pagination: Option<PaginationToken>,
    pub resolved_pagination: Option<SignatureSlot>,
    pub slot_filter: Option<NumericFilter<u64>>,
    pub block_time_filter: Option<NumericFilter<i64>>,
    pub signature_filter: Option<SignatureFilter>,
    pub resolved_signature_filter: Option<ResolvedSignatureFilter>,
    pub status: TransactionStatusFilter,
    pub token_accounts: TokenAccountsFilter,
}

#[derive(Debug)]
pub struct SignatureRecord {
    pub signature: String,
    pub slot: u64,
    // Used for stable ordering/merging in feature-gated head-cache paths.
    #[cfg_attr(not(feature = "grpc-head-cache"), allow(dead_code))]
    pub slot_idx: u32,
    pub err: Option<serde_json::Value>,
    pub memo: Option<String>,
    pub block_time: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SignatureSlot {
    pub(crate) slot: u64,
    pub(crate) slot_idx: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SlotBoundary {
    Position(SignatureSlot),
    Slot(u64),
}

#[derive(Debug)]
pub struct TransactionsForAddressRecord {
    pub signature: String,
    pub slot: u64,
    pub slot_idx: u32,
    pub err: Option<serde_json::Value>,
    pub memo: Option<String>,
    pub block_time: Option<i64>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "disk-cache", derive(serde::Serialize, serde::Deserialize))]
pub struct BlockMetadataRecord {
    pub slot: u64,
    pub parent_slot: u64,
    pub blockhash: [u8; 32],
    pub parent_blockhash: [u8; 32],
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub executed_transaction_count: u64,
    pub entry_count: u64,
    pub rewards_present: bool,
    pub rewards_pubkey: Vec<[u8; 32]>,
    pub rewards_lamports: Vec<i64>,
    pub rewards_post_balance: Vec<u64>,
    pub rewards_type: Vec<Option<String>>,
    pub rewards_commission: Vec<Option<u8>>,
    pub rewards_num_partitions: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct InflationRewardRecord {
    pub pubkey: [u8; 32],
    pub effective_slot: u64,
    pub lamports: i64,
    pub post_balance: u64,
    pub commission: Option<u8>,
}

#[derive(Debug, Clone)]
pub struct StoredBlockRecord {
    pub metadata: BlockMetadataRecord,
    pub transactions: Vec<StoredTransactionRecord>,
}

#[derive(Debug, Clone)]
pub struct StoredAccountsTransactionRecord {
    pub tx_version: Option<u8>,
    pub tx_signatures: Vec<[u8; 64]>,
    pub tx_num_required_signatures: u8,
    pub tx_num_readonly_signed_accounts: u8,
    pub tx_num_readonly_unsigned_accounts: u8,
    pub tx_account_keys: Vec<[u8; 32]>,
    pub tx_instructions_program_id_index: Vec<u8>,
    pub meta_status_ok: bool,
    pub meta_err: Option<String>,
    pub meta_fee: u64,
    pub meta_pre_balances: Vec<u64>,
    pub meta_post_balances: Vec<u64>,
    pub meta_pre_token_balances_present: bool,
    pub meta_pre_token_account_index: Vec<u8>,
    pub meta_pre_token_mint: Vec<[u8; 32]>,
    pub meta_pre_token_owner: Vec<Option<[u8; 32]>>,
    pub meta_pre_token_program_id: Vec<Option<[u8; 32]>>,
    pub meta_pre_token_amount: Vec<String>,
    pub meta_pre_token_decimals: Vec<u8>,
    pub meta_pre_token_ui_amount: Vec<Option<f64>>,
    pub meta_pre_token_ui_amount_string: Vec<String>,
    pub meta_post_token_balances_present: bool,
    pub meta_post_token_account_index: Vec<u8>,
    pub meta_post_token_mint: Vec<[u8; 32]>,
    pub meta_post_token_owner: Vec<Option<[u8; 32]>>,
    pub meta_post_token_program_id: Vec<Option<[u8; 32]>>,
    pub meta_post_token_amount: Vec<String>,
    pub meta_post_token_decimals: Vec<u8>,
    pub meta_post_token_ui_amount: Vec<Option<f64>>,
    pub meta_post_token_ui_amount_string: Vec<String>,
    pub meta_rewards_present: bool,
    pub meta_reward_pubkey: Vec<String>,
    pub meta_reward_lamports: Vec<i64>,
    pub meta_reward_post_balance: Vec<u64>,
    pub meta_reward_type: Vec<Option<String>>,
    pub meta_reward_commission: Vec<Option<u8>>,
    pub meta_loaded_addresses_writable: Vec<[u8; 32]>,
    pub meta_loaded_addresses_readonly: Vec<[u8; 32]>,
}

impl From<StoredTransactionRecord> for StoredAccountsTransactionRecord {
    fn from(record: StoredTransactionRecord) -> Self {
        Self {
            tx_version: record.tx_version,
            tx_signatures: record.tx_signatures,
            tx_num_required_signatures: record.tx_num_required_signatures,
            tx_num_readonly_signed_accounts: record.tx_num_readonly_signed_accounts,
            tx_num_readonly_unsigned_accounts: record.tx_num_readonly_unsigned_accounts,
            tx_account_keys: record.tx_account_keys,
            tx_instructions_program_id_index: record.tx_instructions_program_id_index,
            meta_status_ok: record.meta_status_ok,
            meta_err: record.meta_err,
            meta_fee: record.meta_fee,
            meta_pre_balances: record.meta_pre_balances,
            meta_post_balances: record.meta_post_balances,
            meta_pre_token_balances_present: record.meta_pre_token_balances_present,
            meta_pre_token_account_index: record.meta_pre_token_account_index,
            meta_pre_token_mint: record.meta_pre_token_mint,
            meta_pre_token_owner: record.meta_pre_token_owner,
            meta_pre_token_program_id: record.meta_pre_token_program_id,
            meta_pre_token_amount: record.meta_pre_token_amount,
            meta_pre_token_decimals: record.meta_pre_token_decimals,
            meta_pre_token_ui_amount: record.meta_pre_token_ui_amount,
            meta_pre_token_ui_amount_string: record.meta_pre_token_ui_amount_string,
            meta_post_token_balances_present: record.meta_post_token_balances_present,
            meta_post_token_account_index: record.meta_post_token_account_index,
            meta_post_token_mint: record.meta_post_token_mint,
            meta_post_token_owner: record.meta_post_token_owner,
            meta_post_token_program_id: record.meta_post_token_program_id,
            meta_post_token_amount: record.meta_post_token_amount,
            meta_post_token_decimals: record.meta_post_token_decimals,
            meta_post_token_ui_amount: record.meta_post_token_ui_amount,
            meta_post_token_ui_amount_string: record.meta_post_token_ui_amount_string,
            meta_rewards_present: record.meta_rewards_present,
            meta_reward_pubkey: record.meta_reward_pubkey,
            meta_reward_lamports: record.meta_reward_lamports,
            meta_reward_post_balance: record.meta_reward_post_balance,
            meta_reward_type: record.meta_reward_type,
            meta_reward_commission: record.meta_reward_commission,
            meta_loaded_addresses_writable: record.meta_loaded_addresses_writable,
            meta_loaded_addresses_readonly: record.meta_loaded_addresses_readonly,
        }
    }
}

#[derive(Debug, Clone)]
pub enum StoredBlockPayload {
    Metadata(BlockMetadataRecord),
    Signatures {
        metadata: BlockMetadataRecord,
        signatures: Vec<String>,
    },
    Accounts {
        metadata: BlockMetadataRecord,
        transactions: Vec<StoredAccountsTransactionRecord>,
    },
    Full(StoredBlockRecord),
}

impl StoredBlockPayload {
    pub fn metadata(&self) -> &BlockMetadataRecord {
        match self {
            Self::Metadata(metadata) => metadata,
            Self::Signatures { metadata, .. } => metadata,
            Self::Accounts { metadata, .. } => metadata,
            Self::Full(record) => &record.metadata,
        }
    }

    pub fn observed_transaction_count(&self) -> Option<usize> {
        match self {
            Self::Metadata(_) => None,
            Self::Signatures { signatures, .. } => Some(signatures.len()),
            Self::Accounts { transactions, .. } => Some(transactions.len()),
            Self::Full(record) => Some(record.transactions.len()),
        }
    }
}

#[derive(Debug)]
pub struct SignatureStatusRecord {
    pub signature: String,
    pub slot: u64,
    pub err: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "disk-cache", derive(serde::Serialize, serde::Deserialize))]
pub struct StoredTransactionRecord {
    #[cfg_attr(feature = "disk-cache", serde(with = "serde_big_array::BigArray"))]
    pub signature: [u8; 64],
    pub slot: u64,
    pub slot_idx: u32,
    pub block_time: Option<i64>,
    #[cfg_attr(feature = "disk-cache", serde(skip, default))]
    #[cfg_attr(not(feature = "grpc-streaming"), allow(dead_code))]
    pub is_vote: bool,
    pub tx_version: Option<u8>,
    #[cfg_attr(feature = "disk-cache", serde(with = "serde_sig_vec"))]
    pub tx_signatures: Vec<[u8; 64]>,
    pub tx_num_required_signatures: u8,
    pub tx_num_readonly_signed_accounts: u8,
    pub tx_num_readonly_unsigned_accounts: u8,
    pub tx_account_keys: Vec<[u8; 32]>,
    pub tx_recent_blockhash: [u8; 32],
    pub tx_instructions_program_id_index: Vec<u8>,
    pub tx_instructions_accounts: Vec<Vec<u8>>,
    pub tx_instructions_data: Vec<Vec<u8>>,
    pub tx_address_table_lookups_present: bool,
    pub tx_address_table_lookup_account_key: Vec<[u8; 32]>,
    pub tx_address_table_lookup_writable_indexes: Vec<Vec<u8>>,
    pub tx_address_table_lookup_readonly_indexes: Vec<Vec<u8>>,
    pub meta_status_ok: bool,
    pub meta_err: Option<String>,
    pub meta_fee: u64,
    pub meta_pre_balances: Vec<u64>,
    pub meta_post_balances: Vec<u64>,
    pub meta_inner_instructions_present: bool,
    pub meta_inner_instructions_index: Vec<u8>,
    pub meta_inner_instructions_program_id_index: Vec<Vec<u8>>,
    pub meta_inner_instructions_accounts: Vec<Vec<Vec<u8>>>,
    pub meta_inner_instructions_data: Vec<Vec<Vec<u8>>>,
    pub meta_inner_instructions_stack_height: Vec<Vec<Option<u32>>>,
    pub meta_log_messages_present: bool,
    pub meta_log_messages: Vec<String>,
    pub meta_pre_token_balances_present: bool,
    pub meta_pre_token_account_index: Vec<u8>,
    pub meta_pre_token_mint: Vec<[u8; 32]>,
    pub meta_pre_token_owner: Vec<Option<[u8; 32]>>,
    pub meta_pre_token_program_id: Vec<Option<[u8; 32]>>,
    pub meta_pre_token_amount: Vec<String>,
    pub meta_pre_token_decimals: Vec<u8>,
    pub meta_pre_token_ui_amount: Vec<Option<f64>>,
    pub meta_pre_token_ui_amount_string: Vec<String>,
    pub meta_post_token_balances_present: bool,
    pub meta_post_token_account_index: Vec<u8>,
    pub meta_post_token_mint: Vec<[u8; 32]>,
    pub meta_post_token_owner: Vec<Option<[u8; 32]>>,
    pub meta_post_token_program_id: Vec<Option<[u8; 32]>>,
    pub meta_post_token_amount: Vec<String>,
    pub meta_post_token_decimals: Vec<u8>,
    pub meta_post_token_ui_amount: Vec<Option<f64>>,
    pub meta_post_token_ui_amount_string: Vec<String>,
    pub meta_rewards_present: bool,
    pub meta_reward_pubkey: Vec<String>,
    pub meta_reward_lamports: Vec<i64>,
    pub meta_reward_post_balance: Vec<u64>,
    pub meta_reward_type: Vec<Option<String>>,
    pub meta_reward_commission: Vec<Option<u8>>,
    pub meta_loaded_addresses_writable: Vec<[u8; 32]>,
    pub meta_loaded_addresses_readonly: Vec<[u8; 32]>,
    pub meta_return_data_present: bool,
    pub meta_return_data_program_id: Option<[u8; 32]>,
    pub meta_return_data_data: Option<Vec<u8>>,
    pub meta_compute_units_consumed: Option<u64>,
    pub meta_cost_units: Option<u64>,
}

/// Serde support for `Vec<[u8; 64]>` (serde's built-in array impls stop at 32 elements).
#[cfg(feature = "disk-cache")]
pub(crate) mod serde_sig_vec {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use serde_big_array::BigArray;

    #[derive(Serialize, Deserialize)]
    struct Sig(#[serde(with = "BigArray")] [u8; 64]);

    pub(crate) fn serialize<S: Serializer>(
        value: &[[u8; 64]],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(value.iter().map(|bytes| Sig(*bytes)))
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<[u8; 64]>, D::Error> {
        let sigs = Vec::<Sig>::deserialize(deserializer)?;
        Ok(sigs.into_iter().map(|sig| sig.0).collect())
    }
}
