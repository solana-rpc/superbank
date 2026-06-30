// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::collections::HashSet;
use std::str::FromStr;

use prost::Message;
use solana_sdk::{pubkey::Pubkey, transaction::VersionedTransaction};
use solana_transaction_status::{Reward, RewardType, TransactionStatusMeta};
use tonic::{Code, Status};

use crate::clickhouse::{BlockMetadataRecord, StoredTransactionRecord};
use crate::grpc::generated::confirmed_block as storage_proto;
use crate::grpc::generated::superbank as superbank_proto;
use crate::hydration::{build_transaction_status_meta, build_versioned_transaction};

#[derive(Debug, Clone)]
pub(crate) struct AccountFilters {
    include: Vec<[u8; 32]>,
    exclude: Vec<[u8; 32]>,
    required: Vec<[u8; 32]>,
}

impl AccountFilters {
    pub(crate) fn for_blocks(include: &[String]) -> Result<Self, Status> {
        Ok(Self {
            include: parse_accounts(include, "account_include")?,
            exclude: Vec::new(),
            required: Vec::new(),
        })
    }

    pub(crate) fn for_transactions(
        filter: Option<&superbank_proto::StreamTransactionsFilter>,
    ) -> Result<Self, Status> {
        let Some(filter) = filter else {
            return Ok(Self::empty());
        };
        Ok(Self {
            include: parse_accounts(&filter.account_include, "account_include")?,
            exclude: parse_accounts(&filter.account_exclude, "account_exclude")?,
            required: parse_accounts(&filter.account_required, "account_required")?,
        })
    }

    pub(crate) fn empty() -> Self {
        Self {
            include: Vec::new(),
            exclude: Vec::new(),
            required: Vec::new(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.include.is_empty() && self.exclude.is_empty() && self.required.is_empty()
    }

    pub(crate) fn matches_record(&self, record: &StoredTransactionRecord) -> bool {
        if self.is_empty() {
            return true;
        }
        let accounts = transaction_accounts(record);
        if !self.include.is_empty()
            && !self
                .include
                .iter()
                .any(|account| accounts.contains(account))
        {
            return false;
        }
        if self
            .exclude
            .iter()
            .any(|account| accounts.contains(account))
        {
            return false;
        }
        self.required
            .iter()
            .all(|account| accounts.contains(account))
    }
}

pub(crate) fn transaction_matches_stream_filter(
    record: &StoredTransactionRecord,
    filter: Option<&superbank_proto::StreamTransactionsFilter>,
    account_filters: &AccountFilters,
) -> bool {
    let Some(filter) = filter else {
        return account_filters.matches_record(record);
    };

    if let Some(vote) = filter.vote {
        let is_vote = record.is_vote;
        if vote != is_vote {
            return false;
        }
    }

    if let Some(failed) = filter.failed {
        let is_failed = !record.meta_status_ok;
        if failed != is_failed {
            return false;
        }
    }

    account_filters.matches_record(record)
}

pub(crate) fn block_matches_account_filter(
    transactions: &[StoredTransactionRecord],
    filter: &AccountFilters,
) -> bool {
    filter.include.is_empty()
        || transactions
            .iter()
            .any(|record| filter.matches_record(record))
}

pub(crate) fn encode_block_response(
    metadata: BlockMetadataRecord,
    transactions: Vec<StoredTransactionRecord>,
) -> Result<superbank_proto::BlockResponse, Status> {
    let txs = transactions
        .into_iter()
        .map(encode_transaction)
        .collect::<Result<Vec<_>, _>>()?;
    let rewards = encode_rewards(&metadata)?;

    Ok(superbank_proto::BlockResponse {
        previous_blockhash: metadata.parent_blockhash.to_vec(),
        blockhash: metadata.blockhash.to_vec(),
        parent_slot: metadata.parent_slot,
        slot: metadata.slot,
        block_time: metadata.block_time.unwrap_or_default(),
        block_height: metadata.block_height.unwrap_or_default(),
        transactions: txs,
        rewards,
        num_partitions: metadata.rewards_num_partitions,
    })
}

pub(crate) fn encode_transaction_response(
    record: StoredTransactionRecord,
) -> Result<superbank_proto::TransactionResponse, Status> {
    let slot = record.slot;
    let block_time = record.block_time.unwrap_or_default();
    let index = Some(u64::from(record.slot_idx));
    let transaction = encode_transaction(record)?;
    Ok(superbank_proto::TransactionResponse {
        transaction: Some(transaction),
        slot,
        block_time,
        index,
    })
}

fn encode_transaction(
    record: StoredTransactionRecord,
) -> Result<superbank_proto::Transaction, Status> {
    let index = Some(u64::from(record.slot_idx));
    let transaction = build_versioned_transaction(&record)
        .map_err(internal_status)?
        .pipe(serialize_transaction)?;
    let meta = match build_transaction_status_meta(&record).map_err(internal_status)? {
        Some(meta) => encode_transaction_status_meta(&meta)?,
        None => Vec::new(),
    };

    Ok(superbank_proto::Transaction {
        transaction,
        meta,
        index,
    })
}

fn serialize_transaction(transaction: VersionedTransaction) -> Result<Vec<u8>, Status> {
    bincode::serialize(&transaction).map_err(|err| {
        Status::new(
            Code::Internal,
            format!("failed to encode transaction: {err}"),
        )
    })
}

fn encode_transaction_status_meta(meta: &TransactionStatusMeta) -> Result<Vec<u8>, Status> {
    let proto = storage_proto::TransactionStatusMeta {
        err: match &meta.status {
            Ok(()) => None,
            Err(err) => Some(storage_proto::TransactionError {
                err: bincode::serialize(err).map_err(|err| {
                    Status::new(
                        Code::Internal,
                        format!("failed to encode transaction error: {err}"),
                    )
                })?,
            }),
        },
        fee: meta.fee,
        pre_balances: meta.pre_balances.clone(),
        post_balances: meta.post_balances.clone(),
        inner_instructions: meta
            .inner_instructions
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|inner| storage_proto::InnerInstructions {
                index: u32::from(inner.index),
                instructions: inner
                    .instructions
                    .iter()
                    .map(|ix| storage_proto::InnerInstruction {
                        program_id_index: u32::from(ix.instruction.program_id_index),
                        accounts: ix.instruction.accounts.clone(),
                        data: ix.instruction.data.clone(),
                        stack_height: ix.stack_height,
                    })
                    .collect(),
            })
            .collect(),
        inner_instructions_none: meta.inner_instructions.is_none(),
        log_messages: meta.log_messages.clone().unwrap_or_default(),
        log_messages_none: meta.log_messages.is_none(),
        pre_token_balances: meta
            .pre_token_balances
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(encode_token_balance)
            .collect(),
        post_token_balances: meta
            .post_token_balances
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(encode_token_balance)
            .collect(),
        rewards: meta
            .rewards
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(encode_reward)
            .collect(),
        loaded_writable_addresses: meta
            .loaded_addresses
            .writable
            .iter()
            .map(|address| address.to_bytes().to_vec())
            .collect(),
        loaded_readonly_addresses: meta
            .loaded_addresses
            .readonly
            .iter()
            .map(|address| address.to_bytes().to_vec())
            .collect(),
        return_data: meta
            .return_data
            .as_ref()
            .map(|return_data| storage_proto::ReturnData {
                program_id: return_data.program_id.to_bytes().to_vec(),
                data: return_data.data.clone(),
            }),
        return_data_none: meta.return_data.is_none(),
        compute_units_consumed: meta.compute_units_consumed,
        cost_units: meta.cost_units,
    };

    Ok(proto.encode_to_vec())
}

fn encode_rewards(metadata: &BlockMetadataRecord) -> Result<Vec<u8>, Status> {
    if !metadata.rewards_present {
        return Ok(Vec::new());
    }
    let len = metadata.rewards_pubkey.len();
    if metadata.rewards_lamports.len() != len
        || metadata.rewards_post_balance.len() != len
        || metadata.rewards_type.len() != len
        || metadata.rewards_commission.len() != len
    {
        return Err(Status::new(
            Code::Internal,
            "stored block reward columns have mismatched lengths",
        ));
    }

    let rewards = storage_proto::Rewards {
        rewards: (0..len)
            .map(|idx| {
                let reward_type = metadata.rewards_type[idx]
                    .as_deref()
                    .map(reward_type_from_string)
                    .unwrap_or(storage_proto::RewardType::Unspecified);
                storage_proto::Reward {
                    pubkey: Pubkey::from(metadata.rewards_pubkey[idx]).to_string(),
                    lamports: metadata.rewards_lamports[idx],
                    post_balance: metadata.rewards_post_balance[idx],
                    reward_type: reward_type as i32,
                    commission: metadata.rewards_commission[idx]
                        .map(|commission| commission.to_string())
                        .unwrap_or_default(),
                }
            })
            .collect(),
        num_partitions: metadata
            .rewards_num_partitions
            .map(|num_partitions| storage_proto::NumPartitions { num_partitions }),
    };

    Ok(rewards.encode_to_vec())
}

fn encode_reward(reward: &Reward) -> storage_proto::Reward {
    storage_proto::Reward {
        pubkey: reward.pubkey.clone(),
        lamports: reward.lamports,
        post_balance: reward.post_balance,
        reward_type: reward
            .reward_type
            .map(encode_reward_type)
            .unwrap_or_default() as i32,
        commission: reward
            .commission
            .map(|commission| commission.to_string())
            .unwrap_or_default(),
    }
}

fn encode_reward_type(reward_type: RewardType) -> storage_proto::RewardType {
    match reward_type {
        RewardType::Fee => storage_proto::RewardType::Fee,
        RewardType::Rent => storage_proto::RewardType::Rent,
        RewardType::Staking => storage_proto::RewardType::Staking,
        RewardType::Voting => storage_proto::RewardType::Voting,
    }
}

fn reward_type_from_string(value: &str) -> storage_proto::RewardType {
    match value {
        "Fee" | "fee" => storage_proto::RewardType::Fee,
        "Rent" | "rent" => storage_proto::RewardType::Rent,
        "Staking" | "staking" => storage_proto::RewardType::Staking,
        "Voting" | "voting" => storage_proto::RewardType::Voting,
        _ => storage_proto::RewardType::Unspecified,
    }
}

fn encode_token_balance(
    balance: &solana_transaction_status::TransactionTokenBalance,
) -> storage_proto::TokenBalance {
    storage_proto::TokenBalance {
        account_index: u32::from(balance.account_index),
        mint: balance.mint.clone(),
        ui_token_amount: Some(storage_proto::UiTokenAmount {
            ui_amount: balance.ui_token_amount.ui_amount.unwrap_or_default(),
            decimals: u32::from(balance.ui_token_amount.decimals),
            amount: balance.ui_token_amount.amount.clone(),
            ui_amount_string: balance.ui_token_amount.ui_amount_string.clone(),
        }),
        owner: balance.owner.clone(),
        program_id: balance.program_id.clone(),
    }
}

fn transaction_accounts(record: &StoredTransactionRecord) -> HashSet<[u8; 32]> {
    let mut accounts = HashSet::with_capacity(
        record.tx_account_keys.len()
            + record.meta_loaded_addresses_writable.len()
            + record.meta_loaded_addresses_readonly.len(),
    );
    accounts.extend(record.tx_account_keys.iter().copied());
    accounts.extend(record.meta_loaded_addresses_writable.iter().copied());
    accounts.extend(record.meta_loaded_addresses_readonly.iter().copied());
    accounts
}

fn parse_accounts(values: &[String], field_name: &str) -> Result<Vec<[u8; 32]>, Status> {
    values
        .iter()
        .enumerate()
        .map(|(idx, value)| {
            Pubkey::from_str(value)
                .map(|pubkey| pubkey.to_bytes())
                .map_err(|err| {
                    Status::new(
                        Code::InvalidArgument,
                        format!("invalid {field_name}[{idx}] '{value}': {err}"),
                    )
                })
        })
        .collect()
}

fn internal_status(err: impl std::fmt::Display) -> Status {
    Status::new(Code::Internal, err.to_string())
}

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}

impl<T> Pipe for T {}

#[cfg(test)]
mod tests {
    use super::*;

    fn record_with_accounts(accounts: Vec<[u8; 32]>) -> StoredTransactionRecord {
        StoredTransactionRecord {
            signature: [9; 64],
            slot: 1,
            slot_idx: 2,
            block_time: Some(3),
            is_vote: false,
            tx_version: None,
            tx_signatures: vec![[9; 64]],
            tx_num_required_signatures: 1,
            tx_num_readonly_signed_accounts: 0,
            tx_num_readonly_unsigned_accounts: 0,
            tx_account_keys: accounts,
            tx_recent_blockhash: [7; 32],
            tx_instructions_program_id_index: Vec::new(),
            tx_instructions_accounts: Vec::new(),
            tx_instructions_data: Vec::new(),
            tx_address_table_lookups_present: false,
            tx_address_table_lookup_account_key: Vec::new(),
            tx_address_table_lookup_writable_indexes: Vec::new(),
            tx_address_table_lookup_readonly_indexes: Vec::new(),
            meta_status_ok: true,
            meta_err: None,
            meta_fee: 0,
            meta_pre_balances: vec![1],
            meta_post_balances: vec![1],
            meta_inner_instructions_present: false,
            meta_inner_instructions_index: Vec::new(),
            meta_inner_instructions_program_id_index: Vec::new(),
            meta_inner_instructions_accounts: Vec::new(),
            meta_inner_instructions_data: Vec::new(),
            meta_inner_instructions_stack_height: Vec::new(),
            meta_log_messages_present: false,
            meta_log_messages: Vec::new(),
            meta_pre_token_balances_present: false,
            meta_pre_token_account_index: Vec::new(),
            meta_pre_token_mint: Vec::new(),
            meta_pre_token_owner: Vec::new(),
            meta_pre_token_program_id: Vec::new(),
            meta_pre_token_amount: Vec::new(),
            meta_pre_token_decimals: Vec::new(),
            meta_pre_token_ui_amount: Vec::new(),
            meta_pre_token_ui_amount_string: Vec::new(),
            meta_post_token_balances_present: false,
            meta_post_token_account_index: Vec::new(),
            meta_post_token_mint: Vec::new(),
            meta_post_token_owner: Vec::new(),
            meta_post_token_program_id: Vec::new(),
            meta_post_token_amount: Vec::new(),
            meta_post_token_decimals: Vec::new(),
            meta_post_token_ui_amount: Vec::new(),
            meta_post_token_ui_amount_string: Vec::new(),
            meta_rewards_present: false,
            meta_reward_pubkey: Vec::new(),
            meta_reward_lamports: Vec::new(),
            meta_reward_post_balance: Vec::new(),
            meta_reward_type: Vec::new(),
            meta_reward_commission: Vec::new(),
            meta_loaded_addresses_writable: Vec::new(),
            meta_loaded_addresses_readonly: Vec::new(),
            meta_return_data_present: false,
            meta_return_data_program_id: None,
            meta_return_data_data: None,
            meta_compute_units_consumed: None,
            meta_cost_units: None,
        }
    }

    #[test]
    fn account_filter_matches_loaded_addresses() {
        let mut record = record_with_accounts(vec![[1; 32]]);
        record.meta_loaded_addresses_writable = vec![[2; 32]];
        let filter = AccountFilters {
            include: vec![[2; 32]],
            exclude: Vec::new(),
            required: Vec::new(),
        };

        assert!(filter.matches_record(&record));
    }

    #[test]
    fn transaction_filter_handles_vote_and_failed_tri_state() {
        let mut record = record_with_accounts(vec![[1; 32]]);
        record.is_vote = true;
        record.meta_status_ok = false;
        let filter = superbank_proto::StreamTransactionsFilter {
            vote: Some(true),
            failed: Some(true),
            account_include: Vec::new(),
            account_exclude: Vec::new(),
            account_required: Vec::new(),
        };

        assert!(transaction_matches_stream_filter(
            &record,
            Some(&filter),
            &AccountFilters::empty()
        ));
    }
}
