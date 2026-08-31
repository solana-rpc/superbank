// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use thiserror::Error;
use yellowstone_grpc_proto::prelude::{
    CompiledInstruction, MessageAddressTableLookup, RewardType, SubscribeUpdateTransactionInfo,
    TokenBalance, TransactionError, TransactionStatusMeta,
};

use crate::clickhouse::StoredTransactionRecord;
use crate::solana_sdk;

#[derive(Debug, Error)]
pub(crate) enum ConvertError {
    #[error("missing field: {0}")]
    Missing(&'static str),
    #[error("invalid length for {what}: expected {expected} bytes, got {got}")]
    InvalidLen {
        what: &'static str,
        expected: usize,
        got: usize,
    },
    #[error("{0} out of range")]
    OutOfRange(&'static str),
    #[error("invalid base58 value for {what}")]
    InvalidBase58 { what: &'static str },
    #[error("invalid message: {0}")]
    InvalidMessage(&'static str),
}

pub(crate) fn bytes_to_array<const N: usize>(value: &[u8]) -> Result<[u8; N], ConvertError> {
    value.try_into().map_err(|_| ConvertError::InvalidLen {
        what: "bytes",
        expected: N,
        got: value.len(),
    })
}

fn decode_base58_32(value: &str, what: &'static str) -> Result<[u8; 32], ConvertError> {
    let bytes = bs58::decode(value)
        .into_vec()
        .map_err(|_| ConvertError::InvalidBase58 { what })?;
    bytes_to_array::<32>(&bytes).map_err(|_| ConvertError::InvalidLen {
        what,
        expected: 32,
        got: bytes.len(),
    })
}

fn convert_signatures(signatures: &[Vec<u8>]) -> Result<Vec<[u8; 64]>, ConvertError> {
    let mut out = Vec::with_capacity(signatures.len());
    for sig in signatures {
        out.push(
            bytes_to_array::<64>(sig).map_err(|_| ConvertError::InvalidLen {
                what: "transaction signature",
                expected: 64,
                got: sig.len(),
            })?,
        );
    }
    Ok(out)
}

fn convert_account_keys(keys: &[Vec<u8>]) -> Result<Vec<[u8; 32]>, ConvertError> {
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        out.push(
            bytes_to_array::<32>(key).map_err(|_| ConvertError::InvalidLen {
                what: "account key",
                expected: 32,
                got: key.len(),
            })?,
        );
    }
    Ok(out)
}

type InstructionConversion = (Vec<u8>, Vec<Vec<u8>>, Vec<Vec<u8>>);
type AddressLookupConversion = (Vec<[u8; 32]>, Vec<Vec<u8>>, Vec<Vec<u8>>);
type InnerInstructionsConversion = (
    bool,
    Vec<u8>,
    Vec<Vec<u8>>,
    Vec<Vec<Vec<u8>>>,
    Vec<Vec<Vec<u8>>>,
    Vec<Vec<Option<u32>>>,
);
type RewardsConversion = (
    Vec<String>,
    Vec<i64>,
    Vec<u64>,
    Vec<Option<String>>,
    Vec<Option<u8>>,
    Vec<Option<u16>>,
);
type ReturnDataConversion = (bool, Option<[u8; 32]>, Option<Vec<u8>>);

fn convert_instructions(
    instructions: &[CompiledInstruction],
) -> Result<InstructionConversion, ConvertError> {
    let mut program_ids = Vec::with_capacity(instructions.len());
    let mut accounts = Vec::with_capacity(instructions.len());
    let mut data = Vec::with_capacity(instructions.len());

    for ix in instructions {
        let program_id_index: u8 = ix
            .program_id_index
            .try_into()
            .map_err(|_| ConvertError::OutOfRange("program_id_index"))?;
        program_ids.push(program_id_index);
        accounts.push(ix.accounts.clone());
        data.push(ix.data.clone());
    }

    Ok((program_ids, accounts, data))
}

fn convert_address_table_lookups(
    lookups: &[MessageAddressTableLookup],
) -> Result<AddressLookupConversion, ConvertError> {
    let mut account_keys = Vec::with_capacity(lookups.len());
    let mut writable_indexes = Vec::with_capacity(lookups.len());
    let mut readonly_indexes = Vec::with_capacity(lookups.len());

    for lookup in lookups {
        account_keys.push(bytes_to_array::<32>(&lookup.account_key).map_err(|_| {
            ConvertError::InvalidLen {
                what: "address lookup account key",
                expected: 32,
                got: lookup.account_key.len(),
            }
        })?);
        writable_indexes.push(lookup.writable_indexes.clone());
        readonly_indexes.push(lookup.readonly_indexes.clone());
    }

    Ok((account_keys, writable_indexes, readonly_indexes))
}

fn decode_transaction_error(err: Option<&TransactionError>) -> (bool, Option<String>) {
    let Some(err) = err else {
        return (true, None);
    };

    match bincode::deserialize::<solana_sdk::transaction::TransactionError>(&err.err) {
        Ok(decoded) => {
            let serialized =
                serde_json::to_string(&decoded).unwrap_or_else(|_| format!("{decoded:?}"));
            (false, Some(serialized))
        }
        Err(_) => {
            let fallback = hex::encode(&err.err);
            tracing::warn!("head cache: failed to decode transaction error; storing hex fallback");
            (false, Some(format!("\"{}\"", fallback)))
        }
    }
}

fn convert_inner_instructions(
    meta: &TransactionStatusMeta,
) -> Result<InnerInstructionsConversion, ConvertError> {
    if meta.inner_instructions_none {
        return Ok((
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ));
    }

    let mut indexes = Vec::with_capacity(meta.inner_instructions.len());
    let mut program_ids = Vec::with_capacity(meta.inner_instructions.len());
    let mut accounts = Vec::with_capacity(meta.inner_instructions.len());
    let mut data = Vec::with_capacity(meta.inner_instructions.len());
    let mut stack_heights = Vec::with_capacity(meta.inner_instructions.len());

    for ix in &meta.inner_instructions {
        let index: u8 = ix
            .index
            .try_into()
            .map_err(|_| ConvertError::OutOfRange("inner instruction index"))?;
        indexes.push(index);

        let mut group_program_ids = Vec::with_capacity(ix.instructions.len());
        let mut group_accounts = Vec::with_capacity(ix.instructions.len());
        let mut group_data = Vec::with_capacity(ix.instructions.len());
        let mut group_stack = Vec::with_capacity(ix.instructions.len());

        for inner in &ix.instructions {
            let program_id_index: u8 = inner
                .program_id_index
                .try_into()
                .map_err(|_| ConvertError::OutOfRange("inner program_id_index"))?;
            group_program_ids.push(program_id_index);
            group_accounts.push(inner.accounts.clone());
            group_data.push(inner.data.clone());
            group_stack.push(inner.stack_height);
        }

        program_ids.push(group_program_ids);
        accounts.push(group_accounts);
        data.push(group_data);
        stack_heights.push(group_stack);
    }

    Ok((true, indexes, program_ids, accounts, data, stack_heights))
}

#[allow(clippy::type_complexity)]
fn convert_token_balances(
    balances: &[TokenBalance],
) -> Result<
    (
        bool,
        Vec<u8>,
        Vec<[u8; 32]>,
        Vec<Option<[u8; 32]>>,
        Vec<Option<[u8; 32]>>,
        Vec<String>,
        Vec<u8>,
        Vec<Option<f64>>,
        Vec<String>,
    ),
    ConvertError,
> {
    // gRPC does not encode optionality for token balances, so empty means "present but empty".
    let mut account_indexes = Vec::with_capacity(balances.len());
    let mut mints = Vec::with_capacity(balances.len());
    let mut owners = Vec::with_capacity(balances.len());
    let mut program_ids = Vec::with_capacity(balances.len());
    let mut amounts = Vec::with_capacity(balances.len());
    let mut decimals = Vec::with_capacity(balances.len());
    let mut ui_amounts = Vec::with_capacity(balances.len());
    let mut ui_amount_strings = Vec::with_capacity(balances.len());

    for balance in balances {
        let account_index: u8 = balance
            .account_index
            .try_into()
            .map_err(|_| ConvertError::OutOfRange("token account_index"))?;
        account_indexes.push(account_index);
        mints.push(decode_base58_32(&balance.mint, "token mint")?);

        let owner = if balance.owner.is_empty() {
            None
        } else {
            Some(decode_base58_32(&balance.owner, "token owner")?)
        };
        owners.push(owner);

        let program_id = if balance.program_id.is_empty() {
            None
        } else {
            Some(decode_base58_32(&balance.program_id, "token program_id")?)
        };
        program_ids.push(program_id);

        if let Some(ui) = balance.ui_token_amount.as_ref() {
            amounts.push(ui.amount.clone());
            let decimals_u8: u8 = ui
                .decimals
                .try_into()
                .map_err(|_| ConvertError::OutOfRange("token decimals"))?;
            decimals.push(decimals_u8);
            ui_amounts.push(Some(ui.ui_amount));
            ui_amount_strings.push(ui.ui_amount_string.clone());
        } else {
            amounts.push(String::new());
            decimals.push(0);
            ui_amounts.push(None);
            ui_amount_strings.push(String::new());
        }
    }

    Ok((
        true,
        account_indexes,
        mints,
        owners,
        program_ids,
        amounts,
        decimals,
        ui_amounts,
        ui_amount_strings,
    ))
}

fn convert_rewards(rewards: &[yellowstone_grpc_proto::prelude::Reward]) -> RewardsConversion {
    let mut pubkeys = Vec::with_capacity(rewards.len());
    let mut lamports = Vec::with_capacity(rewards.len());
    let mut post_balances = Vec::with_capacity(rewards.len());
    let mut reward_types = Vec::with_capacity(rewards.len());
    let mut commissions = Vec::with_capacity(rewards.len());
    let mut commission_bps = Vec::with_capacity(rewards.len());

    for reward in rewards {
        pubkeys.push(reward.pubkey.clone());
        lamports.push(reward.lamports);
        post_balances.push(reward.post_balance);
        reward_types.push(reward_type_to_string(reward.reward_type));
        commissions.push(parse_commission(&reward.commission));
        commission_bps.push(parse_commission_bps(&reward.commission_bps));
    }

    (
        pubkeys,
        lamports,
        post_balances,
        reward_types,
        commissions,
        commission_bps,
    )
}

fn reward_type_to_string(value: i32) -> Option<String> {
    let parsed = RewardType::try_from(value).ok()?;
    match parsed {
        RewardType::Unspecified => None,
        other => Some(other.as_str_name().to_string()),
    }
}

fn parse_commission(value: &str) -> Option<u8> {
    if value.is_empty() {
        return None;
    }
    value.parse::<u8>().ok()
}

fn parse_commission_bps(value: &str) -> Option<u16> {
    if value.is_empty() {
        return None;
    }
    value.parse::<u16>().ok()
}

fn convert_pubkeys(keys: &[Vec<u8>], what: &'static str) -> Result<Vec<[u8; 32]>, ConvertError> {
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        out.push(
            bytes_to_array::<32>(key).map_err(|_| ConvertError::InvalidLen {
                what,
                expected: 32,
                got: key.len(),
            })?,
        );
    }
    Ok(out)
}

fn convert_return_data(meta: &TransactionStatusMeta) -> Result<ReturnDataConversion, ConvertError> {
    if meta.return_data_none {
        return Ok((false, None, None));
    }

    let Some(return_data) = meta.return_data.as_ref() else {
        return Ok((false, None, None));
    };

    let program_id =
        bytes_to_array::<32>(&return_data.program_id).map_err(|_| ConvertError::InvalidLen {
            what: "return data program id",
            expected: 32,
            got: return_data.program_id.len(),
        })?;

    Ok((true, Some(program_id), Some(return_data.data.clone())))
}

pub(crate) fn stored_record_from_transaction_info(
    slot: u64,
    tx_info: &SubscribeUpdateTransactionInfo,
) -> Result<StoredTransactionRecord, ConvertError> {
    let signature =
        bytes_to_array::<64>(&tx_info.signature).map_err(|_| ConvertError::InvalidLen {
            what: "signature",
            expected: 64,
            got: tx_info.signature.len(),
        })?;

    let transaction = tx_info
        .transaction
        .as_ref()
        .ok_or(ConvertError::Missing("transaction"))?;
    let meta = tx_info.meta.as_ref().ok_or(ConvertError::Missing("meta"))?;
    let message = transaction
        .message
        .as_ref()
        .ok_or(ConvertError::Missing("message"))?;
    let header = message
        .header
        .as_ref()
        .ok_or(ConvertError::Missing("message.header"))?;
    if message.config.is_some() && !message.address_table_lookups.is_empty() {
        return Err(ConvertError::InvalidMessage(
            "v1 transaction contains address table lookups",
        ));
    }

    let tx_signatures = convert_signatures(&transaction.signatures)?;
    let tx_account_keys = convert_account_keys(&message.account_keys)?;
    let tx_recent_blockhash =
        bytes_to_array::<32>(&message.recent_blockhash).map_err(|_| ConvertError::InvalidLen {
            what: "recent_blockhash",
            expected: 32,
            got: message.recent_blockhash.len(),
        })?;

    let (tx_instructions_program_id_index, tx_instructions_accounts, tx_instructions_data) =
        convert_instructions(&message.instructions)?;

    let (
        tx_address_table_lookup_account_key,
        tx_address_table_lookup_writable_indexes,
        tx_address_table_lookup_readonly_indexes,
    ) = convert_address_table_lookups(&message.address_table_lookups)?;

    let (meta_status_ok, meta_err) = decode_transaction_error(meta.err.as_ref());

    let (
        meta_inner_instructions_present,
        meta_inner_instructions_index,
        meta_inner_instructions_program_id_index,
        meta_inner_instructions_accounts,
        meta_inner_instructions_data,
        meta_inner_instructions_stack_height,
    ) = convert_inner_instructions(meta)?;

    let (meta_log_messages_present, meta_log_messages) = if meta.log_messages_none {
        (false, Vec::new())
    } else {
        (true, meta.log_messages.clone())
    };

    let (
        meta_pre_token_balances_present,
        meta_pre_token_account_index,
        meta_pre_token_mint,
        meta_pre_token_owner,
        meta_pre_token_program_id,
        meta_pre_token_amount,
        meta_pre_token_decimals,
        meta_pre_token_ui_amount,
        meta_pre_token_ui_amount_string,
    ) = convert_token_balances(&meta.pre_token_balances)?;

    let (
        meta_post_token_balances_present,
        meta_post_token_account_index,
        meta_post_token_mint,
        meta_post_token_owner,
        meta_post_token_program_id,
        meta_post_token_amount,
        meta_post_token_decimals,
        meta_post_token_ui_amount,
        meta_post_token_ui_amount_string,
    ) = convert_token_balances(&meta.post_token_balances)?;

    let (
        meta_reward_pubkey,
        meta_reward_lamports,
        meta_reward_post_balance,
        meta_reward_type,
        meta_reward_commission,
        meta_reward_commission_bps,
    ) = convert_rewards(&meta.rewards);

    // gRPC does not distinguish between absent and empty rewards. Treat empty as present.
    let meta_rewards_present = true;

    let meta_loaded_addresses_writable =
        convert_pubkeys(&meta.loaded_writable_addresses, "loaded writable address")?;
    let meta_loaded_addresses_readonly =
        convert_pubkeys(&meta.loaded_readonly_addresses, "loaded readonly address")?;

    let (meta_return_data_present, meta_return_data_program_id, meta_return_data_data) =
        convert_return_data(meta)?;

    Ok(StoredTransactionRecord {
        signature,
        slot,
        slot_idx: tx_info.index.min(u64::from(u32::MAX)) as u32,
        block_time: None,
        is_vote: tx_info.is_vote,
        tx_version: if message.config.is_some() {
            Some(1)
        } else if message.versioned {
            Some(0)
        } else {
            None
        },
        tx_config_priority_fee: message
            .config
            .as_ref()
            .and_then(|config| config.priority_fee),
        tx_config_compute_unit_limit: message
            .config
            .as_ref()
            .and_then(|config| config.compute_unit_limit),
        tx_config_loaded_accounts_data_size_limit: message
            .config
            .as_ref()
            .and_then(|config| config.loaded_accounts_data_size_limit),
        tx_config_heap_size: message.config.as_ref().and_then(|config| config.heap_size),
        tx_signatures,
        tx_num_required_signatures: header
            .num_required_signatures
            .try_into()
            .map_err(|_| ConvertError::OutOfRange("num_required_signatures"))?,
        tx_num_readonly_signed_accounts: header
            .num_readonly_signed_accounts
            .try_into()
            .map_err(|_| ConvertError::OutOfRange("num_readonly_signed_accounts"))?,
        tx_num_readonly_unsigned_accounts: header
            .num_readonly_unsigned_accounts
            .try_into()
            .map_err(|_| ConvertError::OutOfRange("num_readonly_unsigned_accounts"))?,
        tx_account_keys,
        tx_recent_blockhash,
        tx_instructions_program_id_index,
        tx_instructions_accounts,
        tx_instructions_data,
        tx_address_table_lookups_present: !message.address_table_lookups.is_empty(),
        tx_address_table_lookup_account_key,
        tx_address_table_lookup_writable_indexes,
        tx_address_table_lookup_readonly_indexes,
        meta_status_ok,
        meta_err,
        meta_fee: meta.fee,
        meta_pre_balances: meta.pre_balances.clone(),
        meta_post_balances: meta.post_balances.clone(),
        meta_inner_instructions_present,
        meta_inner_instructions_index,
        meta_inner_instructions_program_id_index,
        meta_inner_instructions_accounts,
        meta_inner_instructions_data,
        meta_inner_instructions_stack_height,
        meta_log_messages_present,
        meta_log_messages,
        meta_pre_token_balances_present,
        meta_pre_token_account_index,
        meta_pre_token_mint,
        meta_pre_token_owner,
        meta_pre_token_program_id,
        meta_pre_token_amount,
        meta_pre_token_decimals,
        meta_pre_token_ui_amount,
        meta_pre_token_ui_amount_string,
        meta_post_token_balances_present,
        meta_post_token_account_index,
        meta_post_token_mint,
        meta_post_token_owner,
        meta_post_token_program_id,
        meta_post_token_amount,
        meta_post_token_decimals,
        meta_post_token_ui_amount,
        meta_post_token_ui_amount_string,
        meta_rewards_present,
        meta_reward_pubkey,
        meta_reward_lamports,
        meta_reward_post_balance,
        meta_reward_type,
        meta_reward_commission,
        meta_reward_commission_bps,
        meta_loaded_addresses_writable,
        meta_loaded_addresses_readonly,
        meta_return_data_present,
        meta_return_data_program_id,
        meta_return_data_data,
        meta_compute_units_consumed: meta.compute_units_consumed,
        meta_cost_units: meta.cost_units,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use yellowstone_grpc_proto::prelude::{
        Message, MessageHeader, Transaction, TransactionConfig, TransactionStatusMeta,
    };

    fn v1_update(
        address_table_lookups: Vec<MessageAddressTableLookup>,
    ) -> SubscribeUpdateTransactionInfo {
        SubscribeUpdateTransactionInfo {
            signature: vec![7; 64],
            is_vote: false,
            transaction: Some(Transaction {
                signatures: vec![vec![7; 64]],
                message: Some(Message {
                    header: Some(MessageHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                    }),
                    account_keys: vec![vec![1; 32]],
                    recent_blockhash: vec![9; 32],
                    instructions: Vec::new(),
                    versioned: false,
                    address_table_lookups,
                    config: Some(TransactionConfig {
                        priority_fee: Some(42),
                        compute_unit_limit: Some(1_000_000),
                        loaded_accounts_data_size_limit: Some(65_536),
                        heap_size: Some(32_768),
                    }),
                }),
            }),
            meta: Some(TransactionStatusMeta::default()),
            index: 3,
        }
    }

    #[test]
    fn head_cache_conversion_preserves_v1_fields() {
        let record =
            stored_record_from_transaction_info(55, &v1_update(Vec::new())).expect("convert v1");
        assert_eq!(record.tx_version, Some(1));
        assert_eq!(record.tx_recent_blockhash, [9; 32]);
        assert_eq!(record.tx_config_priority_fee, Some(42));
        assert_eq!(record.tx_config_compute_unit_limit, Some(1_000_000));
        assert_eq!(
            record.tx_config_loaded_accounts_data_size_limit,
            Some(65_536)
        );
        assert_eq!(record.tx_config_heap_size, Some(32_768));
    }

    #[test]
    fn head_cache_conversion_rejects_v1_lookups() {
        let update = v1_update(vec![MessageAddressTableLookup {
            account_key: vec![2; 32],
            writable_indexes: vec![0],
            readonly_indexes: Vec::new(),
        }]);
        assert!(matches!(
            stored_record_from_transaction_info(55, &update),
            Err(ConvertError::InvalidMessage(_))
        ));
    }
}
