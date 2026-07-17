// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use solana_account_decoder_client_types::token::UiTokenAmount;
#[allow(deprecated)]
use solana_sdk::{instruction::InstructionError, pubkey::Pubkey, transaction::TransactionError};
use solana_transaction_context::transaction::TransactionReturnData;
use solana_transaction_status::{
    InnerInstruction, InnerInstructions, Reward, RewardType, TransactionStatusMeta,
    TransactionTokenBalance,
};

use crate::clickhouse::{StoredAccountsTransactionRecord, StoredTransactionRecord};
use crate::hydration::errors::TransactionHydrationError;

// Historically, Solana RPC has varied between null and empty lists for absent fields.
// We emit empty lists only when the stored metadata explicitly marks a field as present.

pub(crate) fn build_transaction_status_meta(
    record: &StoredTransactionRecord,
) -> Result<Option<TransactionStatusMeta>, TransactionHydrationError> {
    if meta_is_missing(record) {
        return Ok(None);
    }
    let status = if record.meta_status_ok {
        if record.meta_err.is_some() {
            return Err(TransactionHydrationError::InvalidStoredMetadata(
                "meta_err set but meta_status_ok is true".to_string(),
            ));
        }
        Ok(())
    } else {
        Err(parse_transaction_error(&record.meta_err)?)
    };

    let inner_instructions = if record.meta_inner_instructions_present {
        Some(build_inner_instructions(record)?)
    } else {
        if !record.meta_inner_instructions_index.is_empty()
            || !record.meta_inner_instructions_program_id_index.is_empty()
            || !record.meta_inner_instructions_accounts.is_empty()
            || !record.meta_inner_instructions_data.is_empty()
            || !record.meta_inner_instructions_stack_height.is_empty()
        {
            return Err(TransactionHydrationError::InvalidStoredMetadata(
                "inner instruction fields populated without meta_inner_instructions_present"
                    .to_string(),
            ));
        }
        None
    };

    let log_messages = if record.meta_log_messages_present {
        Some(record.meta_log_messages.clone())
    } else {
        if !record.meta_log_messages.is_empty() {
            return Err(TransactionHydrationError::InvalidStoredMetadata(
                "log messages populated without meta_log_messages_present".to_string(),
            ));
        }
        None
    };

    let pre_token_balances = if record.meta_pre_token_balances_present {
        Some(build_token_balances(
            "pre",
            &record.meta_pre_token_account_index,
            &record.meta_pre_token_mint,
            &record.meta_pre_token_owner,
            &record.meta_pre_token_program_id,
            &record.meta_pre_token_amount,
            &record.meta_pre_token_decimals,
            &record.meta_pre_token_ui_amount,
            &record.meta_pre_token_ui_amount_string,
        )?)
    } else {
        if !record.meta_pre_token_account_index.is_empty()
            || !record.meta_pre_token_mint.is_empty()
            || !record.meta_pre_token_owner.is_empty()
            || !record.meta_pre_token_program_id.is_empty()
            || !record.meta_pre_token_amount.is_empty()
            || !record.meta_pre_token_decimals.is_empty()
            || !record.meta_pre_token_ui_amount.is_empty()
            || !record.meta_pre_token_ui_amount_string.is_empty()
        {
            return Err(TransactionHydrationError::InvalidStoredMetadata(
                "pre token balances populated without meta_pre_token_balances_present".to_string(),
            ));
        }
        None
    };

    let post_token_balances = if record.meta_post_token_balances_present {
        Some(build_token_balances(
            "post",
            &record.meta_post_token_account_index,
            &record.meta_post_token_mint,
            &record.meta_post_token_owner,
            &record.meta_post_token_program_id,
            &record.meta_post_token_amount,
            &record.meta_post_token_decimals,
            &record.meta_post_token_ui_amount,
            &record.meta_post_token_ui_amount_string,
        )?)
    } else {
        if !record.meta_post_token_account_index.is_empty()
            || !record.meta_post_token_mint.is_empty()
            || !record.meta_post_token_owner.is_empty()
            || !record.meta_post_token_program_id.is_empty()
            || !record.meta_post_token_amount.is_empty()
            || !record.meta_post_token_decimals.is_empty()
            || !record.meta_post_token_ui_amount.is_empty()
            || !record.meta_post_token_ui_amount_string.is_empty()
        {
            return Err(TransactionHydrationError::InvalidStoredMetadata(
                "post token balances populated without meta_post_token_balances_present"
                    .to_string(),
            ));
        }
        None
    };

    let rewards = if record.meta_rewards_present {
        Some(build_rewards(record)?)
    } else {
        if !record.meta_reward_pubkey.is_empty()
            || !record.meta_reward_lamports.is_empty()
            || !record.meta_reward_post_balance.is_empty()
            || !record.meta_reward_type.is_empty()
            || !record.meta_reward_commission.is_empty()
        {
            return Err(TransactionHydrationError::InvalidStoredMetadata(
                "rewards populated without meta_rewards_present".to_string(),
            ));
        }
        Some(Vec::new())
    };

    let loaded_addresses = solana_sdk::message::v0::LoadedAddresses {
        writable: record
            .meta_loaded_addresses_writable
            .iter()
            .map(|addr| Pubkey::from(*addr))
            .collect(),
        readonly: record
            .meta_loaded_addresses_readonly
            .iter()
            .map(|addr| Pubkey::from(*addr))
            .collect(),
    };

    let return_data = if record.meta_return_data_present {
        let program_id = record.meta_return_data_program_id.ok_or_else(|| {
            TransactionHydrationError::InvalidStoredMetadata(
                "return_data present without program id".to_string(),
            )
        })?;
        let data = record.meta_return_data_data.clone().ok_or_else(|| {
            TransactionHydrationError::InvalidStoredMetadata(
                "return_data present without data".to_string(),
            )
        })?;
        Some(TransactionReturnData {
            program_id: Pubkey::from(program_id),
            data,
        })
    } else {
        if record.meta_return_data_program_id.is_some() || record.meta_return_data_data.is_some() {
            return Err(TransactionHydrationError::InvalidStoredMetadata(
                "return_data fields populated without meta_return_data_present".to_string(),
            ));
        }
        None
    };

    Ok(Some(TransactionStatusMeta {
        status,
        fee: record.meta_fee,
        pre_balances: record.meta_pre_balances.clone(),
        post_balances: record.meta_post_balances.clone(),
        inner_instructions,
        log_messages,
        pre_token_balances,
        post_token_balances,
        rewards,
        loaded_addresses,
        return_data,
        compute_units_consumed: record.meta_compute_units_consumed,
        cost_units: record.meta_cost_units,
    }))
}

pub(crate) fn build_transaction_status_meta_for_accounts(
    record: &StoredAccountsTransactionRecord,
) -> Result<Option<TransactionStatusMeta>, TransactionHydrationError> {
    if accounts_meta_is_missing(record) {
        return Ok(None);
    }

    let status = if record.meta_status_ok {
        if record.meta_err.is_some() {
            return Err(TransactionHydrationError::InvalidStoredMetadata(
                "meta_err set but meta_status_ok is true".to_string(),
            ));
        }
        Ok(())
    } else {
        Err(parse_transaction_error(&record.meta_err)?)
    };

    let pre_token_balances = if record.meta_pre_token_balances_present {
        Some(build_token_balances(
            "pre",
            &record.meta_pre_token_account_index,
            &record.meta_pre_token_mint,
            &record.meta_pre_token_owner,
            &record.meta_pre_token_program_id,
            &record.meta_pre_token_amount,
            &record.meta_pre_token_decimals,
            &record.meta_pre_token_ui_amount,
            &record.meta_pre_token_ui_amount_string,
        )?)
    } else {
        if !record.meta_pre_token_account_index.is_empty()
            || !record.meta_pre_token_mint.is_empty()
            || !record.meta_pre_token_owner.is_empty()
            || !record.meta_pre_token_program_id.is_empty()
            || !record.meta_pre_token_amount.is_empty()
            || !record.meta_pre_token_decimals.is_empty()
            || !record.meta_pre_token_ui_amount.is_empty()
            || !record.meta_pre_token_ui_amount_string.is_empty()
        {
            return Err(TransactionHydrationError::InvalidStoredMetadata(
                "pre token balances populated without meta_pre_token_balances_present".to_string(),
            ));
        }
        None
    };

    let post_token_balances = if record.meta_post_token_balances_present {
        Some(build_token_balances(
            "post",
            &record.meta_post_token_account_index,
            &record.meta_post_token_mint,
            &record.meta_post_token_owner,
            &record.meta_post_token_program_id,
            &record.meta_post_token_amount,
            &record.meta_post_token_decimals,
            &record.meta_post_token_ui_amount,
            &record.meta_post_token_ui_amount_string,
        )?)
    } else {
        if !record.meta_post_token_account_index.is_empty()
            || !record.meta_post_token_mint.is_empty()
            || !record.meta_post_token_owner.is_empty()
            || !record.meta_post_token_program_id.is_empty()
            || !record.meta_post_token_amount.is_empty()
            || !record.meta_post_token_decimals.is_empty()
            || !record.meta_post_token_ui_amount.is_empty()
            || !record.meta_post_token_ui_amount_string.is_empty()
        {
            return Err(TransactionHydrationError::InvalidStoredMetadata(
                "post token balances populated without meta_post_token_balances_present"
                    .to_string(),
            ));
        }
        None
    };

    let rewards = if record.meta_rewards_present {
        Some(build_rewards_from_fields(
            &record.meta_reward_pubkey,
            &record.meta_reward_lamports,
            &record.meta_reward_post_balance,
            &record.meta_reward_type,
            &record.meta_reward_commission,
        )?)
    } else {
        if !record.meta_reward_pubkey.is_empty()
            || !record.meta_reward_lamports.is_empty()
            || !record.meta_reward_post_balance.is_empty()
            || !record.meta_reward_type.is_empty()
            || !record.meta_reward_commission.is_empty()
        {
            return Err(TransactionHydrationError::InvalidStoredMetadata(
                "rewards populated without meta_rewards_present".to_string(),
            ));
        }
        None
    };

    let loaded_addresses = solana_sdk::message::v0::LoadedAddresses {
        writable: record
            .meta_loaded_addresses_writable
            .iter()
            .map(|addr| Pubkey::from(*addr))
            .collect(),
        readonly: record
            .meta_loaded_addresses_readonly
            .iter()
            .map(|addr| Pubkey::from(*addr))
            .collect(),
    };

    Ok(Some(TransactionStatusMeta {
        status,
        fee: record.meta_fee,
        pre_balances: record.meta_pre_balances.clone(),
        post_balances: record.meta_post_balances.clone(),
        inner_instructions: None,
        log_messages: None,
        pre_token_balances,
        post_token_balances,
        rewards,
        loaded_addresses,
        return_data: None,
        compute_units_consumed: None,
        cost_units: None,
    }))
}

fn meta_is_missing(record: &StoredTransactionRecord) -> bool {
    if !record.meta_status_ok {
        return false;
    }
    if record.meta_err.is_some() {
        return false;
    }
    if record.meta_fee != 0 {
        return false;
    }
    if !record.meta_pre_balances.is_empty() || !record.meta_post_balances.is_empty() {
        return false;
    }
    if record.meta_inner_instructions_present
        || !record.meta_inner_instructions_index.is_empty()
        || !record.meta_inner_instructions_program_id_index.is_empty()
        || !record.meta_inner_instructions_accounts.is_empty()
        || !record.meta_inner_instructions_data.is_empty()
        || !record.meta_inner_instructions_stack_height.is_empty()
    {
        return false;
    }
    if record.meta_log_messages_present || !record.meta_log_messages.is_empty() {
        return false;
    }
    if record.meta_pre_token_balances_present
        || !record.meta_pre_token_account_index.is_empty()
        || !record.meta_pre_token_mint.is_empty()
        || !record.meta_pre_token_owner.is_empty()
        || !record.meta_pre_token_program_id.is_empty()
        || !record.meta_pre_token_amount.is_empty()
        || !record.meta_pre_token_decimals.is_empty()
        || !record.meta_pre_token_ui_amount.is_empty()
        || !record.meta_pre_token_ui_amount_string.is_empty()
    {
        return false;
    }
    if record.meta_post_token_balances_present
        || !record.meta_post_token_account_index.is_empty()
        || !record.meta_post_token_mint.is_empty()
        || !record.meta_post_token_owner.is_empty()
        || !record.meta_post_token_program_id.is_empty()
        || !record.meta_post_token_amount.is_empty()
        || !record.meta_post_token_decimals.is_empty()
        || !record.meta_post_token_ui_amount.is_empty()
        || !record.meta_post_token_ui_amount_string.is_empty()
    {
        return false;
    }
    if record.meta_rewards_present
        || !record.meta_reward_pubkey.is_empty()
        || !record.meta_reward_lamports.is_empty()
        || !record.meta_reward_post_balance.is_empty()
        || !record.meta_reward_type.is_empty()
        || !record.meta_reward_commission.is_empty()
    {
        return false;
    }
    if !record.meta_loaded_addresses_writable.is_empty()
        || !record.meta_loaded_addresses_readonly.is_empty()
    {
        return false;
    }
    if record.meta_return_data_present
        || record.meta_return_data_program_id.is_some()
        || record.meta_return_data_data.is_some()
    {
        return false;
    }
    if record.meta_compute_units_consumed.is_some() || record.meta_cost_units.is_some() {
        return false;
    }

    true
}

fn accounts_meta_is_missing(record: &StoredAccountsTransactionRecord) -> bool {
    if !record.meta_status_ok {
        return false;
    }
    if record.meta_err.is_some() {
        return false;
    }
    if record.meta_fee != 0 {
        return false;
    }
    if !record.meta_pre_balances.is_empty() || !record.meta_post_balances.is_empty() {
        return false;
    }
    if record.meta_pre_token_balances_present
        || !record.meta_pre_token_account_index.is_empty()
        || !record.meta_pre_token_mint.is_empty()
        || !record.meta_pre_token_owner.is_empty()
        || !record.meta_pre_token_program_id.is_empty()
        || !record.meta_pre_token_amount.is_empty()
        || !record.meta_pre_token_decimals.is_empty()
        || !record.meta_pre_token_ui_amount.is_empty()
        || !record.meta_pre_token_ui_amount_string.is_empty()
    {
        return false;
    }
    if record.meta_post_token_balances_present
        || !record.meta_post_token_account_index.is_empty()
        || !record.meta_post_token_mint.is_empty()
        || !record.meta_post_token_owner.is_empty()
        || !record.meta_post_token_program_id.is_empty()
        || !record.meta_post_token_amount.is_empty()
        || !record.meta_post_token_decimals.is_empty()
        || !record.meta_post_token_ui_amount.is_empty()
        || !record.meta_post_token_ui_amount_string.is_empty()
    {
        return false;
    }
    if record.meta_rewards_present
        || !record.meta_reward_pubkey.is_empty()
        || !record.meta_reward_lamports.is_empty()
        || !record.meta_reward_post_balance.is_empty()
        || !record.meta_reward_type.is_empty()
        || !record.meta_reward_commission.is_empty()
    {
        return false;
    }
    if !record.meta_loaded_addresses_writable.is_empty()
        || !record.meta_loaded_addresses_readonly.is_empty()
    {
        return false;
    }

    true
}

fn parse_transaction_error(
    err: &Option<String>,
) -> Result<TransactionError, TransactionHydrationError> {
    let err = err.as_ref().ok_or_else(|| {
        TransactionHydrationError::InvalidStoredMetadata(
            "meta_status_ok is false but meta_err is missing".to_string(),
        )
    })?;
    let err_trimmed = err.trim();
    let json_err = match serde_json::from_str::<TransactionError>(err_trimmed) {
        Ok(parsed) => return Ok(parsed),
        Err(err) => err,
    };

    if let Ok(value) = serde_json::from_str::<serde_json::Value>(err_trimmed)
        && let Some(as_string) = value.as_str()
        && let Some(parsed) = parse_transaction_error_display(as_string)
    {
        return Ok(parsed);
    }

    if let Some(parsed) = parse_transaction_error_display(err_trimmed) {
        return Ok(parsed);
    }

    Err(TransactionHydrationError::TransactionErrorParse(format!(
        "{json_err} (raw: {err})"
    )))
}

pub(crate) fn parse_transaction_error_display(err: &str) -> Option<TransactionError> {
    let err = err.trim();
    if let Some(rest) = err.strip_prefix("Error processing Instruction ") {
        let (index_str, instruction_err) = rest.split_once(": ")?;
        let index: u8 = index_str.parse().ok()?;
        let instruction_err = parse_instruction_error_display(instruction_err)?;
        return Some(TransactionError::InstructionError(index, instruction_err));
    }

    if let Some(index_str) = err
        .strip_prefix("Transaction contains a duplicate instruction (")
        .and_then(|rest| rest.strip_suffix(") that is not allowed"))
    {
        let index: u8 = index_str.parse().ok()?;
        return Some(TransactionError::DuplicateInstruction(index));
    }

    if let Some(index_str) = err
        .strip_prefix("Transaction results in an account (")
        .and_then(|rest| rest.strip_suffix(") with insufficient funds for rent"))
    {
        let account_index: u8 = index_str.parse().ok()?;
        return Some(TransactionError::InsufficientFundsForRent { account_index });
    }

    if let Some(index_str) = err
        .strip_prefix("Execution of the program referenced by account at index ")
        .and_then(|rest| rest.strip_suffix(" is temporarily restricted."))
    {
        let account_index: u8 = index_str.parse().ok()?;
        return Some(TransactionError::ProgramExecutionTemporarilyRestricted { account_index });
    }

    match err {
        "Account in use" => Some(TransactionError::AccountInUse),
        "Account loaded twice" => Some(TransactionError::AccountLoadedTwice),
        "Attempt to debit an account but found no record of a prior credit." => {
            Some(TransactionError::AccountNotFound)
        }
        "Attempt to load a program that does not exist" => {
            Some(TransactionError::ProgramAccountNotFound)
        }
        "Insufficient funds for fee" => Some(TransactionError::InsufficientFundsForFee),
        "This account may not be used to pay transaction fees" => {
            Some(TransactionError::InvalidAccountForFee)
        }
        "This transaction has already been processed" => Some(TransactionError::AlreadyProcessed),
        "Blockhash not found" => Some(TransactionError::BlockhashNotFound),
        "Loader call chain is too deep" => Some(TransactionError::CallChainTooDeep),
        "Transaction requires a fee but has no signature present" => {
            Some(TransactionError::MissingSignatureForFee)
        }
        "Transaction contains an invalid account reference" => {
            Some(TransactionError::InvalidAccountIndex)
        }
        "Transaction did not pass signature verification" => {
            Some(TransactionError::SignatureFailure)
        }
        "This program may not be used for executing instructions" => {
            Some(TransactionError::InvalidProgramForExecution)
        }
        "Transaction failed to sanitize accounts offsets correctly" => {
            Some(TransactionError::SanitizeFailure)
        }
        "Transactions are currently disabled due to cluster maintenance" => {
            Some(TransactionError::ClusterMaintenance)
        }
        "Transaction processing left an account with an outstanding borrowed reference" => {
            Some(TransactionError::AccountBorrowOutstanding)
        }
        "Transaction would exceed max Block Cost Limit" => {
            Some(TransactionError::WouldExceedMaxBlockCostLimit)
        }
        "Transaction version is unsupported" => Some(TransactionError::UnsupportedVersion),
        "Transaction loads a writable account that cannot be written" => {
            Some(TransactionError::InvalidWritableAccount)
        }
        "Transaction would exceed max account limit within the block" => {
            Some(TransactionError::WouldExceedMaxAccountCostLimit)
        }
        "Transaction would exceed account data limit within the block" => {
            Some(TransactionError::WouldExceedAccountDataBlockLimit)
        }
        "Transaction locked too many accounts" => Some(TransactionError::TooManyAccountLocks),
        "Transaction loads an address table account that doesn't exist" => {
            Some(TransactionError::AddressLookupTableNotFound)
        }
        "Transaction loads an address table account with an invalid owner" => {
            Some(TransactionError::InvalidAddressLookupTableOwner)
        }
        "Transaction loads an address table account with invalid data" => {
            Some(TransactionError::InvalidAddressLookupTableData)
        }
        "Transaction address table lookup uses an invalid index" => {
            Some(TransactionError::InvalidAddressLookupTableIndex)
        }
        "Transaction leaves an account with a lower balance than rent-exempt minimum" => {
            Some(TransactionError::InvalidRentPayingAccount)
        }
        "Transaction would exceed max Vote Cost Limit" => {
            Some(TransactionError::WouldExceedMaxVoteCostLimit)
        }
        "Transaction would exceed total account data limit" => {
            Some(TransactionError::WouldExceedAccountDataTotalLimit)
        }
        "Transaction exceeded max loaded accounts data size cap" => {
            Some(TransactionError::MaxLoadedAccountsDataSizeExceeded)
        }
        "LoadedAccountsDataSizeLimit set for transaction must be greater than 0." => {
            Some(TransactionError::InvalidLoadedAccountsDataSizeLimit)
        }
        "ResanitizationNeeded" => Some(TransactionError::ResanitizationNeeded),
        "Sum of account balances before and after transaction do not match" => {
            Some(TransactionError::UnbalancedTransaction)
        }
        "Program cache hit max limit" => Some(TransactionError::ProgramCacheHitMaxLimit),
        "CommitCancelled" => Some(TransactionError::CommitCancelled),
        _ => None,
    }
}

#[allow(deprecated)]
pub(crate) fn parse_instruction_error_display(err: &str) -> Option<InstructionError> {
    let err = err.trim();
    if let Some(rest) = err.strip_prefix("custom program error: ") {
        let hex = rest.strip_prefix("0x").unwrap_or(rest);
        let value = u32::from_str_radix(hex, 16).ok()?;
        return Some(InstructionError::Custom(value));
    }

    if err == "Failed to serialize or deserialize account data"
        || err.starts_with("Failed to serialize or deserialize account data: ")
    {
        return Some(InstructionError::BorshIoError);
    }

    match err {
        "generic instruction error" => Some(InstructionError::GenericError),
        "invalid program argument" => Some(InstructionError::InvalidArgument),
        "invalid instruction data" => Some(InstructionError::InvalidInstructionData),
        "invalid account data for instruction" => Some(InstructionError::InvalidAccountData),
        "account data too small for instruction" => Some(InstructionError::AccountDataTooSmall),
        "insufficient funds for instruction" => Some(InstructionError::InsufficientFunds),
        "incorrect program id for instruction" => Some(InstructionError::IncorrectProgramId),
        "missing required signature for instruction" => {
            Some(InstructionError::MissingRequiredSignature)
        }
        "instruction requires an uninitialized account" => {
            Some(InstructionError::AccountAlreadyInitialized)
        }
        "instruction requires an initialized account" => {
            Some(InstructionError::UninitializedAccount)
        }
        "sum of account balances before and after instruction do not match" => {
            Some(InstructionError::UnbalancedInstruction)
        }
        "instruction illegally modified the program id of an account" => {
            Some(InstructionError::ModifiedProgramId)
        }
        "instruction spent from the balance of an account it does not own" => {
            Some(InstructionError::ExternalAccountLamportSpend)
        }
        "instruction modified data of an account it does not own" => {
            Some(InstructionError::ExternalAccountDataModified)
        }
        "instruction changed the balance of a read-only account" => {
            Some(InstructionError::ReadonlyLamportChange)
        }
        "instruction modified data of a read-only account" => {
            Some(InstructionError::ReadonlyDataModified)
        }
        "instruction contains duplicate accounts" => Some(InstructionError::DuplicateAccountIndex),
        "instruction changed executable bit of an account" => {
            Some(InstructionError::ExecutableModified)
        }
        "instruction modified rent epoch of an account" => {
            Some(InstructionError::RentEpochModified)
        }
        "insufficient account keys for instruction" => Some(InstructionError::NotEnoughAccountKeys),
        "program other than the account's owner changed the size of the account data" => {
            Some(InstructionError::AccountDataSizeChanged)
        }
        "instruction expected an executable account" => {
            Some(InstructionError::AccountNotExecutable)
        }
        "instruction tries to borrow reference for an account which is already borrowed" => {
            Some(InstructionError::AccountBorrowFailed)
        }
        "instruction left account with an outstanding borrowed reference" => {
            Some(InstructionError::AccountBorrowOutstanding)
        }
        "instruction modifications of multiply-passed account differ" => {
            Some(InstructionError::DuplicateAccountOutOfSync)
        }
        "program returned invalid error code" => Some(InstructionError::InvalidError),
        "instruction changed executable accounts data" => {
            Some(InstructionError::ExecutableDataModified)
        }
        "instruction changed the balance of an executable account" => {
            Some(InstructionError::ExecutableLamportChange)
        }
        "executable accounts must be rent exempt" => {
            Some(InstructionError::ExecutableAccountNotRentExempt)
        }
        "Unsupported program id" => Some(InstructionError::UnsupportedProgramId),
        "Cross-program invocation call depth too deep" => Some(InstructionError::CallDepth),
        "An account required by the instruction is missing" => {
            Some(InstructionError::MissingAccount)
        }
        "Cross-program invocation reentrancy not allowed for this instruction" => {
            Some(InstructionError::ReentrancyNotAllowed)
        }
        "Length of the seed is too long for address generation" => {
            Some(InstructionError::MaxSeedLengthExceeded)
        }
        "Provided seeds do not result in a valid address" => Some(InstructionError::InvalidSeeds),
        "Failed to reallocate account data" => Some(InstructionError::InvalidRealloc),
        "Computational budget exceeded" => Some(InstructionError::ComputationalBudgetExceeded),
        "Cross-program invocation with unauthorized signer or writable account" => {
            Some(InstructionError::PrivilegeEscalation)
        }
        "Failed to create program execution environment" => {
            Some(InstructionError::ProgramEnvironmentSetupFailure)
        }
        "Program failed to complete" => Some(InstructionError::ProgramFailedToComplete),
        "Program failed to compile" => Some(InstructionError::ProgramFailedToCompile),
        "Account is immutable" => Some(InstructionError::Immutable),
        "Incorrect authority provided" => Some(InstructionError::IncorrectAuthority),
        "An account does not have enough lamports to be rent-exempt" => {
            Some(InstructionError::AccountNotRentExempt)
        }
        "Invalid account owner" => Some(InstructionError::InvalidAccountOwner),
        "Program arithmetic overflowed" => Some(InstructionError::ArithmeticOverflow),
        "Unsupported sysvar" => Some(InstructionError::UnsupportedSysvar),
        "Provided owner is not allowed" => Some(InstructionError::IllegalOwner),
        "Accounts data allocations exceeded the maximum allowed per transaction" => {
            Some(InstructionError::MaxAccountsDataAllocationsExceeded)
        }
        "Max accounts exceeded" => Some(InstructionError::MaxAccountsExceeded),
        "Max instruction trace length exceeded" => {
            Some(InstructionError::MaxInstructionTraceLengthExceeded)
        }
        "Builtin programs must consume compute units" => {
            Some(InstructionError::BuiltinProgramsMustConsumeComputeUnits)
        }
        _ => None,
    }
}

fn build_inner_instructions(
    record: &StoredTransactionRecord,
) -> Result<Vec<InnerInstructions>, TransactionHydrationError> {
    let group_len = record.meta_inner_instructions_index.len();
    if record.meta_inner_instructions_program_id_index.len() != group_len
        || record.meta_inner_instructions_accounts.len() != group_len
        || record.meta_inner_instructions_data.len() != group_len
        || record.meta_inner_instructions_stack_height.len() != group_len
    {
        return Err(TransactionHydrationError::InvalidStoredMetadata(format!(
            "inner instruction group length mismatch (index={}, program_id_index={}, accounts={}, data={}, stack_height={})",
            group_len,
            record.meta_inner_instructions_program_id_index.len(),
            record.meta_inner_instructions_accounts.len(),
            record.meta_inner_instructions_data.len(),
            record.meta_inner_instructions_stack_height.len(),
        )));
    }

    let mut groups = Vec::with_capacity(group_len);
    for idx in 0..group_len {
        let program_ids = &record.meta_inner_instructions_program_id_index[idx];
        let accounts = &record.meta_inner_instructions_accounts[idx];
        let data = &record.meta_inner_instructions_data[idx];
        let stack_heights = &record.meta_inner_instructions_stack_height[idx];

        if program_ids.len() != accounts.len()
            || program_ids.len() != data.len()
            || program_ids.len() != stack_heights.len()
        {
            return Err(TransactionHydrationError::InvalidStoredMetadata(format!(
                "inner instruction length mismatch in group {idx} (program_id_index={}, accounts={}, data={}, stack_height={})",
                program_ids.len(),
                accounts.len(),
                data.len(),
                stack_heights.len()
            )));
        }

        let mut instructions = Vec::with_capacity(program_ids.len());
        for inner_idx in 0..program_ids.len() {
            instructions.push(InnerInstruction {
                instruction: solana_sdk::message::compiled_instruction::CompiledInstruction {
                    program_id_index: program_ids[inner_idx],
                    accounts: accounts[inner_idx].clone(),
                    data: data[inner_idx].clone(),
                },
                stack_height: stack_heights[inner_idx],
            });
        }

        groups.push(InnerInstructions {
            index: record.meta_inner_instructions_index[idx],
            instructions,
        });
    }

    Ok(groups)
}

#[allow(clippy::too_many_arguments)]
fn build_token_balances(
    label: &str,
    account_index: &[u8],
    mint: &[[u8; 32]],
    owner: &[Option<[u8; 32]>],
    program_id: &[Option<[u8; 32]>],
    amount: &[String],
    decimals: &[u8],
    ui_amount: &[Option<f64>],
    ui_amount_string: &[String],
) -> Result<Vec<TransactionTokenBalance>, TransactionHydrationError> {
    let len = account_index.len();
    if mint.len() != len
        || owner.len() != len
        || program_id.len() != len
        || amount.len() != len
        || decimals.len() != len
        || ui_amount.len() != len
        || ui_amount_string.len() != len
    {
        return Err(TransactionHydrationError::InvalidStoredMetadata(format!(
            "{label} token balance length mismatch (account_index={len}, mint={}, owner={}, program_id={}, amount={}, decimals={}, ui_amount={}, ui_amount_string={})",
            mint.len(),
            owner.len(),
            program_id.len(),
            amount.len(),
            decimals.len(),
            ui_amount.len(),
            ui_amount_string.len(),
        )));
    }

    let mut balances = Vec::with_capacity(len);
    for idx in 0..len {
        let ui_token_amount = UiTokenAmount {
            amount: amount[idx].clone(),
            decimals: decimals[idx],
            ui_amount: ui_amount[idx],
            ui_amount_string: ui_amount_string[idx].clone(),
        };
        let owner_str = owner[idx]
            .map(|value| Pubkey::from(value).to_string())
            .unwrap_or_default();
        let program_id_str = program_id[idx]
            .map(|value| Pubkey::from(value).to_string())
            .unwrap_or_default();

        balances.push(TransactionTokenBalance {
            account_index: account_index[idx],
            mint: Pubkey::from(mint[idx]).to_string(),
            ui_token_amount,
            owner: owner_str,
            program_id: program_id_str,
        });
    }

    Ok(balances)
}

fn build_rewards(
    record: &StoredTransactionRecord,
) -> Result<Vec<Reward>, TransactionHydrationError> {
    build_rewards_from_fields(
        &record.meta_reward_pubkey,
        &record.meta_reward_lamports,
        &record.meta_reward_post_balance,
        &record.meta_reward_type,
        &record.meta_reward_commission,
    )
}

fn build_rewards_from_fields(
    pubkeys: &[String],
    lamports: &[i64],
    post_balances: &[u64],
    reward_types: &[Option<String>],
    commissions: &[Option<u8>],
) -> Result<Vec<Reward>, TransactionHydrationError> {
    let len = pubkeys.len();
    if lamports.len() != len
        || post_balances.len() != len
        || reward_types.len() != len
        || commissions.len() != len
    {
        return Err(TransactionHydrationError::InvalidStoredMetadata(format!(
            "reward length mismatch (pubkey={len}, lamports={}, post_balance={}, reward_type={}, commission={})",
            lamports.len(),
            post_balances.len(),
            reward_types.len(),
            commissions.len()
        )));
    }

    let mut rewards = Vec::with_capacity(len);
    for idx in 0..len {
        rewards.push(Reward {
            pubkey: pubkeys[idx].clone(),
            lamports: lamports[idx],
            post_balance: post_balances[idx],
            reward_type: parse_reward_type(&reward_types[idx])?,
            commission: commissions[idx],
            commission_bps: None,
        });
    }

    Ok(rewards)
}

pub(crate) fn parse_reward_type(
    value: &Option<String>,
) -> Result<Option<RewardType>, TransactionHydrationError> {
    let Some(value) = value.as_deref() else {
        return Ok(None);
    };

    // Historical data may use different casing ("fee" vs "Fee"). Solana reward types are ASCII.
    let value = value.trim();

    if value.eq_ignore_ascii_case("fee") {
        Ok(Some(RewardType::Fee))
    } else if value.eq_ignore_ascii_case("rent") {
        Ok(Some(RewardType::Rent))
    } else if value.eq_ignore_ascii_case("staking") {
        Ok(Some(RewardType::Staking))
    } else if value.eq_ignore_ascii_case("voting") {
        Ok(Some(RewardType::Voting))
    } else {
        Err(TransactionHydrationError::InvalidStoredMetadata(format!(
            "unknown reward type '{value}'"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::parse_reward_type;
    use solana_transaction_status::RewardType;

    #[test]
    fn parse_reward_type_is_case_insensitive() {
        let cases = [
            ("fee", RewardType::Fee),
            ("Fee", RewardType::Fee),
            ("FEE", RewardType::Fee),
            ("rent", RewardType::Rent),
            ("Rent", RewardType::Rent),
            ("RENT", RewardType::Rent),
            ("staking", RewardType::Staking),
            ("Staking", RewardType::Staking),
            ("STAKING", RewardType::Staking),
            ("voting", RewardType::Voting),
            ("Voting", RewardType::Voting),
            ("VOTING", RewardType::Voting),
        ];

        for (input, expected) in cases {
            let out = parse_reward_type(&Some(input.to_string())).unwrap();
            assert_eq!(out, Some(expected), "input={input}");
        }
    }
}
