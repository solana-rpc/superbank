// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::solana_sdk::{
    hash::Hash,
    message::{
        Message, MessageHeader, VersionedMessage,
        compiled_instruction::CompiledInstruction,
        v0::{Message as V0Message, MessageAddressTableLookup},
        v1::{Message as V1Message, TransactionConfig},
    },
    pubkey::Pubkey,
    signature::Signature,
    transaction::{Transaction, VersionedTransaction},
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, TransactionWithStatusMeta, UiTransactionEncoding,
    VersionedTransactionWithStatusMeta,
};
use tracing::warn;

use crate::clickhouse::{StoredAccountsTransactionRecord, StoredTransactionRecord};
use crate::hydration::errors::TransactionHydrationError;
use crate::hydration::meta::build_transaction_status_meta;

pub(crate) fn hydrate_transaction_record(
    record: &StoredTransactionRecord,
    encoding: UiTransactionEncoding,
    max_supported_transaction_version: Option<u8>,
) -> Result<EncodedConfirmedTransactionWithStatusMeta, TransactionHydrationError> {
    let meta = build_transaction_status_meta(record)?;
    let tx_with_meta = match meta {
        Some(meta) => {
            let transaction = build_versioned_transaction(record)?;
            TransactionWithStatusMeta::Complete(VersionedTransactionWithStatusMeta {
                transaction,
                meta,
            })
        }
        None => {
            let transaction = build_legacy_transaction(record)?;
            TransactionWithStatusMeta::MissingMetadata(transaction)
        }
    };
    let block_time = record.block_time.filter(|value| *value != 0);
    let transaction = tx_with_meta
        .encode(encoding, max_supported_transaction_version, true)
        .map_err(TransactionHydrationError::from)?;
    let encoded = EncodedConfirmedTransactionWithStatusMeta {
        slot: record.slot,
        transaction,
        block_time,
        transaction_index: Some(record.slot_idx),
    };
    Ok(encoded)
}

pub(crate) fn build_versioned_transaction(
    record: &StoredTransactionRecord,
) -> Result<VersionedTransaction, TransactionHydrationError> {
    let header = MessageHeader {
        num_required_signatures: record.tx_num_required_signatures,
        num_readonly_signed_accounts: record.tx_num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: record.tx_num_readonly_unsigned_accounts,
    };

    let signatures = record
        .tx_signatures
        .iter()
        .map(|sig| Signature::from(*sig))
        .collect::<Vec<_>>();
    if signatures.len() != header.num_required_signatures as usize {
        return Err(TransactionHydrationError::InvalidStoredTransaction(
            format!(
                "signature count {} does not match num_required_signatures {}",
                signatures.len(),
                header.num_required_signatures
            ),
        ));
    }

    let account_keys = record
        .tx_account_keys
        .iter()
        .map(|key| Pubkey::from(*key))
        .collect::<Vec<_>>();
    if account_keys.len() < header.num_required_signatures as usize {
        return Err(TransactionHydrationError::InvalidStoredTransaction(
            format!(
                "account_keys length {} is smaller than num_required_signatures {}",
                account_keys.len(),
                header.num_required_signatures
            ),
        ));
    }

    let recent_blockhash = Hash::from(record.tx_recent_blockhash);
    let instructions = build_compiled_instructions(
        &record.tx_instructions_program_id_index,
        &record.tx_instructions_accounts,
        &record.tx_instructions_data,
        "transaction",
    )?;

    let message = match record.tx_version {
        None => {
            if record.tx_address_table_lookups_present
                || !record.tx_address_table_lookup_account_key.is_empty()
                || !record.tx_address_table_lookup_writable_indexes.is_empty()
                || !record.tx_address_table_lookup_readonly_indexes.is_empty()
            {
                return Err(TransactionHydrationError::InvalidStoredTransaction(
                    "legacy transaction contains address table lookups".to_string(),
                ));
            }

            VersionedMessage::Legacy(Message {
                header,
                account_keys,
                recent_blockhash,
                instructions,
            })
        }
        Some(0) => {
            if !record.tx_address_table_lookups_present
                && (!record.tx_address_table_lookup_account_key.is_empty()
                    || !record.tx_address_table_lookup_writable_indexes.is_empty()
                    || !record.tx_address_table_lookup_readonly_indexes.is_empty())
            {
                warn!("v0 transaction address table lookups present but flag is false; proceeding");
            }
            let address_table_lookups = build_address_table_lookups(record)?;
            VersionedMessage::V0(V0Message {
                header,
                account_keys,
                recent_blockhash,
                instructions,
                address_table_lookups,
            })
        }
        Some(1) => {
            if record.tx_address_table_lookups_present
                || !record.tx_address_table_lookup_account_key.is_empty()
                || !record.tx_address_table_lookup_writable_indexes.is_empty()
                || !record.tx_address_table_lookup_readonly_indexes.is_empty()
            {
                return Err(TransactionHydrationError::InvalidStoredTransaction(
                    "v1 transaction contains address table lookups".to_string(),
                ));
            }
            VersionedMessage::V1(V1Message {
                header,
                config: TransactionConfig {
                    priority_fee: record.tx_config_priority_fee,
                    compute_unit_limit: record.tx_config_compute_unit_limit,
                    loaded_accounts_data_size_limit: record
                        .tx_config_loaded_accounts_data_size_limit,
                    heap_size: record.tx_config_heap_size,
                },
                lifetime_specifier: recent_blockhash,
                account_keys,
                instructions,
            })
        }
        Some(version) => {
            return Err(TransactionHydrationError::Encode(
                solana_transaction_status::EncodeError::UnsupportedTransactionVersion(version),
            ));
        }
    };

    Ok(VersionedTransaction {
        signatures,
        message,
    })
}

pub(crate) fn build_accounts_versioned_transaction(
    record: &StoredAccountsTransactionRecord,
) -> Result<VersionedTransaction, TransactionHydrationError> {
    let header = MessageHeader {
        num_required_signatures: record.tx_num_required_signatures,
        num_readonly_signed_accounts: record.tx_num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: record.tx_num_readonly_unsigned_accounts,
    };

    let signatures = record
        .tx_signatures
        .iter()
        .map(|sig| Signature::from(*sig))
        .collect::<Vec<_>>();
    if signatures.len() != header.num_required_signatures as usize {
        return Err(TransactionHydrationError::InvalidStoredTransaction(
            format!(
                "signature count {} does not match num_required_signatures {}",
                signatures.len(),
                header.num_required_signatures
            ),
        ));
    }

    let account_keys = record
        .tx_account_keys
        .iter()
        .map(|key| Pubkey::from(*key))
        .collect::<Vec<_>>();
    if account_keys.len() < header.num_required_signatures as usize {
        return Err(TransactionHydrationError::InvalidStoredTransaction(
            format!(
                "account_keys length {} is smaller than num_required_signatures {}",
                account_keys.len(),
                header.num_required_signatures
            ),
        ));
    }

    let instructions =
        build_accounts_compiled_instructions(&record.tx_instructions_program_id_index);
    let message = match record.tx_version {
        None => {
            if !record.meta_loaded_addresses_writable.is_empty()
                || !record.meta_loaded_addresses_readonly.is_empty()
            {
                return Err(TransactionHydrationError::InvalidStoredTransaction(
                    "legacy transaction contains loaded addresses".to_string(),
                ));
            }
            VersionedMessage::Legacy(Message {
                header,
                account_keys,
                recent_blockhash: Hash::default(),
                instructions,
            })
        }
        Some(0) => VersionedMessage::V0(V0Message {
            header,
            account_keys,
            recent_blockhash: Hash::default(),
            instructions,
            address_table_lookups: Vec::new(),
        }),
        Some(1) => VersionedMessage::V1(V1Message {
            header,
            config: TransactionConfig::empty(),
            lifetime_specifier: Hash::default(),
            account_keys,
            instructions,
        }),
        Some(version) => {
            return Err(TransactionHydrationError::from(
                solana_transaction_status::EncodeError::UnsupportedTransactionVersion(version),
            ));
        }
    };

    Ok(VersionedTransaction {
        signatures,
        message,
    })
}

pub(super) fn build_legacy_transaction(
    record: &StoredTransactionRecord,
) -> Result<Transaction, TransactionHydrationError> {
    if record.tx_version.is_some() {
        return Err(TransactionHydrationError::InvalidStoredTransaction(
            "missing metadata for non-legacy transaction".to_string(),
        ));
    }

    if record.tx_address_table_lookups_present
        || !record.tx_address_table_lookup_account_key.is_empty()
        || !record.tx_address_table_lookup_writable_indexes.is_empty()
        || !record.tx_address_table_lookup_readonly_indexes.is_empty()
    {
        return Err(TransactionHydrationError::InvalidStoredTransaction(
            "legacy transaction contains address table lookups".to_string(),
        ));
    }

    let header = MessageHeader {
        num_required_signatures: record.tx_num_required_signatures,
        num_readonly_signed_accounts: record.tx_num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: record.tx_num_readonly_unsigned_accounts,
    };

    let signatures = record
        .tx_signatures
        .iter()
        .map(|sig| Signature::from(*sig))
        .collect::<Vec<_>>();
    if signatures.len() != header.num_required_signatures as usize {
        return Err(TransactionHydrationError::InvalidStoredTransaction(
            format!(
                "signature count {} does not match num_required_signatures {}",
                signatures.len(),
                header.num_required_signatures
            ),
        ));
    }

    let account_keys = record
        .tx_account_keys
        .iter()
        .map(|key| Pubkey::from(*key))
        .collect::<Vec<_>>();
    if account_keys.len() < header.num_required_signatures as usize {
        return Err(TransactionHydrationError::InvalidStoredTransaction(
            format!(
                "account_keys length {} is smaller than num_required_signatures {}",
                account_keys.len(),
                header.num_required_signatures
            ),
        ));
    }

    let recent_blockhash = Hash::from(record.tx_recent_blockhash);
    let instructions = build_compiled_instructions(
        &record.tx_instructions_program_id_index,
        &record.tx_instructions_accounts,
        &record.tx_instructions_data,
        "transaction",
    )?;

    Ok(Transaction {
        signatures,
        message: Message {
            header,
            account_keys,
            recent_blockhash,
            instructions,
        },
    })
}

pub(crate) fn build_accounts_legacy_transaction(
    record: &StoredAccountsTransactionRecord,
) -> Result<Transaction, TransactionHydrationError> {
    if record.tx_version.is_some() {
        return Err(TransactionHydrationError::InvalidStoredTransaction(
            "missing metadata for non-legacy transaction".to_string(),
        ));
    }

    if !record.meta_loaded_addresses_writable.is_empty()
        || !record.meta_loaded_addresses_readonly.is_empty()
    {
        return Err(TransactionHydrationError::InvalidStoredTransaction(
            "legacy transaction contains loaded addresses".to_string(),
        ));
    }

    let header = MessageHeader {
        num_required_signatures: record.tx_num_required_signatures,
        num_readonly_signed_accounts: record.tx_num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: record.tx_num_readonly_unsigned_accounts,
    };

    let signatures = record
        .tx_signatures
        .iter()
        .map(|sig| Signature::from(*sig))
        .collect::<Vec<_>>();
    if signatures.len() != header.num_required_signatures as usize {
        return Err(TransactionHydrationError::InvalidStoredTransaction(
            format!(
                "signature count {} does not match num_required_signatures {}",
                signatures.len(),
                header.num_required_signatures
            ),
        ));
    }

    let account_keys = record
        .tx_account_keys
        .iter()
        .map(|key| Pubkey::from(*key))
        .collect::<Vec<_>>();
    if account_keys.len() < header.num_required_signatures as usize {
        return Err(TransactionHydrationError::InvalidStoredTransaction(
            format!(
                "account_keys length {} is smaller than num_required_signatures {}",
                account_keys.len(),
                header.num_required_signatures
            ),
        ));
    }

    Ok(Transaction {
        signatures,
        message: Message {
            header,
            account_keys,
            recent_blockhash: Hash::default(),
            instructions: build_accounts_compiled_instructions(
                &record.tx_instructions_program_id_index,
            ),
        },
    })
}

fn build_accounts_compiled_instructions(program_id_indexes: &[u8]) -> Vec<CompiledInstruction> {
    let mut instructions = Vec::with_capacity(program_id_indexes.len());
    for program_id_index in program_id_indexes {
        instructions.push(CompiledInstruction {
            program_id_index: *program_id_index,
            accounts: Vec::new(),
            data: Vec::new(),
        });
    }
    instructions
}

fn build_compiled_instructions(
    program_id_indexes: &[u8],
    accounts: &[Vec<u8>],
    data: &[Vec<u8>],
    context: &str,
) -> Result<Vec<CompiledInstruction>, TransactionHydrationError> {
    if program_id_indexes.len() != accounts.len() || program_id_indexes.len() != data.len() {
        return Err(TransactionHydrationError::InvalidStoredTransaction(
            format!(
                "{context} instruction array length mismatch (program_id_index={}, accounts={}, data={})",
                program_id_indexes.len(),
                accounts.len(),
                data.len()
            ),
        ));
    }

    let mut instructions = Vec::with_capacity(program_id_indexes.len());
    for idx in 0..program_id_indexes.len() {
        instructions.push(CompiledInstruction {
            program_id_index: program_id_indexes[idx],
            accounts: accounts[idx].clone(),
            data: data[idx].clone(),
        });
    }

    Ok(instructions)
}

fn build_address_table_lookups(
    record: &StoredTransactionRecord,
) -> Result<Vec<MessageAddressTableLookup>, TransactionHydrationError> {
    let len = record.tx_address_table_lookup_account_key.len();
    if record.tx_address_table_lookup_writable_indexes.len() != len
        || record.tx_address_table_lookup_readonly_indexes.len() != len
    {
        return Err(TransactionHydrationError::InvalidStoredTransaction(
            format!(
                "address table lookup length mismatch (account_key={}, writable_indexes={}, readonly_indexes={})",
                len,
                record.tx_address_table_lookup_writable_indexes.len(),
                record.tx_address_table_lookup_readonly_indexes.len()
            ),
        ));
    }

    if len == 0 {
        return Ok(Vec::new());
    }

    let mut lookups = Vec::with_capacity(len);
    for idx in 0..len {
        lookups.push(MessageAddressTableLookup {
            account_key: Pubkey::from(record.tx_address_table_lookup_account_key[idx]),
            writable_indexes: record.tx_address_table_lookup_writable_indexes[idx].clone(),
            readonly_indexes: record.tx_address_table_lookup_readonly_indexes[idx].clone(),
        });
    }

    Ok(lookups)
}
