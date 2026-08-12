// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{
    BlockEncodingOptions, ConfirmedBlock, Reward, TransactionDetails, TransactionWithStatusMeta,
    UiConfirmedBlock, UiTransactionEncoding, VersionedTransactionWithStatusMeta,
};

use crate::clickhouse::{
    BlockMetadataRecord, StoredAccountsTransactionRecord, StoredBlockPayload, StoredBlockRecord,
};
use crate::hydration::errors::{BlockHydrationError, TransactionHydrationError};
use crate::hydration::meta::{
    build_transaction_status_meta, build_transaction_status_meta_for_accounts, parse_reward_type,
};
use crate::hydration::transaction::{
    build_accounts_legacy_transaction, build_accounts_versioned_transaction,
    build_legacy_transaction, build_versioned_transaction,
};

pub(crate) fn hydrate_block_payload(
    payload: StoredBlockPayload,
    encoding: UiTransactionEncoding,
    transaction_details: TransactionDetails,
    show_rewards: bool,
    max_supported_transaction_version: Option<u8>,
) -> Result<UiConfirmedBlock, BlockHydrationError> {
    match transaction_details {
        TransactionDetails::None => {
            encode_metadata_only_block(payload.into_metadata(), show_rewards)
        }
        TransactionDetails::Signatures => {
            let (metadata, signatures) = payload.into_signatures()?;
            encode_block_signatures(metadata, signatures, show_rewards)
        }
        TransactionDetails::Accounts => {
            let (metadata, transactions) = payload.into_accounts()?;
            encode_accounts_block(
                metadata,
                transactions,
                show_rewards,
                max_supported_transaction_version,
            )
        }
        TransactionDetails::Full => {
            let record = payload.into_full()?;
            hydrate_full_block_record(
                record,
                encoding,
                show_rewards,
                max_supported_transaction_version,
            )
        }
    }
}

#[cfg(test)]
pub(crate) fn hydrate_block_record(
    record: StoredBlockRecord,
    encoding: UiTransactionEncoding,
    transaction_details: TransactionDetails,
    show_rewards: bool,
    max_supported_transaction_version: Option<u8>,
) -> Result<UiConfirmedBlock, BlockHydrationError> {
    hydrate_block_payload(
        StoredBlockPayload::Full(record),
        encoding,
        transaction_details,
        show_rewards,
        max_supported_transaction_version,
    )
}

fn hydrate_full_block_record(
    record: StoredBlockRecord,
    encoding: UiTransactionEncoding,
    show_rewards: bool,
    max_supported_transaction_version: Option<u8>,
) -> Result<UiConfirmedBlock, BlockHydrationError> {
    let metadata = record.metadata;
    let rewards = if show_rewards {
        build_block_rewards(&metadata)?
    } else {
        Vec::new()
    };

    let mut transactions = Vec::with_capacity(record.transactions.len());
    for tx_record in record.transactions {
        let meta = build_transaction_status_meta(&tx_record)?;
        let tx_with_meta = match meta {
            Some(meta) => {
                let transaction = build_versioned_transaction(&tx_record)?;
                TransactionWithStatusMeta::Complete(VersionedTransactionWithStatusMeta {
                    transaction,
                    meta,
                })
            }
            None => {
                let transaction = build_legacy_transaction(&tx_record)?;
                TransactionWithStatusMeta::MissingMetadata(transaction)
            }
        };
        transactions.push(tx_with_meta);
    }

    confirmed_block(metadata, transactions, rewards)
        .encode_with_options(
            encoding,
            BlockEncodingOptions {
                transaction_details: TransactionDetails::Full,
                show_rewards,
                max_supported_transaction_version,
            },
        )
        .map_err(BlockHydrationError::from)
}

pub(crate) fn encode_metadata_only_block(
    metadata: BlockMetadataRecord,
    show_rewards: bool,
) -> Result<UiConfirmedBlock, BlockHydrationError> {
    let rewards = if show_rewards {
        Some(build_block_rewards(&metadata)?)
    } else {
        None
    };

    Ok(block_template(metadata, rewards))
}

pub(crate) fn encode_block_signatures(
    metadata: BlockMetadataRecord,
    signatures: Vec<String>,
    show_rewards: bool,
) -> Result<UiConfirmedBlock, BlockHydrationError> {
    let rewards = if show_rewards {
        Some(build_block_rewards(&metadata)?)
    } else {
        None
    };

    let mut block = block_template(metadata, rewards);
    block.signatures = Some(signatures);
    Ok(block)
}

pub(crate) fn encode_accounts_block(
    metadata: BlockMetadataRecord,
    transactions: Vec<StoredAccountsTransactionRecord>,
    show_rewards: bool,
    max_supported_transaction_version: Option<u8>,
) -> Result<UiConfirmedBlock, BlockHydrationError> {
    let rewards = if show_rewards {
        build_block_rewards(&metadata)?
    } else {
        Vec::new()
    };

    let mut encoded_transactions = Vec::with_capacity(transactions.len());
    for tx_record in transactions {
        let meta = build_transaction_status_meta_for_accounts(&tx_record)?;
        let tx_with_meta = match meta {
            Some(meta) => {
                let transaction = build_accounts_versioned_transaction(&tx_record)?;
                TransactionWithStatusMeta::Complete(VersionedTransactionWithStatusMeta {
                    transaction,
                    meta,
                })
            }
            None => {
                let transaction = build_accounts_legacy_transaction(&tx_record)?;
                TransactionWithStatusMeta::MissingMetadata(transaction)
            }
        };
        encoded_transactions.push(tx_with_meta);
    }

    confirmed_block(metadata, encoded_transactions, rewards)
        .encode_with_options(
            UiTransactionEncoding::Json,
            BlockEncodingOptions {
                transaction_details: TransactionDetails::Accounts,
                show_rewards,
                max_supported_transaction_version,
            },
        )
        .map_err(BlockHydrationError::from)
}

fn block_template(metadata: BlockMetadataRecord, rewards: Option<Vec<Reward>>) -> UiConfirmedBlock {
    let block_time = metadata.block_time.filter(|value| *value != 0);
    let block_height = match metadata.block_height {
        Some(0) if metadata.slot != 0 => None,
        other => other,
    };

    UiConfirmedBlock {
        previous_blockhash: Hash::from(metadata.parent_blockhash).to_string(),
        blockhash: Hash::from(metadata.blockhash).to_string(),
        parent_slot: metadata.parent_slot,
        transactions: None,
        signatures: None,
        rewards,
        num_reward_partitions: metadata.rewards_num_partitions,
        block_time,
        block_height,
    }
}

fn confirmed_block(
    metadata: BlockMetadataRecord,
    transactions: Vec<TransactionWithStatusMeta>,
    rewards: Vec<Reward>,
) -> ConfirmedBlock {
    let block_time = metadata.block_time.filter(|value| *value != 0);
    let block_height = match metadata.block_height {
        Some(0) if metadata.slot != 0 => None,
        other => other,
    };

    ConfirmedBlock {
        previous_blockhash: Hash::from(metadata.parent_blockhash).to_string(),
        blockhash: Hash::from(metadata.blockhash).to_string(),
        parent_slot: metadata.parent_slot,
        transactions,
        rewards,
        num_partitions: metadata.rewards_num_partitions,
        block_time,
        block_height,
    }
}

fn build_block_rewards(metadata: &BlockMetadataRecord) -> Result<Vec<Reward>, BlockHydrationError> {
    if !metadata.rewards_present {
        if !metadata.rewards_pubkey.is_empty()
            || !metadata.rewards_lamports.is_empty()
            || !metadata.rewards_post_balance.is_empty()
            || !metadata.rewards_type.is_empty()
            || !metadata.rewards_commission.is_empty()
            || !metadata.rewards_commission_bps.is_empty()
        {
            return Err(BlockHydrationError::InvalidBlockMetadata(
                "rewards fields populated without rewards_present".to_string(),
            ));
        }
        return Ok(Vec::new());
    }

    let len = metadata.rewards_pubkey.len();
    if metadata.rewards_lamports.len() != len
        || metadata.rewards_post_balance.len() != len
        || metadata.rewards_type.len() != len
        || metadata.rewards_commission.len() != len
        || (!metadata.rewards_commission_bps.is_empty()
            && metadata.rewards_commission_bps.len() != len)
    {
        return Err(BlockHydrationError::InvalidBlockMetadata(format!(
            "reward length mismatch (pubkey={len}, lamports={}, post_balance={}, reward_type={}, commission={}, commission_bps={})",
            metadata.rewards_lamports.len(),
            metadata.rewards_post_balance.len(),
            metadata.rewards_type.len(),
            metadata.rewards_commission.len(),
            metadata.rewards_commission_bps.len()
        )));
    }

    let mut rewards = Vec::with_capacity(len);
    for idx in 0..len {
        rewards.push(Reward {
            pubkey: Pubkey::from(metadata.rewards_pubkey[idx]).to_string(),
            lamports: metadata.rewards_lamports[idx],
            post_balance: metadata.rewards_post_balance[idx],
            reward_type: parse_reward_type(&metadata.rewards_type[idx])?,
            commission: metadata.rewards_commission[idx],
            commission_bps: metadata.rewards_commission_bps.get(idx).copied().flatten(),
        });
    }

    Ok(rewards)
}

trait StoredBlockPayloadExt {
    fn into_metadata(self) -> BlockMetadataRecord;
    fn into_signatures(self) -> Result<(BlockMetadataRecord, Vec<String>), BlockHydrationError>;
    fn into_accounts(
        self,
    ) -> Result<(BlockMetadataRecord, Vec<StoredAccountsTransactionRecord>), BlockHydrationError>;
    fn into_full(self) -> Result<StoredBlockRecord, BlockHydrationError>;
}

impl StoredBlockPayloadExt for StoredBlockPayload {
    fn into_metadata(self) -> BlockMetadataRecord {
        match self {
            StoredBlockPayload::Metadata(metadata) => metadata,
            StoredBlockPayload::Signatures { metadata, .. } => metadata,
            StoredBlockPayload::Accounts { metadata, .. } => metadata,
            StoredBlockPayload::Full(record) => record.metadata,
        }
    }

    fn into_signatures(self) -> Result<(BlockMetadataRecord, Vec<String>), BlockHydrationError> {
        match self {
            StoredBlockPayload::Signatures {
                metadata,
                signatures,
            } => Ok((metadata, signatures)),
            StoredBlockPayload::Full(record) => {
                let mut signatures = Vec::with_capacity(record.transactions.len());
                for tx in &record.transactions {
                    signatures.push(primary_signature_string(&tx.tx_signatures)?);
                }
                Ok((record.metadata, signatures))
            }
            _ => Err(BlockHydrationError::Transaction(
                TransactionHydrationError::InvalidStoredTransaction(
                    "signatures block payload required for transactionDetails=signatures"
                        .to_string(),
                ),
            )),
        }
    }

    fn into_accounts(
        self,
    ) -> Result<(BlockMetadataRecord, Vec<StoredAccountsTransactionRecord>), BlockHydrationError>
    {
        match self {
            StoredBlockPayload::Accounts {
                metadata,
                transactions,
            } => Ok((metadata, transactions)),
            StoredBlockPayload::Full(record) => Ok((
                record.metadata,
                record.transactions.into_iter().map(Into::into).collect(),
            )),
            _ => Err(BlockHydrationError::Transaction(
                TransactionHydrationError::InvalidStoredTransaction(
                    "accounts block payload required for transactionDetails=accounts".to_string(),
                ),
            )),
        }
    }

    fn into_full(self) -> Result<StoredBlockRecord, BlockHydrationError> {
        match self {
            StoredBlockPayload::Full(record) => Ok(record),
            _ => Err(BlockHydrationError::Transaction(
                TransactionHydrationError::InvalidStoredTransaction(
                    "full block payload required for transactionDetails=full".to_string(),
                ),
            )),
        }
    }
}

fn primary_signature_string(signatures: &[[u8; 64]]) -> Result<String, BlockHydrationError> {
    let signature = signatures.first().ok_or_else(|| {
        BlockHydrationError::Transaction(TransactionHydrationError::InvalidStoredTransaction(
            "transaction is missing primary signature".to_string(),
        ))
    })?;

    Ok(bs58::encode(signature).into_string())
}
