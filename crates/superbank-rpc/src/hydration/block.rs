// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::solana_sdk::hash::Hash;
use crate::solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{
    BlockEncodingOptions, ConfirmedBlock, Reward, TransactionDetails, TransactionWithStatusMeta,
    UiConfirmedBlock, UiTransactionEncoding, VersionedTransactionWithStatusMeta,
};

use crate::clickhouse::{
    BlockMetadataRecord, StoredAccountsTransactionRecord, StoredBlockPayload, StoredBlockRecord,
    StoredTransactionRecord,
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
        transactions.push(hydrate_full_transaction(&tx_record)?);
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

fn hydrate_full_transaction(
    tx_record: &StoredTransactionRecord,
) -> Result<TransactionWithStatusMeta, BlockHydrationError> {
    let meta = build_transaction_status_meta(tx_record)?;
    Ok(match meta {
        Some(meta) => {
            let transaction = build_versioned_transaction(tx_record)?;
            TransactionWithStatusMeta::Complete(VersionedTransactionWithStatusMeta {
                transaction,
                meta,
            })
        }
        None => {
            let transaction = build_legacy_transaction(tx_record)?;
            TransactionWithStatusMeta::MissingMetadata(transaction)
        }
    })
}

fn hydrate_accounts_transaction(
    tx_record: &StoredAccountsTransactionRecord,
) -> Result<TransactionWithStatusMeta, BlockHydrationError> {
    let meta = build_transaction_status_meta_for_accounts(tx_record)?;
    Ok(match meta {
        Some(meta) => {
            let transaction = build_accounts_versioned_transaction(tx_record)?;
            TransactionWithStatusMeta::Complete(VersionedTransactionWithStatusMeta {
                transaction,
                meta,
            })
        }
        None => {
            let transaction = build_accounts_legacy_transaction(tx_record)?;
            TransactionWithStatusMeta::MissingMetadata(transaction)
        }
    })
}

/// Minimum transactions per parallel chunk; smaller blocks hydrate on one thread.
pub(crate) const MIN_TRANSACTIONS_PER_HYDRATION_CHUNK: usize = 128;

/// Number of contiguous chunks a block with `transaction_count` transactions is
/// split into, given at most `parallelism` blocking threads. 1 means sequential.
pub(crate) fn block_hydration_chunk_count(transaction_count: usize, parallelism: usize) -> usize {
    (transaction_count / MIN_TRANSACTIONS_PER_HYDRATION_CHUNK).clamp(1, parallelism.max(1))
}

/// Contiguous slice of a block's transactions, in block order.
pub(crate) enum BlockTransactionChunk {
    Full(Vec<StoredTransactionRecord>),
    Accounts(Vec<StoredAccountsTransactionRecord>),
}

pub(crate) struct SplitBlockPayload {
    pub(crate) metadata: BlockMetadataRecord,
    pub(crate) chunks: Vec<BlockTransactionChunk>,
}

/// Failure while building a block response outside `hydrate_block_payload`.
#[derive(Debug)]
pub(crate) enum BlockBuildError {
    Hydration(BlockHydrationError),
    Serialize(serde_json::Error),
}

/// The sequential path hydrates every transaction before encoding any, and
/// encodes every transaction before serializing. Its error is therefore the
/// lowest-index failure of the earliest failing stage; `Ord` encodes that.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum BlockChunkStage {
    Hydrate,
    Encode,
    Serialize,
}

#[derive(Debug)]
pub(crate) struct BlockChunkFailure {
    stage: BlockChunkStage,
    error: BlockBuildError,
}

/// Splits a full or accounts payload into `chunk_count` contiguous, balanced
/// chunks. Returns the payload unchanged when it cannot be split for the
/// requested detail level (the sequential path then reports the same error it
/// always has) or when `chunk_count < 2`.
pub(crate) fn split_block_payload(
    payload: StoredBlockPayload,
    transaction_details: TransactionDetails,
    chunk_count: usize,
) -> Result<SplitBlockPayload, Box<StoredBlockPayload>> {
    let len = payload.observed_transaction_count().unwrap_or(0);
    if chunk_count < 2 || len < chunk_count {
        return Err(Box::new(payload));
    }
    match (transaction_details, payload) {
        (TransactionDetails::Full, StoredBlockPayload::Full(record)) => Ok(SplitBlockPayload {
            metadata: record.metadata,
            chunks: split_contiguous(record.transactions, chunk_count)
                .into_iter()
                .map(BlockTransactionChunk::Full)
                .collect(),
        }),
        (TransactionDetails::Accounts, StoredBlockPayload::Full(record)) => Ok(SplitBlockPayload {
            metadata: record.metadata,
            chunks: split_contiguous(record.transactions, chunk_count)
                .into_iter()
                .map(|chunk| {
                    BlockTransactionChunk::Accounts(chunk.into_iter().map(Into::into).collect())
                })
                .collect(),
        }),
        (
            TransactionDetails::Accounts,
            StoredBlockPayload::Accounts {
                metadata,
                transactions,
            },
        ) => Ok(SplitBlockPayload {
            metadata,
            chunks: split_contiguous(transactions, chunk_count)
                .into_iter()
                .map(BlockTransactionChunk::Accounts)
                .collect(),
        }),
        (_, payload) => Err(Box::new(payload)),
    }
}

fn split_contiguous<T>(mut items: Vec<T>, chunk_count: usize) -> Vec<Vec<T>> {
    let base = items.len() / chunk_count;
    let extra = items.len() % chunk_count;
    let mut chunks = Vec::with_capacity(chunk_count);
    for index in (1..chunk_count).rev() {
        let size = base + usize::from(index < extra);
        chunks.push(items.split_off(items.len() - size));
    }
    chunks.push(items);
    chunks.reverse();
    chunks
}

/// Hydrates, encodes and serializes one chunk. On success returns the chunk's
/// transactions as comma-separated JSON values (no surrounding brackets).
pub(crate) fn hydrate_serialize_block_chunk(
    chunk: BlockTransactionChunk,
    encoding: UiTransactionEncoding,
    show_rewards: bool,
    max_supported_transaction_version: Option<u8>,
) -> Result<Vec<u8>, BlockChunkFailure> {
    let hydrate_failure = |error| BlockChunkFailure {
        stage: BlockChunkStage::Hydrate,
        error: BlockBuildError::Hydration(error),
    };
    let (transactions, encoding, transaction_details) = match chunk {
        BlockTransactionChunk::Full(records) => {
            let mut transactions = Vec::with_capacity(records.len());
            for tx_record in &records {
                transactions.push(hydrate_full_transaction(tx_record).map_err(hydrate_failure)?);
            }
            (transactions, encoding, TransactionDetails::Full)
        }
        BlockTransactionChunk::Accounts(records) => {
            let mut transactions = Vec::with_capacity(records.len());
            for tx_record in &records {
                transactions
                    .push(hydrate_accounts_transaction(tx_record).map_err(hydrate_failure)?);
            }
            // `encode_accounts_block` always encodes accounts as JSON.
            (
                transactions,
                UiTransactionEncoding::Json,
                TransactionDetails::Accounts,
            )
        }
    };

    // Encode through the same library entry point as the sequential path so
    // per-transaction version checks and errors are identical.
    let encoded = ConfirmedBlock {
        previous_blockhash: String::new(),
        blockhash: String::new(),
        parent_slot: 0,
        transactions,
        rewards: Vec::new(),
        num_partitions: None,
        block_time: None,
        block_height: None,
    }
    .encode_with_options(
        encoding,
        BlockEncodingOptions {
            transaction_details,
            show_rewards,
            max_supported_transaction_version,
        },
    )
    .map_err(|error| BlockChunkFailure {
        stage: BlockChunkStage::Encode,
        error: BlockBuildError::Hydration(BlockHydrationError::from(error)),
    })?;

    let mut bytes = Vec::new();
    for (index, transaction) in encoded.transactions.unwrap_or_default().iter().enumerate() {
        if index > 0 {
            bytes.push(b',');
        }
        serde_json::to_writer(&mut bytes, transaction).map_err(|error| BlockChunkFailure {
            stage: BlockChunkStage::Serialize,
            error: BlockBuildError::Serialize(error),
        })?;
    }
    Ok(bytes)
}

const TRANSACTIONS_PLACEHOLDER: &[u8] = b"\"transactions\":[]";

/// Builds the serialized block from per-chunk results (in block order) so the
/// bytes and the reported error match `serde_json::to_vec` of
/// `hydrate_block_payload` for the unsplit payload.
pub(crate) fn assemble_block_chunks(
    metadata: BlockMetadataRecord,
    encoding: UiTransactionEncoding,
    transaction_details: TransactionDetails,
    show_rewards: bool,
    max_supported_transaction_version: Option<u8>,
    chunks: Vec<Result<Vec<u8>, BlockChunkFailure>>,
) -> Result<Vec<u8>, BlockBuildError> {
    // Block rewards are built before any transaction on the sequential path.
    let rewards = if show_rewards {
        build_block_rewards(&metadata).map_err(BlockBuildError::Hydration)?
    } else {
        Vec::new()
    };

    let mut first_failure: Option<BlockChunkFailure> = None;
    let mut parts = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        match chunk {
            Ok(bytes) => parts.push(bytes),
            // Chunks arrive in block order, so a strictly earlier stage wins and
            // the lowest chunk index breaks ties.
            Err(failure) => {
                if first_failure
                    .as_ref()
                    .is_none_or(|current| failure.stage < current.stage)
                {
                    first_failure = Some(failure);
                }
            }
        }
    }
    if let Some(failure) = first_failure {
        return Err(failure.error);
    }

    let encoding = match transaction_details {
        TransactionDetails::Accounts => UiTransactionEncoding::Json,
        _ => encoding,
    };
    let template = confirmed_block(metadata, Vec::new(), rewards)
        .encode_with_options(
            encoding,
            BlockEncodingOptions {
                transaction_details,
                show_rewards,
                max_supported_transaction_version,
            },
        )
        .map_err(|error| BlockBuildError::Hydration(BlockHydrationError::from(error)))?;
    let template = serde_json::to_vec(&template).map_err(BlockBuildError::Serialize)?;

    // `transactions` follows `previousBlockhash`, `blockhash` (base58) and
    // `parentSlot` (a number), none of which can contain a quote, so the first
    // match is the field itself.
    let position = template
        .windows(TRANSACTIONS_PLACEHOLDER.len())
        .position(|window| window == TRANSACTIONS_PLACEHOLDER)
        .ok_or_else(|| {
            BlockBuildError::Hydration(BlockHydrationError::InvalidBlockMetadata(
                "serialized block template has no transactions field".to_string(),
            ))
        })?;
    let split_at = position + TRANSACTIONS_PLACEHOLDER.len() - 1;
    let body_len: usize = parts.iter().map(|part| part.len() + 1).sum();
    let mut out = Vec::with_capacity(template.len() + body_len);
    out.extend_from_slice(&template[..split_at]);
    let mut wrote_any = false;
    for part in parts.iter().filter(|part| !part.is_empty()) {
        if wrote_any {
            out.push(b',');
        }
        out.extend_from_slice(part);
        wrote_any = true;
    }
    out.extend_from_slice(&template[split_at..]);
    Ok(out)
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
        encoded_transactions.push(hydrate_accounts_transaction(&tx_record)?);
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
