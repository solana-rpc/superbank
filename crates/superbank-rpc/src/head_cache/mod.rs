// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use solana_commitment_config::CommitmentLevel;
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use solana_transaction_status::TransactionDetails;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::clickhouse::{
    BlockMetadataRecord, SlotBoundary, StoredAccountsTransactionRecord, StoredBlockPayload,
    StoredBlockRecord, StoredTransactionRecord, extract_memo,
};

mod convert;
pub(crate) mod dragonsmouth;

#[derive(Debug, Clone, Copy)]
pub(crate) struct SlotIndex {
    pub(crate) slot: u64,
    pub(crate) idx: u32,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct HeadSigKey {
    pub(crate) signature: Signature,
    pub(crate) pos: SlotIndex,
}

#[derive(Debug, Clone)]
pub(crate) struct HeadTxMeta {
    pub(crate) signature_str: Arc<str>,
    pub(crate) pos: SlotIndex,
    pub(crate) err: Option<serde_json::Value>,
    pub(crate) memo: Option<String>,
    pub(crate) block_time: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TransactionCountOverlay {
    pub(crate) start_slot: u64,
    pub(crate) context_slot: u64,
    pub(crate) transaction_count: u64,
}

/// In-memory cache of very recent transactions ("head") that haven't landed in ClickHouse yet.
///
/// This is optimized for read concurrency (DashMap + immutable `Arc` values) and
/// fast merges in the RPC handlers.
pub(crate) struct HeadCache {
    retain_slots: u64,
    max_per_address: usize,

    latest_slot: AtomicU64,

    // slot -> observed block height (when available)
    slot_block_height: DashMap<u64, u64>,
    // slot -> observed blockhash (when available)
    slot_blockhash: DashMap<u64, [u8; 32]>,
    // slot -> observed block time (unix timestamp, when available)
    slot_block_time: DashMap<u64, i64>,
    // slot -> full block metadata when the block-meta stream has delivered a complete envelope
    slot_block_metadata: DashMap<u64, BlockMetadataRecord>,

    // signature -> full transaction record (for hydration)
    tx_by_signature: DashMap<Signature, Arc<StoredTransactionRecord>>,
    // signature -> minimal metadata (for signatures/statuses)
    meta_by_signature: DashMap<Signature, Arc<HeadTxMeta>>,

    // address -> newest-first list of signatures touching the address
    sigs_by_address: DashMap<Pubkey, VecDeque<HeadSigKey>>,
    // slot -> signatures (eviction of the signature-indexed maps)
    sigs_by_slot: DashMap<u64, Vec<Signature>>,
    // slot -> addresses touched (pruning of `sigs_by_address` on slot eviction)
    addrs_by_slot: DashMap<u64, Vec<Pubkey>>,
    // slot -> latest observed commitment
    slot_commitment: DashMap<u64, CommitmentLevel>,
}

impl HeadCache {
    pub(crate) fn new(retain_slots: u64, max_per_address: usize) -> Self {
        Self {
            retain_slots: retain_slots.max(1),
            max_per_address: max_per_address.max(1),
            latest_slot: AtomicU64::new(0),
            slot_block_height: DashMap::new(),
            slot_blockhash: DashMap::new(),
            slot_block_time: DashMap::new(),
            slot_block_metadata: DashMap::new(),
            tx_by_signature: DashMap::new(),
            meta_by_signature: DashMap::new(),
            sigs_by_address: DashMap::new(),
            sigs_by_slot: DashMap::new(),
            addrs_by_slot: DashMap::new(),
            slot_commitment: DashMap::new(),
        }
    }

    pub(crate) fn latest_slot(&self) -> u64 {
        self.latest_slot.load(Ordering::Relaxed)
    }

    pub(crate) fn note_block_height(&self, slot: u64, block_height: u64) {
        self.slot_block_height.insert(slot, block_height);
        if let Some(mut metadata) = self.slot_block_metadata.get_mut(&slot) {
            metadata.block_height = Some(block_height);
        }
    }

    pub(crate) fn note_blockhash(&self, slot: u64, blockhash: [u8; 32]) {
        self.slot_blockhash.insert(slot, blockhash);
        if let Some(mut metadata) = self.slot_block_metadata.get_mut(&slot) {
            metadata.blockhash = blockhash;
        }
    }

    pub(crate) fn note_block_time(&self, slot: u64, block_time: i64) {
        self.slot_block_time.insert(slot, block_time);
        if let Some(mut metadata) = self.slot_block_metadata.get_mut(&slot) {
            metadata.block_time = Some(block_time);
        }
        self.backfill_slot_block_time(slot, block_time);
    }

    pub(crate) fn note_block_metadata(&self, metadata: BlockMetadataRecord) {
        let slot = metadata.slot;
        if let Some(block_height) = metadata.block_height {
            self.slot_block_height.insert(slot, block_height);
        }
        self.slot_blockhash.insert(slot, metadata.blockhash);
        if let Some(block_time) = metadata.block_time {
            self.slot_block_time.insert(slot, block_time);
            self.backfill_slot_block_time(slot, block_time);
        }
        self.slot_block_metadata.insert(slot, metadata);
    }

    pub(crate) fn block_time_for_slot(&self, slot: u64) -> Option<i64> {
        self.slot_block_time.get(&slot).map(|value| *value.value())
    }

    #[cfg(test)]
    pub(crate) fn slot_block_time_for_tests(&self, slot: u64) -> Option<i64> {
        self.block_time_for_slot(slot)
    }

    pub(crate) fn latest_block_height_at_least(
        &self,
        min_commitment: CommitmentLevel,
    ) -> Option<u64> {
        let mut latest: Option<u64> = None;
        for entry in self.slot_block_height.iter() {
            let slot = *entry.key();
            if !commitment_meets(self.slot_commitment(slot), min_commitment) {
                continue;
            }
            let height = *entry.value();
            latest = Some(latest.map_or(height, |prev| prev.max(height)));
        }
        latest
    }

    pub(crate) fn latest_blockhash_info_at_least(
        &self,
        min_commitment: CommitmentLevel,
    ) -> Option<(u64, [u8; 32], u64)> {
        let latest_slot = self.latest_slot();
        if latest_slot != 0 && commitment_meets(self.slot_commitment(latest_slot), min_commitment) {
            let blockhash = self.slot_blockhash.get(&latest_slot).map(|v| *v.value());
            let height = self.slot_block_height.get(&latest_slot).map(|h| *h.value());
            if let (Some(blockhash), Some(height)) = (blockhash, height) {
                return Some((latest_slot, blockhash, height));
            }
        }

        let mut best: Option<(u64, [u8; 32], u64)> = None;
        for entry in self.slot_blockhash.iter() {
            let slot = *entry.key();
            if !commitment_meets(self.slot_commitment(slot), min_commitment) {
                continue;
            }
            let Some(height) = self.slot_block_height.get(&slot).map(|h| *h.value()) else {
                continue;
            };
            let blockhash = *entry.value();
            best = match best {
                Some((best_slot, _, _)) if best_slot >= slot => best,
                _ => Some((slot, blockhash, height)),
            };
        }
        best
    }

    pub(crate) fn blockhash_valid_at_least(
        &self,
        blockhash: [u8; 32],
        min_block_height: u64,
        min_commitment: CommitmentLevel,
    ) -> Option<bool> {
        let mut found_at_commitment = false;
        for entry in self.slot_blockhash.iter() {
            if *entry.value() != blockhash {
                continue;
            }
            let slot = *entry.key();
            if !commitment_meets(self.slot_commitment(slot), min_commitment) {
                continue;
            }
            let Some(height) = self.slot_block_height.get(&slot).map(|h| *h.value()) else {
                continue;
            };
            found_at_commitment = true;
            if height >= min_block_height {
                return Some(true);
            }
        }
        if found_at_commitment {
            Some(false)
        } else {
            None
        }
    }

    pub(crate) fn tx_entries(&self) -> usize {
        self.tx_by_signature.len()
    }

    pub(crate) fn address_entries(&self) -> usize {
        self.sigs_by_address.len()
    }

    pub(crate) fn slot_entries(&self) -> usize {
        self.sigs_by_slot.len()
    }

    pub(crate) fn latest_slot_at_least(&self, min_commitment: CommitmentLevel) -> u64 {
        if min_commitment == CommitmentLevel::Processed {
            return self.latest_slot();
        }

        let mut latest = 0;
        for entry in self.slot_commitment.iter() {
            let slot = *entry.key();
            let commitment = *entry.value();
            if slot > latest && commitment_meets(commitment, min_commitment) {
                latest = slot;
            }
        }
        latest
    }

    pub(crate) fn transaction_count_overlay_at_least(
        &self,
        min_commitment: CommitmentLevel,
        clickhouse_slot: u64,
    ) -> Option<TransactionCountOverlay> {
        let mut candidate_tips = self
            .slot_block_metadata
            .iter()
            .filter_map(|entry| {
                let slot = *entry.key();
                commitment_meets(self.slot_commitment(slot), min_commitment).then_some(slot)
            })
            .collect::<Vec<_>>();
        candidate_tips.sort_unstable_by(|lhs, rhs| rhs.cmp(lhs));
        candidate_tips.dedup();

        for tip in candidate_tips {
            let mut current_slot = tip;
            let mut transaction_count = 0u64;

            while let Some(metadata) = self
                .slot_block_metadata
                .get(&current_slot)
                .map(|metadata| metadata.clone())
            {
                transaction_count =
                    transaction_count.saturating_add(metadata.executed_transaction_count);

                if metadata.parent_slot <= clickhouse_slot {
                    return Some(TransactionCountOverlay {
                        start_slot: current_slot,
                        context_slot: tip,
                        transaction_count,
                    });
                }

                if metadata.parent_slot >= current_slot
                    || !self.slot_block_metadata.contains_key(&metadata.parent_slot)
                {
                    break;
                }

                current_slot = metadata.parent_slot;
            }
        }

        None
    }

    pub(crate) fn slots_in_range_at_least(
        &self,
        start_slot: u64,
        end_slot: u64,
        min_commitment: CommitmentLevel,
    ) -> Vec<u64> {
        if end_slot < start_slot {
            return Vec::new();
        }

        let mut slots = Vec::new();
        for entry in self.slot_commitment.iter() {
            let slot = *entry.key();
            if slot < start_slot || slot > end_slot {
                continue;
            }
            if commitment_meets(*entry.value(), min_commitment) {
                slots.push(slot);
            }
        }
        slots.sort_unstable();
        slots
    }

    pub(crate) fn min_retained_slot(&self) -> u64 {
        let latest = self.latest_slot();
        latest.saturating_sub(self.retain_slots.saturating_sub(1))
    }

    pub(crate) fn slot_commitment(&self, slot: u64) -> CommitmentLevel {
        self.slot_commitment
            .get(&slot)
            .map(|c| *c)
            .unwrap_or(CommitmentLevel::Processed)
    }

    pub(crate) fn signature_position(&self, signature: &Signature) -> Option<SlotIndex> {
        self.meta_by_signature.get(signature).map(|meta| meta.pos)
    }

    pub(crate) fn get_tx(
        &self,
        signature: &Signature,
        min_commitment: CommitmentLevel,
    ) -> Option<Arc<StoredTransactionRecord>> {
        let meta = self.meta_by_signature.get(signature)?;
        if !commitment_meets(self.slot_commitment(meta.pos.slot), min_commitment) {
            return None;
        }
        self.tx_by_signature.get(signature).map(|v| v.clone())
    }

    pub(crate) fn get_meta(
        &self,
        signature: &Signature,
        min_commitment: CommitmentLevel,
    ) -> Option<Arc<HeadTxMeta>> {
        let meta = self.meta_by_signature.get(signature)?;
        if !commitment_meets(self.slot_commitment(meta.pos.slot), min_commitment) {
            return None;
        }
        Some(meta.clone())
    }

    pub(crate) fn confirmation_status_string(&self, slot: u64) -> &'static str {
        commitment_to_str(self.slot_commitment(slot))
    }

    pub(crate) fn get_block(
        &self,
        slot: u64,
        min_commitment: CommitmentLevel,
        transaction_details: TransactionDetails,
    ) -> Option<StoredBlockPayload> {
        if !commitment_meets(self.slot_commitment(slot), min_commitment) {
            return None;
        }

        let metadata = self.slot_block_metadata.get(&slot)?.clone();
        if transaction_details == TransactionDetails::None {
            return Some(StoredBlockPayload::Metadata(metadata));
        }

        let expected_txs = metadata.executed_transaction_count as usize;
        let slot_signatures = if expected_txs == 0 {
            Vec::new()
        } else {
            self.sigs_by_slot.get(&slot).map(|entry| entry.clone())?
        };

        if slot_signatures.len() != expected_txs {
            return None;
        }

        let mut ordered = Vec::with_capacity(slot_signatures.len());
        for signature in slot_signatures {
            let meta = self.meta_by_signature.get(&signature)?;
            if meta.pos.slot != slot {
                return None;
            }
            ordered.push((meta.pos.idx, signature, meta.signature_str.clone()));
        }

        ordered.sort_unstable_by(|lhs, rhs| lhs.0.cmp(&rhs.0).then_with(|| lhs.1.cmp(&rhs.1)));

        if transaction_details == TransactionDetails::Signatures {
            let signatures = ordered
                .into_iter()
                .map(|(_, _, signature)| signature.to_string())
                .collect();
            return Some(StoredBlockPayload::Signatures {
                metadata,
                signatures,
            });
        }

        let mut transactions = Vec::with_capacity(ordered.len());
        for (_, signature, _) in ordered {
            let record = self.tx_by_signature.get(&signature)?.clone();
            transactions.push(record.as_ref().clone());
        }

        match transaction_details {
            TransactionDetails::Accounts => Some(StoredBlockPayload::Accounts {
                metadata,
                transactions: transactions
                    .into_iter()
                    .map(StoredAccountsTransactionRecord::from)
                    .collect(),
            }),
            TransactionDetails::Full => Some(StoredBlockPayload::Full(StoredBlockRecord {
                metadata,
                transactions,
            })),
            TransactionDetails::None | TransactionDetails::Signatures => None,
        }
    }

    /// Snapshot a complete block for the disk cache without deep-cloning records.
    ///
    /// Same completeness rules as [`Self::get_block`] with full details: block
    /// metadata present, exactly `executed_transaction_count` transactions, all
    /// positioned in this slot. Returns records ordered by execution index.
    /// `None` means the head cache holds an incomplete view (or the slot was
    /// already evicted) and the caller must repair from ClickHouse instead.
    #[cfg(feature = "disk-cache")]
    pub(crate) fn finalized_block_snapshot(
        &self,
        slot: u64,
    ) -> Option<(BlockMetadataRecord, Vec<Arc<StoredTransactionRecord>>)> {
        let mut metadata = self.slot_block_metadata.get(&slot)?.clone();
        // The block-meta stream can race individual note_* updates; prefer the
        // per-slot maps where present so the snapshot matches what reads serve.
        if metadata.block_time.is_none() {
            metadata.block_time = self.block_time_for_slot(slot);
        }

        let expected = metadata.executed_transaction_count as usize;
        let slot_signatures = if expected == 0 {
            Vec::new()
        } else {
            self.sigs_by_slot.get(&slot).map(|entry| entry.clone())?
        };
        if slot_signatures.len() != expected {
            return None;
        }

        let mut ordered = Vec::with_capacity(expected);
        for signature in slot_signatures {
            let meta = self.meta_by_signature.get(&signature)?;
            if meta.pos.slot != slot {
                return None;
            }
            let record = self.tx_by_signature.get(&signature)?.clone();
            ordered.push((meta.pos.idx, record));
        }
        ordered.sort_unstable_by_key(|(idx, _)| *idx);

        let records = ordered
            .into_iter()
            .map(|(_, record)| {
                // Normalize block_time so disk contents match what ClickHouse
                // stores for the same transaction.
                if record.block_time.is_none() && metadata.block_time.is_some() {
                    let mut fixed = record.as_ref().clone();
                    fixed.block_time = metadata.block_time;
                    Arc::new(fixed)
                } else {
                    record
                }
            })
            .collect();

        Some((metadata, records))
    }

    pub(crate) fn signatures_for_address(
        &self,
        address: &Pubkey,
        before: Option<SlotBoundary>,
        until: Option<SlotBoundary>,
        limit: usize,
        min_commitment: CommitmentLevel,
    ) -> Vec<Arc<HeadTxMeta>> {
        let Some(keys) = self.sigs_by_address.get(address) else {
            return Vec::new();
        };

        let mut out = Vec::new();
        for key in keys.iter() {
            if let Some(before) = before
                && !position_before_boundary(key.pos, before)
            {
                continue;
            }
            if let Some(until) = until
                && !position_after_boundary(key.pos, until)
            {
                continue;
            }
            if !commitment_meets(self.slot_commitment(key.pos.slot), min_commitment) {
                continue;
            }
            let Some(meta) = self.meta_by_signature.get(&key.signature) else {
                continue;
            };
            out.push(meta.clone());
            if out.len() >= limit {
                break;
            }
        }

        out
    }

    pub(crate) fn note_slot_commitment(&self, slot: u64, commitment: CommitmentLevel) {
        // Eviction is triggered from commitment updates (one per slot), so ingesting many
        // transactions for the same slot doesn't repeatedly scan the slot index.
        let prev_latest = self.latest_slot.fetch_max(slot, Ordering::Relaxed);
        self.slot_commitment
            .entry(slot)
            .and_modify(|existing| {
                if commitment_meets(commitment, *existing) {
                    *existing = commitment;
                }
            })
            .or_insert(commitment);

        if slot > prev_latest {
            self.evict_old_slots();
        }
    }

    pub(crate) fn remove_slot(&self, slot: u64) {
        self.remove_slot_inner(slot);
    }

    fn remove_slot_inner(&self, slot: u64) {
        self.slot_commitment.remove(&slot);
        self.slot_block_height.remove(&slot);
        self.slot_blockhash.remove(&slot);
        self.slot_block_time.remove(&slot);
        self.slot_block_metadata.remove(&slot);

        if let Some((_, sigs)) = self.sigs_by_slot.remove(&slot) {
            for sig in sigs {
                self.tx_by_signature.remove(&sig);
                self.meta_by_signature.remove(&sig);
            }
        }

        let Some((_, mut addrs)) = self.addrs_by_slot.remove(&slot) else {
            return;
        };
        addrs.sort_unstable();
        addrs.dedup();

        for addr in addrs {
            match self.sigs_by_address.entry(addr) {
                Entry::Occupied(mut occ) => {
                    let deque = occ.get_mut();
                    deque.retain(|k| k.pos.slot != slot);

                    if deque.is_empty() {
                        occ.remove();
                    }
                }
                Entry::Vacant(_) => {}
            }
        }
    }

    pub(crate) fn ingest_transaction(
        &self,
        slot: u64,
        tx_info: &yellowstone_grpc_proto::geyser::SubscribeUpdateTransactionInfo,
    ) {
        // Skip transactions that are already outside the retained window. This avoids doing
        // conversion work and prevents address indexes from accumulating stale keys.
        let current_latest = self.latest_slot.load(Ordering::Relaxed);
        let prospective_latest = current_latest.max(slot);
        let min_slot = prospective_latest.saturating_sub(self.retain_slots.saturating_sub(1));
        if slot < min_slot {
            return;
        }

        let signature_bytes: [u8; 64] = match convert::bytes_to_array(&tx_info.signature) {
            Ok(sig) => sig,
            Err(err) => {
                tracing::warn!(slot, "head cache: invalid signature bytes: {err}");
                return;
            }
        };
        let signature = Signature::from(signature_bytes);

        let mut record = match convert::stored_record_from_transaction_info(slot, tx_info) {
            Ok(record) => record,
            Err(err) => {
                tracing::warn!(
                    slot,
                    signature = %bs58::encode(signature_bytes).into_string(),
                    "head cache: failed to convert transaction: {err}"
                );
                return;
            }
        };
        let block_time = self.slot_block_time.get(&slot).map(|value| *value.value());
        record.block_time = block_time;

        let memo = extract_memo(&record);
        let err_value = record
            .meta_err
            .as_ref()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw.trim()).ok());

        let pos = SlotIndex {
            slot,
            idx: tx_info.index.min(u64::from(u32::MAX)) as u32,
        };

        let signature_str: Arc<str> = bs58::encode(signature_bytes).into_string().into();
        let meta = Arc::new(HeadTxMeta {
            signature_str,
            pos,
            err: err_value,
            memo,
            block_time,
        });

        // Insert meta first; if this races, keep the first writer and skip indexing.
        match self.meta_by_signature.entry(signature) {
            Entry::Occupied(_) => return,
            Entry::Vacant(v) => {
                v.insert(meta.clone());
            }
        }

        let record = Arc::new(record);
        self.tx_by_signature.insert(signature, record.clone());

        self.sigs_by_slot.entry(slot).or_default().push(signature);

        let key = HeadSigKey {
            signature,
            pos: meta.pos,
        };

        let mut unique_addrs = Vec::with_capacity(
            record.tx_account_keys.len()
                + record.meta_loaded_addresses_writable.len()
                + record.meta_loaded_addresses_readonly.len(),
        );
        unique_addrs.extend(record.tx_account_keys.iter().copied());
        unique_addrs.extend(record.meta_loaded_addresses_writable.iter().copied());
        unique_addrs.extend(record.meta_loaded_addresses_readonly.iter().copied());
        unique_addrs.sort_unstable();
        unique_addrs.dedup();

        let unique_pubkeys = unique_addrs
            .into_iter()
            .map(Pubkey::from)
            .collect::<Vec<_>>();

        {
            let mut slot_addrs = self.addrs_by_slot.entry(slot).or_default();
            slot_addrs.extend(unique_pubkeys.iter().copied());
        }

        for addr in unique_pubkeys {
            let mut entry = self.sigs_by_address.entry(addr).or_default();
            entry.push_front(key);
            while let Some(back) = entry.back()
                && back.pos.slot < min_slot
            {
                entry.pop_back();
            }
            while entry.len() > self.max_per_address {
                entry.pop_back();
            }
        }

        // In the common path, `note_slot_commitment` has already advanced `latest_slot` for this
        // slot, so avoid the atomic in the hot ingest loop.
        if slot > current_latest {
            let prev_latest = self.latest_slot.fetch_max(slot, Ordering::Relaxed);
            if slot > prev_latest {
                self.evict_old_slots();
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn insert_for_tests(
        &self,
        signature: Signature,
        mut record: StoredTransactionRecord,
        idx: u32,
        addresses: &[Pubkey],
        commitment: CommitmentLevel,
    ) {
        let slot = record.slot;
        self.note_slot_commitment(slot, commitment);
        if record.block_time.is_none() {
            record.block_time = self.slot_block_time.get(&slot).map(|value| *value.value());
        }

        let signature_str: Arc<str> = signature.to_string().into();
        let memo = extract_memo(&record);
        let err_value = if record.meta_status_ok {
            None
        } else {
            record
                .meta_err
                .as_deref()
                .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw.trim()).ok())
        };

        let pos = SlotIndex { slot, idx };
        let meta = Arc::new(HeadTxMeta {
            signature_str,
            pos,
            err: err_value,
            memo,
            block_time: record.block_time,
        });

        match self.meta_by_signature.entry(signature) {
            Entry::Occupied(_) => return,
            Entry::Vacant(v) => {
                v.insert(meta.clone());
            }
        }

        let record = Arc::new(record);
        self.tx_by_signature.insert(signature, record);
        self.sigs_by_slot.entry(slot).or_default().push(signature);

        let min_slot = self.min_retained_slot();
        let key = HeadSigKey { signature, pos };

        {
            let mut slot_addrs = self.addrs_by_slot.entry(slot).or_default();
            slot_addrs.extend(addresses.iter().copied());
        }

        for address in addresses {
            let mut entry = self.sigs_by_address.entry(*address).or_default();
            entry.push_front(key);
            while let Some(back) = entry.back()
                && back.pos.slot < min_slot
            {
                entry.pop_back();
            }
            while entry.len() > self.max_per_address {
                entry.pop_back();
            }
        }

        let prev_latest = self.latest_slot.fetch_max(slot, Ordering::Relaxed);
        if slot > prev_latest {
            self.evict_old_slots();
        }
    }

    fn evict_old_slots(&self) {
        let min_slot = self.min_retained_slot();
        let mut old = Vec::new();
        for entry in self.slot_commitment.iter() {
            let slot = *entry.key();
            if slot < min_slot {
                old.push(slot);
            }
        }
        for slot in old {
            self.remove_slot_inner(slot);
        }
    }

    fn backfill_slot_block_time(&self, slot: u64, block_time: i64) {
        let Some(signatures) = self.sigs_by_slot.get(&slot).map(|entry| entry.clone()) else {
            return;
        };

        for signature in signatures {
            self.backfill_transaction_block_time(&signature, block_time);
            self.backfill_meta_block_time(&signature, block_time);
        }
    }

    fn backfill_transaction_block_time(&self, signature: &Signature, block_time: i64) {
        let Some(mut entry) = self.tx_by_signature.get_mut(signature) else {
            return;
        };
        if entry.block_time == Some(block_time) {
            return;
        }
        let mut updated = entry.as_ref().clone();
        updated.block_time = Some(block_time);
        *entry = Arc::new(updated);
    }

    fn backfill_meta_block_time(&self, signature: &Signature, block_time: i64) {
        let Some(mut entry) = self.meta_by_signature.get_mut(signature) else {
            return;
        };
        if entry.block_time == Some(block_time) {
            return;
        }
        let mut updated = entry.as_ref().clone();
        updated.block_time = Some(block_time);
        *entry = Arc::new(updated);
    }
}

fn slot_index_lt(a: SlotIndex, b: SlotIndex) -> bool {
    a.slot < b.slot || (a.slot == b.slot && a.idx < b.idx)
}

fn slot_index_gt(a: SlotIndex, b: SlotIndex) -> bool {
    a.slot > b.slot || (a.slot == b.slot && a.idx > b.idx)
}

fn position_before_boundary(position: SlotIndex, boundary: SlotBoundary) -> bool {
    match boundary {
        SlotBoundary::Position(boundary) => slot_index_lt(
            position,
            SlotIndex {
                slot: boundary.slot,
                idx: boundary.slot_idx,
            },
        ),
        SlotBoundary::Slot(slot) => position.slot < slot,
    }
}

fn position_after_boundary(position: SlotIndex, boundary: SlotBoundary) -> bool {
    match boundary {
        SlotBoundary::Position(boundary) => slot_index_gt(
            position,
            SlotIndex {
                slot: boundary.slot,
                idx: boundary.slot_idx,
            },
        ),
        SlotBoundary::Slot(slot) => position.slot > slot,
    }
}

fn commitment_rank(c: CommitmentLevel) -> u8 {
    match c {
        CommitmentLevel::Processed => 0,
        CommitmentLevel::Confirmed => 1,
        CommitmentLevel::Finalized => 2,
    }
}

fn commitment_meets(have: CommitmentLevel, want: CommitmentLevel) -> bool {
    commitment_rank(have) >= commitment_rank(want)
}

fn commitment_to_str(c: CommitmentLevel) -> &'static str {
    match c {
        CommitmentLevel::Processed => "processed",
        CommitmentLevel::Confirmed => "confirmed",
        CommitmentLevel::Finalized => "finalized",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::hash::Hash;

    fn base_transaction(slot: u64, blockhash: [u8; 32]) -> StoredTransactionRecord {
        StoredTransactionRecord {
            signature: [0u8; 64],
            slot,
            slot_idx: 0,
            block_time: None,
            is_vote: false,
            tx_version: None,
            tx_signatures: vec![[0u8; 64]],
            tx_num_required_signatures: 1,
            tx_num_readonly_signed_accounts: 0,
            tx_num_readonly_unsigned_accounts: 0,
            tx_account_keys: vec![[1u8; 32]],
            tx_recent_blockhash: blockhash,
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
            meta_pre_balances: Vec::new(),
            meta_post_balances: Vec::new(),
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

    fn base_block_metadata(slot: u64, tx_count: u64) -> BlockMetadataRecord {
        BlockMetadataRecord {
            slot,
            parent_slot: slot.saturating_sub(1),
            blockhash: Hash::new_unique().to_bytes(),
            parent_blockhash: Hash::new_unique().to_bytes(),
            block_time: Some(1_700_000_000),
            block_height: Some(123),
            executed_transaction_count: tx_count,
            entry_count: tx_count,
            rewards_present: false,
            rewards_pubkey: Vec::new(),
            rewards_lamports: Vec::new(),
            rewards_post_balance: Vec::new(),
            rewards_type: Vec::new(),
            rewards_commission: Vec::new(),
            rewards_num_partitions: None,
        }
    }

    #[test]
    fn get_block_orders_transactions_by_slot_index() {
        let cache = HeadCache::new(32, 64);
        let slot = 42u64;
        let metadata = base_block_metadata(slot, 2);
        let blockhash = metadata.blockhash;

        cache.note_block_metadata(metadata.clone());
        cache.note_slot_commitment(slot, CommitmentLevel::Confirmed);

        let sig_a = Signature::new_unique();
        let sig_b = Signature::new_unique();
        let mut tx_a = base_transaction(slot, blockhash);
        tx_a.signature = *sig_a.as_array();
        let mut tx_b = base_transaction(slot, blockhash);
        tx_b.signature = *sig_b.as_array();
        cache.insert_for_tests(
            sig_a,
            tx_a,
            9,
            &[Pubkey::new_unique()],
            CommitmentLevel::Confirmed,
        );
        cache.insert_for_tests(
            sig_b,
            tx_b,
            3,
            &[Pubkey::new_unique()],
            CommitmentLevel::Confirmed,
        );

        let payload = cache
            .get_block(slot, CommitmentLevel::Confirmed, TransactionDetails::Full)
            .expect("complete block available");

        match payload {
            StoredBlockPayload::Full(block) => {
                assert_eq!(block.metadata.slot, slot);
                assert_eq!(block.transactions.len(), 2);
                assert_eq!(block.transactions[0].signature, *sig_b.as_array());
                assert_eq!(block.transactions[1].signature, *sig_a.as_array());
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn get_block_signatures_view_orders_signatures_by_slot_index() {
        let cache = HeadCache::new(32, 64);
        let slot = 43u64;
        let metadata = base_block_metadata(slot, 2);
        let blockhash = metadata.blockhash;

        cache.note_block_metadata(metadata.clone());
        cache.note_slot_commitment(slot, CommitmentLevel::Confirmed);

        let sig_a = Signature::new_unique();
        let sig_b = Signature::new_unique();
        let mut tx_a = base_transaction(slot, blockhash);
        tx_a.signature = *sig_a.as_array();
        let mut tx_b = base_transaction(slot, blockhash);
        tx_b.signature = *sig_b.as_array();
        cache.insert_for_tests(
            sig_a,
            tx_a,
            9,
            &[Pubkey::new_unique()],
            CommitmentLevel::Confirmed,
        );
        cache.insert_for_tests(
            sig_b,
            tx_b,
            3,
            &[Pubkey::new_unique()],
            CommitmentLevel::Confirmed,
        );

        let payload = cache
            .get_block(
                slot,
                CommitmentLevel::Confirmed,
                TransactionDetails::Signatures,
            )
            .expect("signature block available");

        match payload {
            StoredBlockPayload::Signatures {
                metadata: observed,
                signatures,
            } => {
                assert_eq!(observed.slot, slot);
                assert_eq!(signatures, vec![sig_b.to_string(), sig_a.to_string()]);
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn get_block_requires_complete_transaction_count() {
        let cache = HeadCache::new(32, 64);
        let slot = 77u64;
        let metadata = base_block_metadata(slot, 2);

        cache.note_block_metadata(metadata.clone());
        cache.note_slot_commitment(slot, CommitmentLevel::Finalized);
        let signature = Signature::new_unique();
        let mut tx = base_transaction(slot, metadata.blockhash);
        tx.signature = *signature.as_array();
        cache.insert_for_tests(
            signature,
            tx,
            0,
            &[Pubkey::new_unique()],
            CommitmentLevel::Finalized,
        );

        assert!(
            cache
                .get_block(slot, CommitmentLevel::Finalized, TransactionDetails::Full)
                .is_none()
        );
    }

    #[test]
    fn get_block_metadata_view_allows_incomplete_transaction_count() {
        let cache = HeadCache::new(32, 64);
        let slot = 78u64;
        let metadata = base_block_metadata(slot, 2);

        cache.note_block_metadata(metadata.clone());
        cache.note_slot_commitment(slot, CommitmentLevel::Finalized);
        let signature = Signature::new_unique();
        let mut tx = base_transaction(slot, metadata.blockhash);
        tx.signature = *signature.as_array();
        cache.insert_for_tests(
            signature,
            tx,
            0,
            &[Pubkey::new_unique()],
            CommitmentLevel::Finalized,
        );

        match cache
            .get_block(slot, CommitmentLevel::Finalized, TransactionDetails::None)
            .expect("metadata-only block")
        {
            StoredBlockPayload::Metadata(observed) => assert_eq!(observed.slot, slot),
            other => panic!("unexpected payload: {other:?}"),
        }

        assert!(
            cache
                .get_block(slot, CommitmentLevel::Finalized, TransactionDetails::Full)
                .is_none()
        );
    }

    #[test]
    fn get_block_supports_metadata_and_signatures_views() {
        let cache = HeadCache::new(32, 64);
        let slot = 91u64;
        let metadata = base_block_metadata(slot, 1);
        let blockhash = metadata.blockhash;

        cache.note_block_metadata(metadata.clone());
        cache.note_slot_commitment(slot, CommitmentLevel::Confirmed);

        let signature = Signature::new_unique();
        let mut tx = base_transaction(slot, blockhash);
        tx.signature = *signature.as_array();
        cache.insert_for_tests(
            signature,
            tx,
            4,
            &[Pubkey::new_unique()],
            CommitmentLevel::Confirmed,
        );

        match cache
            .get_block(slot, CommitmentLevel::Confirmed, TransactionDetails::None)
            .expect("metadata-only block")
        {
            StoredBlockPayload::Metadata(observed) => assert_eq!(observed.slot, slot),
            other => panic!("unexpected payload: {other:?}"),
        }

        match cache
            .get_block(
                slot,
                CommitmentLevel::Confirmed,
                TransactionDetails::Signatures,
            )
            .expect("signature block")
        {
            StoredBlockPayload::Signatures {
                metadata: observed,
                signatures,
            } => {
                assert_eq!(observed.slot, slot);
                assert_eq!(signatures, vec![signature.to_string()]);
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn transaction_count_overlay_walks_parent_chain_to_clickhouse_boundary() {
        let cache = HeadCache::new(32, 64);

        let mut older = base_block_metadata(100, 3);
        older.parent_slot = 95;
        cache.note_block_metadata(older);

        let mut tip = base_block_metadata(105, 5);
        tip.parent_slot = 100;
        cache.note_block_metadata(tip);
        cache.note_slot_commitment(105, CommitmentLevel::Confirmed);

        let overlay = cache
            .transaction_count_overlay_at_least(CommitmentLevel::Confirmed, 95)
            .expect("overlay");

        assert_eq!(
            overlay,
            TransactionCountOverlay {
                start_slot: 100,
                context_slot: 105,
                transaction_count: 8,
            }
        );
    }

    #[test]
    fn transaction_count_overlay_skips_newer_tip_without_safe_parent_chain() {
        let cache = HeadCache::new(32, 64);

        let mut older = base_block_metadata(100, 3);
        older.parent_slot = 95;
        cache.note_block_metadata(older);

        let mut safe_tip = base_block_metadata(105, 5);
        safe_tip.parent_slot = 100;
        cache.note_block_metadata(safe_tip);
        cache.note_slot_commitment(105, CommitmentLevel::Confirmed);

        let mut unsafe_tip = base_block_metadata(106, 7);
        unsafe_tip.parent_slot = 102;
        cache.note_block_metadata(unsafe_tip);
        cache.note_slot_commitment(106, CommitmentLevel::Confirmed);

        let overlay = cache
            .transaction_count_overlay_at_least(CommitmentLevel::Confirmed, 95)
            .expect("overlay");

        assert_eq!(
            overlay,
            TransactionCountOverlay {
                start_slot: 100,
                context_slot: 105,
                transaction_count: 8,
            }
        );
    }
}
