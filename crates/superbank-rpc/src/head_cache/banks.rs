// SPDX-License-Identifier: AGPL-3.0-only
//! Speculative banks are scoped to one subscription; only a selected bank is published.
use super::{HeadCache, coverage::Link};
use crate::clickhouse::{BlockFooterRecord, BlockMetadataRecord};
use solana_commitment_config::CommitmentLevel;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};
use yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionInfo;

// Extra slots a buffered bank survives while it waits for a Confirmed/Finalized status.
const PENDING_COMMITMENT_SLOTS: u64 = 64;

type BankKey = (u64, u64); // (slot, node-local bank ID), within BankState::session

#[derive(Default)]
pub(super) struct BankState {
    session: u64,
    highest: u64,
    minimum_commitment: u8,
    banks: HashMap<BankKey, Bank>,
    selected: HashMap<u64, u64>,
    canonical: HashMap<u64, u64>,
    discarded: HashSet<BankKey>,
}

#[derive(Default)]
struct Bank {
    metadata: Option<BlockMetadataRecord>,
    footer: Option<BlockFooterRecord>,
    transactions: Option<Vec<SubscribeUpdateTransactionInfo>>,
    sealed_hash: Option<[u8; 32]>,
    entries: Vec<yellowstone_grpc_proto::prelude::SubscribeUpdateEntry>,
    commitment: Arc<AtomicU8>,
    observed_at: Option<std::time::Instant>,
}

pub(super) fn decode_commitment(rank: u8) -> CommitmentLevel {
    match rank {
        2 => CommitmentLevel::Finalized,
        1 => CommitmentLevel::Confirmed,
        _ => CommitmentLevel::Processed,
    }
}

fn rank(commitment: CommitmentLevel) -> u8 {
    match commitment {
        CommitmentLevel::Processed => 0,
        CommitmentLevel::Confirmed => 1,
        CommitmentLevel::Finalized => 2,
    }
}

impl HeadCache {
    pub(super) fn start_bank_session(&self, minimum: CommitmentLevel) -> u64 {
        let mut state = self.banks.write().expect("head bank lock");
        let session = state.session.checked_add(1).expect("head session overflow");
        // Even an identical node-local ID can name a different bank after reconnecting.
        let slots: Vec<_> = self
            .slot_commitment
            .iter()
            .map(|entry| *entry.key())
            .collect();
        for slot in slots {
            self.remove_slot_inner(slot);
        }
        self.latest_slot.store(0, Ordering::Relaxed);
        self.address_truncated_slot.clear();
        *state = BankState {
            session,
            minimum_commitment: rank(minimum),
            ..Default::default()
        };
        self.coverage.write().expect("head coverage lock").connect();
        session
    }

    pub(super) fn end_bank_session(&self, session: u64) {
        let mut state = self.banks.write().expect("head bank lock");
        if state.session != session {
            return;
        }
        let slots: Vec<_> = self
            .slot_commitment
            .iter()
            .map(|entry| *entry.key())
            .collect();
        for slot in slots {
            self.remove_slot_inner(slot);
        }
        state.session = state.session.checked_add(1).expect("head session overflow");
        state.banks.clear();
        state.selected.clear();
        state.canonical.clear();
        state.discarded.clear();
        self.latest_slot.store(0, Ordering::Relaxed);
        self.coverage
            .write()
            .expect("head coverage lock")
            .disconnect();
    }

    pub(super) fn stage_bank_metadata(
        &self,
        session: u64,
        bank_id: u64,
        metadata: BlockMetadataRecord,
    ) {
        let mut state = self.banks.write().expect("head bank lock");
        let key = (metadata.slot, bank_id);
        if !self.accept_bank(&state, session, key) {
            return;
        }
        state.highest = state.highest.max(key.0);
        let bank = state.banks.entry(key).or_default();
        // Frozen metadata is immutable. A delayed envelope cannot replace the sealed hash.
        if bank
            .sealed_hash
            .is_some_and(|hash| hash != metadata.blockhash)
        {
            return;
        }
        let mut metadata = metadata;
        metadata.footer = bank.footer.clone();
        bank.metadata = Some(metadata);
        self.retain_banks(&mut state);
    }

    pub(super) fn stage_bank_footer(
        &self,
        session: u64,
        slot: u64,
        bank_id: u64,
        footer: BlockFooterRecord,
    ) {
        let mut state = self.banks.write().expect("head bank lock");
        let key = (slot, bank_id);
        if !self.accept_bank(&state, session, key) {
            return;
        }
        let selected = state.selected.get(&slot) == Some(&bank_id);
        let bank = state.banks.entry(key).or_default();
        // The first footer for a bank wins. It can arrive before or after the block freezes.
        if bank.footer.is_some() {
            return;
        }
        if let Some(metadata) = bank.metadata.as_mut() {
            metadata.footer = Some(footer.clone());
        }
        bank.footer = Some(footer.clone());
        if selected && let Some(mut published) = self.slot_block_metadata.get_mut(&slot) {
            published.footer = Some(footer);
        }
        self.retain_banks(&mut state);
    }

    pub(super) fn stage_bank_entry(
        &self,
        session: u64,
        entry: &yellowstone_grpc_proto::prelude::SubscribeUpdateEntry,
    ) {
        let mut state = self.banks.write().expect("head bank lock");
        let key = (entry.slot, entry.bank_id);
        if self.accept_bank(&state, session, key) {
            let bank = state.banks.entry(key).or_default();
            if bank.sealed_hash.is_none() {
                bank.entries.push(entry.clone());
            }
        }
    }

    pub(super) fn freeze_bank(
        &self,
        session: u64,
        slot: u64,
        bank_id: u64,
        hash: [u8; 32],
        transactions: Vec<SubscribeUpdateTransactionInfo>,
    ) {
        let mut state = self.banks.write().expect("head bank lock");
        let key = (slot, bank_id);
        if !self.accept_bank(&state, session, key) {
            return;
        }
        let bank = state.banks.entry(key).or_default();
        if bank.sealed_hash.is_some() {
            return;
        }
        if bank
            .metadata
            .as_ref()
            .is_none_or(|meta| meta.blockhash != hash)
        {
            return;
        }
        if !complete_bank(bank, slot, &transactions) {
            tracing::warn!(
                slot,
                bank_id,
                expected_transactions = bank
                    .metadata
                    .as_ref()
                    .map(|meta| meta.executed_transaction_count),
                received_transactions = transactions.len(),
                "head cache: incomplete or malformed bank"
            );
            return;
        }
        bank.sealed_hash = Some(hash);
        bank.transactions = Some(transactions);
        self.publish_bank(&mut state, key);
        self.retain_banks(&mut state);
    }

    pub(super) fn commit_bank(
        &self,
        session: u64,
        slot: u64,
        bank_id: u64,
        commitment: CommitmentLevel,
        parent: Option<u64>,
    ) {
        let mut state = self.banks.write().expect("head bank lock");
        let key = (slot, bank_id);
        if !self.accept_bank(&state, session, key) {
            return;
        }
        if commitment != CommitmentLevel::Processed {
            state.canonical.insert(slot, bank_id);
            if state
                .selected
                .get(&slot)
                .is_some_and(|selected| *selected != bank_id)
            {
                state.selected.remove(&slot);
                self.remove_slot_inner(slot);
            }
            let losers: Vec<_> = state
                .banks
                .keys()
                .copied()
                .filter(|other| other.0 == slot && *other != key)
                .collect();
            for loser in losers {
                state.banks.remove(&loser);
                state.discarded.insert(loser);
            }
        }
        state.highest = state.highest.max(slot);
        let bank = state.banks.entry(key).or_default();
        let previous = bank
            .commitment
            .fetch_max(rank(commitment), Ordering::Release);
        if bank.observed_at.is_none() || rank(commitment) > previous {
            bank.observed_at = Some(std::time::Instant::now());
        }
        self.publish_bank(&mut state, key);
        let mut proof = self.coverage.write().expect("head coverage lock");
        // Track the newest observed status even when its content cannot be sealed yet;
        // latest-slot resolution must fail instead of silently choosing an older bank.
        proof.observe(
            slot,
            commitment,
            state.banks[&key].observed_at.expect("bank status time"),
        );
        if state.selected.get(&slot) == Some(&bank_id) {
            proof.validate_parent(slot, parent);
            proof.publish(slot, commitment);
        }
        proof.retain(slot, self.retain_slots);
        drop(proof);
        self.retain_banks(&mut state);
    }

    pub(super) fn discard_bank(&self, session: u64, slot: u64, bank_id: u64) {
        let mut state = self.banks.write().expect("head bank lock");
        if state.session != session {
            return;
        }
        let key = (slot, bank_id);
        state.banks.remove(&key);
        state.discarded.insert(key);
        if state.selected.get(&slot) == Some(&bank_id) {
            state.selected.remove(&slot);
            self.remove_slot(slot);
            crate::metrics::head_cache_drop_slot(
                self.latest_slot(),
                self.tx_entries(),
                self.address_entries(),
                self.slot_entries(),
            );
        }
        self.retain_banks(&mut state);
    }

    fn accept_bank(&self, state: &BankState, session: u64, key: BankKey) -> bool {
        state.session == session
            && key.0 >= self.bank_floor(state)
            && !state.discarded.contains(&key)
            && state
                .canonical
                .get(&key.0)
                .is_none_or(|winner| *winner == key.1)
    }

    fn publish_bank(&self, state: &mut BankState, key: BankKey) {
        let Some(bank) = state.banks.get(&key) else {
            return;
        };
        let (Some(metadata), Some(transactions)) = (&bank.metadata, &bank.transactions) else {
            return;
        };
        let commitment = decode_commitment(bank.commitment.load(Ordering::Acquire));
        // BlockStream emits FrozenBlock before SlotCommitmentUpdate. A default
        // Processed token is not sufficient for a Confirmed/Finalized session,
        // even while concurrent readers query Processed commitment.
        if rank(commitment) < state.minimum_commitment {
            return;
        }
        let current = state.selected.get(&key.0).copied();
        if current.is_some_and(|id| id != key.1) && commitment == CommitmentLevel::Processed {
            return;
        }
        if current == Some(key.1) {
            // A signature may have first appeared on an abandoned slot. Repair
            // the winner's projection after discard, or transfer a processed
            // projection when this bank gains canonical commitment.
            for tx in transactions {
                self.ingest_bank_transaction(key.0, tx, Some(bank.commitment.clone()));
            }
            self.note_slot_commitment(key.0, commitment);
            return;
        }
        if current.is_some() {
            self.remove_slot_inner(key.0);
        }
        self.note_block_metadata(metadata.clone());
        for tx in transactions {
            self.ingest_bank_transaction(key.0, tx, Some(bank.commitment.clone()));
        }
        self.note_slot_commitment(key.0, commitment);
        state.selected.insert(key.0, key.1);
        let mut proof = self.coverage.write().expect("head coverage lock");
        proof.select_bank(Link {
            slot: metadata.slot,
            hash: metadata.blockhash,
            parent: metadata.parent_slot,
            parent_hash: metadata.parent_blockhash,
        });
        proof.publish(key.0, commitment);
        if let Some(observed_at) = bank.observed_at {
            proof.observe(key.0, commitment, observed_at);
        }
    }

    fn bank_floor(&self, state: &BankState) -> u64 {
        let pending = if state.minimum_commitment > 0 {
            PENDING_COMMITMENT_SLOTS
        } else {
            0
        };
        state
            .highest
            .max(self.latest_slot())
            .saturating_sub(self.retain_slots.saturating_sub(1) + pending)
    }

    fn retain_banks(&self, state: &mut BankState) {
        let floor = self.bank_floor(state);
        state.banks.retain(|key, _| key.0 >= floor);
        state.selected.retain(|slot, _| *slot >= floor);
        state.canonical.retain(|slot, _| *slot >= floor);
        state.discarded.retain(|key| key.0 >= floor);
    }
}

fn complete_bank(bank: &Bank, slot: u64, transactions: &[SubscribeUpdateTransactionInfo]) -> bool {
    let Some(metadata) = &bank.metadata else {
        return false;
    };
    if metadata.executed_transaction_count != transactions.len() as u64
        || metadata.entry_count != bank.entries.len() as u64
    {
        return false;
    }
    let mut indices = HashSet::new();
    let mut signatures = HashSet::new();
    if transactions.iter().any(|tx| {
        tx.index >= transactions.len() as u64
            || !indices.insert(tx.index)
            || !signatures.insert(&tx.signature)
            || super::convert::stored_record_from_transaction_info(slot, tx).is_err()
    }) {
        return false;
    }
    let mut entries = bank.entries.iter().collect::<Vec<_>>();
    entries.sort_unstable_by_key(|entry| entry.index);
    let mut next = 0u64;
    for (index, entry) in entries.into_iter().enumerate() {
        if entry.index != index as u64
            || entry.starting_transaction_index != next
            || entry.hash.len() != 32
        {
            return false;
        }
        let Some(end) = next.checked_add(entry.executed_transaction_count) else {
            return false;
        };
        if end > metadata.executed_transaction_count {
            return false;
        }
        next = end;
    }
    next == metadata.executed_transaction_count
}

#[cfg(test)]
pub(crate) mod tests;
