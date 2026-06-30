// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Signature/address index derivation and reads.
//!
//! [`derive_index_entries`] ports the SQL of the ClickHouse materialized views —
//! `ddl/local/gsfa.sql`, `ddl/local/signatures.sql`, and
//! `ddl/local/token_owner_activity.sql` — to Rust. The disk cache answers in
//! place of ClickHouse, so any divergence from the MV semantics yields different
//! result SETS than ClickHouse, not just staleness. One shared derivation is used
//! by live, backfill, and repair writes so all sources agree.

use once_cell::sync::Lazy;
use rocksdb::{Direction, IteratorMode, ReadOptions};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;

use crate::clickhouse::{
    NumericFilter, ResolvedSignatureFilter, SignatureRecord, SignatureSlot, SlotBoundary,
    SortOrder, StoredTransactionRecord, TokenAccountsFilter, TransactionStatusFilter, extract_memo,
    parse_err_json,
};

use super::{DiskCacheError, DiskCacheInner, DiskGsfaPage, codec, schema};

/// Addresses the gsfa MV never indexes (`gsfa_ignored_addresses` in
/// `ddl/local/gsfa.sql`). The head cache intentionally indexes everything; the
/// disk cache MUST filter, both to mirror ClickHouse result sets and because ten
/// epochs of vote-transaction entries would dwarf the rest of the index.
static GSFA_IGNORED_ADDRESSES: Lazy<[[u8; 32]; 4]> = Lazy::new(|| {
    use std::str::FromStr;
    [
        Pubkey::from_str("11111111111111111111111111111111").expect("system program"),
        Pubkey::from_str("Vote111111111111111111111111111111111111111").expect("vote program"),
        Pubkey::from_str("SysvarC1ock11111111111111111111111111111111").expect("sysvar clock"),
        Pubkey::from_str("SysvarS1otHashes111111111111111111111111111")
            .expect("sysvar slot hashes"),
    ]
    .map(|pubkey| pubkey.to_bytes())
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TokenOwnerEntry {
    pub(crate) owner: [u8; 32],
    pub(crate) token_account: [u8; 32],
    pub(crate) balance_changed: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct TxIndexEntries {
    /// `meta_err` when the transaction failed, `None` on success — the
    /// `if(meta_status_ok = 1, NULL, meta_err)` of the MVs.
    pub(crate) err: Option<String>,
    pub(crate) memo: Option<String>,
    /// Every transaction signature, mirroring `signatures.sql`'s ARRAY JOIN.
    pub(crate) signatures: Vec<[u8; 64]>,
    /// Distinct addresses touched by the transaction, minus the ignored set.
    pub(crate) addresses: Vec<[u8; 32]>,
    pub(crate) token_entries: Vec<TokenOwnerEntry>,
}

pub(crate) fn derive_index_entries(record: &StoredTransactionRecord) -> TxIndexEntries {
    let err = if record.meta_status_ok {
        None
    } else {
        record.meta_err.clone()
    };
    let memo = extract_memo(record);

    // Concatenation order matters: instruction and token-balance account indexes
    // resolve against static keys, then loaded writable, then loaded readonly.
    let mut account_keys_all = Vec::with_capacity(
        record.tx_account_keys.len()
            + record.meta_loaded_addresses_writable.len()
            + record.meta_loaded_addresses_readonly.len(),
    );
    account_keys_all.extend_from_slice(&record.tx_account_keys);
    account_keys_all.extend_from_slice(&record.meta_loaded_addresses_writable);
    account_keys_all.extend_from_slice(&record.meta_loaded_addresses_readonly);

    let mut addresses = account_keys_all.clone();
    addresses.sort_unstable();
    addresses.dedup();
    addresses.retain(|address| !GSFA_IGNORED_ADDRESSES.contains(address));

    let token_entries = derive_token_entries(record, &account_keys_all);

    TxIndexEntries {
        err,
        memo,
        signatures: record.tx_signatures.clone(),
        addresses,
        token_entries,
    }
}

/// Ports the `token_entries` expression of `ddl/local/token_owner_activity.sql`
/// (ClickHouse arrays are 1-based and out-of-range `arrayElement` yields
/// NULL/default; `Option` chains reproduce that).
fn derive_token_entries(
    record: &StoredTransactionRecord,
    account_keys_all: &[[u8; 32]],
) -> Vec<TokenOwnerEntry> {
    let mut token_indices: Vec<u8> = Vec::with_capacity(
        record.meta_pre_token_account_index.len() + record.meta_post_token_account_index.len(),
    );
    token_indices.extend_from_slice(&record.meta_pre_token_account_index);
    token_indices.extend_from_slice(&record.meta_post_token_account_index);
    token_indices.sort_unstable();
    token_indices.dedup();

    let mut entries = Vec::with_capacity(token_indices.len());
    for idx in token_indices {
        let pre_pos = record
            .meta_pre_token_account_index
            .iter()
            .position(|&index| index == idx);
        let post_pos = record
            .meta_post_token_account_index
            .iter()
            .position(|&index| index == idx);

        // coalesce(post_owner[post], pre_owner[pre])
        let owner = post_pos
            .and_then(|pos| record.meta_post_token_owner.get(pos).copied().flatten())
            .or_else(|| {
                pre_pos.and_then(|pos| record.meta_pre_token_owner.get(pos).copied().flatten())
            });
        let Some(owner) = owner else {
            continue;
        };
        // `idx < length(account_keys_all)` bounds guard.
        let Some(token_account) = account_keys_all.get(usize::from(idx)).copied() else {
            continue;
        };

        let balance_changed = match (pre_pos, post_pos) {
            (Some(pre), Some(post)) => {
                record
                    .meta_pre_token_amount
                    .get(pre)
                    .map(String::as_str)
                    .unwrap_or("")
                    != record
                        .meta_post_token_amount
                        .get(post)
                        .map(String::as_str)
                        .unwrap_or("")
            }
            _ => true,
        };

        entries.push(TokenOwnerEntry {
            owner,
            token_account,
            balance_changed,
        });
    }
    entries
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiskSigStatus {
    pub(crate) slot: u64,
    pub(crate) slot_idx: u32,
    pub(crate) err: Option<String>,
}

impl DiskCacheInner {
    /// Resolve a signature to its stored status. Bounded by the eviction floor:
    /// index entries of evicted slots may linger until compaction.
    pub(crate) fn get_sig_status_sync(&self, signature: &Signature) -> Option<DiskSigStatus> {
        let cf = self.cf(schema::CF_SIG).ok()?;
        let raw = self.db.get_pinned_cf(&cf, signature.as_ref()).ok()??;
        let value = codec::decode_sig_value(&raw)?;
        if value.slot < self.min_retained() {
            return None;
        }
        Some(DiskSigStatus {
            slot: value.slot,
            slot_idx: value.idx,
            err: value.err,
        })
    }

    pub(crate) fn signature_position_sync(&self, signature: &Signature) -> Option<SignatureSlot> {
        self.get_sig_status_sync(signature)
            .map(|status| SignatureSlot {
                slot: status.slot,
                slot_idx: status.slot_idx,
            })
    }

    /// Full transaction lookup: signature index, then the slot-keyed record. The
    /// record must list the signature (defense against index/record mismatch).
    pub(crate) fn get_tx_sync(&self, signature: &Signature) -> Option<StoredTransactionRecord> {
        let position = self.signature_position_sync(signature)?;
        let cf = self.cf(schema::CF_TX).ok()?;
        let raw = self
            .db
            .get_pinned_cf(&cf, schema::tx_key(position.slot, position.slot_idx))
            .ok()??;
        let record = codec::decode_record::<StoredTransactionRecord>(&raw)?;
        let sig_bytes: [u8; 64] = *signature.as_array();
        if !record.tx_signatures.contains(&sig_bytes) {
            tracing::warn!(
                signature = %signature,
                slot = position.slot,
                "disk cache: signature index points at a record without that signature"
            );
            return None;
        }
        Some(record)
    }

    /// Newest-first address-index scan within the contiguous tip span.
    ///
    /// `before`/`until` are exclusive, matching gSFA semantics. Returns `None`
    /// when there is no coverage to evaluate. Any decode failure is an error —
    /// a gSFA page must be complete, so the caller degrades to ClickHouse rather
    /// than silently dropping an entry.
    pub(crate) fn signatures_for_address_sync(
        &self,
        address: &Pubkey,
        before: Option<SlotBoundary>,
        until: Option<SlotBoundary>,
        limit: usize,
    ) -> Result<Option<DiskGsfaPage>, DiskCacheError> {
        let Some((tip_floor, _)) = self
            .coverage
            .read()
            .expect("coverage lock")
            .contiguous_tip_span()
        else {
            return Ok(None);
        };
        let floor = tip_floor.max(self.min_retained());
        let address_bytes = address.to_bytes();

        let floor_bound = floor_upper_bound(&address_bytes, floor);
        let until_bound = until.map(|boundary| until_upper_bound(&address_bytes, boundary));
        let (upper_bound, floor_is_effective) = match until_bound {
            Some(until_key) if until_key <= floor_bound => (until_key, false),
            Some(_) | None => (floor_bound, true),
        };
        let seek = seek_key(&address_bytes, before);

        let addr_cf = self.cf(schema::CF_ADDR_SIG)?;
        let mut read_opts = ReadOptions::default();
        read_opts.set_iterate_upper_bound(upper_bound);
        let iter = self.db.iterator_cf_opt(
            &addr_cf,
            read_opts,
            IteratorMode::From(&seek, Direction::Forward),
        );

        let mut records = Vec::new();
        for entry in iter {
            let (key, raw) = entry?;
            let Some(slot) = schema::addr_key_slot(&key) else {
                return Err(DiskCacheError::CorruptIndexEntry { slot: None });
            };
            let idx =
                addr_key_idx(&key).ok_or(DiskCacheError::CorruptIndexEntry { slot: Some(slot) })?;
            let Some(value) = codec::decode_addr_sig_value(&raw) else {
                return Err(DiskCacheError::CorruptIndexEntry { slot: Some(slot) });
            };

            let signature = bs58::encode(value.signature).into_string();
            let err = value
                .err
                .and_then(|raw_err| parse_err_json(&signature, raw_err));
            records.push(SignatureRecord {
                signature,
                slot,
                slot_idx: idx,
                err,
                // Stored pre-formatted ("[len] text"), identical to what
                // format_gsfa_memo yields for ClickHouse rows.
                memo: value.memo,
                block_time: value.block_time,
            });
            if records.len() >= limit {
                break;
            }
        }

        let reached_floor = records.len() < limit && floor_is_effective;
        Ok(Some(DiskGsfaPage {
            records,
            reached_floor,
            floor,
        }))
    }
}

/// Disk-side getTransactionsForAddress query. All signature bounds and the
/// pagination token must already be resolved to positions — the handler skips
/// the disk tier otherwise.
#[derive(Debug, Clone)]
pub(crate) struct DiskTfaQuery {
    pub(crate) limit: usize,
    pub(crate) sort_order: SortOrder,
    /// Exclusive position bound: Desc pages continue strictly below it, Asc
    /// pages strictly above it.
    pub(crate) pagination: Option<SignatureSlot>,
    pub(crate) slot_filter: Option<NumericFilter<u64>>,
    pub(crate) block_time_filter: Option<NumericFilter<i64>>,
    pub(crate) signature_filter: Option<ResolvedSignatureFilter>,
    pub(crate) status: TransactionStatusFilter,
    pub(crate) token_accounts: TokenAccountsFilter,
}

impl DiskCacheInner {
    /// getTransactionsForAddress over the disk indexes: the gsfa index, unioned
    /// with the token-owner index when the token filter is active, deduplicated
    /// by position (one transaction occupies one `(slot, idx)`), with all
    /// resolved bounds folded into the key-space scan window.
    ///
    /// Asc queries must be fully answerable from coverage (the handler gates on
    /// the lower bound being at or above the floor), so `reached_floor` can only
    /// be set for Desc scans.
    pub(crate) fn transactions_for_address_sync(
        &self,
        address: &Pubkey,
        query: &DiskTfaQuery,
    ) -> Result<Option<DiskGsfaPage>, DiskCacheError> {
        let Some((tip_floor, _)) = self
            .coverage
            .read()
            .expect("coverage lock")
            .contiguous_tip_span()
        else {
            return Ok(None);
        };
        let floor = tip_floor.max(self.min_retained());
        let address_bytes = address.to_bytes();

        let (key_low, key_high, floor_is_effective) = tfa_key_window(&address_bytes, query, floor);
        if key_low >= key_high {
            // Bounds are contradictory; nothing in the window. For Desc this can
            // mean the whole window sits below the floor.
            return Ok(Some(DiskGsfaPage {
                records: Vec::new(),
                reached_floor: query.sort_order == SortOrder::Desc && floor_is_effective,
                floor,
            }));
        }

        let gsfa_iter = self.window_iterator(
            schema::CF_ADDR_SIG,
            key_low.clone(),
            key_high.clone(),
            query.sort_order,
        )?;
        let token_iter = (query.token_accounts != TokenAccountsFilter::None)
            .then(|| {
                self.window_iterator(
                    schema::CF_TOKEN_OWNER,
                    key_low.clone(),
                    key_high.clone(),
                    query.sort_order,
                )
            })
            .transpose()?;

        let balance_changed_only = query.token_accounts == TokenAccountsFilter::BalanceChanged;
        let mut gsfa = IndexCursor::new(gsfa_iter, &key_high, query.sort_order, false)?;
        let mut token = match token_iter {
            Some(iter) => Some(IndexCursor::new(
                iter,
                &key_high,
                query.sort_order,
                balance_changed_only,
            )?),
            None => None,
        };

        let mut records: Vec<SignatureRecord> = Vec::new();
        let mut last_position: Option<(u64, u32)> = None;
        while records.len() < query.limit {
            // Pick the next entry across both cursors in scan order.
            let take_gsfa = match (
                &gsfa.current,
                token.as_ref().and_then(|t| t.current.as_ref()),
            ) {
                (None, None) => break,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (Some((g_pos, _)), Some((t_pos, _))) => match query.sort_order {
                    SortOrder::Desc => g_pos >= t_pos,
                    SortOrder::Asc => g_pos <= t_pos,
                },
            };
            let (position, value) = if take_gsfa {
                gsfa.take()?.expect("cursor checked")
            } else {
                token
                    .as_mut()
                    .expect("token cursor present")
                    .take()?
                    .expect("cursor checked")
            };

            // LIMIT 1 BY signature: one transaction occupies one position, and
            // the scan visits positions monotonically.
            if last_position == Some(position) {
                continue;
            }
            last_position = Some(position);

            if !tfa_entry_matches(&value, position.0, query) {
                continue;
            }

            let signature = bs58::encode(value.signature).into_string();
            let err = value
                .err
                .and_then(|raw_err| parse_err_json(&signature, raw_err));
            records.push(SignatureRecord {
                signature,
                slot: position.0,
                slot_idx: position.1,
                err,
                memo: value.memo,
                block_time: value.block_time,
            });
        }

        let reached_floor = query.sort_order == SortOrder::Desc
            && records.len() < query.limit
            && floor_is_effective;
        Ok(Some(DiskGsfaPage {
            records,
            reached_floor,
            floor,
        }))
    }

    fn window_iterator(
        &self,
        cf_name: &str,
        key_low: Vec<u8>,
        key_high: Vec<u8>,
        sort_order: SortOrder,
    ) -> Result<
        rocksdb::DBIteratorWithThreadMode<'_, rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>>,
        super::DiskCacheError,
    > {
        let cf = self.cf(cf_name)?;
        let mut read_opts = ReadOptions::default();
        Ok(match sort_order {
            SortOrder::Desc => {
                read_opts.set_iterate_upper_bound(key_high.clone());
                self.db.iterator_cf_opt(
                    &cf,
                    read_opts,
                    IteratorMode::From(&key_low, Direction::Forward),
                )
            }
            SortOrder::Asc => {
                read_opts.set_iterate_lower_bound(key_low.clone());
                // The reverse start is inclusive; IndexCursor drops any entry at
                // or beyond the exclusive high bound.
                self.db.iterator_cf_opt(
                    &cf,
                    read_opts,
                    IteratorMode::From(&key_high, Direction::Reverse),
                )
            }
        })
    }

    /// Batch full-record fetch by position; `None` per entry on miss (e.g. the
    /// eviction floor passed the slot between the index scan and this fetch).
    pub(crate) fn get_txs_by_position_sync(
        &self,
        positions: &[(u64, u32)],
    ) -> Vec<Option<StoredTransactionRecord>> {
        let tx_cf = match self.cf(schema::CF_TX) {
            Ok(cf) => cf,
            Err(_) => return vec![None; positions.len()],
        };
        positions
            .iter()
            .map(|&(slot, idx)| {
                self.db
                    .get_pinned_cf(&tx_cf, schema::tx_key(slot, idx))
                    .ok()
                    .flatten()
                    .and_then(|raw| codec::decode_record::<StoredTransactionRecord>(&raw))
            })
            .collect()
    }
}

/// One decoded index entry: position plus value.
type IndexEntry = ((u64, u32), codec::AddrSigValue);

/// Streaming decode over one index CF; `current` always holds the next
/// in-window entry (already past the exclusive high bound and flag filters).
struct IndexCursor<'a> {
    iter: rocksdb::DBIteratorWithThreadMode<'a, rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>>,
    key_high: &'a [u8],
    sort_order: SortOrder,
    balance_changed_only: bool,
    current: Option<IndexEntry>,
}

impl<'a> IndexCursor<'a> {
    fn new(
        iter: rocksdb::DBIteratorWithThreadMode<
            'a,
            rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>,
        >,
        key_high: &'a [u8],
        sort_order: SortOrder,
        balance_changed_only: bool,
    ) -> Result<Self, DiskCacheError> {
        let mut cursor = Self {
            iter,
            key_high,
            sort_order,
            balance_changed_only,
            current: None,
        };
        cursor.advance()?;
        Ok(cursor)
    }

    fn take(&mut self) -> Result<Option<IndexEntry>, DiskCacheError> {
        let current = self.current.take();
        self.advance()?;
        Ok(current)
    }

    fn advance(&mut self) -> Result<(), DiskCacheError> {
        self.current = None;
        for entry in self.iter.by_ref() {
            let (key, raw) = entry?;
            // Reverse iteration starts at the exclusive high bound itself.
            if self.sort_order == SortOrder::Asc && key.as_ref() >= self.key_high {
                continue;
            }
            let Some(slot) = schema::addr_key_slot(&key) else {
                return Err(DiskCacheError::CorruptIndexEntry { slot: None });
            };
            let Some(idx) = addr_key_idx(&key) else {
                return Err(DiskCacheError::CorruptIndexEntry { slot: Some(slot) });
            };
            let Some(value) = codec::decode_addr_sig_value(&raw) else {
                return Err(DiskCacheError::CorruptIndexEntry { slot: Some(slot) });
            };
            if self.balance_changed_only && !value.balance_changed {
                continue;
            }
            self.current = Some(((slot, idx), value));
            break;
        }
        Ok(())
    }
}

/// Translate the resolved bounds into one key-space window
/// `[key_low, key_high)` shared by both indexes (their first 44 key bytes have
/// identical layout). Returns whether the floor is the binding old-side bound,
/// which is what decides `reached_floor` for Desc scans.
fn tfa_key_window(
    address: &[u8; 32],
    query: &DiskTfaQuery,
    floor: u64,
) -> (Vec<u8>, Vec<u8>, bool) {
    // Key space is order-reversed: newer positions have smaller keys. The "new"
    // side of the window is key_low, the "old" side is key_high.
    let mut key_low = {
        let mut key = Vec::with_capacity(44);
        key.extend_from_slice(address);
        key.extend_from_slice(&[0u8; 12]);
        key
    };
    let mut key_high = floor_upper_bound(address, floor);
    let mut floor_is_effective = true;

    let tighten_new_side = |candidate: Vec<u8>, key_low: &mut Vec<u8>| {
        if candidate > *key_low {
            *key_low = candidate;
        }
    };
    let tighten_old_side = |candidate: Vec<u8>, key_high: &mut Vec<u8>, floor_eff: &mut bool| {
        // Equality counts: a filter bound that coincides with the floor means
        // the query never extends below coverage, so the floor is not the
        // binding constraint and no ClickHouse remainder exists.
        if candidate <= *key_high {
            *key_high = candidate;
            *floor_eff = false;
        }
    };

    let filter = query.signature_filter.clone().unwrap_or_default();
    let slot_filter = query.slot_filter.clone().unwrap_or_default();

    // New-side (upper position) bounds: pagination for Desc, slot lt/lte,
    // signature lt/lte.
    if query.sort_order == SortOrder::Desc
        && let Some(position) = query.pagination
    {
        let mut key = schema::addr_sig_key(address, position.slot, position.slot_idx).to_vec();
        key.push(0); // strictly older than the position
        tighten_new_side(key, &mut key_low);
    }
    if let Some(slot) = slot_filter.lt
        && slot > 0
    {
        tighten_new_side(
            seek_key(address, Some(SlotBoundary::Slot(slot))),
            &mut key_low,
        );
    }
    if let Some(slot) = slot_filter.lte
        && slot < u64::MAX
    {
        tighten_new_side(
            seek_key(address, Some(SlotBoundary::Slot(slot + 1))),
            &mut key_low,
        );
    }
    if let Some(position) = filter.lte {
        tighten_new_side(
            schema::addr_sig_key(address, position.slot, position.slot_idx).to_vec(),
            &mut key_low,
        );
    }
    if let Some(position) = filter.lt {
        let mut key = schema::addr_sig_key(address, position.slot, position.slot_idx).to_vec();
        key.push(0);
        tighten_new_side(key, &mut key_low);
    }

    // Old-side (lower position) bounds: pagination for Asc, slot gt/gte,
    // signature gt/gte. The floor seeds key_high above.
    if query.sort_order == SortOrder::Asc
        && let Some(position) = query.pagination
    {
        tighten_old_side(
            schema::addr_sig_key(address, position.slot, position.slot_idx).to_vec(),
            &mut key_high,
            &mut floor_is_effective,
        );
    }
    if let Some(slot) = slot_filter.gt {
        tighten_old_side(
            until_upper_bound(address, SlotBoundary::Slot(slot)),
            &mut key_high,
            &mut floor_is_effective,
        );
    }
    if let Some(slot) = slot_filter.gte
        && slot > 0
    {
        tighten_old_side(
            until_upper_bound(address, SlotBoundary::Slot(slot - 1)),
            &mut key_high,
            &mut floor_is_effective,
        );
    }
    if let Some(position) = filter.gte {
        let mut key = schema::addr_sig_key(address, position.slot, position.slot_idx).to_vec();
        key.push(0); // include the position itself
        tighten_old_side(key, &mut key_high, &mut floor_is_effective);
    }
    if let Some(position) = filter.gt {
        tighten_old_side(
            schema::addr_sig_key(address, position.slot, position.slot_idx).to_vec(),
            &mut key_high,
            &mut floor_is_effective,
        );
    }

    (key_low, key_high, floor_is_effective)
}

/// Predicates that cannot be folded into the key window.
fn tfa_entry_matches(value: &codec::AddrSigValue, _slot: u64, query: &DiskTfaQuery) -> bool {
    match query.status {
        TransactionStatusFilter::Any => {}
        TransactionStatusFilter::Succeeded => {
            if value.err.is_some() {
                return false;
            }
        }
        TransactionStatusFilter::Failed => {
            if value.err.is_none() {
                return false;
            }
        }
    }

    if let Some(filter) = query.block_time_filter.as_ref() {
        // Mirrors ClickHouse comparison semantics: a NULL block_time fails
        // every bound.
        let Some(block_time) = value.block_time else {
            return false;
        };
        if let Some(bound) = filter.eq
            && block_time != bound
        {
            return false;
        }
        if let Some(bound) = filter.gte
            && block_time < bound
        {
            return false;
        }
        if let Some(bound) = filter.gt
            && block_time <= bound
        {
            return false;
        }
        if let Some(bound) = filter.lte
            && block_time > bound
        {
            return false;
        }
        if let Some(bound) = filter.lt
            && block_time >= bound
        {
            return false;
        }
    }

    true
}

/// Slot index embedded in an `addr_sig` key.
fn addr_key_idx(key: &[u8]) -> Option<u32> {
    let raw = key.get(40..44)?;
    Some(!u32::from_be_bytes(raw.try_into().ok()?))
}

/// First key of the scan: everything strictly *older* than `before` (exclusive),
/// or the newest entry of the address when unbounded.
fn seek_key(address: &[u8; 32], before: Option<SlotBoundary>) -> Vec<u8> {
    match before {
        None => {
            let mut key = Vec::with_capacity(44);
            key.extend_from_slice(address);
            key.extend_from_slice(&[0u8; 12]);
            key
        }
        Some(SlotBoundary::Position(position)) => {
            // One byte past the exact key: strictly older entries only.
            let mut key = schema::addr_sig_key(address, position.slot, position.slot_idx).to_vec();
            key.push(0);
            key
        }
        Some(SlotBoundary::Slot(slot)) => {
            let Some(prev_slot) = slot.checked_sub(1) else {
                // slot < 0 is unsatisfiable; seek past the whole prefix.
                return floor_upper_bound(address, 0);
            };
            let mut key = Vec::with_capacity(44);
            key.extend_from_slice(address);
            key.extend_from_slice(&schema::rev_slot(prev_slot).to_be_bytes());
            key.extend_from_slice(&[0u8; 4]);
            key
        }
    }
}

/// Exclusive upper bound that keeps `slot >= floor` (and stays within the
/// address prefix when `floor == 0`).
fn floor_upper_bound(address: &[u8; 32], floor: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(45);
    key.extend_from_slice(address);
    match floor.checked_sub(1) {
        Some(prev_slot) => {
            key.extend_from_slice(&schema::rev_slot(prev_slot).to_be_bytes());
            key.extend_from_slice(&[0u8; 4]);
        }
        None => {
            // floor == 0: bound just past the last possible key of the prefix.
            // A longer key sorts after every 44-byte key sharing the prefix.
            key.extend_from_slice(&[0xFFu8; 12]);
            key.push(0);
        }
    }
    key
}

/// Exclusive upper bound for `until`: keeps entries strictly newer than it.
fn until_upper_bound(address: &[u8; 32], until: SlotBoundary) -> Vec<u8> {
    match until {
        // Excludes the until position itself and everything older.
        SlotBoundary::Position(position) => {
            schema::addr_sig_key(address, position.slot, position.slot_idx).to_vec()
        }
        // Excludes slot <= until.
        SlotBoundary::Slot(slot) => {
            let mut key = Vec::with_capacity(44);
            key.extend_from_slice(address);
            key.extend_from_slice(&schema::rev_slot(slot).to_be_bytes());
            key.extend_from_slice(&[0u8; 4]);
            key
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_record() -> StoredTransactionRecord {
        StoredTransactionRecord {
            signature: [9u8; 64],
            slot: 100,
            slot_idx: 0,
            block_time: Some(1_700_000_000),
            tx_version: None,
            tx_signatures: vec![[9u8; 64]],
            tx_num_required_signatures: 1,
            tx_num_readonly_signed_accounts: 0,
            tx_num_readonly_unsigned_accounts: 0,
            tx_account_keys: Vec::new(),
            tx_recent_blockhash: [0u8; 32],
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

    fn pubkey_bytes(seed: u8) -> [u8; 32] {
        [seed; 32]
    }

    // MV parity tests for ddl/local/gsfa.sql, ddl/local/signatures.sql, and
    // ddl/local/token_owner_activity.sql. Update these when those views change.
    #[test]
    fn mv_parity_gsfa_addresses_are_distinct_loaded_and_filtered() {
        use std::str::FromStr;
        let vote_program = Pubkey::from_str("Vote111111111111111111111111111111111111111")
            .unwrap()
            .to_bytes();
        let system_program = Pubkey::from_str("11111111111111111111111111111111")
            .unwrap()
            .to_bytes();

        let mut record = base_record();
        record.tx_account_keys = vec![
            pubkey_bytes(1),
            vote_program,
            pubkey_bytes(2),
            pubkey_bytes(1),
        ];
        record.meta_loaded_addresses_writable = vec![pubkey_bytes(2), pubkey_bytes(3)];
        record.meta_loaded_addresses_readonly = vec![system_program, pubkey_bytes(4)];

        let entries = derive_index_entries(&record);
        let mut expected = vec![
            pubkey_bytes(1),
            pubkey_bytes(2),
            pubkey_bytes(3),
            pubkey_bytes(4),
        ];
        expected.sort_unstable();
        assert_eq!(entries.addresses, expected);
    }

    #[test]
    fn mv_parity_err_follows_meta_status() {
        let mut record = base_record();
        record.meta_status_ok = true;
        record.meta_err = Some("ignored".to_string());
        assert_eq!(derive_index_entries(&record).err, None);

        record.meta_status_ok = false;
        assert_eq!(
            derive_index_entries(&record).err.as_deref(),
            Some("ignored")
        );
    }

    #[test]
    fn mv_parity_signatures_indexes_every_transaction_signature() {
        let mut record = base_record();
        let second_sig = [8u8; 64];
        let third_sig = [7u8; 64];
        record.tx_signatures = vec![record.signature, second_sig, third_sig];

        assert_eq!(
            derive_index_entries(&record).signatures,
            record.tx_signatures
        );
    }

    #[test]
    fn mv_parity_memo_extraction_uses_combined_account_keys() {
        use std::str::FromStr;
        let memo_program = Pubkey::from_str("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr")
            .unwrap()
            .to_bytes();

        let mut record = base_record();
        // Memo program resolved through a loaded address (index 1 = loaded writable).
        record.tx_account_keys = vec![pubkey_bytes(1)];
        record.meta_loaded_addresses_writable = vec![memo_program];
        record.tx_instructions_program_id_index = vec![1];
        record.tx_instructions_data = vec![b"hello".to_vec()];

        let entries = derive_index_entries(&record);
        assert_eq!(entries.memo.as_deref(), Some("[5] hello"));
    }

    #[test]
    fn mv_parity_token_entries_follow_owner_and_balance_semantics() {
        let mut record = base_record();
        record.tx_account_keys = vec![
            pubkey_bytes(1), // idx 0
            pubkey_bytes(2), // idx 1: token account A
            pubkey_bytes(3), // idx 2: token account B
            pubkey_bytes(4), // idx 3: token account C (pre only)
        ];

        // idx 1: pre+post, amounts equal -> balance_changed = false
        // idx 2: pre+post, amounts differ -> balance_changed = true
        // idx 3: pre only -> balance_changed = true, owner from pre
        // idx 9: out of bounds -> dropped
        record.meta_pre_token_account_index = vec![1, 2, 3, 9];
        record.meta_pre_token_owner = vec![
            Some(pubkey_bytes(11)),
            Some(pubkey_bytes(12)),
            Some(pubkey_bytes(13)),
            Some(pubkey_bytes(14)),
        ];
        record.meta_pre_token_amount = vec![
            "100".to_string(),
            "200".to_string(),
            "300".to_string(),
            "400".to_string(),
        ];
        record.meta_post_token_account_index = vec![1, 2];
        record.meta_post_token_owner = vec![Some(pubkey_bytes(21)), None];
        record.meta_post_token_amount = vec!["100".to_string(), "999".to_string()];

        let entries = derive_index_entries(&record).token_entries;
        assert_eq!(
            entries,
            vec![
                TokenOwnerEntry {
                    // post owner wins (coalesce).
                    owner: pubkey_bytes(21),
                    token_account: pubkey_bytes(2),
                    balance_changed: false,
                },
                TokenOwnerEntry {
                    // post owner NULL -> falls back to pre owner.
                    owner: pubkey_bytes(12),
                    token_account: pubkey_bytes(3),
                    balance_changed: true,
                },
                TokenOwnerEntry {
                    owner: pubkey_bytes(13),
                    token_account: pubkey_bytes(4),
                    balance_changed: true,
                },
            ]
        );
    }

    #[test]
    fn mv_parity_token_entry_with_no_owner_is_dropped() {
        let mut record = base_record();
        record.tx_account_keys = vec![pubkey_bytes(1), pubkey_bytes(2)];
        record.meta_pre_token_account_index = vec![1];
        record.meta_pre_token_owner = vec![None];
        record.meta_pre_token_amount = vec!["1".to_string()];

        assert!(derive_index_entries(&record).token_entries.is_empty());
    }
}
