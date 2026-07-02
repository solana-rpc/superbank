// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! RocksDB schema for the disk cache: column families, key encodings, and options.
//!
//! All integers in keys are big-endian so lexicographic order equals numeric order.
//! `addr_sig` / `token_owner` keys store the bitwise NOT of slot/idx so a forward
//! iterator yields newest-first, matching the `ORDER BY slot DESC, slot_idx DESC`
//! of the gsfa / token_owner_activity materialized views.
//!
//! Bump [`SCHEMA_VERSION`] whenever any key or value layout changes; a mismatch at
//! open wipes and rebuilds the database (it is only a cache).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rocksdb::{
    BlockBasedIndexType, BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBCompressionType,
    Options, SliceTransform, compaction_filter::Decision,
};

pub(crate) const SCHEMA_VERSION: u32 = 1;

pub(crate) const CF_META: &str = "meta";
pub(crate) const CF_SLOT_COVERAGE: &str = "slot_coverage";
pub(crate) const CF_BLOCK_META: &str = "block_meta";
pub(crate) const CF_TX: &str = "tx";
pub(crate) const CF_SIG: &str = "sig";
pub(crate) const CF_ADDR_SIG: &str = "addr_sig";
pub(crate) const CF_TOKEN_OWNER: &str = "token_owner";

pub(crate) const ALL_CFS: [&str; 7] = [
    CF_META,
    CF_SLOT_COVERAGE,
    CF_BLOCK_META,
    CF_TX,
    CF_SIG,
    CF_ADDR_SIG,
    CF_TOKEN_OWNER,
];

// `meta` CF keys.
pub(crate) const META_SCHEMA_VERSION: &[u8] = b"schema_version";
pub(crate) const META_MIN_RETAINED: &[u8] = b"min_retained";

// `slot_coverage` value tags.
pub(crate) const COVERAGE_TAG_COVERED: u8 = 1;
pub(crate) const COVERAGE_TAG_SKIPPED: u8 = 2;

// `slot_coverage` source bytes (diagnostics only).
pub(crate) const COVERAGE_SOURCE_LIVE: u8 = 0;
pub(crate) const COVERAGE_SOURCE_BACKFILL: u8 = 1;
pub(crate) const COVERAGE_SOURCE_REPAIR: u8 = 2;

pub(crate) fn coverage_source_label(source: u8) -> &'static str {
    match source {
        COVERAGE_SOURCE_LIVE => "live",
        COVERAGE_SOURCE_BACKFILL => "backfill",
        COVERAGE_SOURCE_REPAIR => "repair",
        _ => "unknown",
    }
}

#[inline]
pub(crate) fn rev_slot(slot: u64) -> u64 {
    !slot
}

#[inline]
pub(crate) fn rev_idx(idx: u32) -> u32 {
    !idx
}

#[inline]
pub(crate) fn slot_key(slot: u64) -> [u8; 8] {
    slot.to_be_bytes()
}

#[inline]
pub(crate) fn tx_key(slot: u64, idx: u32) -> [u8; 12] {
    let mut key = [0u8; 12];
    key[..8].copy_from_slice(&slot.to_be_bytes());
    key[8..].copy_from_slice(&idx.to_be_bytes());
    key
}

/// `addr_sig` key: `address ++ rev(slot) ++ rev(idx)`. The signature lives in the
/// value: `(address, slot, idx)` is unique because one transaction occupies one
/// `(slot, idx)` and addresses are deduplicated per transaction.
#[inline]
pub(crate) fn addr_sig_key(address: &[u8; 32], slot: u64, idx: u32) -> [u8; 44] {
    let mut key = [0u8; 44];
    key[..32].copy_from_slice(address);
    key[32..40].copy_from_slice(&rev_slot(slot).to_be_bytes());
    key[40..44].copy_from_slice(&rev_idx(idx).to_be_bytes());
    key
}

/// `token_owner` key: `owner ++ rev(slot) ++ rev(idx) ++ token_account`. The token
/// account is part of the key because one transaction may touch several token
/// accounts of the same owner (mirrors the token_owner_activity ordering key).
#[inline]
pub(crate) fn token_owner_key(
    owner: &[u8; 32],
    slot: u64,
    idx: u32,
    token_account: &[u8; 32],
) -> [u8; 76] {
    let mut key = [0u8; 76];
    key[..32].copy_from_slice(owner);
    key[32..40].copy_from_slice(&rev_slot(slot).to_be_bytes());
    key[40..44].copy_from_slice(&rev_idx(idx).to_be_bytes());
    key[44..].copy_from_slice(token_account);
    key
}

/// Slot embedded in an `addr_sig` / `token_owner` key (for the compaction filter).
#[inline]
pub(crate) fn addr_key_slot(key: &[u8]) -> Option<u64> {
    let raw = key.get(32..40)?;
    Some(!u64::from_be_bytes(raw.try_into().ok()?))
}

pub(crate) struct DiskCacheOptions {
    pub(crate) block_cache_bytes: usize,
    pub(crate) total_write_buffer_bytes: usize,
    pub(crate) compaction_rate_bytes_per_sec: i64,
}

impl Default for DiskCacheOptions {
    fn default() -> Self {
        Self {
            block_cache_bytes: 4 << 30,
            total_write_buffer_bytes: 2 << 30,
            compaction_rate_bytes_per_sec: 256 << 20,
        }
    }
}

/// SSTs of the index CFs are rewritten at least daily so the compaction filters
/// reclaim evicted entries with bounded lag.
const INDEX_PERIODIC_COMPACTION_SECS: u64 = 86_400;

pub(crate) fn db_options(opts: &DiskCacheOptions) -> Options {
    let mut db = Options::default();
    db.create_if_missing(true);
    db.create_missing_column_families(true);
    // Required for crash consistency of WAL-disabled backfill batches: flushes
    // cover all CFs atomically, so cross-CF batches never land partially.
    db.set_atomic_flush(true);
    db.increase_parallelism(num_cpus().min(16) as i32);
    db.set_max_background_jobs(8);
    db.set_db_write_buffer_size(opts.total_write_buffer_bytes);
    db.set_max_total_wal_size(1 << 30);
    db.set_bytes_per_sync(1 << 20);
    db.set_wal_bytes_per_sync(1 << 20);
    if opts.compaction_rate_bytes_per_sec > 0 {
        db.set_ratelimiter(opts.compaction_rate_bytes_per_sec, 100_000, 10);
    }
    db
}

pub(crate) fn cf_descriptors(
    opts: &DiskCacheOptions,
    min_retained: Arc<AtomicU64>,
) -> Vec<ColumnFamilyDescriptor> {
    let cache = Cache::new_lru_cache(opts.block_cache_bytes);

    let block_opts = |block_size: usize, ribbon: bool, cache: &Cache| {
        let mut block = BlockBasedOptions::default();
        block.set_block_size(block_size);
        block.set_block_cache(cache);
        block.set_cache_index_and_filter_blocks(true);
        block.set_pin_l0_filter_and_index_blocks_in_cache(true);
        // Partitioned filters bound resident filter memory: full filters over the
        // tens of billions of keys in a 10-epoch window would need tens of GB.
        block.set_index_type(BlockBasedIndexType::TwoLevelIndexSearch);
        block.set_partition_filters(true);
        if ribbon {
            block.set_ribbon_filter(10.0);
        } else {
            block.set_bloom_filter(10.0, false);
        }
        block
    };

    let data_compression = |cf: &mut Options| {
        cf.set_compression_per_level(&[
            DBCompressionType::None,
            DBCompressionType::None,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
        ]);
        cf.set_bottommost_compression_type(DBCompressionType::Zstd);
        cf.set_bottommost_zstd_max_train_bytes(16 * 1024 * 100, true);
    };

    let mut descriptors = Vec::with_capacity(ALL_CFS.len());

    let mut meta = Options::default();
    meta.set_write_buffer_size(8 << 20);
    descriptors.push(ColumnFamilyDescriptor::new(CF_META, meta));

    let mut slot_coverage = Options::default();
    slot_coverage.set_write_buffer_size(32 << 20);
    slot_coverage.set_compression_type(DBCompressionType::Lz4);
    descriptors.push(ColumnFamilyDescriptor::new(CF_SLOT_COVERAGE, slot_coverage));

    let mut block_meta = Options::default();
    block_meta.set_write_buffer_size(32 << 20);
    block_meta.set_compression_type(DBCompressionType::Lz4);
    block_meta.set_block_based_table_factory(&block_opts(16 << 10, false, &cache));
    descriptors.push(ColumnFamilyDescriptor::new(CF_BLOCK_META, block_meta));

    let mut tx = Options::default();
    tx.set_write_buffer_size(256 << 20);
    tx.set_target_file_size_base(256 << 20);
    tx.set_level_compaction_dynamic_level_bytes(true);
    data_compression(&mut tx);
    tx.set_prefix_extractor(SliceTransform::create_fixed_prefix(8));
    tx.set_memtable_prefix_bloom_ratio(0.1);
    tx.set_block_based_table_factory(&block_opts(32 << 10, false, &cache));
    descriptors.push(ColumnFamilyDescriptor::new(CF_TX, tx));

    let mut sig = Options::default();
    sig.set_write_buffer_size(64 << 20);
    sig.set_level_compaction_dynamic_level_bytes(true);
    sig.set_compression_type(DBCompressionType::Lz4);
    sig.set_block_based_table_factory(&block_opts(4 << 10, true, &cache));
    sig.set_periodic_compaction_seconds(INDEX_PERIODIC_COMPACTION_SECS);
    let sig_floor = min_retained.clone();
    sig.set_compaction_filter("disk_cache_window_sig", move |_level, _key, value| {
        match crate::disk_cache::codec::sig_value_slot(value) {
            Some(slot) if slot < sig_floor.load(Ordering::Relaxed) => Decision::Remove,
            _ => Decision::Keep,
        }
    });
    descriptors.push(ColumnFamilyDescriptor::new(CF_SIG, sig));

    let addr_like = |name: &'static str, floor: Arc<AtomicU64>, cache: &Cache| {
        let mut cf = Options::default();
        cf.set_write_buffer_size(128 << 20);
        cf.set_level_compaction_dynamic_level_bytes(true);
        data_compression(&mut cf);
        cf.set_prefix_extractor(SliceTransform::create_fixed_prefix(32));
        cf.set_memtable_prefix_bloom_ratio(0.1);
        cf.set_block_based_table_factory(&block_opts(16 << 10, true, cache));
        cf.set_periodic_compaction_seconds(INDEX_PERIODIC_COMPACTION_SECS);
        cf.set_compaction_filter(
            "disk_cache_window_addr",
            move |_level, key: &[u8], _value: &[u8]| match super::schema::addr_key_slot(key) {
                Some(slot) if slot < floor.load(Ordering::Relaxed) => Decision::Remove,
                _ => Decision::Keep,
            },
        );
        ColumnFamilyDescriptor::new(name, cf)
    };

    descriptors.push(addr_like(CF_ADDR_SIG, min_retained.clone(), &cache));
    descriptors.push(addr_like(CF_TOKEN_OWNER, min_retained, &cache));

    descriptors
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_orderings_match_slot_order() {
        // Slot-keyed CFs: ascending slot order.
        assert!(slot_key(5) < slot_key(6));
        assert!(tx_key(5, u32::MAX) < tx_key(6, 0));
        assert!(tx_key(5, 1) < tx_key(5, 2));

        // addr_sig: forward iteration yields newest-first within an address.
        let addr = [9u8; 32];
        assert!(addr_sig_key(&addr, 100, 0) < addr_sig_key(&addr, 99, 0));
        assert!(addr_sig_key(&addr, 100, 5) < addr_sig_key(&addr, 100, 4));

        // Different addresses never interleave.
        let addr_b = [10u8; 32];
        assert!(addr_sig_key(&addr, 0, 0) < addr_sig_key(&addr_b, u64::MAX, u32::MAX));
    }

    #[test]
    fn addr_key_slot_round_trips() {
        let addr = [3u8; 32];
        for slot in [0u64, 1, 432_000, u64::MAX - 1, u64::MAX] {
            let key = addr_sig_key(&addr, slot, 7);
            assert_eq!(addr_key_slot(&key), Some(slot));
            let token_key = token_owner_key(&addr, slot, 7, &[4u8; 32]);
            assert_eq!(addr_key_slot(&token_key), Some(slot));
        }
        assert_eq!(addr_key_slot(&[0u8; 10]), None);
    }

    /// Guards against silent bincode layout drift: if this test fails, the on-disk
    /// format changed — bump [`SCHEMA_VERSION`] in the same change.
    #[test]
    fn stored_record_bincode_fingerprint() {
        use crate::clickhouse::{BlockMetadataRecord, StoredTransactionRecord};

        let record = StoredTransactionRecord {
            signature: [7u8; 64],
            slot: 123,
            slot_idx: 4,
            block_time: Some(1_700_000_000),
            is_vote: false,
            tx_version: Some(0),
            tx_signatures: vec![[7u8; 64], [8u8; 64]],
            tx_num_required_signatures: 1,
            tx_num_readonly_signed_accounts: 0,
            tx_num_readonly_unsigned_accounts: 2,
            tx_account_keys: vec![[1u8; 32], [2u8; 32]],
            tx_recent_blockhash: [3u8; 32],
            tx_instructions_program_id_index: vec![1],
            tx_instructions_accounts: vec![vec![0, 1]],
            tx_instructions_data: vec![vec![9, 9, 9]],
            tx_address_table_lookups_present: true,
            tx_address_table_lookup_account_key: vec![[4u8; 32]],
            tx_address_table_lookup_writable_indexes: vec![vec![0]],
            tx_address_table_lookup_readonly_indexes: vec![vec![1]],
            meta_status_ok: false,
            meta_err: Some("{\"InstructionError\":[0,{\"Custom\":1}]}".to_string()),
            meta_fee: 5000,
            meta_pre_balances: vec![10, 20],
            meta_post_balances: vec![5, 25],
            meta_inner_instructions_present: true,
            meta_inner_instructions_index: vec![0],
            meta_inner_instructions_program_id_index: vec![vec![1]],
            meta_inner_instructions_accounts: vec![vec![vec![0]]],
            meta_inner_instructions_data: vec![vec![vec![1]]],
            meta_inner_instructions_stack_height: vec![vec![Some(2)]],
            meta_log_messages_present: true,
            meta_log_messages: vec!["Program log: hi".to_string()],
            meta_pre_token_balances_present: true,
            meta_pre_token_account_index: vec![1],
            meta_pre_token_mint: vec![[5u8; 32]],
            meta_pre_token_owner: vec![Some([6u8; 32])],
            meta_pre_token_program_id: vec![None],
            meta_pre_token_amount: vec!["100".to_string()],
            meta_pre_token_decimals: vec![6],
            meta_pre_token_ui_amount: vec![Some(0.0001)],
            meta_pre_token_ui_amount_string: vec!["0.0001".to_string()],
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
            meta_loaded_addresses_writable: vec![[11u8; 32]],
            meta_loaded_addresses_readonly: Vec::new(),
            meta_return_data_present: true,
            meta_return_data_program_id: Some([12u8; 32]),
            meta_return_data_data: Some(vec![1, 2, 3]),
            meta_compute_units_consumed: Some(200_000),
            meta_cost_units: Some(300),
        };

        let tx_bytes = bincode::serialize(&record).expect("serialize tx");
        let decoded: StoredTransactionRecord = bincode::deserialize(&tx_bytes).expect("round trip");
        assert_eq!(decoded.signature, record.signature);
        assert_eq!(decoded.tx_signatures, record.tx_signatures);
        assert_eq!(
            fingerprint(&tx_bytes),
            0x7448_21a9_4fd3_d7bb,
            "tx layout drift"
        );

        let meta = BlockMetadataRecord {
            slot: 123,
            parent_slot: 122,
            blockhash: [1u8; 32],
            parent_blockhash: [2u8; 32],
            block_time: Some(1_700_000_000),
            block_height: Some(99),
            executed_transaction_count: 2,
            entry_count: 3,
            rewards_present: true,
            rewards_pubkey: vec![[3u8; 32]],
            rewards_lamports: vec![55],
            rewards_post_balance: vec![99],
            rewards_type: vec![Some("Fee".to_string())],
            rewards_commission: vec![Some(7)],
            rewards_num_partitions: Some(4),
        };
        let meta_bytes = bincode::serialize(&meta).expect("serialize meta");
        assert_eq!(
            fingerprint(&meta_bytes),
            0x3200_ea90_2cf4_50dc,
            "block-meta layout drift"
        );
    }

    /// FNV-1a, dependency-free stand-in for a real hash — stability is all that matters.
    fn fingerprint(bytes: &[u8]) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325u64;
        for &byte in bytes {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100_0000_01b3);
        }
        hash
    }
}
