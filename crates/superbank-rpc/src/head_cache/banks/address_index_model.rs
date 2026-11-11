// SPDX-License-Identifier: AGPL-3.0-only
//! Replays seeded random bank operations on two caches and compares their address indexes.
//! One cache runs in reference mode: every insert searches its position and every removal
//! scans the list.
use super::*;
use crate::clickhouse::SlotBoundary;
use crate::head_cache::HeadSigKey;
use crate::head_cache::dragonsmouth::parse_block_meta;
use crate::solana_sdk::{pubkey::Pubkey, signature::Signature};
use std::collections::BTreeMap;
use yellowstone_grpc_proto::prelude::{
    Message, MessageHeader, SubscribeUpdateBlockMeta, SubscribeUpdateEntry, Transaction,
    TransactionStatusMeta,
};

const RETAIN: u64 = 6;
const CAP: usize = 5;
const ACCOUNTS: u8 = 8;
// Account 0 is in most transactions, so its list reaches the cap.
const BUSY: u8 = 0;
const SEEDS: u64 = 16;
const STEPS: usize = 250;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0 | 1;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> Option<T> {
        (!items.is_empty()).then(|| items[self.below(items.len() as u64) as usize])
    }

    fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            items.swap(i, self.below(i as u64 + 1) as usize);
        }
    }
}

fn account(index: u8) -> Vec<u8> {
    vec![index + 1; 32]
}

fn signature(id: u64) -> [u8; 64] {
    let mut out = [0u8; 64];
    for chunk in out.chunks_mut(8) {
        chunk.copy_from_slice(&id.to_le_bytes());
    }
    out
}

fn blockhash(slot: u64, bank: u64) -> [u8; 32] {
    let mut out = [bank as u8; 32];
    out[..8].copy_from_slice(&slot.to_le_bytes());
    out
}

/// One transaction of a block: its signature id and its account indexes.
type Tx = (u64, Vec<u8>);

#[derive(Debug)]
enum Op {
    Block {
        slot: u64,
        bank: u64,
        txs: Vec<Tx>,
    },
    Commit {
        slot: u64,
        bank: u64,
        level: CommitmentLevel,
    },
    Discard {
        slot: u64,
        bank: u64,
    },
    Restart,
}

fn apply(cache: &HeadCache, session: &mut u64, op: &Op) {
    match op {
        Op::Block { slot, bank, txs } => {
            let metadata = parse_block_meta(&SubscribeUpdateBlockMeta {
                slot: *slot,
                blockhash: bs58::encode(blockhash(*slot, *bank)).into_string(),
                parent_slot: slot - 1,
                parent_blockhash: bs58::encode([0; 32]).into_string(),
                executed_transaction_count: txs.len() as u64,
                entries_count: 1,
                ..Default::default()
            })
            .unwrap();
            cache.stage_bank_metadata(*session, *bank, metadata);
            cache.stage_bank_entry(
                *session,
                &SubscribeUpdateEntry {
                    slot: *slot,
                    bank_id: *bank,
                    hash: vec![9; 32],
                    executed_transaction_count: txs.len() as u64,
                    ..Default::default()
                },
            );
            let transactions = txs
                .iter()
                .enumerate()
                .map(|(index, (id, accounts))| SubscribeUpdateTransactionInfo {
                    signature: signature(*id).to_vec(),
                    index: index as u64,
                    transaction: Some(Transaction {
                        signatures: vec![signature(*id).to_vec()],
                        message: Some(Message {
                            header: Some(MessageHeader {
                                num_required_signatures: 1,
                                ..Default::default()
                            }),
                            account_keys: accounts.iter().map(|index| account(*index)).collect(),
                            recent_blockhash: vec![9; 32],
                            ..Default::default()
                        }),
                    }),
                    meta: Some(TransactionStatusMeta::default()),
                    ..Default::default()
                })
                .collect();
            cache.freeze_bank(
                *session,
                *slot,
                *bank,
                blockhash(*slot, *bank),
                transactions,
            );
        }
        Op::Commit { slot, bank, level } => {
            cache.commit_bank(*session, *slot, *bank, *level, Some(slot - 1));
        }
        Op::Discard { slot, bank } => cache.discard_bank(*session, *slot, *bank),
        Op::Restart => {
            cache.end_bank_session(*session);
            *session = cache.start_bank_session(CommitmentLevel::Processed);
        }
    }
}

/// Generates operations and tracks which slots and signatures they left in the window.
struct Script {
    rng: Rng,
    tip: u64,
    next_signature: u64,
    // Slots with a frozen bank 1, and the signature ids of that block.
    blocks: BTreeMap<u64, Vec<u64>>,
    // Repeated signatures that have not moved yet: id, slot of the first block, later slot.
    repeated: Vec<(u64, u64, u64)>,
}

impl Script {
    fn new(seed: u64) -> Self {
        Self {
            rng: Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03),
            tip: 100,
            next_signature: 1,
            blocks: BTreeMap::new(),
            repeated: Vec::new(),
        }
    }

    fn floor(&self) -> u64 {
        self.tip - (RETAIN - 1)
    }

    fn block(&mut self, slot: u64, bank: u64, reuse_from: Option<u64>) -> Op {
        let count = 1 + self.rng.below(5);
        let mut txs = Vec::new();
        for _ in 0..count {
            let mut accounts = Vec::new();
            if self.rng.chance(70) {
                accounts.push(BUSY);
            }
            for _ in 0..1 + self.rng.below(3) {
                let index = 1 + self.rng.below(u64::from(ACCOUNTS) - 1) as u8;
                if !accounts.contains(&index) {
                    accounts.push(index);
                }
            }
            self.next_signature += 1;
            txs.push((self.next_signature, accounts));
        }
        // A signature that another retained block also holds can be re-projected.
        let reused =
            reuse_from.and_then(|from| Some((from, self.rng.pick(self.blocks.get(&from)?)?)));
        if let Some((from, id)) = reused {
            txs[0].0 = id;
            self.repeated.push((id, from, slot));
        }
        if bank == 1 {
            self.blocks
                .insert(slot, txs.iter().map(|(id, _)| *id).collect());
        }
        Op::Block { slot, bank, txs }
    }

    fn retained(&self) -> Vec<u64> {
        let floor = self.floor();
        self.blocks.range(floor..).map(|(slot, _)| *slot).collect()
    }

    fn level(&mut self) -> CommitmentLevel {
        if self.rng.chance(50) {
            CommitmentLevel::Confirmed
        } else {
            CommitmentLevel::Finalized
        }
    }

    fn step(&mut self) -> Vec<Op> {
        let retained = self.retained();
        match self.rng.below(100) {
            // A block at the tip. It may skip a slot and may repeat an older signature.
            0..=39 => {
                self.tip += 1 + self.rng.below(2);
                let reuse = self
                    .rng
                    .chance(30)
                    .then(|| self.rng.pick(&retained))
                    .flatten();
                let slot = self.tip;
                let mut ops = vec![self.block(slot, 1, reuse)];
                if reuse.is_some() || self.rng.chance(30) {
                    let level = self.level();
                    ops.push(Op::Commit {
                        slot,
                        bank: 1,
                        level,
                    });
                }
                ops
            }
            // A block for an older slot that arrives late.
            40..=51 => {
                let gaps = (self.floor()..self.tip)
                    .filter(|slot| !self.blocks.contains_key(slot))
                    .collect::<Vec<_>>();
                match self.rng.pick(&gaps) {
                    Some(slot) => {
                        let reuse = self.rng.pick(&retained);
                        vec![self.block(slot, 1, reuse)]
                    }
                    None => Vec::new(),
                }
            }
            // A commitment update for a retained block.
            52..=66 => match self.rng.pick(&retained) {
                Some(slot) => {
                    let level = self.level();
                    vec![Op::Commit {
                        slot,
                        bank: 1,
                        level,
                    }]
                }
                None => Vec::new(),
            },
            // A discard in the middle of the window.
            67..=74 => match self.rng.pick(&retained) {
                Some(slot) => {
                    self.blocks.remove(&slot);
                    vec![Op::Discard { slot, bank: 1 }]
                }
                None => Vec::new(),
            },
            // A second bank wins a retained slot.
            75..=82 => match self.rng.pick(&retained) {
                Some(slot) => {
                    self.blocks.remove(&slot);
                    let level = self.level();
                    vec![
                        self.block(slot, 2, None),
                        Op::Commit {
                            slot,
                            bank: 2,
                            level,
                        },
                    ]
                }
                None => Vec::new(),
            },
            // A jump that evicts several slots in one step.
            83..=89 => {
                self.tip += 2 + self.rng.below(RETAIN + 2);
                let slot = self.tip;
                vec![self.block(slot, 1, None)]
            }
            // Several removals in ascending, descending or shuffled slot order.
            90..=95 => {
                let mut slots = retained;
                match self.rng.below(3) {
                    0 => {}
                    1 => slots.reverse(),
                    _ => self.rng.shuffle(&mut slots),
                }
                slots.truncate(1 + self.rng.below(4) as usize);
                slots
                    .into_iter()
                    .map(|slot| {
                        self.blocks.remove(&slot);
                        Op::Discard { slot, bank: 1 }
                    })
                    .collect()
            }
            // A session restart.
            _ => {
                self.blocks.clear();
                vec![Op::Restart]
            }
        }
    }
}

type Lists = BTreeMap<Pubkey, Vec<(Signature, u64, u32)>>;

fn lists(cache: &HeadCache) -> Lists {
    cache
        .sigs_by_address
        .iter()
        .map(|entry| {
            let keys = entry
                .value()
                .iter()
                .map(|key: &HeadSigKey| (key.signature, key.pos.slot, key.pos.idx))
                .collect();
            (*entry.key(), keys)
        })
        .collect()
}

fn truncated(cache: &HeadCache) -> BTreeMap<Pubkey, u64> {
    cache
        .address_truncated_slot
        .iter()
        .map(|entry| (*entry.key(), *entry.value()))
        .collect()
}

/// What `getSignaturesForAddress` reads for every account, commitment, limit and boundary.
fn served(cache: &HeadCache, tip: u64) -> Vec<Vec<(String, u64, u32)>> {
    let mut out = Vec::new();
    for index in 0..ACCOUNTS {
        let address = Pubkey::try_from(account(index).as_slice()).unwrap();
        for level in [
            CommitmentLevel::Processed,
            CommitmentLevel::Confirmed,
            CommitmentLevel::Finalized,
        ] {
            for (before, until, limit) in [
                (None, None, 1),
                (None, None, 3),
                (None, None, 1_000),
                (Some(tip.saturating_sub(1)), None, 1_000),
                (None, Some(tip.saturating_sub(3)), 1_000),
                (Some(tip), Some(tip.saturating_sub(4)), 2),
            ] {
                let rows = cache.signatures_for_address(
                    &address,
                    before.map(SlotBoundary::Slot),
                    until.map(SlotBoundary::Slot),
                    limit,
                    level,
                );
                out.push(
                    rows.iter()
                        .map(|meta| (meta.signature_str.to_string(), meta.pos.slot, meta.pos.idx))
                        .collect(),
                );
            }
        }
        #[cfg(feature = "disk-cache")]
        out.push(vec![(
            format!("{:?}", cache.address_floor(&address, Some(0))),
            u64::from(cache.address_list_full(&address)),
            0,
        )]);
    }
    out
}

/// Arm hits of the cache under test, then re-projected signatures and operations that
/// left a truncation record.
type Coverage = [u64; 8];

/// Replays `seed` on both caches and returns what the replay covered.
fn replay(seed: u64) -> Coverage {
    let fast = HeadCache::new(RETAIN, CAP);
    let reference = HeadCache::new(RETAIN, CAP);
    reference
        .address_index
        .reference
        .store(true, Ordering::Relaxed);
    let mut sessions = (
        fast.start_bank_session(CommitmentLevel::Processed),
        reference.start_bank_session(CommitmentLevel::Processed),
    );
    let mut script = Script::new(seed);
    let (mut reprojected, mut truncations) = (0, 0);
    for step in 0..STEPS {
        for op in script.step() {
            apply(&fast, &mut sessions.0, &op);
            apply(&reference, &mut sessions.1, &op);
            let context = format!("seed {seed} step {step} after {op:?}");
            let fast_lists = lists(&fast);
            for (address, keys) in &fast_lists {
                assert!(
                    keys.windows(2)
                        .all(|pair| (pair[0].1, pair[0].2) >= (pair[1].1, pair[1].2)),
                    "{context}: list of {address} is not newest first: {keys:?}"
                );
            }
            assert_eq!(fast_lists, lists(&reference), "{context}: address lists");
            let records = truncated(&fast);
            truncations += u64::from(!records.is_empty());
            assert_eq!(
                records,
                truncated(&reference),
                "{context}: truncation records"
            );
            // A repeated signature moved while the block that held it first is still published.
            script.repeated.retain(|(id, from, to)| {
                let moved = fast
                    .meta_by_signature
                    .get(&Signature::from(signature(*id)))
                    .is_some_and(|meta| meta.pos.slot == *to)
                    && fast.sigs_by_slot.contains_key(from);
                reprojected += u64::from(moved);
                !moved
            });
            assert_eq!(
                served(&fast, script.tip),
                served(&reference, script.tip),
                "{context}: served signatures"
            );
        }
    }
    let hits = &fast.address_index.hits;
    std::array::from_fn(|at| match at {
        6 => reprojected,
        7 => truncations,
        arm => hits[arm].load(Ordering::Relaxed),
    })
}

#[test]
fn address_index_matches_its_reference_on_random_operations() {
    let mut covered = Coverage::default();
    for seed in 0..SEEDS {
        for (total, count) in covered.iter_mut().zip(replay(seed)) {
            *total += count;
        }
    }
    // First and searched inserts; popped, skipped, scanned and empty-list removals; then
    // the two counts. A removal meets an empty list only after a re-projection emptied it.
    for (at, count) in covered.iter().enumerate() {
        let least = if at == 5 { 5 } else { 100 };
        assert!(*count >= least, "case {at} ran {count} times: {covered:?}");
    }
}

#[test]
fn a_tie_goes_before_its_equal_key_in_both_modes() {
    let key = |id: u64, slot: u64, idx: u32| HeadSigKey {
        signature: Signature::from(signature(id)),
        pos: crate::head_cache::SlotIndex { slot, idx },
    };
    let address = Pubkey::try_from(account(1).as_slice()).unwrap();
    let ids = |cache: &HeadCache| lists(cache)[&address].clone();
    for reference in [false, true] {
        let cache = HeadCache::new(32, 64);
        cache
            .address_index
            .reference
            .store(reference, Ordering::Relaxed);
        // A tie at the front, then a tie in the middle of the list.
        for (id, slot, idx) in [(1, 5, 1), (2, 5, 1), (3, 6, 0), (4, 4, 0), (5, 5, 1)] {
            cache.index_address(address, key(id, slot, idx), 0);
        }
        let expected = [(3, 6, 0), (5, 5, 1), (2, 5, 1), (1, 5, 1), (4, 4, 0)]
            .map(|(id, slot, idx)| (Signature::from(signature(id)), slot, idx));
        assert_eq!(ids(&cache), expected, "reference mode {reference}");
    }
}
