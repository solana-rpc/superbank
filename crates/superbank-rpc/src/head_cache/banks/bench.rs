// SPDX-License-Identifier: AGPL-3.0-only
//! Bank write-lock hold bench on synthetic blocks of realistic size and shape.
//! Run alone: cargo test --release -p superbank-rpc --all-features --locked
//! head_cache_freeze_hold_bench -- --ignored --nocapture --test-threads=1
use super::*;
use crate::clickhouse::StoredBlockPayload;
use crate::head_cache::dragonsmouth::parse_block_meta;
use crate::solana_sdk::signature::Signature;
use solana_transaction_status::TransactionDetails;
use std::{
    sync::atomic::{AtomicBool, AtomicU64},
    thread,
    time::Duration,
};
use yellowstone_grpc_proto::prelude::{
    CompiledInstruction, InnerInstruction, InnerInstructions, Message, MessageAddressTableLookup,
    MessageHeader, SubscribeUpdateBlockMeta, SubscribeUpdateEntry, TokenBalance, Transaction,
    TransactionError, TransactionStatusMeta, UiTokenAmount, UnixTimestamp,
};

const TXS: usize = 1_300;
const ENTRIES: u64 = 64;
const RETAIN: u64 = 32;
// Default of MAX_SIGNATURES_LIMIT, which sizes the per-address list cap.
const MAX_PER_ADDRESS: usize = 1_000;
const WARMUP: u64 = 40;
// Fills the bank window of a confirmed-minimum session, then trims it on every slot.
const CONFIRMED_WARMUP: u64 = RETAIN + PENDING_COMMITMENT_SLOTS + 8;
const MEASURED: u64 = 100;
// A block without vote transactions in a wide retention window of recurring accounts.
const WIDE_TXS: usize = 360;
const WIDE_RETAIN: u64 = 1_000;
const WIDE_ACCOUNT_POOL: u64 = 150_000;
const SLOT_INTERVAL: Duration = Duration::from_millis(200);
const PACED_READ_INTERVAL: Duration = Duration::from_micros(500);
const FIRST_SLOT: u64 = 1_000;
const READERS: u64 = 2;
const OPS: [&str; 5] = [
    "freeze",
    "commit",
    "stage_metadata",
    "stage_entry",
    "discard",
];
const REJECT_REASONS: [&str; 10] = [
    "metadata_missing",
    "blockhash_mismatch",
    "transaction_count",
    "entry_count",
    "transaction_index",
    "duplicate_signature",
    "malformed_transaction",
    "entry_sequence",
    "entry_hash",
    "entry_range",
];
const HOLD_FAMILY: &str = "superbank_head_cache_write_lock_hold_seconds";
const PREPARE_FAMILY: &str = "superbank_head_cache_freeze_prepare_seconds";

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        let mut rng = Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03);
        rng.next();
        rng
    }

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

    fn range(&mut self, low: u64, high_inclusive: u64) -> u64 {
        low + self.below(high_inclusive - low + 1)
    }

    fn bytes<const N: usize>(&mut self) -> [u8; N] {
        let mut out = [0u8; N];
        for chunk in out.chunks_mut(8) {
            let word = self.next().to_le_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
        out
    }

    fn vec(&mut self, len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            out.extend_from_slice(&self.next().to_le_bytes());
        }
        out.truncate(len);
        out
    }
}

fn signature_bytes(slot: u64, index: u64) -> [u8; 64] {
    let mut out = Rng::new(slot << 20 ^ index ^ 0x51).bytes::<64>();
    out[..8].copy_from_slice(&slot.to_le_bytes());
    out[8..16].copy_from_slice(&index.to_le_bytes());
    out
}

fn blockhash(slot: u64) -> [u8; 32] {
    Rng::new(slot ^ 0xB10C).bytes()
}

fn shared_key(tag: u64, index: u64) -> Vec<u8> {
    Rng::new(tag << 32 ^ index).vec(32)
}

fn unique_key(slot: u64, index: u64, position: u64) -> Vec<u8> {
    Rng::new(slot << 24 ^ index << 6 ^ position ^ 0x77).vec(32)
}

// Recurring account of a pool; the cubic skew makes low indexes recur most.
fn pooled_key(rng: &mut Rng, pool: u64) -> Vec<u8> {
    let unit = (rng.next() >> 11) as f64 / (1u64 << 53) as f64;
    shared_key(8, (pool as f64 * unit * unit * unit) as u64)
}

/// Transaction mix and account reuse of a scenario.
#[derive(Clone, Copy)]
struct Shape {
    txs: usize,
    vote_percent: u64,
    // Fewest and most static account keys of a non-vote transaction.
    static_keys: (u64, u64),
    // Size of the recurring account pool; zero gives every transaction its own accounts.
    account_pool: u64,
}

const MIXED: Shape = Shape {
    txs: TXS,
    vote_percent: 70,
    static_keys: (8, 30),
    account_pool: 0,
};

fn base58(bytes: &[u8]) -> String {
    bs58::encode(bytes).into_string()
}

fn instruction_data(rng: &mut Rng, low: u64, high: u64) -> Vec<u8> {
    let len = rng.range(low, high) as usize;
    rng.vec(len)
}

fn log_lines(rng: &mut Rng, low: u64, high: u64) -> Vec<String> {
    let count = rng.range(low, high);
    (0..count)
        .map(|line| {
            let program = base58(&shared_key(5, rng.below(8)));
            match line % 3 {
                0 => format!("Program {program} invoke [{}]", 1 + line % 2),
                1 => format!(
                    "Program log: instruction {} amount {}",
                    line,
                    rng.next() % 1_000_000
                ),
                _ => format!(
                    "Program {program} consumed {} of 200000 compute units",
                    rng.below(150_000)
                ),
            }
        })
        .collect()
}

fn token_balances(
    rng: &mut Rng,
    slot: u64,
    index: u64,
    count: u64,
    keys: u64,
) -> Vec<TokenBalance> {
    (0..count)
        .map(|position| {
            let amount = rng.next() % 10_000_000_000;
            TokenBalance {
                account_index: rng.below(keys) as u32,
                mint: base58(&shared_key(6, rng.below(64))),
                owner: base58(&unique_key(slot, index, 100 + position)),
                program_id: base58(&shared_key(2, 0)),
                ui_token_amount: Some(UiTokenAmount {
                    ui_amount: amount as f64 / 1e6,
                    decimals: 6,
                    amount: amount.to_string(),
                    ui_amount_string: format!("{}", amount as f64 / 1e6),
                }),
            }
        })
        .collect()
}

fn vote_transaction(slot: u64, index: u64, rng: &mut Rng) -> SubscribeUpdateTransactionInfo {
    let mut keys = vec![shared_key(3, rng.below(256)), shared_key(4, rng.below(256))];
    if rng.below(2) == 0 {
        keys.push(shared_key(7, 0));
    }
    keys.push(shared_key(1, 0));
    let program = keys.len() as u32 - 1;
    let accounts = (0..program as u8).collect::<Vec<_>>();
    let signature = signature_bytes(slot, index);
    let balances = (0..keys.len())
        .map(|_| rng.next() % 5_000_000_000)
        .collect::<Vec<_>>();
    SubscribeUpdateTransactionInfo {
        signature: signature.to_vec(),
        is_vote: true,
        index,
        transaction: Some(Transaction {
            signatures: vec![signature.to_vec()],
            message: Some(Message {
                header: Some(MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 1,
                }),
                account_keys: keys,
                recent_blockhash: blockhash(slot.saturating_sub(1)).to_vec(),
                instructions: vec![CompiledInstruction {
                    program_id_index: program,
                    accounts,
                    data: instruction_data(rng, 40, 60),
                }],
                ..Default::default()
            }),
        }),
        meta: Some(TransactionStatusMeta {
            fee: 5_000,
            post_balances: balances.clone(),
            pre_balances: balances,
            log_messages: log_lines(rng, 2, 4),
            return_data_none: true,
            compute_units_consumed: Some(2_100),
            cost_units: Some(3_428),
            ..Default::default()
        }),
    }
}

fn general_transaction(
    slot: u64,
    index: u64,
    rng: &mut Rng,
    shape: Shape,
) -> SubscribeUpdateTransactionInfo {
    let pool = shape.account_pool;
    let static_keys = rng.range(shape.static_keys.0, shape.static_keys.1);
    let hot = rng.range(1, 3).min(static_keys - 1);
    let mut keys = Vec::with_capacity(static_keys as usize);
    while keys.len() < (static_keys - hot) as usize {
        let position = keys.len() as u64;
        // One fee payer in five is an account that no other transaction uses.
        let key = if pool == 0 || (position == 0 && rng.below(5) == 0) {
            unique_key(slot, index, position)
        } else {
            pooled_key(rng, pool)
        };
        if !keys.contains(&key) {
            keys.push(key);
        }
    }
    keys.extend((0..hot).map(|_| shared_key(2, rng.below(8))));
    let versioned = rng.below(2) == 0;
    let (lookups, writable, readonly) = if versioned {
        let writable = rng.range(1, 3);
        let readonly = rng.range(1, 3);
        let lookup = MessageAddressTableLookup {
            account_key: unique_key(slot, index, 200),
            writable_indexes: rng.vec(writable as usize),
            readonly_indexes: rng.vec(readonly as usize),
        };
        (vec![lookup], writable, readonly)
    } else {
        (Vec::new(), 0, 0)
    };
    let mut loaded = |first: u64, count: u64| {
        (0..count)
            .map(|position| match pool {
                0 => unique_key(slot, index, first + position),
                _ => pooled_key(rng, pool),
            })
            .collect::<Vec<_>>()
    };
    let loaded_writable = loaded(300, writable);
    let loaded_readonly = loaded(400, readonly);
    let total_keys = static_keys + writable + readonly;
    let instruction_count = rng.range(3, 6);
    let instructions = (0..instruction_count)
        .map(|_| {
            let account_count = rng.range(2, 8) as usize;
            CompiledInstruction {
                program_id_index: (static_keys - 1 - rng.below(hot)) as u32,
                accounts: (0..account_count)
                    .map(|_| rng.below(total_keys) as u8)
                    .collect(),
                data: instruction_data(rng, 8, 64),
            }
        })
        .collect::<Vec<_>>();
    let inner_instructions = (0..2)
        .map(|_| InnerInstructions {
            index: rng.below(instruction_count) as u32,
            instructions: (0..rng.range(1, 3))
                .map(|_| InnerInstruction {
                    program_id_index: rng.below(static_keys) as u32,
                    accounts: (0..rng.range(2, 5))
                        .map(|_| rng.below(total_keys) as u8)
                        .collect(),
                    data: instruction_data(rng, 8, 32),
                    stack_height: Some(2),
                })
                .collect(),
        })
        .collect::<Vec<_>>();
    let balances = (0..total_keys)
        .map(|_| rng.next() % 50_000_000_000)
        .collect::<Vec<_>>();
    let token_count = rng.range(2, 4);
    let failed = rng.below(100) < 5;
    let signature = signature_bytes(slot, index);
    SubscribeUpdateTransactionInfo {
        signature: signature.to_vec(),
        is_vote: false,
        index,
        transaction: Some(Transaction {
            signatures: vec![signature.to_vec()],
            message: Some(Message {
                header: Some(MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: hot as u32,
                }),
                account_keys: keys,
                recent_blockhash: blockhash(slot.saturating_sub(1)).to_vec(),
                instructions,
                versioned,
                address_table_lookups: lookups,
                config: None,
            }),
        }),
        meta: Some(TransactionStatusMeta {
            err: failed.then(|| TransactionError {
                err: bincode::serialize(
                    &crate::solana_sdk::transaction::TransactionError::AccountNotFound,
                )
                .expect("encode transaction error"),
            }),
            fee: 5_000 + rng.below(100_000),
            pre_balances: balances.clone(),
            post_balances: balances,
            inner_instructions,
            log_messages: log_lines(rng, 10, 20),
            pre_token_balances: token_balances(rng, slot, index, token_count, static_keys),
            post_token_balances: token_balances(rng, slot, index, token_count, static_keys),
            loaded_writable_addresses: loaded_writable,
            loaded_readonly_addresses: loaded_readonly,
            return_data_none: true,
            compute_units_consumed: Some(rng.range(20_000, 400_000)),
            cost_units: Some(rng.range(30_000, 450_000)),
            ..Default::default()
        }),
    }
}

fn realistic_block(slot: u64, shape: Shape) -> Vec<SubscribeUpdateTransactionInfo> {
    (0..shape.txs as u64)
        .map(|index| {
            let mut rng = Rng::new(slot << 24 ^ index ^ 0xA5);
            if rng.below(100) < shape.vote_percent {
                vote_transaction(slot, index, &mut rng)
            } else {
                general_transaction(slot, index, &mut rng, shape)
            }
        })
        .collect()
}

fn block_entries(slot: u64, count: usize) -> Vec<SubscribeUpdateEntry> {
    let base = count as u64 / ENTRIES;
    let extra = count as u64 % ENTRIES;
    let mut start = 0;
    (0..ENTRIES)
        .map(|index| {
            let executed = base + u64::from(index < extra);
            let entry = SubscribeUpdateEntry {
                slot,
                index,
                num_hashes: 12_500,
                hash: Rng::new(slot << 8 ^ index).vec(32),
                executed_transaction_count: executed,
                starting_transaction_index: start,
                bank_id: slot,
            };
            start += executed;
            entry
        })
        .collect()
}

fn block_metadata(slot: u64, count: usize) -> BlockMetadataRecord {
    parse_block_meta(&SubscribeUpdateBlockMeta {
        slot,
        blockhash: base58(&blockhash(slot)),
        parent_slot: slot - 1,
        parent_blockhash: base58(&blockhash(slot - 1)),
        executed_transaction_count: count as u64,
        entries_count: ENTRIES,
        block_time: Some(UnixTimestamp {
            timestamp: 1_700_000_000 + slot as i64,
        }),
        ..Default::default()
    })
    .expect("bench block metadata")
}

#[derive(Default)]
struct Walls {
    stage: Vec<Duration>,
    slot: Vec<Duration>,
    freeze: Vec<Duration>,
    commit_processed: Vec<Duration>,
    commit_confirmed: Vec<Duration>,
    commit_finalized: Vec<Duration>,
}

struct Drive<'a> {
    cache: &'a HeadCache,
    session: u64,
    minimum: CommitmentLevel,
    first: u64,
    published: &'a AtomicU64,
    shape: Shape,
}

impl Drive<'_> {
    fn commit(&self, slot: u64, level: CommitmentLevel) -> Duration {
        let started = Instant::now();
        self.cache
            .commit_bank(self.session, slot, slot, level, Some(slot - 1));
        started.elapsed()
    }

    fn slot(&self, slot: u64, walls: Option<&mut Walls>) {
        let txs = self.shape.txs;
        let transactions = realistic_block(slot, self.shape);
        let (metadata, entries) = (block_metadata(slot, txs), block_entries(slot, txs));
        let started = Instant::now();
        self.cache.stage_bank_metadata(self.session, slot, metadata);
        for entry in &entries {
            self.cache.stage_bank_entry(self.session, entry);
        }
        let staged = started.elapsed();
        let started = Instant::now();
        self.cache
            .freeze_bank(self.session, slot, slot, blockhash(slot), transactions);
        let frozen = started.elapsed();
        let processed = (self.minimum == CommitmentLevel::Processed).then(|| {
            let wall = self.commit(slot, CommitmentLevel::Processed);
            self.published.store(slot, Ordering::Release);
            wall
        });
        let confirmed = (slot >= self.first + 2).then(|| {
            let wall = self.commit(slot - 2, CommitmentLevel::Confirmed);
            if self.minimum == CommitmentLevel::Confirmed {
                self.published.store(slot - 2, Ordering::Release);
            }
            wall
        });
        let finalized =
            (slot >= self.first + 8).then(|| self.commit(slot - 8, CommitmentLevel::Finalized));
        if let Some(walls) = walls {
            let commits = [processed, confirmed, finalized].into_iter().flatten();
            walls.slot.push(staged + frozen + commits.sum::<Duration>());
            walls.stage.push(staged);
            walls.freeze.push(frozen);
            walls.commit_processed.extend(processed);
            walls.commit_confirmed.extend(confirmed);
            walls.commit_finalized.extend(finalized);
        }
    }
}

#[derive(Default)]
struct Samples {
    tx: Vec<u64>,
    block: Vec<u64>,
}

fn read_loop(
    cache: &HeadCache,
    published: &AtomicU64,
    stop: &AtomicBool,
    reader: u64,
    txs: u64,
) -> Samples {
    let mut samples = Samples::default();
    let mut round = 0u64;
    while !stop.load(Ordering::Acquire) {
        let slot = published.load(Ordering::Acquire);
        if slot == 0 {
            thread::yield_now();
            continue;
        }
        round += 1;
        let signature = Signature::from(signature_bytes(slot, (round * 7 + reader) % txs));
        let started = Instant::now();
        let _ = cache.get_tx(&signature, CommitmentLevel::Processed);
        samples.tx.push(started.elapsed().as_nanos() as u64);
        let started = Instant::now();
        let _ = cache.get_block(
            slot,
            CommitmentLevel::Processed,
            TransactionDetails::Signatures,
        );
        samples.block.push(started.elapsed().as_nanos() as u64);
    }
    samples
}

// Reads one signature per interval and measures from the scheduled time, so a read that
// arrives while the lock is held counts its whole wait.
fn paced_read_loop(
    cache: &HeadCache,
    published: &AtomicU64,
    stop: &AtomicBool,
    txs: u64,
) -> Vec<u64> {
    let mut samples = Vec::new();
    let mut due = Instant::now();
    let mut round = 0u64;
    while !stop.load(Ordering::Acquire) {
        while Instant::now() < due {
            std::hint::spin_loop();
        }
        let slot = published.load(Ordering::Acquire);
        if slot != 0 {
            round += 1;
            let signature = Signature::from(signature_bytes(slot, (round * 7) % txs));
            let _ = cache.get_tx(&signature, CommitmentLevel::Processed);
            samples.push(due.elapsed().as_nanos() as u64);
        }
        due += PACED_READ_INTERVAL;
    }
    samples
}

fn percentile(sorted: &[u64], fraction: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((sorted.len() as f64) * fraction).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn print_latency(name: &str, unit: &str, divisor: f64, mut values: Vec<u64>) {
    values.sort_unstable();
    if values.is_empty() {
        println!("{name}_samples=0");
        return;
    }
    for (label, fraction) in [("p50", 0.5), ("p99", 0.99), ("p99.9", 0.999), ("max", 1.0)] {
        println!(
            "{name}_{label}_{unit}={:.3}",
            percentile(&values, fraction) as f64 / divisor
        );
    }
}

fn series(text: &str, family: &str, suffix: &str, op: Option<&str>) -> Option<f64> {
    let name = format!("{family}_{suffix}");
    text.lines().find_map(|line| {
        let rest = line.strip_prefix(name.as_str())?;
        let (labels, value) = match rest.strip_prefix('{') {
            Some(rest) => rest.split_once('}')?,
            None => ("", rest),
        };
        if op.is_some_and(|op| !labels.contains(&format!("op=\"{op}\""))) {
            return None;
        }
        value.split_whitespace().next()?.parse().ok()
    })
}

fn buckets(text: &str, op: &str) -> Vec<(String, f64)> {
    let name = format!("{HOLD_FAMILY}_bucket{{");
    text.lines()
        .filter_map(|line| {
            let rest = line.strip_prefix(name.as_str())?;
            let (labels, value) = rest.split_once('}')?;
            if !labels.contains(&format!("op=\"{op}\"")) {
                return None;
            }
            let bound = labels.split("le=\"").nth(1)?.split('"').next()?;
            Some((
                bound.to_owned(),
                value.split_whitespace().next()?.parse().ok()?,
            ))
        })
        .collect()
}

fn exported() -> String {
    String::from_utf8(crate::metrics::export_metrics().expect("export metrics"))
        .expect("metrics text")
}

fn rejected() -> Vec<u64> {
    REJECT_REASONS
        .iter()
        .map(|reason| crate::metrics::head_cache_rejected_banks_count(reason))
        .collect()
}

struct Scenario {
    name: &'static str,
    minimum: CommitmentLevel,
    retain: u64,
    warmup: u64,
    // Retained slots expected after warm-up and at the end, when the window is known.
    window: Option<(u64, u64)>,
    shape: Shape,
    // Paces the measured slots and adds a paced reader; `None` runs slots back to back.
    interval: Option<Duration>,
}

fn run(scenario: &Scenario) {
    let name = scenario.name;
    let txs = scenario.shape.txs;
    println!(
        "scenario={name} minimum={:?} retain={}",
        scenario.minimum, scenario.retain
    );
    let cache = HeadCache::new(scenario.retain, MAX_PER_ADDRESS);
    let session = cache.start_bank_session(scenario.minimum);
    let published = AtomicU64::new(0);
    let drive = Drive {
        cache: &cache,
        session,
        minimum: scenario.minimum,
        first: FIRST_SLOT,
        published: &published,
        shape: scenario.shape,
    };
    let warmup = scenario.warmup;
    println!("{name}.warmup_slots={warmup}");
    for slot in FIRST_SLOT..FIRST_SLOT + warmup {
        drive.slot(slot, None);
    }
    if scenario.minimum == CommitmentLevel::Confirmed {
        let state = cache.banks.read().expect("head bank lock");
        assert!(
            !state.banks.contains_key(&(FIRST_SLOT, FIRST_SLOT)),
            "{name} oldest bank after warm-up"
        );
        assert_eq!(
            state.banks.len() as u64,
            RETAIN + PENDING_COMMITMENT_SLOTS,
            "{name} bank window after warm-up"
        );
    }
    if let Some((after_warmup, _)) = scenario.window {
        assert_eq!(
            cache.slot_entries() as u64,
            after_warmup,
            "{name} after warm-up"
        );
    }
    let rejected_before = rejected();
    let before = exported();
    let stop = AtomicBool::new(false);
    let mut walls = Walls::default();
    let started = Instant::now();
    let (readers, paced) = thread::scope(|scope| {
        let handles = (0..READERS)
            .map(|reader| {
                let (cache, published, stop) = (&cache, &published, &stop);
                scope.spawn(move || read_loop(cache, published, stop, reader, txs as u64))
            })
            .collect::<Vec<_>>();
        let paced = scenario.interval.map(|_| {
            let (cache, published, stop) = (&cache, &published, &stop);
            scope.spawn(move || paced_read_loop(cache, published, stop, txs as u64))
        });
        let mut due = Instant::now();
        for slot in FIRST_SLOT + warmup..FIRST_SLOT + warmup + MEASURED {
            drive.slot(slot, Some(&mut walls));
            if let Some(interval) = scenario.interval {
                due += interval;
                thread::sleep(due.saturating_duration_since(Instant::now()));
            }
        }
        stop.store(true, Ordering::Release);
        let readers = handles
            .into_iter()
            .map(|handle| handle.join().expect("reader thread"))
            .collect::<Vec<_>>();
        (
            readers,
            paced.map(|handle| handle.join().expect("paced reader")),
        )
    });
    let elapsed = started.elapsed();
    let after = exported();
    let rejected_after = rejected();

    for (reason, (before, after)) in REJECT_REASONS
        .iter()
        .zip(rejected_before.iter().zip(&rejected_after))
    {
        assert_eq!(before, after, "{name} rejected {reason}");
    }
    let last = published.load(Ordering::Acquire);
    match cache.get_block(last, CommitmentLevel::Processed, TransactionDetails::Full) {
        Some(StoredBlockPayload::SharedFull { transactions, .. }) => {
            assert_eq!(transactions.len(), txs, "{name} block {last}")
        }
        other => panic!(
            "{name} block {last}: unexpected payload {}",
            other.is_some()
        ),
    }
    let probe = Signature::from(signature_bytes(last, 0));
    assert!(
        cache.get_tx(&probe, CommitmentLevel::Processed).is_some(),
        "{name} probe"
    );
    if let Some((_, at_end)) = scenario.window {
        assert_eq!(cache.slot_entries() as u64, at_end, "{name} at end");
    }

    let delta = |family: &str, suffix: &str, op: Option<&str>| {
        let get = |text: &str| series(text, family, suffix, op).unwrap_or(0.0);
        get(&after) - get(&before)
    };
    assert_eq!(
        delta(HOLD_FAMILY, "count", Some("freeze")) as u64,
        MEASURED,
        "{name} freeze count"
    );
    println!("{name}.measured_slots={MEASURED}");
    println!("{name}.measured_wall_s={:.3}", elapsed.as_secs_f64());
    println!("{name}.txs_per_block={txs}");
    println!("{name}.tx_entries={}", cache.tx_entries());
    println!("{name}.address_entries={}", cache.address_entries());
    println!("{name}.slot_entries={}", cache.slot_entries());
    let mut total_ms = 0.0;
    for op in OPS {
        let sum = delta(HOLD_FAMILY, "sum", Some(op));
        let count = delta(HOLD_FAMILY, "count", Some(op));
        if matches!(op, "freeze" | "commit" | "stage_metadata" | "stage_entry") {
            total_ms += sum * 1e3;
        }
        println!("{name}.{op}_hold_count={count}");
        if count > 0.0 {
            println!("{name}.{op}_hold_mean_ms={:.4}", sum * 1e3 / count);
        } else {
            println!("{name}.{op}_hold_mean_ms=n/a");
        }
    }
    println!(
        "{name}.total_hold_per_slot_ms={:.4}",
        total_ms / MEASURED as f64
    );
    let (old, new) = (buckets(&before, "freeze"), buckets(&after, "freeze"));
    let mut previous = 0.0;
    let mut line = Vec::new();
    for ((bound, low), (_, high)) in old.iter().zip(&new) {
        let cumulative = high - low;
        line.push(format!("{bound}:{}", (cumulative - previous) as u64));
        previous = cumulative;
    }
    println!("{name}.freeze_hold_buckets={}", line.join(","));
    let prepare_count = delta(PREPARE_FAMILY, "count", None);
    if series(&after, PREPARE_FAMILY, "count", None).is_some() && prepare_count > 0.0 {
        let sum = delta(PREPARE_FAMILY, "sum", None);
        println!("{name}.prepare_mean_ms={:.4}", sum * 1e3 / prepare_count);
    } else {
        println!("{name}.prepare_mean_ms=n/a");
    }
    let wall_ms = |values: &[Duration]| {
        values
            .iter()
            .map(|wall| wall.as_nanos() as u64)
            .collect::<Vec<_>>()
    };
    let mean_ms = |values: &[Duration]| {
        values.iter().sum::<Duration>().as_secs_f64() * 1e3 / values.len().max(1) as f64
    };
    println!("{name}.stage_wall_mean_ms={:.4}", mean_ms(&walls.stage));
    println!("{name}.slot_wall_mean_ms={:.4}", mean_ms(&walls.slot));
    print_latency(
        &format!("{name}.freeze_wall"),
        "ms",
        1e6,
        wall_ms(&walls.freeze),
    );
    print_latency(
        &format!("{name}.commit_processed_wall"),
        "ms",
        1e6,
        wall_ms(&walls.commit_processed),
    );
    print_latency(
        &format!("{name}.commit_confirmed_wall"),
        "ms",
        1e6,
        wall_ms(&walls.commit_confirmed),
    );
    print_latency(
        &format!("{name}.commit_finalized_wall"),
        "ms",
        1e6,
        wall_ms(&walls.commit_finalized),
    );
    let all_tx = readers
        .iter()
        .flat_map(|samples| samples.tx.iter().copied())
        .collect::<Vec<_>>();
    let all_block = readers
        .iter()
        .flat_map(|samples| samples.block.iter().copied())
        .collect::<Vec<_>>();
    println!("{name}.reader_calls={}", all_tx.len() + all_block.len());
    print_latency(&format!("{name}.reader_get_tx"), "us", 1e3, all_tx.clone());
    print_latency(
        &format!("{name}.reader_get_block"),
        "us",
        1e3,
        all_block.clone(),
    );
    print_latency(
        &format!("{name}.reader_all"),
        "us",
        1e3,
        [all_tx, all_block].concat(),
    );
    if let Some(paced) = paced {
        print_latency(&format!("{name}.paced_get_tx"), "us", 1e3, paced);
    }
}

#[test]
#[ignore = "perf: run alone with --release"]
fn head_cache_freeze_hold_bench() {
    println!(
        "bench.config txs={TXS} entries={ENTRIES} retain={RETAIN} max_per_address={MAX_PER_ADDRESS} warmup={WARMUP} measured={MEASURED} readers={READERS}"
    );
    for scenario in [
        Scenario {
            name: "steady",
            minimum: CommitmentLevel::Processed,
            retain: RETAIN,
            warmup: WARMUP,
            window: Some((RETAIN, RETAIN)),
            shape: MIXED,
            interval: None,
        },
        Scenario {
            name: "no_eviction",
            minimum: CommitmentLevel::Processed,
            retain: WARMUP + MEASURED + 64,
            warmup: WARMUP,
            window: Some((WARMUP, WARMUP + MEASURED)),
            shape: MIXED,
            interval: None,
        },
        Scenario {
            name: "confirmed_minimum",
            minimum: CommitmentLevel::Confirmed,
            retain: RETAIN,
            warmup: CONFIRMED_WARMUP,
            window: None,
            shape: MIXED,
            interval: None,
        },
        Scenario {
            name: "wide_window",
            minimum: CommitmentLevel::Processed,
            retain: WIDE_RETAIN,
            warmup: WIDE_RETAIN + 8,
            window: Some((WIDE_RETAIN, WIDE_RETAIN)),
            shape: Shape {
                txs: WIDE_TXS,
                vote_percent: 0,
                static_keys: (6, 24),
                account_pool: WIDE_ACCOUNT_POOL,
            },
            interval: Some(SLOT_INTERVAL),
        },
    ] {
        run(&scenario);
    }
}
