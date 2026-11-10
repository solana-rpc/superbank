// SPDX-License-Identifier: AGPL-3.0-only
//! Serialized recent `getBlock` results built from head-cache blocks, keyed by blockhash.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use axum::body::Bytes;
use solana_commitment_config::CommitmentLevel;
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};
use tokio::sync::{Semaphore, broadcast, watch};

use crate::block_response_cache::BlockResponseCacheKey;
use crate::head_cache::{HeadBankEvent, HeadBankEventKind, HeadCache, recv_bank_event};
use crate::metrics;
use crate::state::AppState;

/// A queued prebuild is dropped once this many newer slots have been confirmed.
const PREBUILD_MAX_LAG_SLOTS: u64 = 8;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) struct RecentBlockKey {
    shape: BlockResponseCacheKey,
    blockhash: [u8; 32],
}

impl RecentBlockKey {
    /// Commitment does not change the bytes, and a requested footer is added per request.
    pub(crate) fn new(shape: BlockResponseCacheKey, blockhash: [u8; 32]) -> Self {
        Self {
            shape: BlockResponseCacheKey {
                commitment: CommitmentLevel::Confirmed,
                footer: false,
                ..shape
            },
            blockhash,
        }
    }

    pub(crate) fn slot(&self) -> u64 {
        self.shape.slot
    }

    pub(crate) fn blockhash(&self) -> [u8; 32] {
        self.blockhash
    }
}

#[derive(Debug)]
pub(crate) enum RecentBuildError {
    /// The head no longer serves a complete block with the key's blockhash.
    Unavailable,
    UnsupportedTransactionVersion(u8),
    Failed(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecentAccess {
    Hit,
    Built,
    Merged,
}

impl RecentAccess {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Built => "miss",
            Self::Merged => "merged",
        }
    }
}

type BuildOutcome = Option<Result<Bytes, Arc<RecentBuildError>>>;

enum Entry {
    Ready(Bytes),
    Building {
        id: u64,
        done: watch::Receiver<BuildOutcome>,
    },
}

#[derive(Default)]
struct Blocks {
    slots: BTreeMap<u64, HashMap<RecentBlockKey, Entry>>,
    resident_bytes: u64,
    ready_entries: u64,
    next_id: u64,
}

impl Blocks {
    fn entry(&self, key: &RecentBlockKey) -> Option<&Entry> {
        self.slots.get(&key.slot())?.get(key)
    }

    fn is_building(&self, key: &RecentBlockKey, id: u64) -> bool {
        matches!(self.entry(key), Some(Entry::Building { id: current, .. }) if *current == id)
    }

    fn insert(&mut self, key: RecentBlockKey, entry: Entry) {
        if let Entry::Ready(bytes) = &entry {
            self.resident_bytes += bytes.len() as u64;
            self.ready_entries += 1;
        }
        let replaced = self.slots.entry(key.slot()).or_default().insert(key, entry);
        if let Some(replaced) = replaced {
            self.forget(&replaced);
        }
    }

    /// Removes the matching entries of one slot and returns how many held bytes.
    fn remove_where(
        &mut self,
        slot: u64,
        matches: impl Fn(&RecentBlockKey, &Entry) -> bool,
    ) -> u64 {
        let Some(entries) = self.slots.get_mut(&slot) else {
            return 0;
        };
        let keys: Vec<_> = entries
            .iter()
            .filter(|(key, entry)| matches(key, entry))
            .map(|(key, _)| key.clone())
            .collect();
        let removed: Vec<_> = keys.iter().filter_map(|key| entries.remove(key)).collect();
        if entries.is_empty() {
            self.slots.remove(&slot);
        }
        removed.iter().filter(|entry| self.forget(entry)).count() as u64
    }

    fn forget(&mut self, entry: &Entry) -> bool {
        let Entry::Ready(bytes) = entry else {
            return false;
        };
        self.resident_bytes -= bytes.len() as u64;
        self.ready_entries -= 1;
        true
    }
}

/// Bounded single-flight store; `GET_BLOCK_RECENT_STORE_MAX_BYTES=0` disables it.
#[derive(Clone)]
pub(crate) struct RecentBlockResponses {
    inner: Option<Arc<Inner>>,
}

struct Inner {
    max_bytes: u64,
    blocks: Mutex<Blocks>,
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, Blocks> {
        self.blocks.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn publish(&self, blocks: &Blocks) {
        metrics::get_block_recent_store_state(
            blocks.ready_entries,
            blocks.resident_bytes,
            self.max_bytes,
        );
    }

    /// Stores a finished build unless an event removed its entry meanwhile.
    fn finish(&self, key: &RecentBlockKey, id: u64, result: &Result<Bytes, Arc<RecentBuildError>>) {
        let mut blocks = self.lock();
        if !blocks.is_building(key, id) {
            return;
        }
        blocks.remove_where(key.slot(), |other, _| other == key);
        if let Ok(bytes) = result {
            if bytes.len() as u64 <= self.max_bytes {
                blocks.insert(key.clone(), Entry::Ready(bytes.clone()));
                self.evict_to_budget(&mut blocks);
            } else {
                metrics::get_block_recent_store_evictions("capacity", 1);
            }
        }
        self.publish(&blocks);
    }

    /// Evicts the lowest slots first: newer blocks are the ones still being requested.
    fn evict_to_budget(&self, blocks: &mut Blocks) {
        let mut evicted = 0;
        let mut next = blocks.slots.keys().next().copied();
        while blocks.resident_bytes > self.max_bytes
            && let Some(slot) = next
        {
            let over = blocks.resident_bytes - self.max_bytes;
            let mut freed = 0;
            let victims: Vec<_> = blocks.slots[&slot]
                .iter()
                .filter_map(|(key, entry)| match entry {
                    Entry::Ready(bytes) if freed < over => {
                        freed += bytes.len() as u64;
                        Some(key.clone())
                    }
                    _ => None,
                })
                .collect();
            evicted += blocks.remove_where(slot, |key, _| victims.contains(key));
            next = blocks
                .slots
                .range(slot.saturating_add(1)..)
                .next()
                .map(|(slot, _)| *slot);
        }
        if evicted > 0 {
            metrics::get_block_recent_store_evictions("capacity", evicted);
        }
    }
}

/// Removes a `Building` entry if its task ends without a result, for example on a panic.
struct BuildGuard {
    inner: Arc<Inner>,
    key: RecentBlockKey,
    id: u64,
    armed: bool,
}

impl Drop for BuildGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut blocks = self.inner.lock();
        if blocks.is_building(&self.key, self.id) {
            blocks.remove_where(self.key.slot(), |key, _| *key == self.key);
        }
    }
}

impl RecentBlockResponses {
    pub(crate) fn new(max_bytes: u64) -> Self {
        let inner = (max_bytes > 0).then(|| {
            Arc::new(Inner {
                max_bytes,
                blocks: Mutex::default(),
            })
        });
        let store = Self { inner };
        if let Some(inner) = store.inner.as_ref() {
            inner.publish(&inner.lock());
        }
        store
    }

    pub(crate) fn enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Whether the key is stored or being built.
    pub(crate) fn contains(&self, key: &RecentBlockKey) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|inner| inner.lock().entry(key).is_some())
    }

    /// Returns the stored bytes, joins a running build, or starts one in its own task.
    /// Errors reach the requests waiting on that build and are never stored.
    pub(crate) async fn get_or_build<F>(
        &self,
        key: RecentBlockKey,
        build: F,
    ) -> Result<(Bytes, RecentAccess), Arc<RecentBuildError>>
    where
        F: Future<Output = Result<Bytes, RecentBuildError>> + Send + 'static,
    {
        let Some(inner) = self.inner.as_ref() else {
            return build
                .await
                .map(|bytes| (bytes, RecentAccess::Built))
                .map_err(Arc::new);
        };
        let (mut done, access) = {
            let mut blocks = inner.lock();
            match blocks.entry(&key) {
                Some(Entry::Ready(bytes)) => return Ok((bytes.clone(), RecentAccess::Hit)),
                Some(Entry::Building { done, .. }) => {
                    let done = done.clone();
                    drop(blocks);
                    // Free what the unused build holds, such as a prebuild permit, before waiting.
                    drop(build);
                    (done, RecentAccess::Merged)
                }
                None => {
                    let id = blocks.next_id;
                    blocks.next_id += 1;
                    let (sender, done) = watch::channel(None);
                    blocks.insert(
                        key.clone(),
                        Entry::Building {
                            id,
                            done: done.clone(),
                        },
                    );
                    let guard = BuildGuard {
                        inner: inner.clone(),
                        key,
                        id,
                        armed: true,
                    };
                    tokio::spawn(run_build(guard, sender, build));
                    (done, RecentAccess::Built)
                }
            }
        };
        let outcome = match done.wait_for(Option::is_some).await {
            Ok(outcome) => outcome.clone(),
            Err(_) => None,
        };
        outcome
            .unwrap_or_else(|| {
                Err(Arc::new(RecentBuildError::Failed(
                    "recent getBlock build stopped".to_string(),
                )))
            })
            .map(|bytes| (bytes, access))
    }

    /// Drops what a head bank event makes unreachable.
    pub(crate) fn apply_event(&self, event: HeadBankEvent) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        let mut blocks = inner.lock();
        let (reason, evicted) = match event.kind {
            HeadBankEventKind::Confirmed | HeadBankEventKind::Finalized => return,
            HeadBankEventKind::Discarded => (
                "discarded",
                blocks.remove_where(event.slot, |key, _| key.blockhash == event.blockhash),
            ),
            // The slot left the head, and so did every older slot the store still holds.
            HeadBankEventKind::Evicted => {
                let slots: Vec<_> = blocks
                    .slots
                    .range(..=event.slot)
                    .map(|(slot, _)| *slot)
                    .collect();
                let evicted = slots
                    .into_iter()
                    .map(|slot| blocks.remove_where(slot, |_, _| true))
                    .sum();
                ("evicted", evicted)
            }
        };
        if evicted > 0 {
            metrics::get_block_recent_store_evictions(reason, evicted);
        }
        inner.publish(&blocks);
    }

    /// Drops every entry, for example after missed head events.
    pub(crate) fn clear(&self, reason: &'static str) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        let mut blocks = inner.lock();
        let evicted = blocks.ready_entries;
        let next_id = blocks.next_id;
        *blocks = Blocks {
            next_id,
            ..Blocks::default()
        };
        if evicted > 0 {
            metrics::get_block_recent_store_evictions(reason, evicted);
        }
        inner.publish(&blocks);
    }

    #[cfg(test)]
    pub(crate) fn resident(&self) -> (u64, u64) {
        self.inner.as_ref().map_or((0, 0), |inner| {
            let blocks = inner.lock();
            (blocks.ready_entries, blocks.resident_bytes)
        })
    }
}

async fn run_build<F>(mut guard: BuildGuard, done: watch::Sender<BuildOutcome>, build: F)
where
    F: Future<Output = Result<Bytes, RecentBuildError>>,
{
    let result = build.await.map_err(Arc::new);
    guard.inner.finish(&guard.key, guard.id, &result);
    guard.armed = false;
    done.send_replace(Some(result));
}

/// One `getBlock` result shape to build when a block is confirmed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PrebuildShape {
    pub(crate) encoding: UiTransactionEncoding,
    pub(crate) transaction_details: TransactionDetails,
    pub(crate) rewards: bool,
    pub(crate) max_supported_transaction_version: Option<u8>,
}

impl PrebuildShape {
    pub(crate) fn key(self, slot: u64, blockhash: [u8; 32]) -> RecentBlockKey {
        RecentBlockKey::new(
            BlockResponseCacheKey::new(
                slot,
                CommitmentLevel::Confirmed,
                self.encoding,
                self.transaction_details,
                self.rewards,
                self.max_supported_transaction_version,
                false,
            ),
            blockhash,
        )
    }
}

/// `GET_BLOCK_PREBUILD_SHAPES`: comma-separated shapes of `:`-separated `getBlock` config fields.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PrebuildShapes(pub(crate) Vec<PrebuildShape>);

pub(crate) fn parse_prebuild_shapes(value: &str) -> Result<PrebuildShapes, String> {
    let mut shapes = Vec::new();
    for shape in value
        .split(',')
        .map(str::trim)
        .filter(|shape| !shape.is_empty())
    {
        let shape = parse_prebuild_shape(shape).map_err(|err| format!("shape '{shape}': {err}"))?;
        if !shapes.contains(&shape) {
            shapes.push(shape);
        }
    }
    Ok(PrebuildShapes(shapes))
}

/// Omitted fields take the `getBlock` defaults, as in a request that omits them.
fn parse_prebuild_shape(value: &str) -> Result<PrebuildShape, String> {
    let mut encoding = None;
    let mut transaction_details = None;
    let mut rewards = None;
    let mut max_version = None;
    for field in value.split(':').map(str::trim) {
        let Some((name, raw)) = field.split_once('=').map(|(n, v)| (n.trim(), v.trim())) else {
            return Err(format!("expected name=value, got '{field}'"));
        };
        let duplicate = match name {
            "encoding" => encoding.replace(serde_value(raw)?).is_some(),
            "transactionDetails" => transaction_details.replace(serde_value(raw)?).is_some(),
            "rewards" => rewards
                .replace(
                    raw.parse::<bool>()
                        .map_err(|_| format!("rewards '{raw}'"))?,
                )
                .is_some(),
            "maxSupportedTransactionVersion" => max_version
                .replace(raw.parse::<u8>().map_err(|_| format!("version '{raw}'"))?)
                .is_some(),
            _ => return Err(format!("unknown field '{name}'")),
        };
        if duplicate {
            return Err(format!("duplicate field '{name}'"));
        }
    }
    let encoding = encoding.unwrap_or(UiTransactionEncoding::Json);
    crate::handlers::encoding::validate_transaction_encoding(encoding, max_version)
        .map_err(str::to_string)?;
    Ok(PrebuildShape {
        encoding,
        transaction_details: transaction_details.unwrap_or_default(),
        rewards: rewards.unwrap_or(true),
        max_supported_transaction_version: max_version,
    })
}

fn serde_value<T: serde::de::DeserializeOwned>(raw: &str) -> Result<T, String> {
    serde_json::from_value(serde_json::Value::String(raw.to_string()))
        .map_err(|_| format!("unsupported value '{raw}'"))
}

/// Shapes built on each Confirmed event, on permits separate from `HYDRATION_CPU_CONCURRENCY`.
#[derive(Clone)]
pub(crate) struct Prebuild {
    shapes: Arc<[PrebuildShape]>,
    permits: Arc<Semaphore>,
    newest_confirmed: Arc<AtomicU64>,
}

impl Prebuild {
    pub(crate) fn new(shapes: Vec<PrebuildShape>, concurrency: usize) -> Option<Self> {
        (!shapes.is_empty()).then(|| Self {
            shapes: shapes.into(),
            permits: Arc::new(Semaphore::new(concurrency.max(1))),
            newest_confirmed: Arc::default(),
        })
    }
}

/// Applies head bank events to the store and prebuilds confirmed blocks.
pub(crate) async fn run(
    state: Arc<AppState>,
    head: Arc<HeadCache>,
    mut events: broadcast::Receiver<HeadBankEvent>,
    prebuild: Option<Prebuild>,
) {
    loop {
        let event = match recv_bank_event(&mut events).await {
            Ok(event) => event,
            Err(broadcast::error::RecvError::Lagged(_)) => {
                state.recent_block_responses.clear("lagged");
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => return,
        };
        state.recent_block_responses.apply_event(event);
        if event.kind != HeadBankEventKind::Confirmed {
            continue;
        }
        if let Some(prebuild) = prebuild.as_ref() {
            prebuild
                .newest_confirmed
                .fetch_max(event.slot, Ordering::Relaxed);
            tokio::spawn(prebuild_block(
                state.clone(),
                head.clone(),
                event,
                prebuild.clone(),
            ));
        }
    }
}

async fn prebuild_block(
    state: Arc<AppState>,
    head: Arc<HeadCache>,
    event: HeadBankEvent,
    prebuild: Prebuild,
) {
    for shape in prebuild.shapes.iter().copied() {
        let outcome = prebuild_shape(&state, &head, event, shape, &prebuild).await;
        metrics::get_block_recent_store_prebuild(outcome);
    }
}

async fn prebuild_shape(
    state: &Arc<AppState>,
    head: &Arc<HeadCache>,
    event: HeadBankEvent,
    shape: PrebuildShape,
    prebuild: &Prebuild,
) -> &'static str {
    let key = shape.key(event.slot, event.blockhash);
    if state.recent_block_responses.contains(&key) {
        return "present";
    }
    let Ok(permit) = prebuild.permits.clone().acquire_owned().await else {
        return "dropped";
    };
    let newest = prebuild.newest_confirmed.load(Ordering::Relaxed);
    if newest > event.slot.saturating_add(PREBUILD_MAX_LAG_SLOTS) {
        return "skipped_lagging";
    }
    if head.latest_slot() == 0 {
        return "skipped_inactive";
    }
    let semaphore = prebuild.permits.clone();
    match crate::handlers::blocks::prebuild_recent_block(state, head, key, shape, semaphore, permit)
        .await
    {
        Ok(RecentAccess::Built) => "built",
        Ok(RecentAccess::Hit | RecentAccess::Merged) => "present",
        Err(err) => match err.as_ref() {
            RecentBuildError::Unavailable => "dropped",
            RecentBuildError::UnsupportedTransactionVersion(_) | RecentBuildError::Failed(_) => {
                "failed"
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    fn key(slot: u64, hash: u8) -> RecentBlockKey {
        PrebuildShape {
            encoding: UiTransactionEncoding::Json,
            transaction_details: TransactionDetails::Full,
            rewards: false,
            max_supported_transaction_version: Some(0),
        }
        .key(slot, [hash; 32])
    }

    fn event(slot: u64, hash: u8, kind: HeadBankEventKind) -> HeadBankEvent {
        HeadBankEvent {
            slot,
            blockhash: [hash; 32],
            kind,
        }
    }

    async fn fill(store: &RecentBlockResponses, key: RecentBlockKey, body: &'static [u8]) {
        let (bytes, access) = store
            .get_or_build(key, async move { Ok(Bytes::from_static(body)) })
            .await
            .expect("built");
        assert_eq!((bytes.as_ref(), access), (body, RecentAccess::Built));
    }

    async fn stored(store: &RecentBlockResponses, key: RecentBlockKey) -> Option<Bytes> {
        let (bytes, access) = store
            .get_or_build(key, async { Err(RecentBuildError::Unavailable) })
            .await
            .ok()?;
        assert_eq!(access, RecentAccess::Hit);
        Some(bytes)
    }

    #[tokio::test]
    async fn disabled_store_builds_every_request() {
        let store = RecentBlockResponses::new(0);
        assert!(!store.enabled());
        fill(&store, key(1, 1), b"one").await;
        fill(&store, key(1, 1), b"one").await;
        assert!(!store.contains(&key(1, 1)));
        assert_eq!(store.resident(), (0, 0));
    }

    #[test]
    fn key_ignores_commitment_and_footer_but_not_shape_or_blockhash() {
        let shape = key(7, 1).shape;
        let finalized_footer = BlockResponseCacheKey {
            commitment: CommitmentLevel::Finalized,
            footer: true,
            ..shape.clone()
        };
        assert_eq!(RecentBlockKey::new(finalized_footer, [1; 32]), key(7, 1));
        assert_ne!(key(7, 2), key(7, 1));
        let rewards = BlockResponseCacheKey {
            show_rewards: true,
            ..shape
        };
        assert_ne!(RecentBlockKey::new(rewards, [1; 32]), key(7, 1));
    }

    #[tokio::test]
    async fn another_blockhash_for_the_same_slot_misses() {
        let store = RecentBlockResponses::new(1024);
        fill(&store, key(5, 1), b"fork-a").await;
        assert_eq!(stored(&store, key(5, 2)).await, None);
        fill(&store, key(5, 2), b"fork-b").await;
        assert_eq!(stored(&store, key(5, 1)).await.unwrap(), "fork-a");
        assert_eq!(stored(&store, key(5, 2)).await.unwrap(), "fork-b");
    }

    #[tokio::test]
    async fn concurrent_requests_share_one_build() {
        let store = RecentBlockResponses::new(1024);
        let builds = Arc::new(AtomicUsize::new(0));
        let tasks: Vec<_> = (0..8)
            .map(|_| {
                let store = store.clone();
                let builds = builds.clone();
                tokio::spawn(async move {
                    store
                        .get_or_build(key(2, 1), async move {
                            builds.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            Ok(Bytes::from_static(b"shared"))
                        })
                        .await
                        .expect("value")
                })
            })
            .collect();
        let mut built = 0;
        for task in tasks {
            let (bytes, access) = task.await.expect("task");
            assert_eq!(bytes, "shared");
            built += usize::from(access == RecentAccess::Built);
        }
        assert_eq!(builds.load(Ordering::Relaxed), 1);
        assert_eq!(built, 1);
        assert_eq!(store.resident(), (1, 6));
    }

    #[tokio::test]
    async fn errors_reach_waiters_but_are_not_stored() {
        let store = RecentBlockResponses::new(1024);
        let builds = Arc::new(AtomicUsize::new(0));
        let attempt = || {
            let builds = builds.clone();
            store.get_or_build(key(3, 1), async move {
                builds.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(20)).await;
                Err(RecentBuildError::UnsupportedTransactionVersion(0))
            })
        };
        let (first, second) = tokio::join!(attempt(), attempt());
        for result in [first, second] {
            assert!(matches!(
                result.unwrap_err().as_ref(),
                RecentBuildError::UnsupportedTransactionVersion(0)
            ));
        }
        assert_eq!(builds.load(Ordering::Relaxed), 1);
        assert!(!store.contains(&key(3, 1)));
        assert!(attempt().await.is_err());
        assert_eq!(builds.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn bank_events_evict_what_left_the_head() {
        use HeadBankEventKind::{Confirmed, Discarded, Evicted, Finalized};
        let store = RecentBlockResponses::new(1024);
        for (slot, hash) in [(10, 1), (10, 2), (11, 1), (12, 1), (13, 1)] {
            fill(&store, key(slot, hash), b"block").await;
        }
        store.apply_event(event(10, 1, Confirmed));
        store.apply_event(event(10, 1, Finalized));
        assert_eq!(store.resident().0, 5);

        store.apply_event(event(10, 1, Discarded));
        assert!(!store.contains(&key(10, 1)));
        assert!(store.contains(&key(10, 2)), "other forks of the slot stay");

        store.apply_event(event(12, 9, Evicted));
        for (slot, hash) in [(10, 2), (11, 1), (12, 1)] {
            assert!(!store.contains(&key(slot, hash)), "{slot}");
        }
        assert!(store.contains(&key(13, 1)));
        assert_eq!(store.resident(), (1, 5));

        store.clear("lagged");
        assert_eq!(store.resident(), (0, 0));
    }

    #[tokio::test]
    async fn build_removed_by_an_event_is_not_stored() {
        let store = RecentBlockResponses::new(1024);
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        let pending = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .get_or_build(key(4, 1), async move {
                        wait.await.ok();
                        Ok(Bytes::from_static(b"stale"))
                    })
                    .await
            })
        };
        while !store.contains(&key(4, 1)) {
            tokio::task::yield_now().await;
        }
        store.apply_event(event(4, 1, HeadBankEventKind::Discarded));
        release.send(()).unwrap();
        let (bytes, access) = pending.await.unwrap().unwrap();
        assert_eq!(
            (bytes.as_ref(), access),
            (&b"stale"[..], RecentAccess::Built)
        );
        assert!(!store.contains(&key(4, 1)));
        assert_eq!(store.resident(), (0, 0));
    }

    #[tokio::test]
    async fn merged_request_releases_its_build_before_waiting() {
        let store = RecentBlockResponses::new(1024);
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        let first = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .get_or_build(key(6, 1), async move {
                        wait.await.ok();
                        Ok(Bytes::from_static(b"block"))
                    })
                    .await
            })
        };
        while !store.contains(&key(6, 1)) {
            tokio::task::yield_now().await;
        }
        // Like a prebuild, the merged request's unused build owns a permit.
        let permits = Arc::new(Semaphore::new(1));
        let permit = permits.clone().acquire_owned().await.unwrap();
        let merged = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .get_or_build(key(6, 1), async move {
                        let _permit = permit;
                        Err(RecentBuildError::Unavailable)
                    })
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            while permits.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the merged request dropped its build while the first build ran");
        release.send(()).unwrap();
        let (bytes, access) = merged.await.unwrap().unwrap();
        assert_eq!(
            (bytes.as_ref(), access),
            (&b"block"[..], RecentAccess::Merged)
        );
        assert_eq!(first.await.unwrap().unwrap().1, RecentAccess::Built);
    }

    #[tokio::test]
    async fn budget_evicts_the_lowest_slots_first() {
        let store = RecentBlockResponses::new(10);
        fill(&store, key(20, 1), b"aaaa").await;
        fill(&store, key(22, 1), b"bbbb").await;
        fill(&store, key(21, 1), b"cccc").await;
        assert!(!store.contains(&key(20, 1)));
        assert!(store.contains(&key(21, 1)));
        assert!(store.contains(&key(22, 1)));
        assert_eq!(store.resident(), (2, 8));

        fill(&store, key(23, 1), b"too large for it").await;
        assert!(!store.contains(&key(23, 1)));
        assert_eq!(store.resident(), (2, 8));
    }

    #[test]
    fn prebuild_shapes_parse_with_get_block_defaults() {
        let shapes = parse_prebuild_shapes(
            " encoding=base64:transactionDetails=accounts:maxSupportedTransactionVersion=0 ,,\
             rewards=false",
        )
        .unwrap();
        assert_eq!(
            shapes.0,
            vec![
                PrebuildShape {
                    encoding: UiTransactionEncoding::Base64,
                    transaction_details: TransactionDetails::Accounts,
                    rewards: true,
                    max_supported_transaction_version: Some(0),
                },
                PrebuildShape {
                    encoding: UiTransactionEncoding::Json,
                    transaction_details: TransactionDetails::Full,
                    rewards: false,
                    max_supported_transaction_version: None,
                },
            ]
        );
        assert_eq!(
            parse_prebuild_shapes("").unwrap(),
            PrebuildShapes::default()
        );
        assert!(parse_prebuild_shapes("encoding=jsonParsed:footer=true").is_err());
        assert!(Prebuild::new(Vec::new(), 1).is_none());
    }

    #[tokio::test]
    async fn prebuild_skips_lagging_inactive_absent_and_present_blocks() {
        let head = Arc::new(HeadCache::new(32, 64));
        let mut state = crate::tests::test_state();
        {
            let state = Arc::get_mut(&mut state).unwrap();
            state.head_cache = Some(head.clone());
            state.recent_block_responses = RecentBlockResponses::new(1024);
        }
        let shape = parse_prebuild_shapes("rewards=false").unwrap().0[0];
        let prebuild = Prebuild::new(vec![shape], 1).unwrap();
        let confirmed = |slot| event(slot, 3, HeadBankEventKind::Confirmed);
        let outcome = |slot| prebuild_shape(&state, &head, confirmed(slot), shape, &prebuild);

        assert_eq!(outcome(60).await, "skipped_inactive");
        prebuild.newest_confirmed.store(60 + 9, Ordering::Relaxed);
        assert_eq!(outcome(60).await, "skipped_lagging");
        prebuild.newest_confirmed.store(60, Ordering::Relaxed);

        let mut record = crate::tests::base_transaction_record();
        record.slot = 60;
        head.insert_for_tests(
            crate::solana_sdk::signature::Signature::from(record.signature),
            record,
            0,
            &[],
            CommitmentLevel::Confirmed,
        );
        assert_eq!(outcome(60).await, "dropped", "no block in the head");
        fill(
            &state.recent_block_responses,
            shape.key(60, [3; 32]),
            b"block",
        )
        .await;
        assert_eq!(outcome(60).await, "present");
        assert_eq!(prebuild.permits.available_permits(), 1);
    }
}
