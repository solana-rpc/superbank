// SPDX-License-Identifier: AGPL-3.0-only
//! Ordered, partition-scoped reads under one cache-attempt deadline.
use super::{
    DiskCache, DiskGsfaPage, DiskSigStatus, DiskStatusLookup, DiskStatusLookups,
    DiskTransactionResult, SlotStatus, clamp_until_to_floor,
    index::DiskTfaQuery,
    key_index::{Family, SignatureCandidates, SignatureHash},
    lower_bound_reaches_floor, upper_bound_reaches_tip,
};
use crate::clickhouse::{
    CacheAdmissionBusy, ClickHouseClient, NumericFilter, PaginationToken, SignatureRecord,
    SignatureSlot, SignatureStatusRecord, SlotBoundary, SortOrder, StoredTransactionRecord,
    TokenAccountsFilter, TransactionsForAddressQuery,
};
use crate::processing::{ProcessingError, ProcessingResult};
use crate::solana_sdk::{pubkey::Pubkey, signature::Signature};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use tokio::time::Instant;

// Slot bounds are conservative for position cursors: SQL resolves the index
// within the boundary slot. Saturation may retain one impossible edge slot.
fn gsfa_window(
    (mut floor, mut tip): (u64, u64),
    before: Option<SlotBoundary>,
    until: Option<SlotBoundary>,
) -> (u64, u64) {
    tip = tip.min(match before {
        Some(SlotBoundary::Position(p)) => p.slot,
        Some(SlotBoundary::Slot(slot)) => slot.saturating_sub(1),
        None => u64::MAX,
    });
    floor = floor.max(match until {
        Some(SlotBoundary::Position(p)) => p.slot,
        Some(SlotBoundary::Slot(slot)) => slot.saturating_add(1),
        None => 0,
    });
    (floor, tip)
}

/// Lowest slot above `tip` inside a newest-first request window `(lower, upper)`: the
/// start of the rows the request owes that a local page ending at `tip` did not read.
fn tip_gap_start(tip: u64, (lower, upper): (u64, u64)) -> Option<u64> {
    let start = lower.max(tip.saturating_add(1));
    (start <= upper).then_some(start)
}

/// [`tip_gap_start`] for a getSignaturesForAddress window.
pub(crate) fn gsfa_tip_gap(
    tip: u64,
    before: Option<SlotBoundary>,
    until: Option<SlotBoundary>,
) -> Option<u64> {
    tip_gap_start(tip, gsfa_window((0, u64::MAX), before, until))
}

/// [`tip_gap_start`] for a descending getTransactionsForAddress window. An ascending
/// page starts from its oldest rows, so a page that filled before the tip owes nothing
/// newer, and one that reached the tip already sends the remainder above it.
pub(crate) fn tfa_tip_gap(tip: u64, query: &TransactionsForAddressQuery) -> Option<u64> {
    match query.sort_order {
        SortOrder::Asc => None,
        SortOrder::Desc => tip_gap_start(tip, tfa_window((0, u64::MAX), query)),
    }
}

/// Whether a local newest-first address page may stand in for the primary's rows above
/// its tip. The local tip trails the source's finalized tip (the forwarder lag), so rows
/// in `[gap_start, upper]` are owed unless none exist in the window (`gap_start` is
/// `None`) or the head cache merged into the response holds every row of the address
/// from `head_floor` up (`head_floor` is only evaluated when a gap exists). A floor at or
/// below `gap_start` is the same test as "the head's proof has no gap in
/// `[gap_start, head tip]`": the proof is one interval ending at the head tip.
pub(crate) fn tip_gap_covered(
    operation: &'static str,
    gap_start: Option<u64>,
    head_floor: impl FnOnce() -> Option<u64>,
) -> bool {
    let Some(start) = gap_start else {
        return true;
    };
    let covered = head_floor().is_some_and(|floor| floor <= start);
    crate::metrics::disk_cache_tip_gap(operation, if covered { "head" } else { "primary" });
    covered
}

fn numeric_window((floor, tip): (u64, u64), filter: &NumericFilter<u64>) -> (u64, u64) {
    let lower = [
        Some(floor),
        filter.gte,
        filter.gt.map(|v| v.saturating_add(1)),
        filter.eq,
    ];
    let upper = [
        Some(tip),
        filter.lte,
        filter.lt.map(|v| v.saturating_sub(1)),
        filter.eq,
    ];
    (
        lower.into_iter().flatten().max().unwrap_or(floor),
        upper.into_iter().flatten().min().unwrap_or(tip),
    )
}

fn tfa_window(mut span: (u64, u64), query: &TransactionsForAddressQuery) -> (u64, u64) {
    if let Some(filter) = &query.slot_filter {
        span = numeric_window(span, filter);
    }
    if let Some(filter) = &query.resolved_signature_filter {
        for position in [filter.gt, filter.gte].into_iter().flatten() {
            span.0 = span.0.max(position.slot);
        }
        for position in [filter.lt, filter.lte].into_iter().flatten() {
            span.1 = span.1.min(position.slot);
        }
    }
    if let Some(position) = query.resolved_pagination {
        match query.sort_order {
            SortOrder::Asc => span.0 = span.0.max(position.slot),
            SortOrder::Desc => span.1 = span.1.min(position.slot),
        }
    }
    span
}

#[derive(Default)]
struct AddressRows {
    records: Vec<SignatureRecord>,
    seen: HashSet<String>,
}
impl AddressRows {
    fn append(&mut self, page: Vec<crate::clickhouse::TransactionsForAddressRecord>) {
        self.records.extend(
            page.into_iter()
                .filter(|r| self.seen.insert(r.signature.clone()))
                .map(|r| SignatureRecord {
                    signature: r.signature,
                    slot: r.slot,
                    slot_idx: r.slot_idx,
                    err: r.err,
                    memo: r.memo,
                    block_time: r.block_time,
                }),
        );
    }
}

/// Address requests may wait for local admission for at most this fraction of their
/// remaining budget; past that the cache is busy and the primary answers instead.
const ADDRESS_ADMISSION_WAIT_DIVISOR: u32 = 10;

/// Source of the error when a read meets more unindexed partitions than it may probe.
#[derive(Debug, thiserror::Error)]
#[error("disk cache unknown partition probe budget exhausted")]
struct ProbeBudgetExhausted;

// Two unknown partitions allow both intentionally unindexed retention edges.
// A third needs source fallback: skipping it could silently truncate a page.
struct ProbeBudget(usize);
impl ProbeBudget {
    fn candidate(&mut self, membership: Option<bool>) -> ProcessingResult<bool> {
        if let Some(present) = membership {
            return Ok(present);
        }
        // Still a timeout to callers (fall back to source), but distinguishable in metrics.
        self.0 = self
            .0
            .checked_sub(1)
            .ok_or_else(|| ProcessingError::Timeout {
                context: ProbeBudgetExhausted.to_string(),
                source: Some(Box::new(ProbeBudgetExhausted)),
            })?;
        Ok(true)
    }
}

fn is_probe_budget_exhausted(err: &ProcessingError) -> bool {
    matches!(err, ProcessingError::Timeout { source: Some(source), .. } if source.is::<ProbeBudgetExhausted>())
}

/// Metric outcome for a failed cache attempt.
fn error_outcome(err: &ProcessingError) -> &'static str {
    match err {
        ProcessingError::Timeout {
            source: Some(source),
            ..
        } if source.is::<CacheAdmissionBusy>() => "busy",
        ProcessingError::Timeout {
            source: Some(source),
            ..
        } if source.is::<ProbeBudgetExhausted>() => "probe_budget",
        ProcessingError::Timeout { .. } => "timeout",
        ProcessingError::Database { context, .. } if context.contains("TIMEOUT_EXCEEDED") => {
            "timeout"
        }
        // A cursor shares the attempt's deadline and is polled first, so a stream cut off
        // at the deadline surfaces as the driver's own timeout rather than the attempt's.
        ProcessingError::Database {
            source: Some(source),
            ..
        } if matches!(
            source.downcast_ref::<clickhouse::error::Error>(),
            Some(clickhouse::error::Error::TimedOut)
        ) =>
        {
            "timeout"
        }
        _ => "error",
    }
}

// A status query costs about one granule read per signature for every partition its
// slot range spans, whether or not the index lists that partition for the signature
// (~10 µs each on a warm local server), while each query costs a local admission and
// round trip and re-matches its signatures against every part (~2 ms and up). One
// range query replaces the per-partition queries while it stays within this many
// signature-partition lookups, or within four times the lookups those queries would
// issue anyway (unknown partitions make every signature a candidate). Larger sparse
// batches keep the per-partition queries, which read only candidate partitions.
const STATUS_SPAN_MAX_LOOKUPS: usize = 1024;
const STATUS_SPAN_MAX_OVERREAD: usize = 4;

/// Oldest and newest candidate partitions and the distinct signatures for one status
/// query over their span, or `None` when that query would read far more than
/// per-partition queries.
fn status_span(by_partition: &BTreeMap<u64, Vec<String>>) -> Option<(u64, u64, Vec<String>)> {
    let (&oldest, _) = by_partition.first_key_value()?;
    let (&newest, _) = by_partition.last_key_value()?;
    let pairs: usize = by_partition.values().map(Vec::len).sum();
    let pending: BTreeSet<&String> = by_partition.values().flatten().collect();
    let span = usize::try_from(newest - oldest).map_or(usize::MAX, |d| d.saturating_add(1));
    let lookups = pending.len().saturating_mul(span);
    (lookups <= STATUS_SPAN_MAX_LOOKUPS.max(pairs.saturating_mul(STATUS_SPAN_MAX_OVERREAD)))
        .then(|| (oldest, newest, pending.into_iter().cloned().collect()))
}

/// A signature the read never matched (a Bloom negative or a query miss) is absent
/// only when the read proved its span; a row outside coverage proves nothing.
fn classify_status(found: Option<&Option<DiskSigStatus>>, provable: bool) -> DiskStatusLookup {
    match found {
        Some(Some(status)) => DiskStatusLookup::Found(status.clone()),
        Some(None) => DiskStatusLookup::Unknown,
        None if provable => DiskStatusLookup::Absent,
        None => DiskStatusLookup::Unknown,
    }
}

struct Read {
    deadline: Instant,
    epoch: u64,
    data_epoch: u64,
    unknown_probe_limit: usize,
    /// Part of an interactive address request: admission waits are bounded.
    address_request: bool,
}
impl Read {
    fn candidate(
        &self,
        budget: &mut ProbeBudget,
        membership: Option<bool>,
    ) -> ProcessingResult<bool> {
        self.check()?;
        budget.candidate(membership)
    }
    fn check(&self) -> ProcessingResult<()> {
        if Instant::now() >= self.deadline {
            return Err(ProcessingError::timeout_msg("disk cache deadline exceeded"));
        }
        Ok(())
    }
}
/// Scope a local-cache client to what remains of the read's budget.
fn apply_budget(client: &mut ClickHouseClient, read: &Read) {
    let remaining = read.deadline.saturating_duration_since(Instant::now());
    client.query_timeout = remaining;
    client.cache_admission_wait = read
        .address_request
        .then(|| remaining / ADDRESS_ADMISSION_WAIT_DIVISOR);
}
/// What changed between the start and the completion of a local read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadChange {
    /// Ready throughout and never invalidated: every outcome stands.
    None,
    /// Only the eviction floor or routing membership moved. Negatives may be stale;
    /// a found row is still valid while its slot remains covered.
    Negatives,
    /// Rows at covered slots may have changed (poison, repair, schema rebuild), or the
    /// cache is not ready: no outcome may be served.
    Data,
}
/// Bounded `get_tx_reason` labels, finer than the `get_tx` hit/miss/timeout/error outcomes.
mod tx_reason {
    /// Found, and nothing invalidated the read.
    pub(super) const FOUND: &str = "found";
    /// Found; the eviction floor moved during the read but the row's slot is still covered.
    pub(super) const FOUND_REVALIDATED: &str = "found_revalidated";
    /// A covered or skipped pinned slot proved absence.
    pub(super) const ABSENT: &str = "absent";
    /// No candidate partition (Bloom negative everywhere, or no coverage); no query ran.
    pub(super) const BLOOM_ABSENT: &str = "bloom_absent";
    /// Candidate partitions were probed and held no position.
    pub(super) const PROBE_EMPTY: &str = "probe_empty";
    /// One lookup over the fused read's whole span found no position, so the
    /// per-partition probes (which could only find none) were skipped.
    pub(super) const SPAN_EMPTY: &str = "span_empty";
    /// A position without its payload row (or outside coverage).
    pub(super) const INDEX_WITHOUT_PAYLOAD: &str = "index_without_payload";
    /// The read completed but was invalidated before it could be served.
    pub(super) const INVALIDATED: &str = "invalidated";
    /// The cache was not ready; no read was attempted.
    pub(super) const NOT_READY: &str = "not_ready";
    pub(super) const TIMEOUT: &str = "timeout";
    /// The shorter unknown-membership budget expired (signature index still building).
    pub(super) const UNKNOWN_TIMEOUT: &str = "unknown_timeout";
    pub(super) const ERROR: &str = "error";
}
/// What the fused read established. Only `Row` is served; the rest fall back.
#[derive(Debug)]
enum FusedRead {
    /// The payload row today's newest-first search returns.
    Row(Box<StoredTransactionRecord>),
    /// The query returned no row: either no position in the span, or a position
    /// whose payload does not match (stale, secondary signature, missing payload).
    Empty,
    /// A row came back that the per-partition search would not return (skipped
    /// partition, uncovered slot), so a position exists in the span.
    Rejected,
    /// The query did not run.
    NotRun,
}
/// Whether an empty fused read first asks its whole span for any position before
/// the per-partition probes. The span contains every candidate partition, so no
/// position there means every probe finds none and the probes' verdict is already
/// known. With one candidate the span lookup is that probe, so it is not repeated.
fn span_check_first(fused: &FusedRead, candidate_partitions: usize) -> bool {
    matches!(fused, FusedRead::Empty) && candidate_partitions > 1
}
fn partition_slot_span(width: u64, newest: u64, oldest: u64) -> (u64, u64) {
    (
        oldest.saturating_mul(width),
        newest
            .saturating_add(1)
            .saturating_mul(width)
            .saturating_sub(1),
    )
}
/// Returns the served value, the `get_tx` outcome label and the `get_tx_reason` label.
fn transaction_outcome(
    result: Result<
        ProcessingResult<(DiskTransactionResult, &'static str)>,
        tokio::time::error::Elapsed,
    >,
    change: ReadChange,
    covers_slot: impl FnOnce(u64) -> bool,
) -> (DiskTransactionResult, &'static str, &'static str) {
    match result {
        Ok(Ok((value, reason))) if change == ReadChange::None => {
            let outcome = if matches!(value, DiskTransactionResult::Found(_)) {
                "hit"
            } else {
                "miss"
            };
            (value, outcome, reason)
        }
        // Checked at completion: a slot evicted or poisoned meanwhile is no longer covered.
        Ok(Ok((DiskTransactionResult::Found(record), _)))
            if change == ReadChange::Negatives && covers_slot(record.slot) =>
        {
            (
                DiskTransactionResult::Found(record),
                "hit",
                tx_reason::FOUND_REVALIDATED,
            )
        }
        Ok(Ok(_)) => (
            DiskTransactionResult::Unavailable,
            "miss",
            tx_reason::INVALIDATED,
        ),
        Ok(Err(ProcessingError::Timeout { .. })) | Err(_) => (
            DiskTransactionResult::Unavailable,
            "timeout",
            tx_reason::TIMEOUT,
        ),
        Ok(Err(ProcessingError::Database { context, .. }))
            if context.contains("TIMEOUT_EXCEEDED") =>
        {
            (
                DiskTransactionResult::Unavailable,
                "timeout",
                tx_reason::TIMEOUT,
            )
        }
        Ok(Err(_)) => (
            DiskTransactionResult::Unavailable,
            "error",
            tx_reason::ERROR,
        ),
    }
}
/// Budget for one local getTransaction attempt. It never exceeds the shared
/// read timeout, so other reads keep their full budget.
fn get_tx_budget(cfg: &super::DiskCacheConfig) -> std::time::Duration {
    cfg.get_tx_timeout.min(cfg.query_timeout)
}
/// Unknown-membership signature partitions a get_tx may face at the normal budget.
/// Outside a signature index (re)build only a repairing or failed fill leaves
/// partitions unknown, one or two at a time; after a restart or reset every
/// partition in the span is. Above this, a miss probes each one in turn.
const GET_TX_UNKNOWN_PARTITION_LIMIT: u64 = 4;
/// The budget for a get_tx that faces `unknown_partitions` unknown-membership
/// partitions, and whether the shorter unknown-membership budget applies. That
/// budget never exceeds the normal one, so it can only end the attempt sooner.
fn get_tx_budget_for(
    cfg: &super::DiskCacheConfig,
    unknown_partitions: u64,
) -> (std::time::Duration, bool) {
    let budget = get_tx_budget(cfg);
    if unknown_partitions > GET_TX_UNKNOWN_PARTITION_LIMIT {
        (cfg.get_tx_unknown_timeout.min(budget), true)
    } else {
        (budget, false)
    }
}
/// A timeout under the unknown-membership budget gets its own reason label.
fn gated_reason(reason: &'static str, gated: bool) -> &'static str {
    if gated && reason == tx_reason::TIMEOUT {
        tx_reason::UNKNOWN_TIMEOUT
    } else {
        reason
    }
}
impl DiskCache {
    fn read(&self) -> Option<Read> {
        self.ready().then(|| {
            let (epoch, data_epoch) = self.inner.key_index.epochs();
            Read {
                deadline: Instant::now() + self.inner.cfg.query_timeout,
                epoch,
                data_epoch,
                unknown_probe_limit: usize::MAX,
                address_request: false,
            }
        })
    }
    /// The primary starts only when a local attempt gives up, so getTransaction
    /// gets its own, shorter budget, and a shorter one still while the signature
    /// index has many unknown-membership partitions. Also returns whether that
    /// unknown-membership budget applies.
    fn get_tx_read(&self) -> Option<(Read, bool)> {
        let mut read = self.read()?;
        let (budget, gated) =
            get_tx_budget_for(&self.inner.cfg, self.unknown_signature_partitions());
        read.deadline = read.deadline.min(Instant::now() + budget);
        Some((read, gated))
    }
    /// One absolute deadline shared by every cache stage of an address request.
    pub(crate) fn address_request_deadline(&self) -> Instant {
        Instant::now() + self.inner.cfg.address_query_timeout
    }
    fn read_until(&self, deadline: Instant) -> Option<Read> {
        let mut read = self.read()?;
        read.deadline = read.deadline.min(deadline);
        read.unknown_probe_limit = 2;
        read.address_request = true;
        read.check().ok()?;
        Some(read)
    }
    fn valid_read(&self, read: &Read) -> bool {
        self.ready() && read.epoch == self.inner.key_index.epoch()
    }
    fn read_change(&self, read: &Read) -> ReadChange {
        let (epoch, data_epoch) = self.inner.key_index.epochs();
        if !self.ready() || data_epoch != read.data_epoch {
            ReadChange::Data
        } else if epoch != read.epoch {
            ReadChange::Negatives
        } else {
            ReadChange::None
        }
    }
    fn scoped_client(
        &self,
        base: &ClickHouseClient,
        partition: u64,
        read: &Read,
    ) -> ClickHouseClient {
        let mut client = base.clone();
        client.cache_partition = Some((self.inner.cfg.partition_slots, partition));
        apply_budget(&mut client, read);
        client
    }
    /// Newest-first candidate partitions for an address read. Stops where the former
    /// per-partition loop would have exhausted its probe budget and returns that error
    /// beside the prefix: the loop raised it only when the prefix left the page short.
    fn address_candidates(
        &self,
        (floor, tip): (u64, u64),
        families: &[Family],
        key: &[u8],
        read: &Read,
    ) -> ProcessingResult<(Vec<u64>, Option<ProcessingError>)> {
        let mut budget = ProbeBudget(read.unknown_probe_limit);
        let mut partitions = Vec::new();
        for partition in self.partitions(floor, tip, SortOrder::Desc) {
            match self.candidate(partition, families, key, &mut budget, read) {
                Ok(true) => partitions.push(partition),
                Ok(false) => {}
                Err(err) if is_probe_budget_exhausted(&err) => return Ok((partitions, Some(err))),
                Err(err) => return Err(err),
            }
        }
        Ok((partitions, None))
    }
    /// Inclusive slot bounds from the oldest to the newest candidate partition. One
    /// range keeps primary-key analysis on binary search; partitions skipped inside it
    /// are definite index negatives, so callers must not treat their rows as candidates.
    fn partition_slot_span(&self, newest: u64, oldest: u64) -> (u64, u64) {
        partition_slot_span(self.inner.cfg.partition_slots, newest, oldest)
    }
    pub(super) fn key_span(&self) -> Option<(u64, u64)> {
        self.inner
            .coverage
            .read()
            .expect("coverage lock")
            .covered_span()
    }
    fn partitions(
        &self,
        floor: u64,
        tip: u64,
        order: SortOrder,
    ) -> impl Iterator<Item = u64> + use<> {
        let width = self.inner.cfg.partition_slots;
        let mut range = (floor / width..=tip / width).filter(move |_| floor <= tip);
        std::iter::from_fn(move || match order {
            SortOrder::Asc => range.next(),
            SortOrder::Desc => range.next_back(),
        })
    }
    fn candidate(
        &self,
        partition: u64,
        families: &[Family],
        key: &[u8],
        budget: &mut ProbeBudget,
        read: &Read,
    ) -> ProcessingResult<bool> {
        read.candidate(
            budget,
            self.inner.key_index.membership(partition, families, key),
        )
        .inspect(|candidate| {
            crate::metrics::disk_cache_read(
                "key_partition",
                if *candidate { "probed" } else { "skipped" },
            );
        })
    }
    fn signature_candidates(
        &self,
        floor: u64,
        tip: u64,
        signature: &Signature,
    ) -> SignatureCandidates {
        let started = std::time::Instant::now();
        let width = self.inner.cfg.partition_slots;
        let candidates = self.inner.key_index.signature_candidates(
            floor / width,
            tip / width,
            SignatureHash::new(signature.as_ref()),
        );
        let elapsed = started.elapsed().as_secs_f64();
        crate::metrics::disk_cache_signature_membership(candidates.outcome(), elapsed);
        crate::metrics::disk_cache_read_count(
            "key_partition",
            "skipped",
            candidates.total - candidates.partitions.len() as u64,
        );
        candidates
    }
    async fn attempt<T>(
        &self,
        operation: &'static str,
        read: &Read,
        future: impl Future<Output = ProcessingResult<Option<T>>>,
    ) -> Option<T> {
        let started = Instant::now();
        let result = tokio::time::timeout_at(read.deadline, future).await;
        let (value, outcome) = match result {
            Ok(Ok(Some(value))) if self.valid_read(read) => (Some(value), "hit"),
            Ok(Ok(_)) => (None, "miss"),
            Ok(Err(err)) => (None, error_outcome(&err)),
            Err(_) => (None, "timeout"),
        };
        crate::metrics::disk_cache_read(operation, outcome);
        crate::metrics::disk_cache_key_seconds(operation, outcome, started.elapsed().as_secs_f64());
        value
    }
    /// Newest-first candidate partitions, fixed when the search begins.
    fn position_candidates(&self, signature: &Signature) -> Option<SignatureCandidates> {
        let (floor, tip) = self.key_span()?;
        let candidates = self.signature_candidates(floor, tip, signature);
        (!candidates.partitions.is_empty()).then_some(candidates)
    }
    async fn find_position(
        &self,
        signature: Signature,
        read: &Read,
    ) -> ProcessingResult<Option<SignatureSlot>> {
        match self.position_candidates(&signature) {
            Some(candidates) => self.find_position_in(&candidates, signature, read).await,
            None => Ok(None),
        }
    }
    async fn find_position_in(
        &self,
        candidates: &SignatureCandidates,
        signature: Signature,
        read: &Read,
    ) -> ProcessingResult<Option<SignatureSlot>> {
        let base = self.query_client();
        let signature = signature.to_string();
        let mut budget = ProbeBudget(read.unknown_probe_limit);
        for &partition in &candidates.partitions {
            read.candidate(
                &mut budget,
                (!candidates.unknown_partitions.contains(&partition)).then_some(true),
            )?;
            crate::metrics::disk_cache_read("key_partition", "probed");
            let client = self.scoped_client(&base, partition, read);
            if let (Some(position), _) = client.get_signature_slot(&signature).await? {
                return Ok(self.covers_slot(position.slot).then_some(position));
            }
        }
        Ok(None)
    }
    #[cfg(test)]
    pub(crate) async fn signature_position(&self, signature: Signature) -> Option<SignatureSlot> {
        let read = self.read()?;
        self.attempt(
            "signature_position",
            &read,
            self.find_position(signature, &read),
        )
        .await
    }
    pub(crate) async fn signature_position_until(
        &self,
        signature: Signature,
        deadline: Instant,
    ) -> Option<SignatureSlot> {
        let read = self.read_until(deadline)?;
        self.attempt(
            "signature_position",
            &read,
            self.find_position(signature, &read),
        )
        .await
    }
    pub(crate) async fn get_tx(
        &self,
        signature: Signature,
        requested_slot: Option<u64>,
    ) -> DiskTransactionResult {
        let Some((read, gated)) = self.get_tx_read() else {
            crate::metrics::disk_cache_get_tx_reason(tx_reason::NOT_READY);
            return DiskTransactionResult::Unavailable;
        };
        let started = Instant::now();
        let result = tokio::time::timeout_at(
            read.deadline,
            self.read_transaction(signature, requested_slot, &read),
        )
        .await;
        let (value, outcome, reason) =
            transaction_outcome(result, self.read_change(&read), |slot| {
                self.covers_slot(slot)
            });
        let reason = gated_reason(reason, gated);
        let elapsed = started.elapsed().as_secs_f64();
        crate::metrics::disk_cache_read("get_tx", outcome);
        crate::metrics::disk_cache_key_seconds("get_tx", outcome, elapsed);
        crate::metrics::disk_cache_get_tx_reason(reason);
        value
    }

    async fn read_transaction(
        &self,
        signature: Signature,
        requested_slot: Option<u64>,
        read: &Read,
    ) -> ProcessingResult<(DiskTransactionResult, &'static str)> {
        // Appends need not invalidate the epoch. A slot published after the
        // signature search began cannot turn that earlier miss into proof.
        let covered_slot = requested_slot.filter(|slot| self.covers_slot(*slot));
        let candidates = self.position_candidates(&signature);
        if let Some(candidates) = &candidates {
            // DISK_CACHE_FUSED_GET_TX=false: the two-step lookup alone, as before the fused read.
            if !self.inner.cfg.fused_get_tx {
                // No span check either: the empty-span short-circuit rides on the fused read.
                return self
                    .two_step_transaction(candidates, signature, covered_slot, false, read)
                    .await;
            }
            let fused = self.fused_transaction(candidates, &signature, read).await?;
            // DISK_CACHE_GET_TX_SPAN_CHECK=false: always the per-partition probes.
            let span_check = self.inner.cfg.get_tx_span_check
                && span_check_first(&fused, candidates.partitions.len());
            if let FusedRead::Row(record) = fused {
                return Ok((
                    DiskTransactionResult::Found(Arc::from(record)),
                    tx_reason::FOUND,
                ));
            }
            let started = Instant::now();
            let result = self
                .two_step_transaction(candidates, signature, covered_slot, span_check, read)
                .await;
            let outcome = match &result {
                Ok((DiskTransactionResult::Found(_), _)) => "hit",
                Ok((DiskTransactionResult::Absent, _)) => "absent",
                Ok((DiskTransactionResult::Unavailable, _)) => "miss",
                Err(err) => error_outcome(err),
            };
            crate::metrics::disk_cache_key_seconds(
                "get_tx_fallback",
                outcome,
                started.elapsed().as_secs_f64(),
            );
            return result;
        }
        Ok(self.absent_at(covered_slot, tx_reason::BLOOM_ABSENT).await)
    }

    /// One local query for the position and its payload. Only `Row` is served and
    /// nothing here is absence: zero rows (a Bloom false positive, a stale position,
    /// a missing payload) and rows today's newest-first search would not return all
    /// go to the two-step path, which alone decides a miss.
    async fn fused_transaction(
        &self,
        candidates: &SignatureCandidates,
        signature: &Signature,
        read: &Read,
    ) -> ProcessingResult<FusedRead> {
        let started = Instant::now();
        let result = self.fused_read(candidates, signature, read).await;
        let outcome = match &result {
            Ok(FusedRead::Row(_)) => "hit",
            Ok(_) => "fallback",
            Err(err) => error_outcome(err),
        };
        crate::metrics::disk_cache_key_seconds(
            "get_tx_fused",
            outcome,
            started.elapsed().as_secs_f64(),
        );
        result
    }
    async fn fused_read(
        &self,
        candidates: &SignatureCandidates,
        signature: &Signature,
        read: &Read,
    ) -> ProcessingResult<FusedRead> {
        let (Some(&newest), Some(&oldest)) =
            (candidates.partitions.first(), candidates.partitions.last())
        else {
            return Ok(FusedRead::NotRun);
        };
        // Same admission checks as the per-partition loop; a probe budget it would
        // exhaust is left for that loop to raise.
        let mut budget = ProbeBudget(read.unknown_probe_limit);
        for partition in &candidates.partitions {
            match read.candidate(
                &mut budget,
                (!candidates.unknown_partitions.contains(partition)).then_some(true),
            ) {
                Ok(_) => {}
                Err(err) if is_probe_budget_exhausted(&err) => return Ok(FusedRead::NotRun),
                Err(err) => return Err(err),
            }
        }
        crate::metrics::disk_cache_read_count(
            "key_partition",
            "probed",
            candidates.partitions.len() as u64,
        );
        let mut client = self.scoped_client(&self.query_client(), newest, read);
        client.cache_slot_range = Some(self.partition_slot_span(newest, oldest));
        let (record, _) = client.get_transaction_fused(signature).await?;
        // The newest position may sit in a skipped partition inside the range, or
        // outside coverage, where the loop would instead stop or look elsewhere.
        let width = self.inner.cfg.partition_slots;
        Ok(match record {
            None => FusedRead::Empty,
            Some(record)
                if self.covers_slot(record.slot)
                    && candidates.partitions.contains(&(record.slot / width)) =>
            {
                FusedRead::Row(Box::new(record))
            }
            Some(_) => FusedRead::Rejected,
        })
    }

    async fn two_step_transaction(
        &self,
        candidates: &SignatureCandidates,
        signature: Signature,
        covered_slot: Option<u64>,
        span_check: bool,
        read: &Read,
    ) -> ProcessingResult<(DiskTransactionResult, &'static str)> {
        if span_check && !self.span_has_position(candidates, &signature, read).await? {
            // The probes below could only find nothing; decide exactly as they would.
            return Ok(self.absent_at(covered_slot, tx_reason::SPAN_EMPTY).await);
        }
        if let Some(position) = self.find_position_in(candidates, signature, read).await? {
            let client = self.scoped_client(
                &self.query_client(),
                position.slot / self.inner.cfg.partition_slots,
                read,
            );
            let (record, _) = client
                .get_transaction_by_signature_and_position(&signature.to_string(), position)
                .await?;
            // An index entry without its payload is inconsistent, not proof of absence.
            return Ok(
                match record.filter(|record| self.covers_slot(record.slot)) {
                    Some(record) => (
                        DiskTransactionResult::Found(Arc::new(record)),
                        tx_reason::FOUND,
                    ),
                    None => (
                        DiskTransactionResult::Unavailable,
                        tx_reason::INDEX_WITHOUT_PAYLOAD,
                    ),
                },
            );
        }
        Ok(self.absent_at(covered_slot, tx_reason::PROBE_EMPTY).await)
    }

    /// The fused read's inner position lookup on its own: the same table, bucket,
    /// signature and slot span. Each per-partition probe reads a subset of this
    /// span, so `false` means every probe would return no position.
    async fn span_has_position(
        &self,
        candidates: &SignatureCandidates,
        signature: &Signature,
        read: &Read,
    ) -> ProcessingResult<bool> {
        let (Some(&newest), Some(&oldest)) =
            (candidates.partitions.first(), candidates.partitions.last())
        else {
            // No span to ask: leave the verdict to the probes.
            return Ok(true);
        };
        read.check()?;
        let mut client = self.scoped_client(&self.query_client(), newest, read);
        client.cache_slot_range = Some(self.partition_slot_span(newest, oldest));
        let (position, _) = client.get_signature_slot(&signature.to_string()).await?;
        Ok(position.is_some())
    }

    /// Only a successful signature miss reaches here; a covered or skipped pinned slot
    /// then proves absence. Otherwise the miss is reported with `miss_reason`.
    async fn absent_at(
        &self,
        covered_slot: Option<u64>,
        miss_reason: &'static str,
    ) -> (DiskTransactionResult, &'static str) {
        if let Some(slot) = covered_slot
            && matches!(
                self.slot_status(slot).await,
                SlotStatus::Covered { .. } | SlotStatus::Skipped
            )
        {
            return (DiskTransactionResult::Absent, tx_reason::ABSENT);
        }
        (DiskTransactionResult::Unavailable, miss_reason)
    }
    #[cfg(test)]
    pub(crate) async fn get_sig_statuses(
        &self,
        signatures: Vec<Signature>,
    ) -> Vec<Option<DiskSigStatus>> {
        self.get_sig_statuses_detailed(signatures)
            .await
            .statuses
            .into_iter()
            .map(DiskStatusLookup::found)
            .collect()
    }
    /// Local statuses that also prove absence where the read allows it (see
    /// [`DiskStatusLookup::Absent`]). `DISK_CACHE_STATUS_SPAN_QUERY` picks the span query
    /// or the per-partition loop, as before.
    pub(crate) async fn get_sig_statuses_detailed(
        &self,
        signatures: Vec<Signature>,
    ) -> DiskStatusLookups {
        self.sig_statuses(signatures, self.inner.cfg.status_span_query)
            .await
    }
    /// The former per-partition queries only, as a reference for the single query.
    #[cfg(test)]
    pub(crate) async fn get_sig_statuses_per_partition(
        &self,
        signatures: Vec<Signature>,
    ) -> Vec<Option<DiskSigStatus>> {
        self.sig_statuses(signatures, false)
            .await
            .statuses
            .into_iter()
            .map(DiskStatusLookup::found)
            .collect()
    }
    async fn sig_statuses(
        &self,
        signatures: Vec<Signature>,
        single_query: bool,
    ) -> DiskStatusLookups {
        let Some(read) = self.read() else {
            return DiskStatusLookups {
                statuses: vec![DiskStatusLookup::Unknown; signatures.len()],
                span: None,
            };
        };
        // One coverage sample bounds both the query and what its misses prove.
        let (span, contiguous) = {
            let coverage = self.inner.coverage.read().expect("coverage lock");
            let span = coverage.covered_span();
            (
                span,
                span.is_some() && span == coverage.contiguous_tip_span(),
            )
        };
        let mut found = HashMap::new();
        let mut encoded = HashMap::new();
        let mut completed = false;
        let result = self
            .attempt("signature_statuses", &read, async {
                self.find_statuses(
                    span,
                    &signatures,
                    &read,
                    &mut found,
                    &mut encoded,
                    single_query,
                )
                .await?;
                completed = true;
                Ok(found.values().any(Option::is_some).then_some(()))
            })
            .await;
        let valid = self.valid_read(&read);
        // Retain independently validated results after a timeout, but never after invalidation.
        if result.is_none() && !valid {
            found.clear();
        }
        let span = (completed && valid && contiguous)
            .then_some(span)
            .flatten()
            .map(|(floor, tip)| (floor.max(self.min_retained_slot()), tip));
        let statuses = signatures
            .iter()
            .map(|signature| {
                let key = encoded.get(signature);
                classify_status(key.and_then(|key| found.get(key)), span.is_some())
            })
            .collect();
        DiskStatusLookups { statuses, span }
    }
    fn status_candidates(
        &self,
        floor: u64,
        tip: u64,
        signatures: &[Signature],
        encoded: &mut HashMap<Signature, String>,
    ) -> BTreeMap<u64, Vec<String>> {
        let mut by_partition = BTreeMap::<u64, Vec<String>>::new();
        for signature in signatures {
            let candidates = self.signature_candidates(floor, tip, signature);
            if candidates.partitions.is_empty() {
                continue;
            }
            let key = encoded
                .entry(*signature)
                .or_insert_with(|| signature.to_string());
            for partition in candidates.partitions {
                by_partition.entry(partition).or_default().push(key.clone());
            }
        }
        by_partition
    }

    async fn find_statuses(
        &self,
        span: Option<(u64, u64)>,
        signatures: &[Signature],
        read: &Read,
        found: &mut HashMap<String, Option<DiskSigStatus>>,
        encoded: &mut HashMap<Signature, String>,
        single_query: bool,
    ) -> ProcessingResult<()> {
        let Some((floor, tip)) = span else {
            return Ok(());
        };
        let by_partition = self.status_candidates(floor, tip, signatures, encoded);
        // One query over the candidates' slot span: one admission and one round trip
        // instead of one per partition (an unknown partition is a candidate for every
        // signature). Partitions skipped inside the span are definite index negatives,
        // and argMax over the span selects the newest row, as the newest-first loop did.
        if single_query && let Some((oldest, newest, pending)) = status_span(&by_partition) {
            read.check()?;
            crate::metrics::disk_cache_read_count(
                "key_partition",
                "probed",
                by_partition.values().map(|p| p.len() as u64).sum(),
            );
            let mut client = self.scoped_client(&self.query_client(), newest, read);
            client.cache_slot_range = Some(self.partition_slot_span(newest, oldest));
            let (records, _) = client.get_signature_statuses(&pending).await?;
            self.record_statuses(records, found);
            return Ok(());
        }
        let mut base = None;
        for (partition, mut pending) in by_partition.into_iter().rev() {
            read.check()?;
            pending.retain(|signature| !found.contains_key(signature));
            if pending.is_empty() {
                continue;
            }
            crate::metrics::disk_cache_read_count("key_partition", "probed", pending.len() as u64);
            let client = self.scoped_client(
                base.get_or_insert_with(|| self.query_client()),
                partition,
                read,
            );
            let (records, _) = client.get_signature_statuses(&pending).await?;
            self.record_statuses(records, found);
        }
        Ok(())
    }
    fn record_statuses(
        &self,
        records: Vec<SignatureStatusRecord>,
        found: &mut HashMap<String, Option<DiskSigStatus>>,
    ) {
        for record in records {
            found.insert(
                record.signature,
                self.covers_slot(record.slot).then_some(DiskSigStatus {
                    slot: record.slot,
                    err: record.err.and_then(|e| serde_json::to_string(&e).ok()),
                }),
            );
        }
    }
    fn address_families(&self, address: &Pubkey, tokens: TokenAccountsFilter) -> Vec<Family> {
        if self.query_client().is_gsfa_hot_address(address) {
            return vec![Family::HotAddress];
        }
        if tokens == TokenAccountsFilter::None {
            vec![Family::Address]
        } else {
            vec![Family::Address, Family::Owner]
        }
    }
    #[cfg(test)]
    pub(crate) async fn signatures_for_address(
        &self,
        address: Pubkey,
        before: Option<SlotBoundary>,
        until: Option<SlotBoundary>,
        limit: usize,
    ) -> Option<DiskGsfaPage> {
        self.signatures_for_address_until(
            address,
            before,
            until,
            limit,
            self.address_request_deadline(),
        )
        .await
    }
    pub(crate) async fn signatures_for_address_until(
        &self,
        address: Pubkey,
        before: Option<SlotBoundary>,
        until: Option<SlotBoundary>,
        limit: usize,
        deadline: Instant,
    ) -> Option<DiskGsfaPage> {
        let read = self.read_until(deadline)?;
        let (floor, tip) = self.tip_span()?;
        let (until, floor_effective) = clamp_until_to_floor(until, floor);
        let base = self.query_client_for_address(&address, TokenAccountsFilter::None)?;
        let families = self.address_families(&address, TokenAccountsFilter::None);
        self.attempt("signatures_for_address", &read, async {
            let window = gsfa_window((floor, tip), before, until);
            let (partitions, exhausted) = if limit == 0 {
                (Vec::new(), None)
            } else {
                self.address_candidates(window, &families, address.as_ref(), &read)?
            };
            crate::metrics::disk_cache_address_partitions(
                "signatures_for_address",
                partitions.len(),
            );
            // One query over every candidate: one admission and one round trip, so a
            // short (partial) page is denied as quickly as a full page is served. It
            // spans one slot range from the oldest to the newest candidate: partitions
            // inside it that were skipped are definite index negatives with no rows for
            // this address, and a range keeps primary-key analysis on binary search.
            // The query orders by slot, so this equals the former newest-first loop.
            let records = match (partitions.first(), partitions.last()) {
                (None, _) | (_, None) => Vec::new(),
                (Some(&newest), Some(&oldest)) => {
                    let mut client = self.scoped_client(&base, newest, &read);
                    client.cache_slot_range = Some(self.partition_slot_span(newest, oldest));
                    client
                        .get_signatures_for_address_with_positions(
                            &address.to_string(),
                            limit as u64,
                            before,
                            until,
                        )
                        .await?
                        .0
                }
            };
            if records.len() < limit
                && let Some(err) = exhausted
            {
                return Err(err);
            }
            let reached_floor = records.len() < limit && floor_effective;
            Ok(self.address_page(records, floor, tip, reached_floor, false))
        })
        .await
    }
    #[cfg(test)]
    pub(crate) async fn transactions_for_address(
        &self,
        address: Pubkey,
        query: DiskTfaQuery,
    ) -> Option<DiskGsfaPage> {
        self.transactions_for_address_until(address, query, self.address_request_deadline())
            .await
    }
    pub(crate) async fn transactions_for_address_until(
        &self,
        address: Pubkey,
        query: DiskTfaQuery,
        deadline: Instant,
    ) -> Option<DiskGsfaPage> {
        let read = self.read_until(deadline)?;
        let (floor, tip) = self.tip_span()?;
        let base = self.query_client_for_address(&address, query.token_accounts)?;
        self.attempt("transactions_for_address", &read, async {
            let records = self
                .address_transactions(&base, address, &query, (floor, tip), &read)
                .await?;
            let exhausted = records.len() < query.limit;
            let reached_floor = exhausted
                && query.sort_order == SortOrder::Desc
                && lower_bound_reaches_floor(&query, floor);
            let reached_tip = exhausted
                && query.sort_order == SortOrder::Asc
                && upper_bound_reaches_tip(&query, tip);
            Ok(self.address_page(records, floor, tip, reached_floor, reached_tip))
        })
        .await
    }
    async fn address_transactions(
        &self,
        base: &ClickHouseClient,
        address: Pubkey,
        query: &DiskTfaQuery,
        (floor, tip): (u64, u64),
        read: &Read,
    ) -> ProcessingResult<Vec<SignatureRecord>> {
        let mut slot_filter = query.slot_filter.clone().unwrap_or_default();
        slot_filter.gte = Some(slot_filter.gte.map_or(floor, |v| v.max(floor)));
        slot_filter.lte = Some(slot_filter.lte.map_or(tip, |v| v.min(tip)));
        let mut q = TransactionsForAddressQuery {
            address: address.to_string(),
            limit: query.limit as u64,
            sort_order: query.sort_order,
            pagination: query.pagination.map(|p| PaginationToken::SlotIndex {
                slot: p.slot,
                idx: p.slot_idx,
            }),
            resolved_pagination: query.pagination,
            slot_filter: Some(slot_filter),
            block_time_filter: query.block_time_filter.clone(),
            signature_filter: None,
            resolved_signature_filter: query.signature_filter.clone(),
            status: query.status,
            token_accounts: query.token_accounts,
        };
        let families = self.address_families(&address, query.token_accounts);
        let mut rows = AddressRows::default();
        let mut budget = ProbeBudget(read.unknown_probe_limit);
        let (scan_floor, scan_tip) = tfa_window((floor, tip), &q);
        for partition in self.partitions(scan_floor, scan_tip, query.sort_order) {
            if rows.records.len() >= query.limit {
                break;
            }
            if !self.candidate(partition, &families, address.as_ref(), &mut budget, read)? {
                continue;
            }
            let client = self.scoped_client(base, partition, read);
            self.address_partition(&client, &mut q, &mut rows, query.limit, read)
                .await?;
        }
        Ok(rows.records)
    }
    async fn address_partition(
        &self,
        client: &ClickHouseClient,
        query: &mut TransactionsForAddressQuery,
        rows: &mut AddressRows,
        limit: usize,
        read: &Read,
    ) -> ProcessingResult<()> {
        while rows.records.len() < limit {
            read.check()?;
            query.limit = (limit - rows.records.len()) as u64;
            let mut client = client.clone();
            apply_budget(&mut client, read);
            let (page, _) = client
                .get_transactions_for_address_signatures(query)
                .await?;
            let exhausted = page.len() < query.limit as usize;
            let last = page.last().map(|r| SignatureSlot {
                slot: r.slot,
                slot_idx: r.slot_idx,
            });
            rows.append(page);
            if exhausted {
                break;
            }
            if let Some(last) = last {
                query.pagination = Some(PaginationToken::SlotIndex {
                    slot: last.slot,
                    idx: last.slot_idx,
                });
                query.resolved_pagination = Some(last);
            }
        }
        Ok(())
    }
    fn address_page(
        &self,
        records: Vec<SignatureRecord>,
        floor: u64,
        tip: u64,
        reached_floor: bool,
        reached_tip: bool,
    ) -> Option<DiskGsfaPage> {
        let (current_floor, current_tip) = self.tip_span()?;
        if current_floor > floor
            || current_tip < tip
            || records.iter().any(|r| !self.covers_slot(r.slot))
        {
            return None;
        }
        Some(DiskGsfaPage {
            records,
            floor,
            tip,
            reached_floor,
            reached_tip,
        })
    }
}

#[cfg(test)]
mod window_tests {
    use super::*;
    use crate::clickhouse::{ResolvedSignatureFilter, TransactionStatusFilter};

    #[test]
    fn unknown_edges_are_probed_but_an_unfinished_page_requires_fallback() {
        let mut budget = ProbeBudget(2);
        assert!(budget.candidate(None).unwrap()); // active tip
        assert!(!budget.candidate(Some(false)).unwrap());
        assert!(budget.candidate(Some(true)).unwrap());
        assert!(budget.candidate(None).unwrap()); // partial floor
        assert!(budget.candidate(None).is_err());
        // Known membership remains usable; exhaustion is not false absence.
        assert!(budget.candidate(Some(true)).unwrap());
    }

    #[test]
    fn expired_request_does_not_start_another_cursor_or_page_probe() {
        let read = Read {
            deadline: Instant::now(),
            epoch: 0,
            data_epoch: 0,
            unknown_probe_limit: 2,
            address_request: true,
        };
        let mut budget = ProbeBudget(read.unknown_probe_limit);
        assert!(read.candidate(&mut budget, Some(true)).is_err());
        assert!(read.candidate(&mut budget, None).is_err());
        assert_eq!(budget.0, 2);
    }

    #[test]
    fn failed_attempt_outcomes_separate_busy_and_probe_budget_from_timeouts() {
        let exhausted = ProbeBudget(0).candidate(None).unwrap_err();
        assert!(is_probe_budget_exhausted(&exhausted));
        assert_eq!(error_outcome(&exhausted), "probe_budget");
        let busy = ProcessingError::Timeout {
            context: "busy".into(),
            source: Some(Box::new(CacheAdmissionBusy)),
        };
        assert!(!is_probe_budget_exhausted(&busy));
        assert_eq!(error_outcome(&busy), "busy");
        let deadline = ProcessingError::timeout_msg("disk cache deadline exceeded");
        assert!(!is_probe_budget_exhausted(&deadline));
        assert_eq!(error_outcome(&deadline), "timeout");
        assert_eq!(
            error_outcome(&ProcessingError::database_msg(
                "Code: 159. TIMEOUT_EXCEEDED"
            )),
            "timeout"
        );
        assert_eq!(
            error_outcome(&ProcessingError::database_msg("boom")),
            "error"
        );
        let cursor_deadline = clickhouse::error::Error::TimedOut;
        assert_eq!(
            error_outcome(&ProcessingError::database(
                cursor_deadline.to_string(),
                cursor_deadline
            )),
            "timeout"
        );
        let other = clickhouse::error::Error::RowNotFound;
        assert_eq!(
            error_outcome(&ProcessingError::database(other.to_string(), other)),
            "error"
        );
    }

    fn found(slot: u64) -> DiskTransactionResult {
        let mut record = crate::tests::base_transaction_record();
        record.slot = slot;
        DiskTransactionResult::Found(Arc::new(record))
    }

    fn covered(_: u64) -> bool {
        true
    }

    #[test]
    fn invalidated_absence_is_unavailable() {
        for change in [ReadChange::Negatives, ReadChange::Data] {
            for (value, reason) in [
                (DiskTransactionResult::Absent, tx_reason::ABSENT),
                (DiskTransactionResult::Unavailable, tx_reason::PROBE_EMPTY),
                (DiskTransactionResult::Unavailable, tx_reason::BLOOM_ABSENT),
            ] {
                let (value, outcome, reason) =
                    transaction_outcome(Ok(Ok((value, reason))), change, covered);
                assert!(matches!(value, DiskTransactionResult::Unavailable));
                assert_eq!((outcome, reason), ("miss", tx_reason::INVALIDATED));
            }
        }
        let (value, outcome, reason) = transaction_outcome(
            Ok(Ok((DiskTransactionResult::Absent, tx_reason::ABSENT))),
            ReadChange::None,
            covered,
        );
        assert!(matches!(value, DiskTransactionResult::Absent));
        assert_eq!((outcome, reason), ("miss", tx_reason::ABSENT));
    }

    #[test]
    fn found_survives_a_floor_move_only_while_its_slot_is_covered() {
        // Eviction or routing-membership change only: a covered row is served.
        let (value, outcome, reason) = transaction_outcome(
            Ok(Ok((found(45), tx_reason::FOUND))),
            ReadChange::Negatives,
            |slot| slot == 45,
        );
        assert!(matches!(value, DiskTransactionResult::Found(ref r) if r.slot == 45));
        assert_eq!((outcome, reason), ("hit", tx_reason::FOUND_REVALIDATED));
        // Evicted (or poisoned) by completion: covers_slot is false, so fall back.
        let (value, outcome, reason) = transaction_outcome(
            Ok(Ok((found(45), tx_reason::FOUND))),
            ReadChange::Negatives,
            |_| false,
        );
        assert!(matches!(value, DiskTransactionResult::Unavailable));
        assert_eq!((outcome, reason), ("miss", tx_reason::INVALIDATED));
        // Poison, repair, schema rebuild or not ready: never served, even if covered.
        let (value, outcome, reason) = transaction_outcome(
            Ok(Ok((found(45), tx_reason::FOUND))),
            ReadChange::Data,
            covered,
        );
        assert!(matches!(value, DiskTransactionResult::Unavailable));
        assert_eq!((outcome, reason), ("miss", tx_reason::INVALIDATED));
        // Unchanged reads keep today's behaviour.
        let (value, outcome, reason) = transaction_outcome(
            Ok(Ok((found(45), tx_reason::FOUND))),
            ReadChange::None,
            |_| unreachable!("an unchanged read is not re-checked"),
        );
        assert!(matches!(value, DiskTransactionResult::Found(_)));
        assert_eq!((outcome, reason), ("hit", tx_reason::FOUND));
    }

    #[test]
    fn transaction_errors_never_prove_absence() {
        type MakeError = fn() -> ProcessingError;
        let errors: [(MakeError, &str); 3] = [
            (|| ProcessingError::timeout_msg("admission"), "timeout"),
            (
                || ProcessingError::database_msg("TIMEOUT_EXCEEDED"),
                "timeout",
            ),
            (
                || ProcessingError::database_msg("unavailable table"),
                "error",
            ),
        ];
        for (error, expected) in errors {
            for change in [ReadChange::None, ReadChange::Negatives, ReadChange::Data] {
                let (value, outcome, reason) =
                    transaction_outcome(Ok(Err(error())), change, covered);
                assert!(matches!(value, DiskTransactionResult::Unavailable));
                assert_eq!((outcome, reason), (expected, expected));
            }
        }
    }

    #[tokio::test]
    async fn elapsed_budget_is_a_timeout_not_absence() {
        let elapsed = tokio::time::timeout(
            std::time::Duration::ZERO,
            std::future::pending::<ProcessingResult<(DiskTransactionResult, &'static str)>>(),
        )
        .await;
        let (value, outcome, reason) = transaction_outcome(elapsed, ReadChange::None, covered);
        assert!(matches!(value, DiskTransactionResult::Unavailable));
        assert_eq!((outcome, reason), ("timeout", tx_reason::TIMEOUT));
    }

    #[test]
    fn only_an_empty_multi_partition_fused_read_checks_its_span_first() {
        let row = || FusedRead::Row(Box::new(crate::tests::base_transaction_record()));
        for (fused, partitions, expected) in [
            // No position in the span proves every probe empty; one lookup replaces N.
            (FusedRead::Empty, 2, true),
            (FusedRead::Empty, 77, true),
            // One candidate: the span lookup is the only probe, so it is not repeated.
            (FusedRead::Empty, 1, false),
            // A returned row, even one the filter rejected, proves a position exists.
            (FusedRead::Rejected, 4, false),
            (row(), 4, false),
            // The fused query did not run, so it proved nothing.
            (FusedRead::NotRun, 4, false),
        ] {
            assert_eq!(
                span_check_first(&fused, partitions),
                expected,
                "{fused:?} with {partitions} candidates"
            );
        }
    }

    #[test]
    fn span_contains_every_candidate_partition_probe() {
        // A per-partition probe reads `intDiv(slot, width) = p`; the span check is
        // only equivalent if each candidate's whole slot range lies inside the span.
        for width in [1u64, 10, 43_200] {
            for (newest, oldest) in [(0u64, 0u64), (4, 1), (87, 0), (1_000_000, 3)] {
                let (low, high) = partition_slot_span(width, newest, oldest);
                for p in [oldest, oldest + (newest - oldest) / 2, newest] {
                    let first = p * width;
                    let last = first + (width - 1);
                    assert!(
                        low <= first && last <= high,
                        "{width} {p} in {low}..={high}"
                    );
                }
            }
        }
    }

    #[test]
    fn get_tx_budget_is_capped_by_the_shared_read_timeout() {
        use std::time::Duration;
        let mut cfg = super::super::key_tests::config(String::new(), String::new());
        cfg.query_timeout = Duration::from_millis(2_000);
        cfg.get_tx_timeout = Duration::from_millis(1_000);
        assert_eq!(get_tx_budget(&cfg), Duration::from_millis(1_000));
        cfg.get_tx_timeout = Duration::from_millis(5_000);
        assert_eq!(get_tx_budget(&cfg), Duration::from_millis(2_000));
    }

    #[test]
    fn many_unknown_partitions_select_the_shorter_capped_budget() {
        use std::time::Duration;
        let mut cfg = super::super::key_tests::config(String::new(), String::new());
        cfg.query_timeout = Duration::from_millis(2_000);
        cfg.get_tx_timeout = Duration::from_millis(1_000);
        cfg.get_tx_unknown_timeout = Duration::from_millis(150);
        let normal = (Duration::from_millis(1_000), false);
        let gated = (Duration::from_millis(150), true);
        // Known membership and the one or two partitions a repair leaves unknown keep
        // the normal budget; a rebuilding index gets the short one.
        for unknown in 0..=GET_TX_UNKNOWN_PARTITION_LIMIT {
            assert_eq!(get_tx_budget_for(&cfg, unknown), normal, "{unknown}");
        }
        for unknown in [GET_TX_UNKNOWN_PARTITION_LIMIT + 1, 87, 128, u64::MAX] {
            assert_eq!(get_tx_budget_for(&cfg, unknown), gated, "{unknown}");
        }
        // Capped by the getTransaction budget, and through it by the shared timeout.
        cfg.get_tx_unknown_timeout = Duration::from_millis(5_000);
        assert_eq!(
            get_tx_budget_for(&cfg, 128),
            (Duration::from_millis(1_000), true)
        );
        cfg.get_tx_timeout = Duration::from_millis(5_000);
        cfg.query_timeout = Duration::from_millis(100);
        assert_eq!(
            get_tx_budget_for(&cfg, 128),
            (Duration::from_millis(100), true)
        );
        assert_eq!(
            get_tx_budget_for(&cfg, 0),
            (Duration::from_millis(100), false)
        );
    }

    #[tokio::test]
    async fn expired_unknown_budget_is_unavailable_not_absence() {
        let elapsed = tokio::time::timeout(
            std::time::Duration::ZERO,
            std::future::pending::<ProcessingResult<(DiskTransactionResult, &'static str)>>(),
        )
        .await;
        let (value, outcome, reason) = transaction_outcome(elapsed, ReadChange::None, covered);
        assert!(matches!(value, DiskTransactionResult::Unavailable));
        assert_eq!(outcome, "timeout");
        assert_eq!(gated_reason(reason, true), tx_reason::UNKNOWN_TIMEOUT);
        assert_eq!(gated_reason(reason, false), tx_reason::TIMEOUT);
        // Only timeouts are relabelled; a gated read that completes keeps its reason.
        for other in [
            tx_reason::FOUND,
            tx_reason::ABSENT,
            tx_reason::PROBE_EMPTY,
            tx_reason::INVALIDATED,
            tx_reason::ERROR,
        ] {
            assert_eq!(gated_reason(other, true), other);
        }
    }

    #[test]
    fn gsfa_cursors_skip_newer_partitions_and_preserve_boundary_slots() {
        let p = SlotBoundary::Position(SignatureSlot {
            slot: 25,
            slot_idx: 3,
        });
        assert_eq!(gsfa_window((10, 99), Some(p), Some(p)), (25, 25));
        assert_eq!(
            gsfa_window((10, 99), Some(SlotBoundary::Slot(25)), None),
            (10, 24)
        );
        assert_eq!(
            gsfa_window((10, 99), None, Some(SlotBoundary::Slot(25))),
            (26, 99)
        );
    }

    #[test]
    fn numeric_bounds_intersect_including_contradictions() {
        let mut f = NumericFilter {
            eq: Some(25),
            ..Default::default()
        };
        assert_eq!(numeric_window((10, 99), &f), (25, 25));
        f.gt = Some(25);
        assert_eq!(numeric_window((10, 99), &f), (26, 25));
        f.eq = None;
        f.lt = Some(30);
        assert_eq!(numeric_window((10, 99), &f), (26, 29));
    }

    #[test]
    fn tfa_cursor_direction_and_signature_bounds_intersect() {
        let position = SignatureSlot {
            slot: 25,
            slot_idx: 3,
        };
        let mut q = TransactionsForAddressQuery {
            address: String::new(),
            limit: 100,
            sort_order: SortOrder::Desc,
            pagination: None,
            resolved_pagination: Some(position),
            slot_filter: None,
            block_time_filter: None,
            signature_filter: None,
            resolved_signature_filter: None,
            status: TransactionStatusFilter::Any,
            token_accounts: TokenAccountsFilter::None,
        };
        assert_eq!(tfa_window((10, 99), &q), (10, 25));
        q.sort_order = SortOrder::Asc;
        assert_eq!(tfa_window((10, 99), &q), (25, 99));
        q.resolved_signature_filter = Some(ResolvedSignatureFilter {
            lt: Some(position),
            ..Default::default()
        });
        assert_eq!(tfa_window((10, 99), &q), (25, 25));
    }

    #[test]
    fn tip_gap_is_the_window_above_the_local_tip() {
        let at = |slot| Some(SlotBoundary::Position(SignatureSlot { slot, slot_idx: 1 }));
        // First page and `until` at or above the tip owe every newer row.
        assert_eq!(gsfa_tip_gap(100, None, None), Some(101));
        assert_eq!(gsfa_tip_gap(100, None, at(100)), Some(101));
        assert_eq!(gsfa_tip_gap(100, None, at(103)), Some(103));
        assert_eq!(
            gsfa_tip_gap(100, None, Some(SlotBoundary::Slot(103))),
            Some(104)
        );
        // A cursor at or below the tip owes nothing above it.
        assert_eq!(gsfa_tip_gap(100, at(100), None), None);
        assert_eq!(gsfa_tip_gap(100, Some(SlotBoundary::Slot(101)), None), None);
        assert_eq!(
            gsfa_tip_gap(100, Some(SlotBoundary::Slot(102)), None),
            Some(101)
        );
        assert_eq!(gsfa_tip_gap(100, at(105), at(103)), Some(103));
        assert_eq!(gsfa_tip_gap(100, at(102), at(103)), None);

        let mut q = TransactionsForAddressQuery {
            address: String::new(),
            limit: 10,
            sort_order: SortOrder::Desc,
            pagination: None,
            resolved_pagination: None,
            slot_filter: None,
            block_time_filter: None,
            signature_filter: None,
            resolved_signature_filter: None,
            status: TransactionStatusFilter::Any,
            token_accounts: TokenAccountsFilter::None,
        };
        assert_eq!(tfa_tip_gap(100, &q), Some(101));
        q.slot_filter = Some(NumericFilter {
            lte: Some(100),
            ..Default::default()
        });
        assert_eq!(tfa_tip_gap(100, &q), None);
        q.slot_filter = Some(NumericFilter {
            gt: Some(102),
            ..Default::default()
        });
        assert_eq!(tfa_tip_gap(100, &q), Some(103));
        q.slot_filter = None;
        q.resolved_pagination = Some(SignatureSlot {
            slot: 100,
            slot_idx: 0,
        });
        assert_eq!(tfa_tip_gap(100, &q), None);
        q.resolved_pagination = Some(SignatureSlot {
            slot: 104,
            slot_idx: 0,
        });
        assert_eq!(tfa_tip_gap(100, &q), Some(101));
        q.sort_order = SortOrder::Asc;
        assert_eq!(tfa_tip_gap(100, &q), None);
    }

    #[test]
    fn tip_gap_needs_a_head_floor_at_or_below_its_start() {
        assert!(tip_gap_covered("test", None, || panic!(
            "no gap, no head read"
        )));
        assert!(tip_gap_covered("test", Some(101), || Some(101)));
        assert!(tip_gap_covered("test", Some(101), || Some(40)));
        assert!(!tip_gap_covered("test", Some(101), || Some(102)));
        assert!(!tip_gap_covered("test", Some(101), || None));
    }

    #[test]
    fn status_classification_requires_a_proven_span() {
        let status = DiskSigStatus { slot: 7, err: None };
        for provable in [false, true] {
            // A row inside coverage is found whatever else the read proved.
            assert_eq!(
                classify_status(Some(&Some(status.clone())), provable),
                DiskStatusLookup::Found(status.clone())
            );
            // A row outside coverage shows the signature exists: never absent.
            assert_eq!(
                classify_status(Some(&None), provable),
                DiskStatusLookup::Unknown
            );
        }
        // Never matched (Bloom negative or query miss): absent only when the read
        // completed, stayed valid and searched one contiguous covered span.
        assert_eq!(classify_status(None, true), DiskStatusLookup::Absent);
        assert_eq!(classify_status(None, false), DiskStatusLookup::Unknown);
        assert_eq!(
            DiskStatusLookup::Found(status.clone()).found(),
            Some(status)
        );
        assert_eq!(DiskStatusLookup::Absent.found(), None);
        assert_eq!(DiskStatusLookup::Unknown.found(), None);
    }

    #[test]
    fn status_span_bounds_lookups_per_query() {
        let sigs = |n: usize| (0..n).map(|i| format!("s{i}")).collect::<Vec<_>>();
        let known = |spread: &[(u64, usize)]| {
            let mut map = BTreeMap::<u64, Vec<String>>::new();
            let mut next = 0;
            for &(partition, count) in spread {
                map.entry(partition)
                    .or_default()
                    .extend((next..next + count).map(|i| format!("s{i}")));
                next += count;
            }
            map
        };
        assert!(status_span(&BTreeMap::new()).is_none());
        // One signature over every partition, and a duplicate signature queried once.
        let mut map: BTreeMap<u64, Vec<String>> = (0..90).map(|p| (p, sigs(1))).collect();
        map.get_mut(&89).unwrap().push("s0".into());
        assert_eq!(status_span(&map), Some((0, 89, sigs(1))));
        // Sparse, known partitions: within the absolute lookup cap, then beyond it.
        let small = known(&[(10, 4), (41, 4)]);
        assert_eq!(status_span(&small).unwrap().2.len(), 8);
        assert!(status_span(&known(&[(10, 128), (14, 128)])).is_none());
        assert!(status_span(&known(&[(10, 128), (11, 128)])).is_some());
        // Unknown partitions list every signature: 256 signatures over 23 unknown of
        // 88 partitions stay one query, 11 unknown fall back.
        let unknown = |count: u64| -> BTreeMap<u64, Vec<String>> {
            (0..88)
                .filter(|p| p % (88 / count).max(1) == 0)
                .take(count as usize)
                .chain([87])
                .map(|p| (p, sigs(256)))
                .collect()
        };
        assert!(status_span(&unknown(58)).is_some());
        assert!(status_span(&unknown(23)).is_some());
        assert!(status_span(&unknown(11)).is_none());
    }
}
