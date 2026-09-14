// SPDX-License-Identifier: AGPL-3.0-only
//! Ordered, partition-scoped reads under one cache-attempt deadline.
use super::{
    DiskCache, DiskGsfaPage, DiskSigStatus, clamp_until_to_floor,
    index::DiskTfaQuery,
    key_index::{Family, SignatureCandidates, SignatureHash},
    lower_bound_reaches_floor, upper_bound_reaches_tip,
};
use crate::clickhouse::{
    ClickHouseClient, NumericFilter, PaginationToken, SignatureRecord, SignatureSlot, SlotBoundary,
    SortOrder, StoredTransactionRecord, TokenAccountsFilter, TransactionsForAddressQuery,
};
use crate::processing::{ProcessingError, ProcessingResult};
use crate::solana_sdk::{pubkey::Pubkey, signature::Signature};
use std::collections::{HashMap, HashSet};
use std::future::Future;
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

struct Read {
    deadline: Instant,
    epoch: u64,
}
impl Read {
    fn check(&self) -> ProcessingResult<()> {
        if Instant::now() >= self.deadline {
            return Err(ProcessingError::timeout_msg("disk cache deadline exceeded"));
        }
        Ok(())
    }
}
impl DiskCache {
    fn read(&self) -> Option<Read> {
        self.ready().then(|| Read {
            deadline: Instant::now() + self.inner.cfg.query_timeout,
            epoch: self.inner.key_index.epoch(),
        })
    }
    fn valid_read(&self, read: &Read) -> bool {
        self.ready() && read.epoch == self.inner.key_index.epoch()
    }
    fn scoped_client(
        &self,
        base: &ClickHouseClient,
        partition: u64,
        read: &Read,
    ) -> ClickHouseClient {
        let mut client = base.clone();
        client.cache_partition = Some((self.inner.cfg.partition_slots, partition));
        client.query_timeout = read.deadline.saturating_duration_since(Instant::now());
        client
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
    fn candidate(&self, partition: u64, families: &[Family], key: &[u8]) -> bool {
        let candidate = self.inner.key_index.may_contain(partition, families, key);
        crate::metrics::disk_cache_read(
            "key_partition",
            if candidate { "probed" } else { "skipped" },
        );
        candidate
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
            Ok(Err(ProcessingError::Timeout { .. })) => (None, "timeout"),
            Ok(Err(ProcessingError::Database { context, .. }))
                if context.contains("TIMEOUT_EXCEEDED") =>
            {
                (None, "timeout")
            }
            Ok(Err(_)) => (None, "error"),
            Err(_) => (None, "timeout"),
        };
        crate::metrics::disk_cache_read(operation, outcome);
        crate::metrics::disk_cache_key_seconds(operation, outcome, started.elapsed().as_secs_f64());
        value
    }
    async fn find_position(
        &self,
        signature: Signature,
        read: &Read,
    ) -> ProcessingResult<Option<SignatureSlot>> {
        let Some((floor, tip)) = self.key_span() else {
            return Ok(None);
        };
        let candidates = self.signature_candidates(floor, tip, &signature);
        if candidates.partitions.is_empty() {
            return Ok(None);
        }
        let base = self.query_client();
        let signature = signature.to_string();
        for partition in candidates.partitions {
            read.check()?;
            crate::metrics::disk_cache_read("key_partition", "probed");
            let client = self.scoped_client(&base, partition, read);
            if let (Some(position), _) = client.get_signature_slot(&signature).await? {
                return Ok(self.covers_slot(position.slot).then_some(position));
            }
        }
        Ok(None)
    }
    pub(crate) async fn signature_position(&self, signature: Signature) -> Option<SignatureSlot> {
        let read = self.read()?;
        self.attempt(
            "signature_position",
            &read,
            self.find_position(signature, &read),
        )
        .await
    }
    pub(crate) async fn get_tx(&self, signature: Signature) -> Option<StoredTransactionRecord> {
        let read = self.read()?;
        self.attempt("get_tx", &read, async {
            let Some(position) = self.find_position(signature, &read).await? else {
                return Ok(None);
            };
            let client = self.scoped_client(
                &self.query_client(),
                position.slot / self.inner.cfg.partition_slots,
                &read,
            );
            let (record, _) = client
                .get_transaction_by_signature_and_slot(&signature.to_string(), position.slot)
                .await?;
            Ok(record.filter(|record| self.covers_slot(record.slot)))
        })
        .await
    }
    pub(crate) async fn get_sig_statuses(
        &self,
        signatures: Vec<Signature>,
    ) -> Vec<Option<DiskSigStatus>> {
        let Some(read) = self.read() else {
            return vec![None; signatures.len()];
        };
        let mut found = HashMap::new();
        let mut encoded = HashMap::new();
        let result = self
            .attempt("signature_statuses", &read, async {
                self.find_statuses(&signatures, &read, &mut found, &mut encoded)
                    .await?;
                Ok(found.values().any(Option::is_some).then_some(()))
            })
            .await;
        // Retain independently validated results after a timeout, but never after invalidation.
        if result.is_none() && !self.valid_read(&read) {
            found.clear();
        }
        signatures
            .iter()
            .map(|signature| {
                encoded
                    .get(signature)
                    .and_then(|key| found.get(key))
                    .cloned()
                    .flatten()
            })
            .collect()
    }
    fn status_candidates(
        &self,
        floor: u64,
        tip: u64,
        signatures: &[Signature],
        encoded: &mut HashMap<Signature, String>,
    ) -> std::collections::BTreeMap<u64, Vec<String>> {
        let mut by_partition = std::collections::BTreeMap::<u64, Vec<String>>::new();
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
        signatures: &[Signature],
        read: &Read,
        found: &mut HashMap<String, Option<DiskSigStatus>>,
        encoded: &mut HashMap<Signature, String>,
    ) -> ProcessingResult<()> {
        let Some((floor, tip)) = self.key_span() else {
            return Ok(());
        };
        let by_partition = self.status_candidates(floor, tip, signatures, encoded);
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
        Ok(())
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
    pub(crate) async fn signatures_for_address(
        &self,
        address: Pubkey,
        before: Option<SlotBoundary>,
        until: Option<SlotBoundary>,
        limit: usize,
    ) -> Option<DiskGsfaPage> {
        let read = self.read()?;
        let (floor, tip) = self.tip_span()?;
        let (until, floor_effective) = clamp_until_to_floor(until, floor);
        let base = self.query_client_for_address(&address, TokenAccountsFilter::None)?;
        let families = self.address_families(&address, TokenAccountsFilter::None);
        self.attempt("signatures_for_address", &read, async {
            let mut records = Vec::new();
            let (scan_floor, scan_tip) = gsfa_window((floor, tip), before, until);
            for partition in self.partitions(scan_floor, scan_tip, SortOrder::Desc) {
                read.check()?;
                if records.len() >= limit {
                    break;
                }
                if !self.candidate(partition, &families, address.as_ref()) {
                    continue;
                }
                let client = self.scoped_client(&base, partition, &read);
                let (page, _) = client
                    .get_signatures_for_address_with_positions(
                        &address.to_string(),
                        (limit - records.len()) as u64,
                        before,
                        until,
                    )
                    .await?;
                records.extend(page);
            }
            let reached_floor = records.len() < limit && floor_effective;
            Ok(self.address_page(records, floor, tip, reached_floor, false))
        })
        .await
    }
    pub(crate) async fn transactions_for_address(
        &self,
        address: Pubkey,
        query: DiskTfaQuery,
    ) -> Option<DiskGsfaPage> {
        let read = self.read()?;
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
        let (scan_floor, scan_tip) = tfa_window((floor, tip), &q);
        for partition in self.partitions(scan_floor, scan_tip, query.sort_order) {
            read.check()?;
            if rows.records.len() >= query.limit {
                break;
            }
            if !self.candidate(partition, &families, address.as_ref()) {
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
            client.query_timeout = read.deadline.saturating_duration_since(Instant::now());
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
}
