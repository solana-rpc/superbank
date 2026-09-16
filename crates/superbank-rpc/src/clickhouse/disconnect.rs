// SPDX-License-Identifier: AGPL-3.0-only
/* Copyright 2025-2026 Triton One Limited. All rights reserved. */

//! Admission accounting for primary HTTP reads abandoned by their caller.
//!
//! Disconnect is the cancellation mechanism. These read-only probes observe
//! termination; they never issue KILL or acquire a source-query permit.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use clickhouse::Client as HttpClient;
use serde::Deserialize;
use tokio::sync::{Notify, OnceCell, OwnedSemaphorePermit};
use tokio::time::Instant;

use super::util::next_required_query_id;
use crate::processing::{ProcessingError, ProcessingResult};

const PROBE_TIMEOUT: Duration = Duration::from_secs(1);
const POLL_INTERVAL: Duration = Duration::from_millis(250);
const UNCONFIRMED_AFTER: Duration = Duration::from_secs(5);
const INITIALIZATION_BACKOFF: Duration = Duration::from_secs(1);

#[cfg(test)]
mod initialization_tests;
#[cfg(test)]
mod integration_tests;

#[derive(Clone)]
pub(crate) struct DisconnectVerifier(Arc<Inner>);

struct Inner {
    client: HttpClient,
    cluster: Option<String>,
    topology: OnceCell<Topology>,
    retry_after: Mutex<Option<Instant>>,
    pending: Mutex<BTreeMap<String, Pending>>,
    notify: Arc<Notify>,
    worker_running: AtomicBool,
}

#[derive(Debug)]
struct Topology {
    nodes: HashSet<String>,
    cluster: Option<String>,
}

struct Pending {
    _permit: OwnedSemaphorePermit,
    _additional: Vec<OwnedSemaphorePermit>,
    _workflow: Vec<Arc<OwnedSemaphorePermit>>,
    abandoned_at: Instant,
    quiet_observations: u8,
    unconfirmed: bool,
    operation: &'static str,
    target: &'static str,
}

impl Drop for Pending {
    fn drop(&mut self) {
        record_pending(self.operation, self.target, -1);
    }
}

fn record_pending(operation: &'static str, target: &'static str, delta: i64) {
    crate::metrics::read_disconnect_pending(operation, target, delta);
    if operation == "signature_statuses" {
        if delta > 0 {
            crate::metrics::signature_status_disconnect_pending_inc();
        } else {
            crate::metrics::signature_status_disconnect_pending_dec();
        }
    }
}

fn record_verification(entry: &Pending, outcome: &'static str) {
    crate::metrics::read_disconnect_verification(entry.operation, entry.target, outcome);
    if entry.operation == "signature_statuses" {
        crate::metrics::signature_status_disconnect_verification(outcome);
    }
}

pub(crate) struct DisconnectGuard {
    owner: Arc<Inner>,
    query_id: String,
    permit: Option<OwnedSemaphorePermit>,
    operation: &'static str,
    target: &'static str,
    submitted: bool,
    additional: Vec<OwnedSemaphorePermit>,
    workflow: Vec<Arc<OwnedSemaphorePermit>>,
}

impl DisconnectVerifier {
    pub(crate) fn new(client: HttpClient, cluster: Option<String>) -> Self {
        Self(Arc::new(Inner {
            client,
            cluster,
            topology: OnceCell::new(),
            retry_after: Mutex::new(None),
            pending: Mutex::new(BTreeMap::new()),
            notify: Arc::new(Notify::new()),
            worker_running: AtomicBool::new(false),
        }))
    }

    /// Discover complete coverage and validate required HTTP settings before
    /// submitting source work. Discovery failure is safe to release admission.
    #[cfg(test)]
    pub(crate) async fn arm(
        &self,
        query_id: String,
        permit: OwnedSemaphorePermit,
    ) -> ProcessingResult<DisconnectGuard> {
        self.0
            .topology
            .get_or_try_init(|| self.initialize())
            .await?;
        start_worker(&self.0);
        Ok(DisconnectGuard {
            owner: self.0.clone(),
            query_id,
            permit: Some(permit),
            operation: "signature_statuses",
            target: "primary",
            submitted: true,
            additional: Vec::new(),
            workflow: Vec::new(),
        })
    }

    pub(crate) async fn initialize_ready(&self) -> ProcessingResult<()> {
        self.0
            .topology
            .get_or_try_init(|| self.initialize())
            .await
            .map(|_| ())
    }

    pub(crate) fn require_ready(&self) -> ProcessingResult<()> {
        if self.0.topology.get().is_some() {
            Ok(())
        } else {
            Err(ProcessingError::database_msg(
                "ClickHouse read cancellation is not initialized",
            ))
        }
    }

    pub(crate) fn arm_ready(
        &self,
        query_id: String,
        permit: OwnedSemaphorePermit,
        operation: &'static str,
        target: &'static str,
    ) -> ProcessingResult<DisconnectGuard> {
        self.require_ready()?;
        Ok(DisconnectGuard {
            owner: self.0.clone(),
            query_id,
            permit: Some(permit),
            operation,
            target,
            submitted: false,
            additional: Vec::new(),
            workflow: Vec::new(),
        })
    }

    async fn initialize(&self) -> ProcessingResult<Topology> {
        let mut attempt = InitializationAttempt::begin(&self.0.retry_after)?;
        let topology = discover(&self.0).await?;
        attempt.succeeded = true;
        Ok(topology)
    }
}

// OnceCell serializes attempts. This guard also applies backoff when the caller
// drops initialization while it is awaiting ClickHouse; no lock crosses an await.
struct InitializationAttempt<'a> {
    retry_after: &'a Mutex<Option<Instant>>,
    succeeded: bool,
}

impl<'a> InitializationAttempt<'a> {
    fn begin(retry_after: &'a Mutex<Option<Instant>>) -> ProcessingResult<Self> {
        if retry_after
            .lock()
            .expect("disconnect initialization state poisoned")
            .is_some_and(|deadline| Instant::now() < deadline)
        {
            return Err(ProcessingError::database_msg(
                "primary disconnect verification initialization is backing off",
            ));
        }
        Ok(Self {
            retry_after,
            succeeded: false,
        })
    }
}

impl Drop for InitializationAttempt<'_> {
    fn drop(&mut self) {
        *self
            .retry_after
            .lock()
            .expect("disconnect initialization state poisoned") =
            (!self.succeeded).then(|| Instant::now() + INITIALIZATION_BACKOFF);
    }
}

impl DisconnectGuard {
    #[cfg(all(test, feature = "disk-cache"))]
    pub(crate) fn query_id(&self) -> &str {
        &self.query_id
    }
    pub(crate) fn retain_workflow(&mut self, leases: Vec<Arc<OwnedSemaphorePermit>>) {
        self.workflow = leases;
    }
    pub(crate) fn retain(&mut self, permit: OwnedSemaphorePermit) {
        self.additional.push(permit);
    }
    pub(crate) fn submitted(&mut self) {
        self.submitted = true;
    }
    #[cfg(any(test, feature = "disk-cache"))]
    pub(crate) fn set_query_id(&mut self, id: String) {
        self.query_id = id;
    }

    /// Only call after a fully consumed successful response, or before submission.
    pub(crate) fn disarm(&mut self) {
        self.permit = None;
        self.additional.clear();
        self.workflow.clear();
    }
}

impl Drop for DisconnectGuard {
    fn drop(&mut self) {
        enqueue_abandoned(self);
    }
}

fn enqueue_abandoned(guard: &mut DisconnectGuard) {
    if !guard.submitted {
        return;
    }
    let Some(permit) = guard.permit.take() else {
        return;
    };
    record_pending(guard.operation, guard.target, 1);
    let pending = Pending {
        _permit: permit,
        _additional: std::mem::take(&mut guard.additional),
        _workflow: std::mem::take(&mut guard.workflow),
        abandoned_at: Instant::now(),
        quiet_observations: 0,
        unconfirmed: false,
        operation: guard.operation,
        target: guard.target,
    };
    // Required query IDs are unique across the client lifetime. Keeping the
    // map in the owner also retains admission if no runtime can run probes.
    guard
        .owner
        .pending
        .lock()
        .expect("disconnect state poisoned")
        .insert(guard.query_id.clone(), pending);
    start_worker(&guard.owner);
    guard.owner.notify.notify_one();
}

struct WorkerExit(Weak<Inner>);

impl Drop for WorkerExit {
    fn drop(&mut self) {
        if let Some(owner) = self.0.upgrade() {
            owner.worker_running.store(false, Ordering::Release);
        }
    }
}

fn start_worker(owner: &Arc<Inner>) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    if owner
        .worker_running
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    let weak = Arc::downgrade(owner);
    let notify = owner.notify.clone();
    let exit = WorkerExit(weak.clone());
    runtime.spawn(worker(weak, notify, exit));
}

async fn worker(weak: Weak<Inner>, notify: Arc<Notify>, _exit: WorkerExit) {
    loop {
        let Some(owner) = weak.upgrade() else { return };
        let ids: Vec<String> = owner
            .pending
            .lock()
            .expect("disconnect state poisoned")
            .keys()
            .cloned()
            .collect();
        if ids.is_empty() {
            drop(owner);
            // Periodic weak upgrade notices service shutdown without retaining it.
            tokio::select! {
                _ = notify.notified() => {},
                _ = tokio::time::sleep(PROBE_TIMEOUT) => {},
            }
            continue;
        }
        let interval = probe_batches(&owner, &ids).await;
        drop(owner);
        // New arrivals cannot accelerate an existing ID's second quiet check.
        tokio::time::sleep(interval).await;
    }
}

async fn probe_batches(owner: &Inner, ids: &[String]) -> Duration {
    let mut interval = POLL_INTERVAL;
    for batch in ids.chunks(128) {
        let started = Instant::now();
        let result = probe(owner, batch).await;
        crate::metrics::read_disconnect_probe(
            if owner.cluster.is_some() {
                "cluster"
            } else {
                "local"
            },
            started.elapsed().as_secs_f64(),
            if result.is_ok() { "success" } else { "error" },
        );
        interval = apply_observation(owner, batch, result.as_ref().ok());
        if let Err(error) = result {
            tracing::warn!(%error, "Unable to verify abandoned ClickHouse reads");
        }
    }
    interval
}

fn apply_observation(owner: &Inner, ids: &[String], active: Option<&HashSet<String>>) -> Duration {
    let mut pending = owner.pending.lock().expect("disconnect state poisoned");
    for id in ids {
        let Some(entry) = pending.get_mut(id) else {
            continue;
        };
        entry.quiet_observations = match active {
            Some(active) if !active.contains(id) => entry.quiet_observations + 1,
            _ => 0,
        };
        if entry.quiet_observations >= 2 {
            tracing::debug!(query_id = %id, "Observed ClickHouse read termination");
            record_verification(entry, "confirmed_absent");
            pending.remove(id);
        } else if entry.abandoned_at.elapsed() >= UNCONFIRMED_AFTER && !entry.unconfirmed {
            entry.unconfirmed = true;
            tracing::warn!(query_id = %id, "ClickHouse read termination unconfirmed; retaining admission");
            record_verification(entry, "unconfirmed");
        }
    }
    if pending.values().all(|entry| entry.unconfirmed) {
        PROBE_TIMEOUT
    } else {
        POLL_INTERVAL
    }
}

#[derive(clickhouse::Row, Deserialize)]
struct DiscoveryRow {
    node: String,
    expected: u64,
    coordinator: u8,
}

#[derive(clickhouse::Row, Deserialize)]
struct ProbeRow {
    node: String,
    active_id: String,
}

fn quoted(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn table(cluster: Option<&str>, name: &str) -> String {
    match cluster {
        Some(cluster) => format!("clusterAllReplicas({}, system.{name})", quoted(cluster)),
        None => format!("system.{name}"),
    }
}

fn discovery_sql(cluster: Option<&str>) -> String {
    let Some(cluster) = cluster else {
        return "SELECT hostName() AS node, toUInt64(1) AS expected, toUInt8(1) AS coordinator FROM system.one".into();
    };
    format!(
        "SELECT hostName() AS node, toUInt64(0) AS expected, toUInt8(0) AS coordinator FROM {} \
         UNION ALL SELECT hostName() AS node, ifNull((SELECT count() FROM system.clusters WHERE cluster = {}), toUInt64(0)) AS expected, \
         toUInt8(1) AS coordinator FROM system.one",
        table(Some(cluster), "one"),
        quoted(cluster),
    )
}

#[derive(clickhouse::Row, Deserialize)]
struct MacroRow {
    cluster: String,
}

// Table functions expand macros, but system.clusters contains resolved names.
// Resolve once through ClickHouse so both discovery and probes use that name.
fn macro_sql(cluster: &str) -> ProcessingResult<String> {
    let mut remaining = cluster;
    let mut pieces = Vec::new();
    while let Some(start) = remaining.find('{') {
        pieces.push(quoted(&remaining[..start]));
        let end = remaining[start..]
            .find('}')
            .ok_or_else(|| ProcessingError::database_msg("unclosed ClickHouse cluster macro"))?
            + start;
        let name = &remaining[start + 1..end];
        if name.is_empty() || name.contains('{') {
            return Err(ProcessingError::database_msg(
                "invalid ClickHouse cluster macro",
            ));
        }
        pieces.push(format!("getMacro({})", quoted(name)));
        remaining = &remaining[end + 1..];
    }
    pieces.push(quoted(remaining));
    Ok(format!(
        "SELECT concat({}) AS cluster FROM system.one",
        pieces.join(", ")
    ))
}

async fn resolve_cluster(owner: &Inner) -> ProcessingResult<Option<String>> {
    let Some(cluster) = owner.cluster.as_deref() else {
        return Ok(None);
    };
    if !cluster.contains('{') {
        return Ok(Some(cluster.into()));
    }
    let rows = fetch::<MacroRow>(&owner.client, &macro_sql(cluster)?, "macro_resolution").await?;
    let mut rows = rows.into_iter();
    let resolved = rows
        .next()
        .filter(|row| !row.cluster.is_empty())
        .ok_or_else(|| {
            ProcessingError::database_msg("empty ClickHouse cluster macro resolution")
        })?;
    if rows.next().is_some() {
        return Err(ProcessingError::database_msg(
            "ambiguous ClickHouse cluster macro resolution",
        ));
    }
    Ok(Some(resolved.cluster))
}

async fn discover(owner: &Inner) -> ProcessingResult<Topology> {
    let topology = discover_topology(owner).await?;
    // Exercise the same table, decoder, coverage checks and settings needed to
    // release admission. A unique nonempty ID prevents an empty-set shortcut.
    let id = next_required_query_id("status_disconnect_preflight");
    probe_topology(&owner.client, &topology, &[id], "capability_probe").await?;
    Ok(topology)
}

async fn discover_topology(owner: &Inner) -> ProcessingResult<Topology> {
    let cluster = resolve_cluster(owner).await?;
    let rows = fetch::<DiscoveryRow>(
        &owner.client,
        &discovery_sql(cluster.as_deref()),
        "discovery",
    )
    .await?;
    Ok(Topology {
        nodes: validate_topology(rows, cluster.is_some())?,
        cluster,
    })
}

fn validate_topology(
    rows: Vec<DiscoveryRow>,
    distributed: bool,
) -> ProcessingResult<HashSet<String>> {
    let mut expected = None;
    let mut coordinator = None;
    let mut nodes = HashSet::new();
    for row in rows {
        if row.coordinator == 1 {
            if coordinator.replace(row.node.clone()).is_some() {
                return Err(ProcessingError::database_msg(
                    "duplicate verification coordinator",
                ));
            }
            expected = Some(row.expected);
            if !distributed {
                nodes.insert(row.node);
            }
        } else if row.node.is_empty() || !nodes.insert(row.node) {
            return Err(ProcessingError::database_msg(
                "ambiguous verification replica identity",
            ));
        }
    }
    let complete = expected == Some(nodes.len() as u64)
        && !nodes.is_empty()
        && coordinator
            .as_ref()
            .is_some_and(|node| nodes.contains(node));
    if !complete {
        return Err(ProcessingError::database_msg(
            "incomplete primary disconnect verification topology",
        ));
    }
    Ok(nodes)
}

fn probe_sql(cluster: Option<&str>, ids: &[String]) -> String {
    let ids = ids
        .iter()
        .map(|id| quoted(id))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT hostName() AS node, '' AS active_id FROM {} \
         UNION ALL SELECT hostName() AS node, if(query_id IN ({ids}), query_id, initial_query_id) AS active_id \
         FROM {} WHERE query_id IN ({ids}) OR initial_query_id IN ({ids})",
        table(cluster, "one"),
        table(cluster, "processes"),
    )
}

async fn probe(owner: &Inner, ids: &[String]) -> ProcessingResult<HashSet<String>> {
    let topology = owner
        .topology
        .get()
        .expect("topology precedes source submission");
    probe_topology(&owner.client, topology, ids, "termination_probe").await
}

async fn probe_topology(
    client: &HttpClient,
    topology: &Topology,
    ids: &[String],
    phase: &'static str,
) -> ProcessingResult<HashSet<String>> {
    let rows =
        fetch::<ProbeRow>(client, &probe_sql(topology.cluster.as_deref(), ids), phase).await?;
    validate_observation(rows, &topology.nodes)
}

fn validate_observation(
    rows: Vec<ProbeRow>,
    expected: &HashSet<String>,
) -> ProcessingResult<HashSet<String>> {
    let mut covered = HashSet::new();
    let mut active = HashSet::new();
    for row in rows {
        if !expected.contains(&row.node) {
            return Err(ProcessingError::database_msg(
                "primary verification topology changed",
            ));
        }
        if row.active_id.is_empty() {
            covered.insert(row.node);
        } else {
            active.insert(row.active_id);
        }
    }
    if &covered != expected {
        return Err(ProcessingError::database_msg(
            "incomplete primary verification coverage",
        ));
    }
    Ok(active)
}

async fn fetch<T: clickhouse::RowOwned + clickhouse::RowRead>(
    client: &HttpClient,
    sql: &str,
    phase: &'static str,
) -> ProcessingResult<Vec<T>> {
    let query = client
        .query(sql)
        .with_setting(
            "query_id",
            next_required_query_id("status_disconnect_verification"),
        )
        .with_setting("readonly", "2")
        .with_setting("cancel_http_readonly_queries_on_client_close", "1")
        .with_setting("max_threads", "1")
        .with_setting("max_threads_for_indexes", "1")
        .with_setting("max_parallel_replicas", "1")
        .with_setting("use_hedged_requests", "0")
        .with_setting("skip_unavailable_shards", "0")
        .with_setting("max_execution_time", "1")
        .with_setting("max_execution_time_leaf", "1")
        .with_setting("timeout_overflow_mode", "throw")
        .with_setting("timeout_overflow_mode_leaf", "throw")
        .with_setting("timeout_before_checking_execution_speed", "0")
        .with_setting("use_query_cache", "0");
    tokio::time::timeout(PROBE_TIMEOUT, query.fetch_all::<T>())
        .await
        .map_err(|error| {
            tracing::warn!(phase, %error, "Primary disconnect verification timed out");
            ProcessingError::timeout("primary disconnect verification", error)
        })?
        .map_err(|error| {
            // Log the SDK cause before wrapping it in the stable RPC-facing context.
            // Do not log the client, credentials or SQL/request payload.
            tracing::warn!(phase, ?error, "Primary disconnect verification failed");
            ProcessingError::database("primary disconnect verification", error)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Semaphore;

    fn verifier() -> DisconnectVerifier {
        let verifier = DisconnectVerifier::new(HttpClient::default(), None);
        verifier
            .0
            .topology
            .set(Topology {
                nodes: HashSet::from(["node1".into()]),
                cluster: None,
            })
            .unwrap();
        verifier
    }

    fn pending(verifier: &DisconnectVerifier, semaphore: &Arc<Semaphore>, id: &str) {
        crate::metrics::signature_status_disconnect_pending_inc();
        verifier.0.pending.lock().unwrap().insert(
            id.into(),
            Pending {
                _permit: semaphore.clone().try_acquire_owned().unwrap(),
                _additional: Vec::new(),
                _workflow: Vec::new(),
                abandoned_at: Instant::now(),
                quiet_observations: 0,
                unconfirmed: false,
                operation: "signature_statuses",
                target: "primary",
            },
        );
    }

    #[test]
    fn discovery_rejects_missing_ambiguous_or_outside_coordinator() {
        let row = |node: &str, expected, coordinator| DiscoveryRow {
            node: node.into(),
            expected,
            coordinator,
        };
        assert!(validate_topology(vec![row("a", 0, 0), row("a", 1, 1)], true).is_ok());
        assert!(validate_topology(vec![row("a", 0, 0), row("a", 2, 1)], true).is_err());
        assert!(
            validate_topology(vec![row("a", 0, 0), row("a", 0, 0), row("a", 2, 1)], true).is_err()
        );
        assert!(validate_topology(vec![row("a", 0, 0), row("gateway", 1, 1)], true).is_err());
        assert!(validate_topology(Vec::new(), true).is_err());
    }

    #[test]
    fn probes_require_complete_sentinels_and_track_initial_ids() {
        let expected = HashSet::from(["a".into(), "b".into()]);
        let row = |node: &str, id: &str| ProbeRow {
            node: node.into(),
            active_id: id.into(),
        };
        assert!(validate_observation(vec![row("a", ""), row("b", "q")], &expected).is_err());
        assert!(validate_observation(vec![row("a", ""), row("new", "")], &expected).is_err());
        let active =
            validate_observation(vec![row("a", ""), row("b", ""), row("b", "q")], &expected)
                .unwrap();
        assert_eq!(active, HashSet::from(["q".into()]));
        let sql = probe_sql(Some("rbx2"), &["q".into()]);
        assert!(sql.contains("initial_query_id"));
        assert!(!sql.contains("KILL"));
        assert_eq!(quoted("a'\\b"), "'a\\'\\\\b'");
    }

    #[tokio::test]
    async fn errors_and_active_work_reset_quiet_new_ids_do_not_inherit_observations() {
        let verifier = verifier();
        let semaphore = Arc::new(Semaphore::new(2));
        pending(&verifier, &semaphore, "old");
        let old = vec!["old".into()];
        let quiet = HashSet::new();
        apply_observation(&verifier.0, &old, Some(&quiet));
        apply_observation(&verifier.0, &old, None);
        apply_observation(&verifier.0, &old, Some(&quiet));
        assert_eq!(semaphore.available_permits(), 1);
        apply_observation(&verifier.0, &old, Some(&HashSet::from(["old".into()])));
        apply_observation(&verifier.0, &old, Some(&quiet));
        pending(&verifier, &semaphore, "new");
        apply_observation(&verifier.0, &old, Some(&quiet));
        assert_eq!(semaphore.available_permits(), 1);
        let new = vec!["new".into()];
        apply_observation(&verifier.0, &new, Some(&quiet));
        assert_eq!(semaphore.available_permits(), 1);
        apply_observation(&verifier.0, &new, Some(&quiet));
        assert_eq!(semaphore.available_permits(), 2);
    }

    #[tokio::test]
    async fn unconfirmed_work_retains_admission_until_recovery() {
        let verifier = verifier();
        let semaphore = Arc::new(Semaphore::new(1));
        pending(&verifier, &semaphore, "q");
        verifier
            .0
            .pending
            .lock()
            .unwrap()
            .get_mut("q")
            .unwrap()
            .abandoned_at = Instant::now() - Duration::from_secs(6);
        let ids = vec!["q".into()];
        assert_eq!(apply_observation(&verifier.0, &ids, None), PROBE_TIMEOUT);
        assert_eq!(semaphore.available_permits(), 0);
        assert!(verifier.0.pending.lock().unwrap()["q"].unconfirmed);
        apply_observation(&verifier.0, &ids, Some(&HashSet::new()));
        assert_eq!(semaphore.available_permits(), 0);
        apply_observation(&verifier.0, &ids, Some(&HashSet::new()));
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[test]
    fn missing_runtime_retains_admission_until_owner_shutdown() {
        let verifier = verifier();
        let semaphore = Arc::new(Semaphore::new(1));
        let guard = DisconnectGuard {
            owner: verifier.0.clone(),
            query_id: "q".into(),
            permit: Some(semaphore.clone().try_acquire_owned().unwrap()),
            operation: "signature_statuses",
            target: "primary",
            submitted: true,
            additional: Vec::new(),
            workflow: Vec::new(),
        };
        drop(guard);
        assert_eq!(semaphore.available_permits(), 0);
        assert!(!verifier.0.worker_running.load(Ordering::Acquire));
        drop(verifier);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[test]
    fn successful_guard_releases_without_probe() {
        let verifier = verifier();
        let semaphore = Arc::new(Semaphore::new(1));
        let mut guard = DisconnectGuard {
            owner: verifier.0.clone(),
            query_id: "q".into(),
            permit: Some(semaphore.clone().try_acquire_owned().unwrap()),
            operation: "signature_statuses",
            target: "primary",
            submitted: true,
            additional: Vec::new(),
            workflow: Vec::new(),
        };
        guard.disarm();
        drop(guard);
        assert_eq!(semaphore.available_permits(), 1);
        assert!(verifier.0.pending.lock().unwrap().is_empty());
    }

    #[test]
    fn cluster_macros_are_resolved_before_topology_lookup() {
        assert_eq!(
            macro_sql("prefix-{cluster}-{zone}").unwrap(),
            "SELECT concat('prefix-', getMacro('cluster'), '-', getMacro('zone'), '') AS cluster FROM system.one"
        );
        assert!(macro_sql("{unclosed").is_err());
        assert!(macro_sql("{}").is_err());
        assert!(macro_sql("{{nested}").is_err());
    }

    #[tokio::test]
    async fn slow_probe_keeps_permit_and_inflight_worker_exits_on_owner_shutdown() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (sender, mut requests) = tokio::sync::mpsc::unbounded_channel();
        let app = axum::Router::new().fallback(move || {
            let sender = sender.clone();
            async move {
                let _ = sender.send(());
                tokio::time::sleep(Duration::from_secs(10)).await;
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            }
        });
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let verifier = DisconnectVerifier::new(HttpClient::default().with_url(url), None);
        verifier
            .0
            .topology
            .set(Topology {
                nodes: HashSet::from(["node1".into()]),
                cluster: None,
            })
            .unwrap();
        let semaphore = Arc::new(Semaphore::new(1));
        drop(
            verifier
                .arm("q".into(), semaphore.clone().acquire_owned().await.unwrap())
                .await
                .unwrap(),
        );
        tokio::time::timeout(Duration::from_secs(2), requests.recv())
            .await
            .unwrap()
            .unwrap();
        // The second probe proves the first timed out and reset quiet accounting.
        tokio::time::timeout(Duration::from_secs(2), requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(semaphore.available_permits(), 0);
        assert_eq!(
            verifier.0.pending.lock().unwrap()["q"].quiet_observations,
            0
        );
        let weak = Arc::downgrade(&verifier.0);
        drop(verifier);
        tokio::time::timeout(Duration::from_secs(2), async {
            while weak.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(semaphore.available_permits(), 1);
        server.abort();
    }

    async fn read_http_request(socket: &mut tokio::net::TcpStream) {
        use tokio::io::AsyncReadExt;
        let mut request = Vec::new();
        let mut buffer = [0_u8; 2048];
        let header_end = loop {
            let n = socket.read(&mut buffer).await.unwrap();
            assert!(n > 0);
            request.extend_from_slice(&buffer[..n]);
            if let Some(end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                break end + 4;
            }
        };
        let headers = std::str::from_utf8(&request[..header_end]).unwrap();
        let content_length: usize = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().unwrap())
            })
            .expect("sized ClickHouse query body");
        while request.len() < header_end + content_length {
            let n = socket.read(&mut buffer).await.unwrap();
            assert!(n > 0);
            request.extend_from_slice(&buffer[..n]);
        }
    }

    async fn assert_transport_disconnect(streaming: bool) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_http_request(&mut socket).await;
            if streaming {
                socket.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: application/octet-stream\r\n\r\n8\r\n").await.unwrap();
                socket.write_all(&42_u64.to_le_bytes()).await.unwrap();
                socket.write_all(b"\r\n").await.unwrap();
            }
            ready_tx.send(()).unwrap();
            let mut byte = [0_u8; 1];
            let closed = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
                .await
                .expect("dropping query must close upstream socket");
            assert!(
                matches!(closed, Ok(0)) || closed.is_err(),
                "upstream socket remained readable"
            );
        });
        let client = super::super::client::build_clickhouse_http_client(
            &url,
            "default",
            "",
            "",
            PROBE_TIMEOUT,
        )
        .with_validation(false)
        .with_compression(clickhouse::Compression::None);
        let query = client
            .query("SELECT toUInt64(42)")
            .with_setting("readonly", "2")
            .with_setting("cancel_http_readonly_queries_on_client_close", "1");
        if streaming {
            let mut cursor = query.fetch::<u64>().unwrap();
            assert_eq!(cursor.next().await.unwrap(), Some(42));
            ready_rx.await.unwrap();
            drop(cursor);
        } else {
            let task = tokio::spawn(query.fetch_all::<u64>());
            ready_rx.await.unwrap();
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn production_http_transport_closes_when_dropped_before_headers() {
        assert_transport_disconnect(false).await;
    }

    #[tokio::test]
    async fn production_http_transport_closes_when_streaming_cursor_is_dropped() {
        assert_transport_disconnect(true).await;
    }

    fn string(bytes: &mut Vec<u8>, value: &str) {
        assert!(value.len() < 128);
        bytes.push(value.len() as u8);
        bytes.extend_from_slice(value.as_bytes());
    }

    fn is_termination_probe(sql: &str) -> bool {
        sql.contains("system.processes") && !sql.contains("status_disconnect_preflight")
    }

    #[tokio::test]
    async fn worker_batches_ids_uses_http_settings_and_stops_with_owner() {
        use axum::extract::OriginalUri;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (sender, mut requests) = tokio::sync::mpsc::unbounded_channel();
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let active_handler = active.clone();
        let maximum_handler = maximum.clone();
        let app = axum::Router::new().fallback(
            move |OriginalUri(uri): OriginalUri, body: axum::body::Bytes| {
                let sender = sender.clone();
                let active = active_handler.clone();
                let maximum = maximum_handler.clone();
                async move {
                    let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(count, Ordering::SeqCst);
                    let sql = String::from_utf8(body.to_vec()).unwrap();
                    sender.send((uri.to_string(), sql.clone())).unwrap();
                    let mut bytes = Vec::new();
                    string(&mut bytes, "node1");
                    if sql.contains("AS coordinator") {
                        bytes.extend_from_slice(&1_u64.to_le_bytes());
                        bytes.push(1);
                    } else {
                        string(&mut bytes, "");
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    bytes
                }
            },
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = HttpClient::default()
            .with_url(url)
            .with_validation(false)
            .with_compression(clickhouse::Compression::None);
        let verifier = DisconnectVerifier::new(client, None);
        let semaphore = Arc::new(Semaphore::new(2));
        let first = verifier
            .arm(
                "q1".into(),
                semaphore.clone().acquire_owned().await.unwrap(),
            )
            .await
            .unwrap();
        let second = verifier
            .arm(
                "q2".into(),
                semaphore.clone().acquire_owned().await.unwrap(),
            )
            .await
            .unwrap();
        drop(first);
        drop(second);
        tokio::time::timeout(Duration::from_secs(3), async {
            while semaphore.available_permits() != 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let mut probes = 0;
        while let Ok((uri, sql)) = requests.try_recv() {
            let url = reqwest::Url::parse(&format!("http://localhost{uri}")).unwrap();
            let params: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
            assert_eq!(params["readonly"], "2");
            assert_eq!(params["cancel_http_readonly_queries_on_client_close"], "1");
            assert_eq!(params["skip_unavailable_shards"], "0");
            assert_eq!(params["max_execution_time_leaf"], "1");
            assert!(params.contains_key("query_id"));
            assert!(!sql.contains("KILL"));
            if is_termination_probe(&sql) {
                assert!(sql.contains("q1") && sql.contains("q2"));
                probes += 1;
            }
        }
        assert_eq!(probes, 2);
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
        let weak = Arc::downgrade(&verifier.0);
        drop(verifier);
        tokio::time::timeout(Duration::from_secs(2), async {
            while weak.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        server.abort();
    }
}
