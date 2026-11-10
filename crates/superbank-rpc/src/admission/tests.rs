// SPDX-License-Identifier: AGPL-3.0-only
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;
use tower::ServiceExt;

use super::*;
use crate::clickhouse::read_query::admission as read_admission;
use crate::clickhouse::{ClickHouseClient, ClickHouseClientOptions, RoutingPolicy};
use crate::clickhouse::{RoutingScope, RoutingTransport};

const QUEUE_TIMEOUT: Duration = Duration::from_millis(40);

fn exported_value(name: &str) -> f64 {
    let exported = String::from_utf8(metrics::export_metrics().expect("metrics export"))
        .expect("metrics are UTF-8");
    exported
        .lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' ')?.parse().ok())
        .unwrap_or(0.0)
}

fn source_client(admission: Option<Arc<BackendAdmission>>) -> ClickHouseClient {
    let mut client = ClickHouseClient::new(
        "http://127.0.0.1:1",
        "default",
        "default",
        "",
        ClickHouseClientOptions::new(
            RoutingPolicy {
                transport: RoutingTransport::Http,
                scope: RoutingScope::Distributed,
            },
            None,
            Vec::new(),
            "default.gsfa_hot".to_string(),
            "default.gsfa_hot_local".to_string(),
        ),
    );
    client.set_backend_admission(admission);
    client
}

fn holds(leases: &[Arc<OwnedSemaphorePermit>], admission: &BackendAdmission) -> bool {
    leases
        .iter()
        .any(|lease| Arc::ptr_eq(lease.semaphore(), admission.semaphore()))
}

#[tokio::test]
async fn global_admission_queues_per_route_like_concurrency_limit() {
    let entered = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let handler = {
        let entered = entered.clone();
        let release = release.clone();
        tower::service_fn(move |_: ()| {
            let entered = entered.clone();
            let release = release.clone();
            async move {
                entered.fetch_add(1, Ordering::SeqCst);
                release.notified().await;
                Ok::<_, Infallible>(())
            }
        })
    };
    let layer = GlobalAdmissionLayer::new(1);
    let rpc = layer.layer(handler.clone());
    let health = layer.layer(tower::service_fn(|_: ()| async { Ok::<_, Infallible>(()) }));
    let waits_before = exported_value("superbank_rpc_admission_wait_seconds_count");
    let inflight_before = exported_value("superbank_rpc_inflight");

    let first = tokio::spawn(rpc.clone().oneshot(()));
    while entered.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    let second = tokio::spawn(rpc.clone().oneshot(()));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(entered.load(Ordering::SeqCst), 1, "second call is queued");
    assert_eq!(
        exported_value("superbank_rpc_inflight") - inflight_before,
        1.0
    );

    // Another route has its own permits, as with tower's ConcurrencyLimitLayer.
    tokio::time::timeout(Duration::from_secs(1), health.oneshot(()))
        .await
        .expect("other route is not queued")
        .unwrap();

    release.notify_one();
    first.await.unwrap().unwrap();
    while entered.load(Ordering::SeqCst) < 2 {
        tokio::task::yield_now().await;
    }
    release.notify_one();
    second.await.unwrap().unwrap();
    assert_eq!(exported_value("superbank_rpc_inflight"), inflight_before);
    assert!(exported_value("superbank_rpc_admission_wait_seconds_count") - waits_before >= 3.0);
}

#[tokio::test]
async fn defaults_leave_backend_reads_unlimited() {
    assert!(BackendAdmission::new(0, QUEUE_TIMEOUT).is_none());
    assert!(source_client(None).backend_admission.is_none());
    assert!(admit_backend("op").await.unwrap().is_none());
    let inside = backend_request_scope(None, async {
        assert!(current_backend_request().is_none());
        admit_backend("op").await
    })
    .await;
    assert!(inside.unwrap().is_none());

    let client = source_client(None);
    let leases = backend_request_scope(
        client.backend_admission.clone(),
        client.with_operation_timeout("op", async { Ok(read_admission::current().len()) }),
    )
    .await;
    assert_eq!(leases.unwrap(), 0);
}

#[tokio::test]
async fn a_call_takes_one_permit_and_shares_it() {
    let admission = BackendAdmission::new(1, Duration::ZERO).unwrap();
    let (first, second, child) = backend_request_scope(Some(admission.clone()), async {
        let first = admit_backend("op").await.unwrap().unwrap();
        let second = admit_backend("op").await.unwrap().unwrap();
        // A zero queue timeout sheds any second acquisition, so the child must share.
        let child = tokio::spawn(read_admission::scope_with(Vec::new(), async {
            admit_backend("child").await
        }))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        (first, second, child)
    })
    .await;
    assert!(Arc::ptr_eq(&first, &second));
    assert!(Arc::ptr_eq(&first, &child));
    assert_eq!(admission.semaphore().available_permits(), 0);
    drop((first, second, child));
    assert_eq!(admission.semaphore().available_permits(), 1);
}

#[tokio::test]
async fn saturated_admission_sheds_after_the_queue_timeout() {
    let admission = BackendAdmission::new(1, QUEUE_TIMEOUT).unwrap();
    let _held = admission.semaphore().clone().acquire_owned().await.unwrap();
    let (shed, again) = backend_request_scope(Some(admission.clone()), async {
        let started = Instant::now();
        let shed = admit_backend("op").await.unwrap_err();
        assert!(started.elapsed() >= QUEUE_TIMEOUT);
        let started = Instant::now();
        let again = admit_backend("op").await.unwrap_err();
        assert!(started.elapsed() < QUEUE_TIMEOUT, "a shed call stays shed");
        (shed, again)
    })
    .await;
    for error in [&shed, &again] {
        assert!(is_backend_shed(error), "{error}");
        // Not a transient replica failure, so it never triggers replica failover.
        assert_eq!(
            crate::clickhouse::transient_shard_local_error_reason(error),
            None
        );
    }
}

#[tokio::test]
async fn primary_operations_admit_once_and_nest() {
    let admission = BackendAdmission::new(1, Duration::ZERO).unwrap();
    let client = source_client(Some(admission.clone()));
    let nested = backend_request_scope(
        client.backend_admission.clone(),
        client.with_operation_timeout("outer", async {
            assert!(holds(&read_admission::current(), &admission));
            client
                .with_operation_timeout("inner", async {
                    Ok(holds(&read_admission::current(), &admission))
                })
                .await
        }),
    )
    .await;
    assert!(nested.unwrap());
    assert_eq!(admission.semaphore().available_permits(), 1);

    // Outside a JSON-RPC call (background work, gRPC streams) the limit does not apply.
    let _held = admission.semaphore().clone().acquire_owned().await.unwrap();
    client
        .with_operation_timeout("background", async { Ok(()) })
        .await
        .unwrap();
}

#[tokio::test]
async fn saturated_primary_operation_fails_with_the_shed_error() {
    let admission = BackendAdmission::new(1, QUEUE_TIMEOUT).unwrap();
    let client = source_client(Some(admission.clone()));
    let _held = admission.semaphore().clone().acquire_owned().await.unwrap();
    let ran = AtomicUsize::new(0);
    let started = Instant::now();
    let error = backend_request_scope(
        client.backend_admission.clone(),
        client.with_operation_timeout("op", async {
            ran.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
    )
    .await
    .unwrap_err();
    assert!(is_backend_shed(&error), "{error}");
    assert_eq!(ran.load(Ordering::SeqCst), 0);
    assert!(started.elapsed() < client.query_timeout);
}

#[tokio::test]
async fn non_primary_lanes_are_never_admitted() {
    let admission = BackendAdmission::new(1, Duration::ZERO).unwrap();
    let _held = admission.semaphore().clone().acquire_owned().await.unwrap();
    let mut client = source_client(Some(admission.clone()));
    client.read_endpoint = client.read_endpoint.background(1);
    backend_request_scope(
        Some(admission),
        client.with_operation_timeout("local", async { Ok(()) }),
    )
    .await
    .unwrap();
}
