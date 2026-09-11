// SPDX-License-Identifier: AGPL-3.0-only
//! Workflow admission inherited by submitted read guards. The context holds weak
//! references: only the workflow and its outstanding reads retain capacity.

use std::future::Future;
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore};

tokio::task_local! {
    static LEASES: Mutex<Vec<Weak<OwnedSemaphorePermit>>>;
}

#[derive(Clone, Debug)]
pub(crate) struct AdmissionLease(Arc<OwnedSemaphorePermit>);

impl AdmissionLease {
    /// Attach admission acquired outside a task-local scope to its query workflow.
    pub(crate) async fn scope<T>(&self, future: impl Future<Output = T>) -> T {
        let mut inherited = current();
        inherited.push(self.0.clone());
        scope_with(inherited, future).await
    }
}

/// Acquire workflow capacity and make it visible to reads submitted in this scope.
pub(crate) async fn acquire(semaphore: &Arc<Semaphore>) -> Result<AdmissionLease, AcquireError> {
    let lease = AdmissionLease(Arc::new(semaphore.clone().acquire_owned().await?));
    let _ = LEASES.try_with(|leases| {
        let mut leases = leases.lock().expect("read admission context poisoned");
        leases.retain(|lease| lease.strong_count() != 0);
        leases.push(Arc::downgrade(&lease.0));
    });
    Ok(lease)
}

/// Keep a registered fanout lease alive throughout the operation, including errors.
pub(crate) async fn run_with_permit<T, E>(
    semaphore: Arc<Semaphore>,
    map_error: impl FnOnce(AcquireError) -> E,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    let _lease = acquire(&semaphore).await.map_err(map_error)?;
    future.await
}

/// Start a nested workflow context while preserving its active parent admission.
/// Tokio-spawned tasks do not inherit task locals; establish their scope before
/// acquiring their own workflow permits.
pub(crate) async fn scope<T>(future: impl Future<Output = T>) -> T {
    scope_with(current(), future).await
}

/// Seed a spawned workflow with its parent's leases. The task owns these leases
/// until completion so parent cancellation cannot release capacity beneath it.
pub(crate) async fn scope_with<T>(
    inherited: Vec<Arc<OwnedSemaphorePermit>>,
    future: impl Future<Output = T>,
) -> T {
    let context = inherited.iter().map(Arc::downgrade).collect();
    let result = LEASES.scope(Mutex::new(context), future).await;
    drop(inherited);
    result
}

/// Snapshot active workflow leases for ownership by a submitted read guard.
pub(crate) fn current() -> Vec<Arc<OwnedSemaphorePermit>> {
    LEASES
        .try_with(|leases| {
            let mut leases = leases.lock().expect("read admission context poisoned");
            let active: Vec<_> = leases.iter().filter_map(Weak::upgrade).collect();
            leases.retain(|lease| lease.strong_count() != 0);
            active
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn explicit_workflow_scope_retains_preacquired_admission() {
        let semaphore = Arc::new(Semaphore::new(1));
        let lease = acquire(&semaphore).await.unwrap();
        let reads = lease.scope(async { current() }).await;
        drop(lease);
        assert_eq!(semaphore.available_permits(), 0);
        assert_eq!(reads.len(), 1);
        drop(reads);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[tokio::test]
    async fn spawned_scope_retains_parent_admission_before_its_first_read() {
        let semaphore = Arc::new(Semaphore::new(1));
        let (release, wait) = tokio::sync::oneshot::channel();
        let (snapshots, receive) = tokio::sync::oneshot::channel();
        let (task,) = scope(async {
            let _parent = acquire(&semaphore).await.unwrap();
            (tokio::spawn(scope_with(current(), async move {
                wait.await.unwrap();
                snapshots.send(current()).unwrap();
            })),)
        })
        .await;
        assert_eq!(semaphore.available_permits(), 0);
        release.send(()).unwrap();
        let read_leases = receive.await.unwrap();
        task.await.unwrap();
        assert_eq!(read_leases.len(), 1);
        assert_eq!(semaphore.available_permits(), 0);
        drop(read_leases);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[tokio::test]
    async fn outstanding_read_retains_cancelled_workflow_admission() {
        let semaphore = Arc::new(Semaphore::new(1));
        let (send, receive) = tokio::sync::oneshot::channel();
        let workflow_semaphore = semaphore.clone();
        let task = tokio::spawn(scope(async move {
            let _lease = acquire(&workflow_semaphore).await.unwrap();
            send.send(current()).unwrap();
            std::future::pending::<()>().await;
        }));
        let read_leases = receive.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(semaphore.available_permits(), 0);
        drop(read_leases);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[tokio::test]
    async fn nested_scope_inherits_parent_without_retaining_finished_leases() {
        let semaphore = Arc::new(Semaphore::new(2));
        scope(async {
            let parent = acquire(&semaphore).await.unwrap();
            scope(async {
                assert_eq!(current().len(), 1);
                let child = acquire(&semaphore).await.unwrap();
                assert_eq!(current().len(), 2);
                drop(child);
                assert_eq!(current().len(), 1);
            })
            .await;
            drop(parent);
            assert!(current().is_empty());
            assert_eq!(semaphore.available_permits(), 2);
        })
        .await;
        assert!(current().is_empty());
    }

    #[tokio::test]
    async fn cancelled_wait_does_not_register_a_lease() {
        let semaphore = Arc::new(Semaphore::new(0));
        scope(async {
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(1), acquire(&semaphore),)
                    .await
                    .is_err()
            );
            assert!(current().is_empty());
        })
        .await;
    }

    #[tokio::test]
    async fn acquisition_without_context_retains_normal_raii_behavior() {
        let semaphore = Arc::new(Semaphore::new(1));
        let lease = acquire(&semaphore).await.unwrap();
        assert!(current().is_empty());
        assert_eq!(semaphore.available_permits(), 0);
        drop(lease);
        assert_eq!(semaphore.available_permits(), 1);
    }
}
