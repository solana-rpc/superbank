// SPDX-License-Identifier: AGPL-3.0-only
//! Request admission. The global limit bounds in-flight HTTP requests per route. The optional
//! backend limit bounds JSON-RPC calls that read source ClickHouse, keyed on request class.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};
use tower::{Layer, Service};

use crate::metrics;
use crate::processing::{ProcessingError, ProcessingResult};

/// Like `tower::limit::ConcurrencyLimitLayer`, each wrapped route gets its own permits.
#[derive(Clone, Copy, Debug)]
pub(crate) struct GlobalAdmissionLayer {
    limit: usize,
}

impl GlobalAdmissionLayer {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit: limit.max(1),
        }
    }
}

impl<S> Layer<S> for GlobalAdmissionLayer {
    type Service = GlobalAdmission<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GlobalAdmission {
            inner,
            semaphore: Arc::new(Semaphore::new(self.limit)),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct GlobalAdmission<S> {
    inner: S,
    semaphore: Arc<Semaphore>,
}

impl<S, Request> Service<Request> for GlobalAdmission<S>
where
    S: Service<Request> + Clone + Send + 'static,
    S::Future: Send + 'static,
    Request: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<S::Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        // Call the service that was polled ready and keep a fresh clone for the next call.
        let fresh = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, fresh);
        let semaphore = self.semaphore.clone();
        Box::pin(async move {
            let started = Instant::now();
            let permit = semaphore
                .acquire_owned()
                .await
                .expect("global admission semaphore is never closed");
            metrics::rpc_admission_wait(started.elapsed());
            let _inflight = Inflight::enter(permit);
            inner.call(request).await
        })
    }
}

struct Inflight {
    _permit: OwnedSemaphorePermit,
}

impl Inflight {
    fn enter(permit: OwnedSemaphorePermit) -> Self {
        metrics::rpc_inflight(1);
        Self { _permit: permit }
    }
}

impl Drop for Inflight {
    fn drop(&mut self) {
        metrics::rpc_inflight(-1);
    }
}

/// Limits JSON-RPC calls that read source ClickHouse. A call takes one permit at its first
/// source read and keeps it until the call and its outstanding reads finish.
#[derive(Debug)]
pub(crate) struct BackendAdmission {
    semaphore: Arc<Semaphore>,
    queue_timeout: Duration,
}

impl BackendAdmission {
    /// `None` when `limit` is zero, which leaves source reads unlimited.
    pub(crate) fn new(limit: usize, queue_timeout: Duration) -> Option<Arc<Self>> {
        (limit > 0).then(|| {
            Arc::new(Self {
                semaphore: Arc::new(Semaphore::new(limit)),
                queue_timeout,
            })
        })
    }

    #[cfg(test)]
    pub(crate) fn semaphore(&self) -> &Arc<Semaphore> {
        &self.semaphore
    }

    async fn acquire(&self, operation: &'static str) -> Option<Arc<OwnedSemaphorePermit>> {
        let started = Instant::now();
        // `timeout` polls the acquire before the timer, so a zero wait is a try-acquire.
        let acquired =
            tokio::time::timeout(self.queue_timeout, self.semaphore.clone().acquire_owned()).await;
        let permit = match acquired {
            Ok(Ok(permit)) => Some(Arc::new(permit)),
            Ok(Err(_)) | Err(_) => None,
        };
        let outcome = if permit.is_some() { "admitted" } else { "shed" };
        metrics::backend_admission(operation, outcome, started.elapsed());
        permit
    }
}

/// Source of the error returned when a backend-bound call waits past its queue timeout.
#[derive(Debug, thiserror::Error)]
#[error("backend admission queue full")]
pub(crate) struct BackendAdmissionShed;

/// One JSON-RPC call's backend admission, shared with the tasks the call spawns.
#[derive(Debug)]
pub(crate) struct BackendRequest {
    admission: Arc<BackendAdmission>,
    permit: OnceCell<Option<Arc<OwnedSemaphorePermit>>>,
}

tokio::task_local! {
    static BACKEND_REQUEST: Arc<BackendRequest>;
}

/// Runs one JSON-RPC call as its own backend admission unit; `None` leaves it unlimited.
pub(crate) async fn backend_request_scope<T>(
    admission: Option<Arc<BackendAdmission>>,
    future: impl Future<Output = T>,
) -> T {
    let request = admission.map(|admission| {
        Arc::new(BackendRequest {
            admission,
            permit: OnceCell::new(),
        })
    });
    within_backend_request(request, future).await
}

/// The calling task's backend request, captured before spawning so the child inherits it.
pub(crate) fn current_backend_request() -> Option<Arc<BackendRequest>> {
    BACKEND_REQUEST.try_with(Arc::clone).ok()
}

pub(crate) async fn within_backend_request<T>(
    request: Option<Arc<BackendRequest>>,
    future: impl Future<Output = T>,
) -> T {
    match request {
        Some(request) => BACKEND_REQUEST.scope(request, future).await,
        None => future.await,
    }
}

/// Admits the calling JSON-RPC call to source ClickHouse once; later reads share the permit,
/// and a shed call stays shed. Outside a backend request this returns `None` at once.
pub(crate) async fn admit_backend(
    operation: &'static str,
) -> ProcessingResult<Option<Arc<OwnedSemaphorePermit>>> {
    let Some(request) = current_backend_request() else {
        return Ok(None);
    };
    let permit = request
        .permit
        .get_or_init(|| request.admission.acquire(operation))
        .await;
    match permit {
        Some(permit) => Ok(Some(permit.clone())),
        None => Err(ProcessingError::Database {
            context: format!(
                "backend admission queue full after {:?}",
                request.admission.queue_timeout
            ),
            source: Some(Box::new(BackendAdmissionShed)),
        }),
    }
}

/// Whether `error` is a backend admission shed.
#[cfg(test)]
pub(crate) fn is_backend_shed(error: &ProcessingError) -> bool {
    matches!(
        error,
        ProcessingError::Database { source: Some(source), .. } if source.is::<BackendAdmissionShed>()
    )
}

#[cfg(test)]
mod tests;
