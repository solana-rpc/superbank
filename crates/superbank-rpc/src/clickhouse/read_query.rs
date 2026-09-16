// SPDX-License-Identifier: AGPL-3.0-only
//! HTTP SELECT ownership: successful EOF releases capacity; abandonment retains it
//! until the separate control connection observes termination. Writes never use this API.

use std::sync::Arc;
use std::time::Duration;

use clickhouse::error::{Error, Result};
use clickhouse::{Client, Row, RowOwned, RowRead};
use tokio::sync::Semaphore;
use tokio::time::Instant;

pub(crate) mod admission;

use super::disconnect::{DisconnectGuard, DisconnectVerifier};
use super::util::next_required_query_id;
use crate::processing::{ProcessingError, ProcessingResult};

#[derive(Clone)]
pub(crate) struct ReadEndpoint {
    verifier: DisconnectVerifier,
    admission: Arc<Semaphore>,
    timeout: Duration,
    target: &'static str,
}

impl ReadEndpoint {
    #[cfg(feature = "disk-cache")]
    pub(crate) fn with_target(&self, target: &'static str) -> Self {
        Self {
            target,
            ..self.clone()
        }
    }
    pub(crate) fn new(
        control: Client,
        cluster: Option<String>,
        capacity: usize,
        timeout: Duration,
        target: &'static str,
    ) -> Self {
        Self {
            verifier: DisconnectVerifier::new(control, cluster),
            admission: Arc::new(Semaphore::new(capacity.max(1))),
            timeout,
            target,
        }
    }

    pub(crate) fn with_timeout(&self, timeout: Duration) -> Self {
        Self {
            timeout,
            ..self.clone()
        }
    }

    pub(crate) async fn initialize(&self) -> ProcessingResult<()> {
        self.verifier.initialize_ready().await
    }

    #[cfg(any(test, feature = "disk-cache"))]
    pub(crate) fn background(&self, capacity: usize) -> Self {
        Self {
            admission: Arc::new(Semaphore::new(capacity.max(1))),
            target: "background",
            ..self.clone()
        }
    }

    pub(crate) async fn query(
        &self,
        client: &Client,
        sql: &str,
        operation: &'static str,
    ) -> ProcessingResult<ReadQuery> {
        self.query_with_id(client, sql, operation, None).await
    }

    pub(crate) async fn fetch<T: Row>(
        &self,
        client: &Client,
        sql: &str,
        operation: &'static str,
    ) -> Result<ReadCursor<T>> {
        self.query(client, sql, operation)
            .await
            .map_err(|error| Error::Other(Box::new(error)))?
            .fetch::<T>()
    }

    pub(crate) async fn fetch_one<T: RowOwned + RowRead>(
        &self,
        client: &Client,
        sql: &str,
        operation: &'static str,
    ) -> Result<T> {
        self.query(client, sql, operation)
            .await
            .map_err(|error| Error::Other(Box::new(error)))?
            .fetch_one::<T>()
            .await
    }

    pub(crate) async fn fetch_optional<T: RowOwned + RowRead>(
        &self,
        client: &Client,
        sql: &str,
        operation: &'static str,
    ) -> Result<Option<T>> {
        self.query(client, sql, operation)
            .await
            .map_err(|error| Error::Other(Box::new(error)))?
            .fetch_optional::<T>()
            .await
    }

    pub(crate) async fn fetch_all<T: RowOwned + RowRead>(
        &self,
        client: &Client,
        sql: &str,
        operation: &'static str,
    ) -> Result<Vec<T>> {
        self.query(client, sql, operation)
            .await
            .map_err(|error| Error::Other(Box::new(error)))?
            .fetch_all::<T>()
            .await
    }

    pub(crate) async fn query_with_id(
        &self,
        client: &Client,
        sql: &str,
        operation: &'static str,
        id: Option<String>,
    ) -> ProcessingResult<ReadQuery> {
        // No discovery, probes, or IO on the ready query path.
        self.verifier.require_ready()?;
        let deadline = Instant::now() + self.timeout;
        let permit = tokio::time::timeout_at(deadline, self.admission.clone().acquire_owned())
            .await
            .map_err(|e| ProcessingError::timeout("ClickHouse read admission", e))?
            .map_err(|_| ProcessingError::database_msg("ClickHouse read admission closed"))?;
        let id = id.unwrap_or_else(|| next_required_query_id(operation));
        let mut guard = self
            .verifier
            .arm_ready(id.clone(), permit, operation, self.target)?;
        guard.retain_workflow(admission::current());
        Ok(ReadQuery {
            query: client.query(sql).with_setting("query_id", id),
            guard: Some(guard),
            deadline,
        })
    }
}

pub(crate) struct ReadQuery {
    query: clickhouse::query::Query,
    guard: Option<DisconnectGuard>,
    deadline: Instant,
}

impl ReadQuery {
    #[cfg(all(test, feature = "disk-cache"))]
    pub(crate) fn query_id(&self) -> &str {
        self.guard
            .as_ref()
            .expect("read query owns guard")
            .query_id()
    }
    pub(crate) fn retain(&mut self, permit: tokio::sync::OwnedSemaphorePermit) {
        if let Some(guard) = self.guard.as_mut() {
            guard.retain(permit);
        }
    }

    #[cfg(any(test, feature = "disk-cache"))]
    pub(crate) fn with_setting(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        // Keep verification aligned with the submitted query identity.
        let name = name.into();
        let value = value.into();
        if name == "query_id"
            && let Some(guard) = self.guard.as_mut()
        {
            guard.set_query_id(value.clone());
        }
        self.query = self.query.with_setting(name, value);
        self
    }

    fn protected_query(query: clickhouse::query::Query) -> clickhouse::query::Query {
        query
            .with_setting("readonly", "2")
            .with_setting("cancel_http_readonly_queries_on_client_close", "1")
    }

    pub(crate) fn fetch<T: Row>(mut self) -> Result<ReadCursor<T>> {
        let result = Self::protected_query(self.query).fetch::<T>();
        match result {
            Ok(cursor) => {
                if let Some(guard) = self.guard.as_mut() {
                    guard.submitted();
                }
                Ok(ReadCursor {
                    cursor: Some(cursor),
                    guard: self.guard.take(),
                    deadline: self.deadline,
                })
            }
            Err(error) => {
                if let Some(guard) = self.guard.as_mut() {
                    guard.disarm();
                }
                Err(error)
            }
        }
    }

    #[cfg(any(test, feature = "disk-cache"))]
    pub(crate) fn fetch_bytes(mut self, format: impl AsRef<str>) -> Result<ReadBytesCursor> {
        match Self::protected_query(self.query).fetch_bytes(format) {
            Ok(cursor) => {
                if let Some(guard) = self.guard.as_mut() {
                    guard.submitted();
                }
                Ok(ReadBytesCursor {
                    cursor: Some(cursor),
                    guard: self.guard.take(),
                    deadline: self.deadline,
                })
            }
            Err(error) => {
                if let Some(guard) = self.guard.as_mut() {
                    guard.disarm();
                }
                Err(error)
            }
        }
    }

    pub(crate) async fn fetch_optional<T: RowOwned + RowRead>(self) -> Result<Option<T>> {
        self.fetch::<T>()?.next_optional().await
    }

    pub(crate) async fn fetch_one<T: RowOwned + RowRead>(self) -> Result<T> {
        self.fetch_optional::<T>().await?.ok_or(Error::RowNotFound)
    }

    pub(crate) async fn fetch_all<T: RowOwned + RowRead>(self) -> Result<Vec<T>> {
        let mut cursor = self.fetch::<T>()?;
        let mut rows = Vec::new();
        while let Some(row) = cursor.next().await? {
            rows.push(row);
        }
        Ok(rows)
    }
}

// Fields deliberately drop in declaration order: close response BEFORE enqueuing verification.
pub(crate) struct ReadCursor<T> {
    cursor: Option<clickhouse::query::RowCursor<T>>,
    guard: Option<DisconnectGuard>,
    deadline: Instant,
}

impl<T> ReadCursor<T> {
    pub(crate) fn received_bytes(&self) -> u64 {
        self.cursor
            .as_ref()
            .map_or(0, |cursor| cursor.received_bytes())
    }
    pub(crate) fn decoded_bytes(&self) -> u64 {
        self.cursor
            .as_ref()
            .map_or(0, |cursor| cursor.decoded_bytes())
    }
}

impl<T: RowOwned + RowRead> ReadCursor<T> {
    pub(crate) async fn next(&mut self) -> Result<Option<T>> {
        let Some(cursor) = self.cursor.as_mut() else {
            return Err(Error::BadResponse("read cursor already failed".into()));
        };
        let result = tokio::time::timeout_at(self.deadline, cursor.next())
            .await
            .unwrap_or(Err(Error::TimedOut));
        if result.is_err() {
            // Close the connection even if the caller retains this failed cursor.
            self.cursor.take();
            self.guard.take();
        } else if matches!(result, Ok(None))
            && let Some(guard) = self.guard.as_mut()
        {
            guard.disarm();
        }
        result
    }
    /// Return the first row only after the response reaches successful EOF.
    pub(crate) async fn next_optional(&mut self) -> Result<Option<T>> {
        let first = self.next().await?;
        self.finish().await?;
        Ok(first)
    }

    pub(crate) async fn finish(&mut self) -> Result<()> {
        while self.next().await?.is_some() {}
        Ok(())
    }
}

#[cfg(any(test, feature = "disk-cache"))]
pub(crate) struct ReadBytesCursor {
    cursor: Option<clickhouse::query::BytesCursor>,
    guard: Option<DisconnectGuard>,
    deadline: Instant,
}

#[cfg(any(test, feature = "disk-cache"))]
impl ReadBytesCursor {
    pub(crate) async fn next(&mut self) -> Result<Option<axum::body::Bytes>> {
        let Some(cursor) = self.cursor.as_mut() else {
            return Err(Error::BadResponse("read cursor already failed".into()));
        };
        let result = tokio::time::timeout_at(self.deadline, cursor.next())
            .await
            .unwrap_or(Err(Error::TimedOut));
        if result.is_err() {
            // Close the connection even if the caller retains this failed cursor.
            self.cursor.take();
            self.guard.take();
        } else if matches!(result, Ok(None))
            && let Some(guard) = self.guard.as_mut()
        {
            guard.disarm();
        }
        result
    }
}

#[cfg(test)]
mod tests;
