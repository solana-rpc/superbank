// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::time::Duration;

use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::config::RpcConfig;

const MAX_RESPONSE_BYTES: usize = 64 * 1024;
const FAILURE_RETRY_INTERVAL: Duration = Duration::from_secs(1);

// Agave v4.3.0's WireBlockCertMessage. Keep the wire types local rather than
// pulling the validator's consensus implementation into this storage RPC.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct AgGenesisCertificate {
    block: CertificateBlock,
    signature: CertificateSignature,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct CertificateBlock {
    slot: u64,
    block_id: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct CertificateSignature {
    #[serde(with = "serde_big_array::BigArray")]
    signature: [u8; 192],
    bitmap: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CertificateSourceError {
    NotConfigured,
    Timeout,
    Unavailable,
    InvalidResponse,
    UpstreamRpc(i32),
}

impl CertificateSourceError {
    pub(crate) fn data(self) -> Value {
        match self {
            Self::NotConfigured => json!({"reason": "source_not_configured"}),
            Self::Timeout => json!({"reason": "source_timeout"}),
            Self::Unavailable => json!({"reason": "upstream_unavailable"}),
            Self::InvalidResponse => json!({"reason": "invalid_upstream_response"}),
            Self::UpstreamRpc(code) => json!({
                "reason": if code == -32601 { "upstream_unsupported" } else { "upstream_error" },
                "upstreamCode": code,
            }),
        }
    }
}

type CertificateResult = Result<Option<AgGenesisCertificate>, CertificateSourceError>;

struct Upstream {
    client: Client,
    url: Url,
}

struct CachedEvidence {
    result: CertificateResult,
    expires_at: Option<Instant>,
}

/// One explicitly trusted same-cluster RPC source. A missing source, failed call,
/// or unsupported method is unknown evidence, never a TowerBFT observation.
#[derive(Default)]
pub(crate) struct AgGenesisCertSource {
    upstream: Option<Upstream>,
    timeout: Duration,
    refresh_interval: Duration,
    cache: Mutex<Option<CachedEvidence>>,
}

impl AgGenesisCertSource {
    pub(crate) fn from_config(config: &RpcConfig) -> Result<Self, &'static str> {
        let Some(url) = config.ag_genesis_cert_rpc_url.as_deref() else {
            return Ok(Self::default());
        };
        if config.ag_genesis_cert_rpc_timeout_ms >= config.rpc_request_timeout_ms {
            return Err("AG_GENESIS_CERT_RPC_TIMEOUT_MS must be below RPC_REQUEST_TIMEOUT_MS");
        }
        let url = Url::parse(url.trim()).map_err(|_| "Invalid AG_GENESIS_CERT_RPC_URL")?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || url.fragment().is_some()
        {
            return Err(
                "AG_GENESIS_CERT_RPC_URL must be an HTTP(S) URL with a host and no fragment",
            );
        }
        let timeout = Duration::from_millis(config.ag_genesis_cert_rpc_timeout_ms);
        let client = Client::builder()
            .connect_timeout(timeout)
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|_| "Failed to initialize Alpenglow genesis certificate RPC client")?;
        Ok(Self {
            upstream: Some(Upstream { client, url }),
            timeout,
            refresh_interval: Duration::from_secs(config.ag_genesis_cert_refresh_interval_secs),
            cache: Mutex::new(None),
        })
    }

    pub(crate) async fn get(&self) -> CertificateResult {
        let upstream = self
            .upstream
            .as_ref()
            .ok_or(CertificateSourceError::NotConfigured)?;
        // The same deadline covers waiting for another caller and the complete
        // fetch. Holding the mutex coalesces requests; cancellation releases it.
        let deadline = Instant::now() + self.timeout;
        let mut cache = tokio::time::timeout_at(deadline, self.cache.lock())
            .await
            .map_err(|_| CertificateSourceError::Timeout)?;
        if let Some(evidence) = cache.as_ref()
            && evidence
                .expires_at
                .is_none_or(|expiry| Instant::now() < expiry)
        {
            return evidence.result.clone();
        }
        let result = tokio::time::timeout_at(deadline, upstream.fetch())
            .await
            .unwrap_or(Err(CertificateSourceError::Timeout));
        let ttl = match &result {
            // The certificate on the finalized bank is immutable for this cluster.
            Ok(Some(_)) => None,
            Ok(None) => Some(self.refresh_interval),
            Err(_) => Some(FAILURE_RETRY_INTERVAL),
        };
        *cache = Some(CachedEvidence {
            result: result.clone(),
            expires_at: ttl.map(|ttl| Instant::now() + ttl),
        });
        result
    }
}

impl Upstream {
    async fn fetch(&self) -> CertificateResult {
        let mut response = self
            .client
            .post(self.url.clone())
            .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "getAgGenesisCert"}))
            .send()
            .await
            .map_err(transport_error)?;
        if !response.status().is_success() {
            return Err(CertificateSourceError::Unavailable);
        }
        if response
            .content_length()
            .is_some_and(|len| len > MAX_RESPONSE_BYTES as u64)
        {
            return Err(CertificateSourceError::InvalidResponse);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(transport_error)? {
            if chunk.len() > MAX_RESPONSE_BYTES - bytes.len() {
                return Err(CertificateSourceError::InvalidResponse);
            }
            bytes.extend_from_slice(&chunk);
        }
        decode_response(&bytes)
    }
}

fn transport_error(error: reqwest::Error) -> CertificateSourceError {
    // Never include a reqwest diagnostic or upstream message: URLs may contain credentials.
    if error.is_timeout() {
        CertificateSourceError::Timeout
    } else {
        CertificateSourceError::Unavailable
    }
}

fn decode_response(bytes: &[u8]) -> CertificateResult {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|_| CertificateSourceError::InvalidResponse)?;
    if value.get("jsonrpc") != Some(&json!("2.0")) || value.get("id") != Some(&json!(1)) {
        return Err(CertificateSourceError::InvalidResponse);
    }
    match (value.get("result"), value.get("error")) {
        (Some(Value::Null), None) => Ok(None),
        (Some(result), None) => serde_json::from_value(result.clone())
            .map(Some)
            .map_err(|_| CertificateSourceError::InvalidResponse),
        (None, Some(error)) => {
            let error: crate::rpc::types::JsonRpcError = serde_json::from_value(error.clone())
                .map_err(|_| CertificateSourceError::InvalidResponse)?;
            Err(CertificateSourceError::UpstreamRpc(error.code))
        }
        _ => Err(CertificateSourceError::InvalidResponse),
    }
}

#[cfg(test)]
#[path = "tests/ag_genesis_cert.rs"]
mod tests;
