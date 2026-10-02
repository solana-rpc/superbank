// SPDX-License-Identifier: AGPL-3.0-only
//! Resolve the last historical PoH slot from trusted cluster certificate evidence.

use crate::cli::Args;
use crate::verify::poh::Hash32;
use anyhow::{Result, anyhow, bail, ensure};
use serde::Deserialize;
use std::time::Duration;

const MAX_CERT_RESPONSE_BYTES: usize = 64 * 1024;

#[derive(Deserialize)]
struct GenesisCert {
    block: GenesisBlock,
    signature: GenesisSignature,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GenesisBlock {
    slot: u64,
    block_id: Hash32,
}

#[derive(Deserialize)]
struct GenesisSignature {
    signature: Vec<u8>,
    bitmap: Vec<u8>,
}

fn decode_response(body: &[u8]) -> Result<Option<(u64, Hash32)>> {
    let value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|_| anyhow!("malformed trusted getAgGenesisCert JSON"))?;
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("malformed trusted RPC envelope"))?;
    ensure!(
        object.get("jsonrpc") == Some(&serde_json::json!("2.0"))
            && object.get("id") == Some(&serde_json::json!(1)),
        "malformed trusted RPC envelope"
    );
    if let Some(error) = object.get("error") {
        ensure!(
            !object.contains_key("result"),
            "ambiguous trusted RPC envelope"
        );
        let code = error
            .get("code")
            .and_then(|code| code.as_i64())
            .ok_or_else(|| anyhow!("malformed trusted RPC error"))?;
        ensure!(
            error
                .get("message")
                .is_some_and(|message| message.is_string()),
            "malformed trusted RPC error"
        );
        if code == -32601 {
            bail!("trusted getAgGenesisCert is unsupported");
        }
        bail!("trusted getAgGenesisCert returned RPC error {code}");
    }
    let result = object
        .get("result")
        .ok_or_else(|| anyhow!("trusted RPC response has no result"))?;
    if result.is_null() {
        return Ok(None);
    }
    let cert: GenesisCert = serde_json::from_value(result.clone())
        .map_err(|_| anyhow!("malformed trusted genesis certificate"))?;
    ensure!(
        cert.signature.signature.len() == 192,
        "malformed trusted genesis certificate signature"
    );
    let _ = cert.signature.bitmap; // Wire shape is checked; BLS/stake validity comes from operator trust.
    Ok(Some((cert.block.slot, cert.block.block_id)))
}

async fn fetch_genesis_block(url: &str) -> Result<Option<(u64, Hash32)>> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|_| anyhow!("build trusted Agave RPC client"))?;
    let mut response = client
        .post(url)
        .json(&serde_json::json!({"jsonrpc":"2.0", "id":1, "method":"getAgGenesisCert"}))
        .send()
        .await
        .map_err(|_| anyhow!("trusted getAgGenesisCert is unavailable"))?;
    ensure!(
        response.status().is_success(),
        "trusted getAgGenesisCert HTTP status {}",
        response.status().as_u16()
    );
    ensure!(
        response
            .content_length()
            .is_none_or(|len| len <= MAX_CERT_RESPONSE_BYTES as u64),
        "trusted certificate response exceeds size limit"
    );
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| anyhow!("trusted certificate response failed"))?
    {
        ensure!(
            body.len().saturating_add(chunk.len()) <= MAX_CERT_RESPONSE_BYTES,
            "trusted certificate response exceeds size limit"
        );
        body.extend_from_slice(&chunk);
    }
    decode_response(&body)
}

fn reconcile_genesis_block(
    remote: Option<(u64, Hash32)>,
    offline: Option<(u64, Hash32)>,
) -> Result<Option<(u64, Hash32)>> {
    ensure!(
        offline.is_none_or(|block| remote == Some(block)),
        "trusted Agave genesis certificate does not match --alpenglow-genesis-block"
    );
    Ok(remote)
}

pub(crate) async fn resolve_genesis_block(args: &Args) -> Result<Option<(u64, Hash32)>> {
    let Some(url) = args.alpenglow_rpc_url.as_deref() else {
        return Ok(args.alpenglow_genesis_block);
    };
    reconcile_genesis_block(
        fetch_genesis_block(url).await?,
        args.alpenglow_genesis_block,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    fn certificate(slot: u64) -> serde_json::Value {
        serde_json::json!({"jsonrpc":"2.0", "id":1, "result":{"block":{"slot":slot,"blockId":vec![7;32]}, "signature":{"signature":vec![8;192],"bitmap":[1,255]}}})
    }
    fn decode(value: serde_json::Value) -> Result<Option<(u64, Hash32)>> {
        decode_response(&serde_json::to_vec(&value).unwrap())
    }
    #[test]
    fn trusted_null_and_valid_certificate_are_distinct_evidence() {
        assert_eq!(
            decode(serde_json::json!({"jsonrpc":"2.0","id":1,"result":null})).unwrap(),
            None
        );
        for slot in [0, 42, u64::MAX] {
            assert_eq!(decode(certificate(slot)).unwrap(), Some((slot, [7; 32])));
        }
        assert!(reconcile_genesis_block(None, Some((42, [7; 32]))).is_err());
        assert!(reconcile_genesis_block(Some((42, [7; 32])), Some((42, [8; 32]))).is_err());
        assert_eq!(
            reconcile_genesis_block(Some((42, [7; 32])), Some((42, [7; 32]))).unwrap(),
            Some((42, [7; 32]))
        );
    }
    #[test]
    fn malformed_and_unsupported_evidence_never_becomes_null() {
        for value in [
            serde_json::json!({}),
            serde_json::json!({"jsonrpc":"2.0","id":1}),
            serde_json::json!({"jsonrpc":"2.0","id":2,"result":null}),
            serde_json::json!({"jsonrpc":"1.0","id":1,"result":null}),
            serde_json::json!({"jsonrpc":"2.0","id":1,"result":null,"error":{"code":-32601,"message":"secret"}}),
            serde_json::json!({"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"secret"}}),
        ] {
            let err = decode(value).unwrap_err().to_string();
            assert!(!err.contains("secret"));
        }
        for path in ["blockId", "signature", "bitmap"] {
            let mut cert = certificate(42);
            if path == "blockId" {
                cert["result"]["block"][path] = serde_json::json!([1]);
            } else {
                cert["result"]["signature"][path] = serde_json::json!([256]);
            }
            assert!(decode(cert).is_err());
        }
        assert!(decode_response(b"not json").is_err());
    }
    #[tokio::test]
    async fn discovery_rejects_http_redirect_oversize_and_transport_failure() {
        use axum::{Router, http::StatusCode, response::IntoResponse};
        use tokio::net::TcpListener;
        for (status, body) in [
            (
                StatusCode::OK,
                serde_json::to_string(&certificate(42)).unwrap(),
            ),
            (StatusCode::OK, " ".repeat(MAX_CERT_RESPONSE_BYTES + 1)),
            (StatusCode::FOUND, "".into()),
            (StatusCode::SERVICE_UNAVAILABLE, "credentials".into()),
        ] {
            let app = Router::new().fallback(move || {
                let body = body.clone();
                async move { (status, body).into_response() }
            });
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let result = fetch_genesis_block(&format!("http://secret:password@{addr}")).await;
            match (status, result) {
                (StatusCode::OK, Ok(block)) => assert_eq!(block, Some((42, [7; 32]))),
                (_, Err(error)) => {
                    let error = error.to_string();
                    assert!(!error.contains("secret"));
                    assert!(!error.contains("password"));
                }
                _ => panic!("failed HTTP evidence must not resolve a certificate"),
            }
            server.abort();
        }
        assert!(
            fetch_genesis_block("http://secret:password@127.0.0.1:1")
                .await
                .unwrap_err()
                .to_string()
                .contains("unavailable")
        );
    }
}
