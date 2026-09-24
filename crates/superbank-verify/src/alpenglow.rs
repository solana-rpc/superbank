// SPDX-License-Identifier: AGPL-3.0-only
//! Resolve the first Alpenglow bank from a trusted Agave RPC or an offline pair.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::cli::Args;
use crate::verify::poh::Hash32;

#[derive(Deserialize)]
struct GenesisCert {
    block: GenesisBlock,
    signature: serde_json::Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GenesisBlock {
    slot: u64,
    block_id: Hash32,
}

#[derive(Deserialize)]
struct RpcResponse {
    result: Option<GenesisCert>,
    error: Option<serde_json::Value>,
}

pub(crate) async fn resolve_genesis_block(args: &Args) -> Result<Option<(u64, Hash32)>> {
    let Some(url) = args.alpenglow_rpc_url.as_deref() else {
        return Ok(args.alpenglow_genesis_block);
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("build trusted Agave RPC client")?;
    let response = client
        .post(url)
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAgGenesisCert",
        }))
        .send()
        .await
        .context("call trusted Agave getAgGenesisCert")?
        .error_for_status()
        .context("trusted Agave getAgGenesisCert HTTP error")?
        .json::<RpcResponse>()
        .await
        .context("decode trusted Agave getAgGenesisCert")?;
    if let Some(error) = response.error {
        bail!("trusted Agave getAgGenesisCert returned {error}");
    }
    let remote = response.result.map(|cert| {
        let _ = cert.signature;
        (cert.block.slot, cert.block.block_id)
    });
    if let Some(override_block) = args.alpenglow_genesis_block
        && remote != Some(override_block)
    {
        bail!("trusted Agave genesis certificate does not match --alpenglow-genesis-block");
    }
    Ok(remote)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_agave_genesis_block_identity() {
        let body = serde_json::json!({
            "result": {"block": {"slot": 42, "blockId": vec![7u8; 32]}, "signature": {}},
            "error": null,
        });
        let parsed: RpcResponse = serde_json::from_value(body).unwrap();
        let block = parsed.result.unwrap().block;
        assert_eq!(block.slot, 42);
        assert_eq!(block.block_id, [7; 32]);
    }
}
