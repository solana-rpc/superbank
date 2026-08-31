// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Export one slot's block metadata, entries, and per-transaction signatures
//! as a JSON fixture — used to capture real mainnet blocks as golden test
//! vectors for the verifier.

use std::path::Path;

use anyhow::{Context, Result, bail};
use serde_json::json;
use tracing::info;

use crate::chdb::ChDb;
use crate::cli::Args;
use crate::verify::b58;

pub(crate) async fn export_fixture(args: &Args, slot: u64, out: &Path) -> Result<()> {
    let db = ChDb::new(args);
    let Some(block) = db.fetch_block_at(slot).await? else {
        bail!("slot {slot} not found in {}", db.blocks_table);
    };
    let entries = db
        .fetch_entries(slot, slot)
        .await?
        .remove(&slot)
        .unwrap_or_default();
    let transactions = db
        .fetch_tx_signatures(slot, slot)
        .await?
        .remove(&slot)
        .unwrap_or_default();

    let fixture = json!({
        "slot": block.slot,
        "parent_slot": block.parent_slot,
        "blockhash": b58(&block.blockhash),
        "parent_blockhash": b58(&block.parent_blockhash),
        "executed_transaction_count": block.executed_transaction_count,
        "entry_count": block.entry_count,
        "entries": entries.iter().map(|entry| json!({
            "entry_index": entry.entry_index,
            "num_hashes": entry.num_hashes,
            "hash": b58(&entry.hash),
            "starting_transaction_index": entry.starting_transaction_index,
            "transaction_count": entry.transaction_count,
        })).collect::<Vec<_>>(),
        "transactions": transactions.iter().map(|(slot_idx, signatures)| json!({
            "slot_idx": slot_idx,
            "signatures": signatures.iter().map(|sig| b58(sig)).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
    });

    std::fs::write(out, serde_json::to_vec_pretty(&fixture)?)
        .with_context(|| format!("write fixture to {}", out.display()))?;
    info!(
        slot,
        entries = entries.len(),
        transactions = transactions.len(),
        out = %out.display(),
        "fixture exported"
    );
    Ok(())
}
