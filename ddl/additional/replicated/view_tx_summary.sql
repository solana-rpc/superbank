-- SPDX-License-Identifier: AGPL-3.0-only

-- Analyst-friendly projection of default.transactions (non-vote transactions only).
--
-- Keys are re-encoded to base58, and accounts loaded through address lookup
-- tables are merged into `accounts` / `programs`, so filters on those columns
-- do not silently miss v0 transactions. `balance_changes` pairs each account
-- with its lamport delta (zero deltas removed).
--
-- Note: this is a plain (non-materialized) view; encoding happens at query
-- time and predicates on `accounts` scan the underlying table. For fast
-- per-address lookups, resolve (slot, signature) through default.gsfa first
-- (see docs/analyst-views.md).
CREATE VIEW IF NOT EXISTS default.tx_summary ON CLUSTER '{cluster}' AS
WITH arrayConcat(tx_account_keys, meta_loaded_addresses_writable, meta_loaded_addresses_readonly) AS all_keys
SELECT
    base58Encode(signature) AS signature,
    slot,
    slot_idx,
    fromUnixTimestamp(block_time) AS block_time,
    meta_status_ok = 1 AS success,
    meta_err AS err,
    tx_version,
    base58Encode(tx_account_keys[1]) AS fee_payer,
    arrayMap(k -> base58Encode(k), all_keys) AS accounts,
    arrayDistinct(arrayMap(i -> base58Encode(all_keys[toUInt16(i) + 1]), tx_instructions_program_id_index)) AS programs,
    meta_fee AS fee_lamports,
    meta_compute_units_consumed AS compute_units,
    arrayFilter(c -> c.2 != 0,
        arrayZip(
            arrayMap(k -> base58Encode(k), all_keys),
            arrayMap((post, pre) -> toInt64(post) - toInt64(pre), meta_post_balances, meta_pre_balances))) AS balance_changes,
    meta_log_messages AS logs
FROM default.transactions
WHERE is_vote = 0;
