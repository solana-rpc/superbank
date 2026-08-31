-- SPDX-License-Identifier: AGPL-3.0-only

-- One row per (transaction, account) whose SOL balance changed.
--
-- Pre/post balance arrays cover static account keys followed by address
-- lookup table accounts, in that order, so the concat below stays aligned
-- with them. Failed transactions are kept (the fee payer is still debited);
-- use `success` to exclude them.
CREATE VIEW IF NOT EXISTS default.sol_transfers AS
WITH arrayConcat(tx_account_keys, meta_loaded_addresses_writable, meta_loaded_addresses_readonly) AS all_keys
SELECT
    base58Encode(signature) AS signature,
    slot,
    slot_idx,
    fromUnixTimestamp(block_time) AS block_time,
    meta_status_ok = 1 AS success,
    base58Encode(acct) AS account,
    toInt64(post_lamports) - toInt64(pre_lamports) AS delta_lamports,
    delta_lamports / 1e9 AS delta_sol,
    pre_lamports,
    post_lamports
FROM default.transactions
ARRAY JOIN
    all_keys AS acct,
    meta_pre_balances AS pre_lamports,
    meta_post_balances AS post_lamports
WHERE is_vote = 0 AND pre_lamports != post_lamports;
