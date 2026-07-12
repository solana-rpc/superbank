-- SPDX-License-Identifier: AGPL-3.0-only

-- One row per (transaction, token account) whose SPL token balance changed.
--
-- Pre and post token balances are stored as parallel arrays keyed by the
-- account index within the transaction; a token account may appear in only
-- one side (created or emptied during the transaction), in which case the
-- missing side counts as zero. `delta_raw` is in base units, `delta` is
-- scaled by the mint decimals.
CREATE VIEW IF NOT EXISTS default.token_transfers AS
SELECT
    signature,
    slot,
    slot_idx,
    block_time,
    token_account,
    mint,
    owner,
    delta_raw,
    decimals,
    delta
FROM
(
    WITH arrayConcat(tx_account_keys, meta_loaded_addresses_writable, meta_loaded_addresses_readonly) AS all_keys
    SELECT
        base58Encode(signature) AS signature,
        slot,
        slot_idx,
        fromUnixTimestamp(block_time) AS block_time,
        base58Encode(all_keys[toUInt16(idx) + 1]) AS token_account,
        indexOf(meta_pre_token_account_index, idx) AS pre_pos,
        indexOf(meta_post_token_account_index, idx) AS post_pos,
        base58Encode(if(post_pos > 0, meta_post_token_mint[post_pos], meta_pre_token_mint[pre_pos])) AS mint,
        if(post_pos > 0, meta_post_token_owner[post_pos], meta_pre_token_owner[pre_pos]) AS owner_raw,
        if(owner_raw IS NULL, NULL, base58Encode(assumeNotNull(owner_raw))) AS owner,
        if(post_pos > 0, toInt128OrZero(meta_post_token_amount[post_pos]), 0)
            - if(pre_pos > 0, toInt128OrZero(meta_pre_token_amount[pre_pos]), 0) AS delta_raw,
        if(post_pos > 0, meta_post_token_decimals[post_pos], meta_pre_token_decimals[pre_pos]) AS decimals,
        toFloat64(delta_raw) / pow(10, decimals) AS delta
    FROM default.transactions
    ARRAY JOIN arrayDistinct(arrayConcat(meta_pre_token_account_index, meta_post_token_account_index)) AS idx
    WHERE is_vote = 0 AND delta_raw != 0
);
