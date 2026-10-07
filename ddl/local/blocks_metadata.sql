-- SPDX-License-Identifier: AGPL-3.0-only
--
-- Copyright 2025-2026 Triton One Limited. All rights reserved.
--

-- Local-only block metadata table (single-node deployments).
CREATE TABLE IF NOT EXISTS default.blocks_metadata
(
    slot                         UInt64,
    parent_slot                  UInt64,
    blockhash                    FixedString(32),
    parent_blockhash             FixedString(32),
    bank_id                      Nullable(UInt64) DEFAULT NULL,
    bank_hash                    Nullable(FixedString(32)) DEFAULT NULL,
    block_producer_time_nanos    Nullable(UInt64) DEFAULT NULL,
    block_user_agent             Nullable(String) DEFAULT NULL,
    block_time                   Nullable(Int64),
    block_height                 Nullable(UInt64),
    executed_transaction_count   UInt64,
    entry_count                  UInt64,
    rewards_present              UInt8,
    rewards_pubkey               Array(FixedString(32)),
    rewards_lamports             Array(Int64),
    rewards_post_balance         Array(UInt64),
    rewards_type                 Array(Nullable(String)),
    rewards_commission           Array(Nullable(UInt8)),
    rewards_commission_bps       Array(Nullable(UInt16)),
    rewards_num_partitions       Nullable(UInt64)
)
ENGINE = ReplacingMergeTree(slot)
PARTITION BY intDiv(slot, 432000)
ORDER BY (slot);

ALTER TABLE default.blocks_metadata
    ADD COLUMN IF NOT EXISTS rewards_commission_bps Array(Nullable(UInt16))
    AFTER rewards_commission;

ALTER TABLE default.blocks_metadata
    ADD COLUMN IF NOT EXISTS bank_id Nullable(UInt64) DEFAULT NULL AFTER parent_blockhash;

-- Give these columns an explicit default.
ALTER TABLE default.blocks_metadata
    MODIFY COLUMN bank_id Nullable(UInt64) DEFAULT NULL;

ALTER TABLE default.blocks_metadata
    ADD COLUMN IF NOT EXISTS bank_hash Nullable(FixedString(32)) DEFAULT NULL AFTER bank_id;
ALTER TABLE default.blocks_metadata
    ADD COLUMN IF NOT EXISTS block_producer_time_nanos Nullable(UInt64) DEFAULT NULL AFTER bank_hash;
ALTER TABLE default.blocks_metadata
    ADD COLUMN IF NOT EXISTS block_user_agent Nullable(String) DEFAULT NULL AFTER block_producer_time_nanos;

ALTER TABLE default.blocks_metadata
    MODIFY COLUMN bank_hash Nullable(FixedString(32)) DEFAULT NULL;
ALTER TABLE default.blocks_metadata
    MODIFY COLUMN block_producer_time_nanos Nullable(UInt64) DEFAULT NULL;
ALTER TABLE default.blocks_metadata
    MODIFY COLUMN block_user_agent Nullable(String) DEFAULT NULL;
