-- SPDX-License-Identifier: AGPL-3.0-only
--
-- Copyright 2025-2026 Triton One Limited. All rights reserved.
--

-- Block metadata (per-slot) for fast getBlock-style lookups.
CREATE TABLE IF NOT EXISTS default.blocks_metadata_local ON CLUSTER '{cluster}'
(
    slot                         UInt64,
    parent_slot                  UInt64,
    blockhash                    FixedString(32),
    parent_blockhash             FixedString(32),
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

CREATE TABLE IF NOT EXISTS default.blocks_metadata ON CLUSTER '{cluster}'
(
    slot                         UInt64,
    parent_slot                  UInt64,
    blockhash                    FixedString(32),
    parent_blockhash             FixedString(32),
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
ENGINE = Distributed('{cluster}', 'default', 'blocks_metadata_local', intDiv(slot, 432000));

ALTER TABLE default.blocks_metadata_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS rewards_commission_bps Array(Nullable(UInt16)) AFTER rewards_commission;
ALTER TABLE default.blocks_metadata ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS rewards_commission_bps Array(Nullable(UInt16)) AFTER rewards_commission;
