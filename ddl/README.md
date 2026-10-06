## DDL Layout

The schema files are organized by deployment mode:

- `local/`: single-node ClickHouse schemas for local development.
- `cluster/`: clustered schemas with non-replicated shard-local `ReplacingMergeTree` tables.
- `replicated/`: clustered schemas with replicated shard-local `ReplicatedReplacingMergeTree` tables.

Each folder contains the same file basenames:

- `transactions.sql`
- `blocks_metadata.sql`
- `block_footers.sql`
- `entries.sql`
- `gsfa.sql`
- `gsfa_nohot.sql`
- `gsfa_hot.sql`
- `signatures.sql`
- `token_owner_activity.sql`

Pick one folder and apply the matching schema set consistently.
The schema files include idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` statements for
additive upgrades. Apply DDL before deploying binaries that write or query newly added columns;
running these repository files never occurs automatically against an existing deployment.
Apply `transactions.sql` before materialized-view files such as `gsfa*.sql`, `signatures.sql`, and
`token_owner_activity.sql`; those views select from the transactions table and will fail if it does
not exist yet.
`gsfa_nohot.sql` is an alternative to `gsfa.sql`; do not apply both for the same schema set.
`entries.sql` is required for Superbank Fumarole/gRPC source defaults and for PoH entry ingestion
from Old Faithful / Jetstreamer. RPC and Bigtable sources do not populate `entries`.
`block_footers.sql` stores the Alpenglow bank hash, producer time, and user agent from an Agave 4.3 Yellowstone gRPC source. Apply it before deploying a gRPC ingestor. Every writer, including RPC and Bigtable backfills, requires the updated `blocks_metadata.sql` before upgrading. Its CREATE/ALTER statements give `bank_id` an explicit `DEFAULT NULL`, allowing older writers to omit the column during a rolling deployment. Reapply the file even if the column already exists: the `MODIFY COLUMN` repairs previously installed no-default columns on both shard-local and Distributed tables. Historical rows retain NULL identity.

Agave 4.2 adds nullable v1 transaction-config columns. Apply `transactions.sql` before upgrading
the RPC or ingestor binaries; old rows and Parquet archives naturally read as `NULL`. Reapply the
selected GSFA and token-owner materialized-view files; their idempotent `ALTER TABLE ... MODIFY
QUERY` statements update existing views without dropping stored data so memo-v4 is recognized for
new rows. The rebuild scripts under `scripts/analysis/` are optional historical backfills for
memo-v4 transactions ingested before this deployment and do not need to run during the online
upgrade.

GSFA note:
- Current GSFA DDL defines `default.gsfa` as the materialized view and query surface.
- In clustered deployments, `default.gsfa` uses `ENGINE = Distributed(..., 'gsfa_local',
  cityHash64(address))`, so derived rows are routed to the correct shard-local `gsfa_local`
  storage table.

Optional analyst-friendly views over `default.transactions` live in `additional/` and are not
applied by the base install (Compose, Tilt, k8s). See `additional/README.md` and
`docs/analyst-views.md`.

## Finalized footer identity and existing-table migration

New `block_footers` storage tables use `ORDER BY (slot)` with ReplacingMergeTree. Only finalized canonical footers are written; `bank_id` is retained for diagnostics but belongs to a producer subscription and cannot be a cross-reconnect dedup key. Reingesting a finalized slot with a different node-local bank ID replaces the row. Do not join stored bank IDs across subscriptions or use them to repair replay gaps.

Existing `(slot, bank_id)` tables require a planned rebuild: `CREATE IF NOT EXISTS` cannot change their key, and ClickHouse cannot shorten the existing primary key with `MODIFY ORDER BY`. Pause footer writes, retain/export the old table, audit same-slot records for conflicting bank hashes, and create a replacement from the matching new schema with a temporary name. Copy one qualified canonical row per slot, verify counts and hashes, then rename/swap storage tables while writers are paused. For clusters perform this per shard and update the Distributed target; for replicated tables use a distinct Keeper path for the replacement and verify all replicas before swapping. Keep the original table as rollback evidence. Do not blindly collapse conflicting bank hashes with an arbitrary bank ID. Resume writes only after qualification. This is a footer-only migration; transaction, entry and block-metadata data stay slot-keyed throughout.
