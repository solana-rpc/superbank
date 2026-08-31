## DDL Layout

The schema files are organized by deployment mode:

- `local/`: single-node ClickHouse schemas for local development.
- `cluster/`: clustered schemas with non-replicated shard-local `ReplacingMergeTree` tables.
- `replicated/`: clustered schemas with replicated shard-local `ReplicatedReplacingMergeTree` tables.

Each folder contains the same file basenames:

- `transactions.sql`
- `blocks_metadata.sql`
- `entries.sql`
- `gsfa.sql`
- `gsfa_nohot.sql`
- `gsfa_hot.sql`
- `signatures.sql`
- `token_owner_activity.sql`
- `transfers.sql`

Pick one folder and apply the matching schema set consistently.
The schema files include idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` statements for
additive upgrades. Apply DDL before deploying binaries that write or query newly added columns;
running these repository files never occurs automatically against an existing deployment.
Apply `transactions.sql` before materialized-view files such as `gsfa*.sql`, `signatures.sql`, and
`token_owner_activity.sql`/`transfers.sql`; those views select from the transactions
table and will fail if it does not exist yet.
`transfers.sql` starts indexing with subsequent transaction inserts; it does not backfill existing
transactions. Treat `getTransfersByAddress` as historically complete only after verified coverage
has been recorded using the operational design in
[`docs/get-transfers-by-address-operations.md`](../docs/get-transfers-by-address-operations.md).
`gsfa_nohot.sql` is an alternative to `gsfa.sql`; do not apply both for the same schema set.
`entries.sql` is required for Superbank Fumarole/gRPC source defaults and for PoH entry ingestion
from Old Faithful / Jetstreamer. RPC and Bigtable sources do not populate `entries`.

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
