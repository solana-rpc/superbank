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
`blocks_metadata.sql` also stores the Alpenglow footer fields (`bank_hash`, `block_producer_time_nanos`, `block_user_agent`) and `bank_id`. All of them are nullable with an explicit `DEFAULT NULL`, so older writers can omit them during a rolling deployment. Apply the updated file before deploying any ingestor or backfill that writes `blocks_metadata`. Reapply it even if the columns already exist: the `MODIFY COLUMN` statements repair columns installed without a default, on both shard-local and Distributed tables. Rows without a footer read as `NULL`. That covers blocks before Alpenglow activation and any slot whose footer was not available at ingest time.

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

## Footer columns in `blocks_metadata`

A finalized block row carries its footer fields when the gRPC source holds the matching footer. `bank_id` belongs to one producer subscription and is kept for diagnostics only. Do not join it across subscriptions or use it to repair replay gaps.

`blocks_metadata` is `ReplacingMergeTree(slot) ORDER BY (slot)`, so re-inserting a slot replaces the whole row. The gRPC and Fumarole sources copy the stored footer fields into a replayed row that has none, so a restart does not erase them. RPC, Bigtable and Jetstreamer backfills over an existing range do not, and they write `NULL` footer columns.
