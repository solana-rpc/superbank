## Additional DDL

Optional ClickHouse objects that are **not** part of the base Superbank install.
They are not applied by Docker Compose, Tilt, or the k8s DDL job.

### Analyst-friendly views

Plain (non-materialized) views over `default.transactions`. They store nothing;
cost is query-time only. Apply them after `transactions.sql`.

Same basenames in each deployment-mode folder:

- `local/`: single-node
- `cluster/`: clustered, `ON CLUSTER '{cluster}'`
- `replicated/`: identical to `cluster/` (plain views are not stored)

```bash
# single-node example
for f in ddl/additional/local/view_*.sql; do
  clickhouse-client --multiquery < "$f"
done
```

Semantics, consistency checks, and example queries: `docs/analyst-views.md`.
