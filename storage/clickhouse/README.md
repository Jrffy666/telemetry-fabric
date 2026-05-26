# ClickHouse Storage

This directory defines the blockchain analytical storage layer for ClickHouse.
It owns schemas, migrations, query templates, writer guidance, partitioning, and
retention policy.

## Layout

- `migrations/` - ordered DDL files.
- `queries/` - reusable analysis SQL templates with ClickHouse parameters.
- `writers/` - Go and Rust writer notes/placeholders.
- `docs/` - schema, partitioning, TTL, and tiering guidance.

## Migration Order

Apply the files in lexical order:

1. `001_blocks.sql`
2. `002_transactions.sql`
3. `003_logs.sql`
4. `004_token_transfers.sql`
5. `005_address_activity.sql`
6. `006_node_health.sql`
7. `007_alerts.sql`
8. `008_reorg_events.sql`
9. `009_discard_metrics.sql`

Example with `clickhouse-client` from the repo root:

```powershell
Get-ChildItem .\storage\clickhouse\migrations\*.sql |
  Sort-Object Name |
  ForEach-Object { clickhouse-client --multiquery --queries-file $_.FullName }
```

The migrations create database `telemetry_fabric`. If an environment requires a
different database, copy the DDL into that environment's migration pipeline and
replace the database qualifier consistently.

## Batch Inserts

Raw and rollup tables are designed for at-least-once batch insertion.

```sql
INSERT INTO telemetry_fabric.chain_token_transfers FORMAT JSONEachRow
```

Use the same stable `dedupe_key` when retrying a raw event. For rollups and
metrics, keep `batch_id` stable for the retried batch. `ReplacingMergeTree`
removes duplicate rows eventually; query with `FINAL` when immediate
deduplication is required.

## Common Queries

Templates in `queries/` cover:

- Address activity.
- Large token transfers.
- Token inflow/outflow.
- RPC health.
- Crawler lag.
- Alert history.
- Block gap detection.
- Duplicate physical row detection.

They use ClickHouse named parameters such as `{tenant_id:String}`.

## Retention And Tiering

Migrations include delete TTLs. Hot/cold storage movement is documented in
`docs/partitioning.md` and `docs/ttl.md`; enable it with `ALTER TABLE` after a
cluster storage policy exists.
