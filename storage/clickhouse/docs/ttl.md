# TTL Policy

The migration files define conservative delete TTLs:

| Table | Delete TTL |
| --- | --- |
| `chain_blocks` | 3650 days |
| `chain_transactions` | 1095 days |
| `chain_logs` | 1095 days |
| `chain_token_transfers` | 1095 days |
| `chain_address_activity` | 1095 days |
| `chain_node_health` | 90 days |
| `chain_alert_events` | 365 days |
| `chain_reorg_events` | 3650 days |
| `chain_discard_metrics` | 180 days |

TTL expressions use the event time column for each table, not `ingest_time`.
This keeps retention stable across replay and backfill.

## Operational Notes

- `ttl_only_drop_parts = 1` is enabled so ClickHouse can remove fully expired
  monthly parts cheaply.
- Replays of old data may be deleted quickly if their event time is already past
  TTL. Run backfills into a staging database or temporarily relax TTL when this
  is not desired.
- Use `system.parts` and `system.mutations` to monitor TTL progress.

## Adding Hot/Cold TTL

Define a ClickHouse storage policy first, then alter table TTL. Example policy
names vary by deployment; the SQL below assumes a volume named `cold`.

```sql
ALTER TABLE telemetry_fabric.chain_logs
MODIFY TTL
    block_timestamp + INTERVAL 30 DAY TO VOLUME 'cold',
    block_timestamp + INTERVAL 1095 DAY DELETE;
```

For replicated clusters, run the alter through the same migration mechanism used
for the table DDL and validate that every replica has the volume.
