# Partitioning And Ordering

All tables are partitioned by chain, network, and a date bucket:

```sql
PARTITION BY (chain, network, toYYYYMM(<date_column>))
```

Monthly date buckets keep part counts manageable while still allowing fast
partition pruning for chain/network/date ranges. If a deployment has very high
volume per chain, daily partitioning can be introduced with
`toYYYYMMDD(<date_column>)`, but it should be validated against part churn.

## Sorting Keys

Sorting keys are chosen around common access paths:

| Table | Sorting key shape |
| --- | --- |
| `chain_blocks` | `tenant_id, chain, network, block_number, block_hash` |
| `chain_transactions` | `tenant_id, chain, network, block_number, tx_index, tx_hash` |
| `chain_logs` | `tenant_id, chain, network, contract_address, block_number, tx_hash, log_index` |
| `chain_token_transfers` | `tenant_id, chain, network, token_address, from_address, to_address, block_number, tx_hash, log_index, transfer_index` |
| `chain_address_activity` | `tenant_id, chain, network, address, activity_date, bucket_start, contract_address, token_address, event_type, address_role, batch_id` |
| `chain_node_health` | `tenant_id, chain, network, node_id, rpc_endpoint_id, checked_at` |
| `chain_alert_events` | `tenant_id, chain, network, alert_id, rule_id, dedupe_key` |
| `chain_reorg_events` | `tenant_id, chain, network, detected_at, common_ancestor_number, old_head_hash, new_head_hash` |
| `chain_discard_metrics` | `tenant_id, chain, network, metric_date, bucket_start, source, event_type, rule_id, rule_version, discard_reason, batch_id` |

The raw event tables also define bloom-filter skip indexes for hashes and
addresses that are not always early in the sort key.

## Hot And Cold Data

Recommended tiering:

- Hot NVMe/local SSD: latest 7-30 days for raw event tables and all node health.
- Warm object-backed or slower SSD volume: 30-180 days for logs/transfers.
- Cold object storage volume: older raw events retained for replay/audit.
- Aggregates: keep `chain_address_activity` and `chain_discard_metrics` longer
  than high-cardinality raw logs if dashboards depend on them.

The migrations only use delete TTL so they work on a default ClickHouse
installation. After a storage policy is configured, add `TO VOLUME 'cold'` TTL
clauses with `ALTER TABLE ... MODIFY TTL`.

Example:

```sql
ALTER TABLE telemetry_fabric.chain_token_transfers
MODIFY TTL
    block_timestamp + INTERVAL 30 DAY TO VOLUME 'cold',
    block_timestamp + INTERVAL 1095 DAY DELETE;
```
