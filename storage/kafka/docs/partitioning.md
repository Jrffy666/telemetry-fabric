# Partitioning

Partition keys decide ordering, locality, and consumer scaling. Use the smallest
key that preserves the ordering the consumer needs.

## Recommended Keys

| Strategy | Key | Use Case |
| --- | --- | --- |
| `chain_block` | `chain + network + block_number` | Block-order processing, replay, storage writes. |
| `address` | `chain + network + address` | Wallet, contract, and counterparty analysis. |
| `priority` | `priority` | Alert routing and operational queues. |
| `endpoint` | `chain + network + rpc_endpoint_id` | Node health processing. |

## Topic Defaults

- `chain.events.raw`: `chain_block`
- `chain.events.critical`: `priority`
- `chain.events.important`: `chain_block`
- `chain.events.aggregate`: `address`
- `chain.events.dead_letter`: `chain_block`
- `chain.alerts`: `priority`
- `chain.reorgs`: `chain_block`
- `chain.node_health`: `endpoint`

## Ordering Expectations

Kafka only guarantees ordering within a partition. Consumers that need strict
chain order should use `chain_block` and still check block continuity with the
payload checkpoint. Reorg-aware consumers must treat previously observed blocks
as provisional until the configured finality depth is reached.

Address-oriented consumers should use the best available address:

1. `contract_address` for contract analytics.
2. `from_address` or `to_address` for wallet analytics.
3. `token_address` for token-wide analytics.

If a message has no suitable address, use `chain_block` to avoid hot partitions
from empty keys.

## Partition Counts

Initial partition counts are defined in `topics.yaml`. Increasing partitions is
allowed, but it changes key-to-partition assignment for future messages. Do not
increase partitions for order-sensitive consumers without a rollout plan.
