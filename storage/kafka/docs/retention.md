# Retention Policy

Kafka retention balances replay safety with broker cost. Long-term historical
replay should come from chain RPC, object storage, or indexed analytical
storage; Kafka is the short-to-medium replay buffer.

## Retention Matrix

| Topic | Retention | Cleanup | Reason |
| --- | ---: | --- | --- |
| `chain.events.raw` | 14 days | `delete` | Short backfills and replay from adapter output. |
| `chain.events.critical` | 30 days | `delete` | Incident review and operational replay. |
| `chain.events.important` | 14 days | `delete` | Matches raw stream replay window. |
| `chain.events.aggregate` | 7 days | `delete` | Reproducible lower-priority traffic. |
| `chain.events.dead_letter` | 30 days | `delete` | Operator triage and repair. |
| `chain.alerts` | 90 days | `delete` | Audit and incident analysis window. |
| `chain.reorgs` | 90 days | `compact,delete` | Keep latest repair state while bounding storage. |
| `chain.node_health` | 30 days | `compact,delete` | Recent health history plus latest endpoint state. |

## Operational Notes

- `retention.ms` should be derived from `topics.yaml`.
- `retention.bytes` is `-1` by default; enforce per-cluster quotas outside this
  contract if required.
- Compacted topics still need `delete` cleanup so old keys eventually expire.
- Consumers must not rely on Kafka as permanent storage.
- Replay jobs should record starting offsets and checkpoints before consuming.

## Retention Changes

Retention can be increased without a schema change. Reducing retention is an
operationally breaking change and should be announced to stream consumers before
rollout.
