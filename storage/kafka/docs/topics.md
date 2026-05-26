# Topic Contracts

Kafka topics in this directory carry blockchain module traffic through shared
contracts. Producers publish `telemetry.fabric.platform.v1.Envelope`; consumers
route by envelope metadata and decode `payload` according to `event_type` and
`schema_version`.

## Topics

| Topic | Purpose | Primary Payload |
| --- | --- | --- |
| `chain.events.raw` | Raw normalized chain events emitted by adapters before priority routing. | `blockchain.chain_event.*` |
| `chain.events.critical` | Critical events for low-latency operational processing and alerting. | `blockchain.chain_event.*` |
| `chain.events.important` | Important events for near-real-time enrichment and storage pipelines. | `blockchain.chain_event.*` |
| `chain.events.aggregate` | Lower-priority events for aggregation and batch-like stream jobs. | `blockchain.chain_event.*` |
| `chain.events.dead_letter` | Events that failed schema validation, decode, or safe delivery. | `blockchain.dead_letter` |
| `chain.alerts` | Alert events emitted by rule evaluation or stream processors. | `blockchain.alert` |
| `chain.reorgs` | Reorg repair signals used to invalidate or replay affected blocks. | `blockchain.reorg` |
| `chain.node_health` | RPC endpoint and node health observations. | `blockchain.node_health` |

## Contract Rules

- Every message value is an Envelope.
- `tenant_id`, `event_type`, `schema_version`, `priority`, `dedupe_key`, and
  `checkpoint` should be set before publishing.
- Producers should set Kafka headers for `tenant_id`, `schema_version`,
  `event_type`, and `trace_id` when available.
- Domain payloads must match the schema version named in the envelope.
- Unknown fields must be tolerated by consumers.
- Consumers must be idempotent by `dedupe_key` or by the chain coordinates in
  the payload.

## Topic Flow

1. Chain adapters publish normalized events to `chain.events.raw`.
2. Routing or filtering processors split traffic into `critical`, `important`,
   and `aggregate` topics using envelope priority and rules.
3. Rule evaluators publish alert payloads to `chain.alerts`.
4. Reorg detectors publish repair signals to `chain.reorgs`.
5. Health checks publish operational status to `chain.node_health`.
6. Any producer or consumer that cannot safely handle a message writes a
   contextual record to `chain.events.dead_letter`.

## Schema Compatibility

The compatibility mode is `backward`. New schema versions may add optional
fields and enum values. They must not reuse field numbers, remove required
semantics, or change existing field meaning in place.

Consumers should pin the highest schema version they understand and reject only
when a message cannot be safely decoded or interpreted. Rejections go to
`chain.events.dead_letter` with the original topic, partition, offset, and error
class.

## Replay Strategy

`chain.events.raw` is the primary Kafka replay stream. Replay workers should use
Kafka offsets for broker position and envelope `checkpoint` for chain position.
Priority topics are derived streams and can be rebuilt from `chain.events.raw`
when routing rules are deterministic for the target rule version.

Replay rules:

- Record source topic, partition, starting offset, and ending offset before a
  replay run starts.
- Use `dedupe_key` and chain coordinates to make replay idempotent.
- Treat `chain.reorgs` as repair signals that may require invalidating already
  emitted events before replaying replacement blocks.
- Replay from `chain.events.dead_letter` requires manual approval and a repair
  reason.
- Consumers must tolerate duplicate messages during replay.
