# Kafka Storage

`storage/kafka/` defines the blockchain streaming backbone. Topic payloads are
contract messages, not private crawler or processor structs.

Primary files:

- `topics.yaml`: machine-readable topic, retention, partitioning, schema, DLQ,
  and replay policy.
- `docs/topics.md`: topic contract and usage.
- `docs/retention.md`: retention policy.
- `docs/partitioning.md`: partition key strategy.
- `docs/dlq.md`: dead-letter handling.
- Replay policy is documented in `docs/topics.md` and encoded in
  `topics.yaml`.
- `schemas/`: reserved for generated or registry-specific schema artifacts.
- `producers/` and `consumers/`: language skeletons for wiring generated
  contract types to Kafka clients.

All blockchain event topics use `telemetry.fabric.platform.v1.Envelope` as the
wire boundary. Domain payloads should match the protobuf contracts under
`contracts/proto/blockchain`.

Services must not bypass contracts by publishing private implementation structs.
