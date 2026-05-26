# Schemas

Kafka payload schemas are sourced from `contracts/proto`.

Expected registry subjects:

- `chain.events.raw-value`
- `chain.events.critical-value`
- `chain.events.important-value`
- `chain.events.aggregate-value`
- `chain.events.dead_letter-value`
- `chain.alerts-value`
- `chain.reorgs-value`
- `chain.node_health-value`

All value subjects should use `telemetry.fabric.platform.v1.Envelope` as the
outer message and `BACKWARD` compatibility. Domain payload compatibility is
controlled by the schema named in envelope `schema_version`.
