# Chaos Tests

Chaos tests describe controlled failure scenarios for crawler durability and
observability. They are separated from smoke and integration tests because they
may require Docker Compose, Kubernetes, Kafka, ClickHouse, or process control.

## Scenarios

- `kafka_outage_scenario.md`
- `clickhouse_outage_scenario.md`
- `worker_crash_scenario.md`
- `config_rollback_scenario.md`

## Common Acceptance

- Critical events are not lost during a short outage.
- Export retry metrics and lag metrics expose the outage.
- Checkpoints do not advance beyond durable export.
- Recovery does not create duplicate externally visible events.
