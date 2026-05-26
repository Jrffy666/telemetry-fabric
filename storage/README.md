# Storage

`storage/` contains storage definitions and operational guidance for the data
fabric. It is not a place for service business logic.

## ClickHouse

`storage/clickhouse/` owns analytical storage design:

- Databases and tables.
- Materialized views.
- Partitioning and ordering keys.
- Retention and TTL policy.
- Query patterns and performance notes.
- Migration guidance.

ClickHouse schemas should be derived from shared contracts and documented
module mappings, not from private service structs.

## Kafka

`storage/kafka/` owns streaming backbone definitions:

- Topic naming.
- Partitioning strategy.
- Retention policy.
- Consumer-group conventions.
- Schema references.
- Dead-letter and replay topic conventions.

Kafka topic payloads should reference `contracts/`.

## S3

`storage/s3/` owns object storage layout:

- Bucket and prefix conventions.
- Raw, normalized, and curated zones.
- File formats and compression.
- Lifecycle and retention rules.
- Replay and backfill layout.
- Compaction policy.

S3 objects should use contract-defined schemas or documented column formats.

## Ownership Rules

- Storage schemas are shared coordination points.
- Multiple agents can work in different storage engines concurrently.
- Do not edit the same schema or migration file from multiple agents at once.
- Do not bind storage layouts to private service implementation types.
- Add tests or validation scripts when storage definitions become executable.
