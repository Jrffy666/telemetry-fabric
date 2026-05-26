# Rust Writer Notes

This directory is reserved for Rust batch writer implementations owned by the
ClickHouse storage layer. Keep it contract-driven and independent from
platform-core internals.

Recommended implementation:

- Use the `clickhouse` crate or HTTP `FORMAT JSONEachRow` for simple writers.
- Serialize protobuf contract fields by name.
- Preserve string numeric fields such as `amount_raw`, `amount_decimal`, and
  `amount_usd`.
- Generate stable `dedupe_key` values and rollup `batch_id` values.
- Retry batches idempotently.
- Use UTC `DateTime64(3)` timestamps.

For high-throughput writers, prefer Native format and table-specific structs
that mirror the migration columns.
