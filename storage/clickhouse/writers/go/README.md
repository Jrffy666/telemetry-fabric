# Go Writer Notes

This directory is reserved for Go batch writer implementations owned by the
ClickHouse storage layer. Do not import crawler-private structs here.

Recommended implementation:

- Use `github.com/ClickHouse/clickhouse-go/v2`.
- Buffer rows per table and write with `PrepareBatch`.
- Generate stable `dedupe_key` values before enqueueing a row.
- Use one `batch_id` per committed rollup/discard batch.
- Retry whole batches; `ReplacingMergeTree` handles duplicate physical rows.
- Normalize UTC timestamps to millisecond precision.
- Lowercase EVM addresses before insert.

Batch insert shape:

```go
batch, err := conn.PrepareBatch(ctx, "INSERT INTO telemetry_fabric.chain_token_transfers")
// batch.Append(...)
// batch.Send()
```
