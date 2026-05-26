# Reports

This directory is reserved for generated offline analytics reports and report
templates.

Reports should read from ClickHouse, Kafka replay topics, or S3/Parquet
snapshots. They must not connect to blockchain RPC endpoints or run inside the
crawler hot path.
