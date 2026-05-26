# Analytics Python

Offline Python analytics for blockchain data. This service reads durable
outputs from ClickHouse, Kafka replay topics, or S3/Parquet snapshots. It does
not connect to blockchain RPC endpoints and is not part of the crawler hot path.

## Layout

- `analytics/clients/` - ClickHouse, Kafka, and S3/Parquet clients.
- `analytics/features/` - address profiles, token flow, large transfer, and
  risk feature builders.
- `analytics/graph/` - relationship graph and blacklist hop analysis.
- `analytics/models/` - anomaly detection and risk score prototypes.
- `jobs/` - example offline CLI jobs.
- `notebooks/` - notebook skeletons for exploratory analysis.
- `tests/` - dependency-light pytest coverage for the core skeleton.

## Install

```powershell
cd services\analytics-python
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e .
```

Run tests:

```powershell
python -m pip install -e ".[test]"
python -m pytest
```

For notebooks and local development:

```powershell
python -m pip install -e ".[dev]"
```

Lockfile strategy: keep `pyproject.toml` as the source of dependency intent.
Production environments should generate a platform-specific lockfile with
`uv lock`, `pip-tools`, or the deployment system's resolver and commit it next
to the deployment manifest, not inside exploratory notebooks.

## Configuration

Use environment variables or a YAML file. Keep secrets in environment variables
or a secret manager; notebooks should never contain credentials.

```powershell
copy config.example.yaml config.local.yaml
$env:ANALYTICS_CONFIG="config.local.yaml"
$env:CLICKHOUSE_PASSWORD="..."
```

YAML supports `clickhouse`, `kafka`, `s3`, and `query_safety` sections. The
client defaults also read `CLICKHOUSE_*`, `KAFKA_*`, `S3_*`, and
`ANALYTICS_QUERY_*` environment variables.

## ClickHouse

Configure with environment variables:

- `CLICKHOUSE_HOST`
- `CLICKHOUSE_PORT`
- `CLICKHOUSE_USER` or `CLICKHOUSE_USERNAME`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_DATABASE`
- `CLICKHOUSE_SECURE`
- `CLICKHOUSE_CONNECT_TIMEOUT_SECONDS`
- `CLICKHOUSE_SEND_RECEIVE_TIMEOUT_SECONDS`
- `CLICKHOUSE_RETRY_ATTEMPTS`
- `CLICKHOUSE_RETRY_BACKOFF_SECONDS`

Example:

```python
from analytics.clients.clickhouse import ClickHouseClient

client = ClickHouseClient()
df = client.query_dataframe(
    """
    SELECT *
    FROM telemetry_fabric.chain_token_transfers FINAL
    WHERE chain = {chain:String}
      AND network = {network:String}
    """,
    {"chain": "ethereum", "network": "mainnet"},
    limit=100,
)
```

Safety defaults:

- Safe reads only allow `SELECT`/`WITH` statements.
- Queries without `LIMIT` are wrapped with `ANALYTICS_QUERY_DEFAULT_LIMIT`.
- `max_execution_time`, `max_result_rows`, and retry settings are applied.
- Use `query_dataframe_chunks(..., total_limit=..., chunk_size=...)` or
  `stream_row_blocks(...)` for larger reads.

## Kafka

Kafka consumption is for offline replay or feature jobs only. It should not be
used by crawler workers.

Environment variables:

- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_GROUP_ID`
- `KAFKA_TOPICS` as a comma-separated list
- `KAFKA_AUTO_OFFSET_RESET`
- `KAFKA_ENABLE_AUTO_COMMIT`
- `KAFKA_ENABLE_AUTO_OFFSET_STORE`
- `KAFKA_MAX_POLL_RECORDS`
- `KAFKA_POLL_TIMEOUT_SECONDS`

Example:

```python
from analytics.clients.kafka import KafkaConsumerClient

consumer = KafkaConsumerClient()
for records in consumer.record_batches(max_records=500):
    # process records, then commit offsets manually
    consumer.commit(records)
```

## S3 / Parquet

Use S3/Parquet for backfills, snapshots, curated datasets, and notebook
analysis.

Environment variables:

- `S3_ENDPOINT_URL`
- `S3_REGION`
- `S3_ACCESS_KEY_ID`
- `S3_SECRET_ACCESS_KEY`
- `S3_SESSION_TOKEN`

Example:

```python
from analytics.clients.s3 import S3ParquetReader

reader = S3ParquetReader()
df = reader.scan_polars(
    "s3://telemetry-fabric-curated/blockchain/token_transfers/**/*.parquet"
)
```

Prefer `scan_polars`, `iter_pyarrow_batches`, or bounded `read_pandas` calls for
large datasets. Avoid unbounded `read_*` calls in notebooks.

## Example Jobs

```powershell
python jobs\address_profile_job.py --chain ethereum --network mainnet --address 0x... --limit 10000 --output reports\out\address.json
python jobs\token_flow_job.py --chain ethereum --network mainnet --address 0x... --output reports\out\token_flow.json
python jobs\large_transfer_job.py --chain ethereum --network mainnet --min-amount-usd 100000 --output reports\out\large.jsonl
python jobs\blacklist_graph_job.py --chain ethereum --network mainnet --address 0x... --blacklist 0xabc,0xdef --output reports\out\blacklist.json
python jobs\risk_features_job.py --chain ethereum --network mainnet --address 0x... --output reports\out\features.json
python jobs\anomaly_detection_job.py --chain ethereum --network mainnet --field amount_usd --output reports\out\anomalies.json
python jobs\risk_score_job.py --chain ethereum --network mainnet --address 0x... --output reports\out\risk.json
```

Every report writes a sibling `.manifest.json` file with row count, generation
time, job name, and chain/network metadata.

## Boundaries

- No blockchain RPC clients.
- No crawler hot-path participation.
- No imports from Rust, Go, or Elixir services.
- This layer may read durable outputs and write derived analytical artifacts.
