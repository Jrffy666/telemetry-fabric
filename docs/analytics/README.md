# Analytics Python Operations

The analytics Python service is an offline and batch analysis layer. It reads
durable data from ClickHouse, Kafka replay topics, and S3/Parquet snapshots. It
must not connect to blockchain RPC endpoints and must not run in the crawler hot
path.

## Dependency Management

`services/analytics-python/pyproject.toml` is the source of dependency intent.

- Runtime dependencies include dataframes, ClickHouse, Kafka, Parquet, graph,
  and prototype ML libraries.
- `.[test]` installs only pytest dependencies.
- `.[notebook]` and `.[viz]` are optional for exploratory work.
- `.[dev]` includes test, notebook, visualization, lint, and type-check tools.

Lockfile policy:

- Generate environment-specific lockfiles with `uv lock`, `pip-tools`, Conda,
  or the deployment platform.
- Do not put credentials or notebook-local paths in lockfiles.
- Keep notebooks free of secrets; use env vars or uncommitted local YAML.

## Data Safety Defaults

ClickHouse reads are bounded by default:

- Safe reads only allow `SELECT` and `WITH`.
- Queries without a `LIMIT` are wrapped with a configured default limit.
- `max_execution_time`, `max_result_rows`, and retry settings are applied.
- Large reads should use `query_dataframe_chunks` or `stream_row_blocks`.

S3/Parquet reads should prefer lazy or streaming APIs:

- `scan_polars` for lazy plans.
- `iter_pyarrow_batches` for batch streaming.
- bounded `read_pandas(..., row_limit=...)` for local experiments.

Kafka is configured for manual offset handling:

1. Poll records.
2. Process records and persist output.
3. Store and commit offsets.

Auto-commit and auto-offset-store are disabled by default.

## Reproducibility

Batch jobs accept `--config`, `--limit`, and `--output`. Report outputs support
JSON, JSONL, and Parquet. Each output writes a sibling manifest containing
metadata and row counts.

Use `services/analytics-python/notebooks/offline_analysis_template.ipynb` for
exploration and `services/analytics-python/jobs/job_template.py` for new batch
jobs.
