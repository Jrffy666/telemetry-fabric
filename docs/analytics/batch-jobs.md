# Batch Job Catalog

Run from `services/analytics-python` after installing dependencies.

```powershell
python -m pip install -e ".[test]"
python -m pytest
```

## Jobs

| Job | Purpose |
| --- | --- |
| `address_profile_job.py` | Address profile from token transfer history. |
| `token_flow_job.py` | Inbound/outbound token flow summary. |
| `large_transfer_job.py` | Large transfer extraction with thresholding. |
| `blacklist_graph_job.py` | Relationship graph and blacklist hop distance. |
| `risk_features_job.py` | Numeric risk feature generation. |
| `anomaly_detection_job.py` | Robust z-score anomaly prototype. |
| `risk_score_job.py` | Deterministic prototype score from risk features. |
| `job_template.py` | Starting point for new bounded offline jobs. |

All jobs read ClickHouse analytical tables. They do not connect to blockchain
RPC endpoints and do not participate in real-time collection.

## Output Format

Use `--output` with `.json`, `.jsonl`, or `.parquet`.

```powershell
python jobs\large_transfer_job.py `
  --chain ethereum `
  --network mainnet `
  --min-amount-usd 100000 `
  --limit 50000 `
  --output reports\out\large_transfers.jsonl
```

This writes:

- `large_transfers.jsonl`
- `large_transfers.jsonl.manifest.json`

The manifest contains the job name, chain/network, row count, output format,
and generation timestamp.
