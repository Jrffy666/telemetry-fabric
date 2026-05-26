"""Offline anomaly detection prototype job."""

from __future__ import annotations

import argparse
import json

from analytics.models.anomaly_detection import robust_zscore_anomalies
from _common import add_runtime_args, clickhouse_from_args, jsonable, write_job_report


FEATURE_SQL = """
SELECT
    address,
    transfer_count,
    amount_usd_value AS amount_usd
FROM telemetry_fabric.chain_address_activity FINAL
WHERE tenant_id = {tenant_id:String}
  AND chain = {chain:String}
  AND network = {network:String}
  AND activity_date >= today() - {lookback_days:UInt32}
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a dependency-light anomaly prototype.")
    add_runtime_args(parser)
    parser.add_argument("--field", default="amount_usd")
    parser.add_argument("--threshold", type=float, default=3.5)
    parser.add_argument("--lookback-days", type=int, default=30)
    args = parser.parse_args()

    frame = clickhouse_from_args(args).query_dataframe(
        FEATURE_SQL,
        {
            "tenant_id": args.tenant_id,
            "chain": args.chain,
            "network": args.network,
            "lookback_days": args.lookback_days,
        },
        limit=args.limit,
    )
    rows = frame.to_dict(orient="records") if hasattr(frame, "to_dict") else list(frame)
    anomalies = jsonable(robust_zscore_anomalies(rows, args.field, threshold=args.threshold))
    if args.output:
        write_job_report(anomalies, args, job_name="anomaly_detection")
    else:
        print(json.dumps(anomalies, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
