"""Template for new offline analytics jobs.

Copy this file when adding a new batch analysis. Keep reads bounded, write a
manifested report, and do not connect to blockchain RPC endpoints.
"""

from __future__ import annotations

import argparse

from _common import add_runtime_args, clickhouse_from_args, write_job_report


SAFE_SQL = """
SELECT *
FROM telemetry_fabric.chain_token_transfers FINAL
WHERE tenant_id = {tenant_id:String}
  AND chain = {chain:String}
  AND network = {network:String}
  AND block_timestamp >= now64(3, 'UTC') - toIntervalDay({lookback_days:UInt32})
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Offline analytics job template.")
    add_runtime_args(parser)
    parser.add_argument("--lookback-days", type=int, default=7)
    args = parser.parse_args()

    frame = clickhouse_from_args(args).query_dataframe(
        SAFE_SQL,
        {
            "tenant_id": args.tenant_id,
            "chain": args.chain,
            "network": args.network,
            "lookback_days": args.lookback_days,
        },
        limit=args.limit,
    )
    rows = frame.to_dict(orient="records") if hasattr(frame, "to_dict") else list(frame)
    write_job_report(rows, args, job_name="job_template")


if __name__ == "__main__":
    main()
