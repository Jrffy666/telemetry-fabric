"""Offline large transfer analysis job."""

from __future__ import annotations

import argparse
import json

from analytics.features.token_flow import detect_large_transfers
from _common import add_runtime_args, clickhouse_from_args, jsonable, write_job_report


TRANSFERS_SQL = """
SELECT
    block_timestamp,
    block_number,
    tx_hash,
    token_address,
    token_symbol,
    from_address,
    to_address,
    amount_usd,
    amount_usd_value
FROM telemetry_fabric.chain_token_transfers FINAL
WHERE tenant_id = {tenant_id:String}
  AND chain = {chain:String}
  AND network = {network:String}
  AND block_timestamp >= now64(3, 'UTC') - toIntervalDay({lookback_days:UInt32})
  AND reorged = 0
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Find large token transfers offline.")
    add_runtime_args(parser)
    parser.add_argument("--min-amount-usd", default="100000")
    parser.add_argument("--lookback-days", type=int, default=7)
    args = parser.parse_args()

    transfers = clickhouse_from_args(args).query_dataframe(
        TRANSFERS_SQL,
        {
            "tenant_id": args.tenant_id,
            "chain": args.chain,
            "network": args.network,
            "lookback_days": args.lookback_days,
        },
        limit=args.limit,
    )
    rows = jsonable(detect_large_transfers(transfers, args.min_amount_usd, limit=args.limit))
    if args.output:
        write_job_report(rows, args, job_name="large_transfers")
    else:
        print(json.dumps(rows, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
