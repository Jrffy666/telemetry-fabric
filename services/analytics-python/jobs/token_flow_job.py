"""Offline token flow analysis job."""

from __future__ import annotations

import argparse
import json

from analytics.features.token_flow import summarize_token_flow
from _common import add_runtime_args, clickhouse_from_args, jsonable, write_job_report


TRANSFERS_SQL = """
SELECT
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
  AND (from_address = {address:String} OR to_address = {address:String})
  AND block_timestamp >= now64(3, 'UTC') - toIntervalDay({lookback_days:UInt32})
  AND reorged = 0
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Build an offline token flow summary.")
    add_runtime_args(parser)
    parser.add_argument("--address", required=True)
    parser.add_argument("--token-address")
    parser.add_argument("--lookback-days", type=int, default=30)
    args = parser.parse_args()

    transfers = clickhouse_from_args(args).query_dataframe(
        TRANSFERS_SQL,
        {
            "tenant_id": args.tenant_id,
            "chain": args.chain,
            "network": args.network,
            "address": args.address.lower(),
            "lookback_days": args.lookback_days,
        },
        limit=args.limit,
    )
    row = jsonable(summarize_token_flow(transfers, args.address, args.token_address))
    if args.output:
        write_job_report([row], args, job_name="token_flow")
    else:
        print(json.dumps(row, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
