"""Example offline address profiling job."""

from __future__ import annotations

import argparse
import json

from analytics.features.address_profile import profile_address, profile_to_dict
from _common import add_runtime_args, clickhouse_from_args, jsonable, write_job_report


TRANSFERS_SQL = """
SELECT
    tx_hash,
    block_timestamp,
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
    parser = argparse.ArgumentParser(description="Build an offline address profile.")
    add_runtime_args(parser)
    parser.add_argument("--address", required=True)
    parser.add_argument("--lookback-days", type=int, default=30)
    args = parser.parse_args()

    client = clickhouse_from_args(args)
    dataframe = client.query_dataframe(
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
    profile = profile_address(dataframe, args.address)
    row = jsonable(profile_to_dict(profile))
    if args.output:
        write_job_report([row], args, job_name="address_profile")
    else:
        print(json.dumps(row, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
