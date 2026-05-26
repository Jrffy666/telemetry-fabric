"""Offline blacklist graph analysis job."""

from __future__ import annotations

import argparse
import json

from analytics.graph.blacklist_hops import blacklist_hop_distance, blacklist_paths
from analytics.graph.flow_graph import build_flow_graph
from _common import add_runtime_args, clickhouse_from_args, jsonable, write_job_report


TRANSFERS_SQL = """
SELECT
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
    parser = argparse.ArgumentParser(description="Evaluate blacklist graph proximity.")
    add_runtime_args(parser)
    parser.add_argument("--address", required=True)
    parser.add_argument("--blacklist", required=True, help="Comma-separated blacklist addresses.")
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--max-hops", type=int, default=3)
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
    blacklist = [item.strip().lower() for item in args.blacklist.split(",") if item.strip()]
    graph = build_flow_graph(transfers)
    row = jsonable(
        {
            "address": args.address.lower(),
            "blacklist_hops": blacklist_hop_distance(
                graph,
                args.address,
                blacklist,
                max_hops=args.max_hops,
            ),
            "paths": blacklist_paths(graph, args.address, blacklist, max_hops=args.max_hops),
        }
    )
    if args.output:
        write_job_report([row], args, job_name="blacklist_graph")
    else:
        print(json.dumps(row, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
