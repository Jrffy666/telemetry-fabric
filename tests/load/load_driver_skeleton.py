#!/usr/bin/env python3
"""Minimal load driver for fixture-backed EVM RPC scenarios."""

from __future__ import annotations

import argparse
import json
import time
from statistics import mean
from typing import Any
from urllib.request import Request, urlopen


def rpc_call(endpoint: str, method: str, params: list[Any]) -> Any:
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    request = Request(endpoint, data=payload, headers={"content-type": "application/json"})
    with urlopen(request, timeout=10) as response:
        body = json.loads(response.read().decode("utf-8"))
    if "error" in body:
        raise RuntimeError(body["error"])
    return body["result"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a simple fixture RPC load loop.")
    parser.add_argument("--rpc", required=True)
    parser.add_argument("--from-block", required=True)
    parser.add_argument("--to-block", required=True)
    parser.add_argument("--iterations", type=int, default=100)
    args = parser.parse_args()

    latencies: list[float] = []
    total_logs = 0
    for _ in range(args.iterations):
        started = time.perf_counter()
        logs = rpc_call(
            args.rpc,
            "eth_getLogs",
            [{"fromBlock": args.from_block, "toBlock": args.to_block}],
        )
        latencies.append((time.perf_counter() - started) * 1000)
        total_logs += len(logs)

    print(
        json.dumps(
            {
                "iterations": args.iterations,
                "total_logs": total_logs,
                "avg_latency_ms": round(mean(latencies), 3) if latencies else 0,
                "max_latency_ms": round(max(latencies), 3) if latencies else 0,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
