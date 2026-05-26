"""Shared helpers for offline analytics jobs."""

from __future__ import annotations

import argparse
from dataclasses import asdict, is_dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from analytics.clients.clickhouse import ClickHouseClient
from analytics.config import load_config
from analytics.reports import write_report


def add_runtime_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", help="Optional YAML config path. Secrets should come from env.")
    parser.add_argument("--tenant-id", default="")
    parser.add_argument("--chain", required=True)
    parser.add_argument("--network", required=True)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--output", help="Output path ending in .json, .jsonl, or .parquet.")


def clickhouse_from_args(args: argparse.Namespace) -> ClickHouseClient:
    config = load_config(args.config)
    return ClickHouseClient(config.clickhouse, config.query_safety)


def write_job_report(
    rows: list[Mapping[str, Any]],
    args: argparse.Namespace,
    *,
    job_name: str,
) -> None:
    output = args.output or str(Path("reports") / "out" / f"{job_name}.json")
    write_report(
        rows,
        output,
        metadata={
            "job": job_name,
            "tenant_id": args.tenant_id,
            "chain": args.chain,
            "network": args.network,
        },
    )


def jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if is_dataclass(value):
        return {key: jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [jsonable(item) for item in value]
    return value
