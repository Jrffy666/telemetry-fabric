#!/usr/bin/env python3
"""Fixture replay validator for crawler integration scenarios."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any


def hex_to_int(value: str | int) -> int:
    if isinstance(value, int):
        return value
    return int(value, 16) if value.startswith("0x") else int(value)


def event_key(log: dict[str, Any], chain_id: int) -> str:
    return "|".join(
        [
            str(chain_id),
            log["blockHash"],
            log["transactionHash"],
            str(hex_to_int(log["logIndex"])),
        ]
    )


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def collect_logs(blocks: list[dict[str, Any]], gaps: set[int]) -> list[dict[str, Any]]:
    logs: list[dict[str, Any]] = []
    for block in blocks:
        if hex_to_int(block["number"]) in gaps:
            continue
        logs.extend(block.get("logs", []))
    return logs


def detect_parent_breaks(blocks: list[dict[str, Any]], gaps: set[int]) -> list[dict[str, Any]]:
    ordered = sorted(blocks, key=lambda block: hex_to_int(block["number"]))
    breaks: list[dict[str, Any]] = []
    previous: dict[str, Any] | None = None
    for block in ordered:
        number = hex_to_int(block["number"])
        if number in gaps:
            continue
        if previous is not None:
            previous_number = hex_to_int(previous["number"])
            if number == previous_number + 1 and block["parentHash"] != previous["hash"]:
                breaks.append(
                    {
                        "height": block["number"],
                        "parentHash": block["parentHash"],
                        "expectedParentHash": previous["hash"],
                    }
                )
        previous = block
    return breaks


def summarize(scenario: dict[str, Any]) -> dict[str, Any]:
    chain_id = hex_to_int(scenario.get("chain_id", "0x0"))
    gaps = {hex_to_int(height) for height in scenario.get("gaps", [])}
    blocks = scenario.get("blocks", [])
    logs = collect_logs(blocks, gaps)
    keys = [event_key(log, chain_id) for log in logs]
    duplicate_keys = sorted(key for key, count in Counter(keys).items() if count > 1)

    return {
        "scenario": scenario.get("name", "unnamed"),
        "chain_id": chain_id,
        "head": scenario.get("head"),
        "finalized": scenario.get("finalized"),
        "block_count": len(blocks),
        "gap_count": len(gaps),
        "gaps": sorted(hex(height) for height in gaps),
        "log_count": len(logs),
        "unique_event_key_count": len(set(keys)),
        "duplicate_event_keys": duplicate_keys,
        "parent_breaks": detect_parent_breaks(blocks, gaps),
        "reorg_count": len(scenario.get("reorgs", [])),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate crawler replay fixture metadata.")
    parser.add_argument("--scenario", required=True, type=Path)
    parser.add_argument("--expect-gap", action="store_true")
    parser.add_argument("--expect-duplicates", action="store_true")
    parser.add_argument("--expect-reorg", action="store_true")
    args = parser.parse_args()

    summary = summarize(load_json(args.scenario))
    errors: list[str] = []

    if args.expect_gap and summary["gap_count"] == 0:
        errors.append("expected at least one block gap")
    if not args.expect_gap and summary["gap_count"] != 0:
        errors.append(f"unexpected block gaps: {summary['gaps']}")
    if args.expect_duplicates and not summary["duplicate_event_keys"]:
        errors.append("expected duplicate event keys")
    if not args.expect_duplicates and summary["duplicate_event_keys"]:
        errors.append(f"unexpected duplicate event keys: {summary['duplicate_event_keys']}")
    if args.expect_reorg and summary["reorg_count"] == 0:
        errors.append("expected at least one reorg")
    if not args.expect_reorg and summary["reorg_count"] != 0:
        errors.append(f"unexpected reorg count: {summary['reorg_count']}")
    if summary["parent_breaks"]:
        errors.append(f"parent hash continuity breaks: {summary['parent_breaks']}")

    print(json.dumps(summary, indent=2, sort_keys=True))
    if errors:
        raise SystemExit("; ".join(errors))


if __name__ == "__main__":
    main()
