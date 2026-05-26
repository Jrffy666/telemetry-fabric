#!/usr/bin/env python3
"""Consistency check skeleton for crawler end-to-end validation.

This file is intentionally executable but does not call real services yet. It
defines the checks that a future CI job should wire to crawler, Kafka, and
ClickHouse endpoints.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass


@dataclass
class ConsistencyCheck:
    name: str
    status: str
    acceptance: str
    source: str


CHECKS = [
    ConsistencyCheck(
        name="historical_backfill_has_no_gaps",
        status="skeleton",
        acceptance="historical backfill block gap count equals 0",
        source="crawler metrics and replay manifest",
    ),
    ConsistencyCheck(
        name="checkpoint_advances_in_order",
        status="skeleton",
        acceptance="checkpoint height never advances past missing or failed ranges",
        source="checkpoint store and worker completion trace",
    ),
    ConsistencyCheck(
        name="reorgs_are_marked_or_corrected",
        status="skeleton",
        acceptance="reorged events are marked reorged and replacement events are emitted",
        source="event sink and reorg metrics",
    ),
    ConsistencyCheck(
        name="duplicates_are_deduped",
        status="skeleton",
        acceptance="duplicate input event keys collapse to one exported event",
        source="dedupe key manifest and sink query",
    ),
    ConsistencyCheck(
        name="critical_data_survives_sink_outage",
        status="skeleton",
        acceptance="Kafka or ClickHouse outage does not lose critical events",
        source="durable buffer, exporter retry metrics, and sink counts",
    ),
    ConsistencyCheck(
        name="metrics_are_observable",
        status="skeleton",
        acceptance="required crawler metrics are present and monotonic counters are sane",
        source="/metrics endpoint",
    ),
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Print crawler consistency check skeleton.")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    args = parser.parse_args()

    payload = [asdict(check) for check in CHECKS]
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return

    for check in CHECKS:
        print(f"[{check.status}] {check.name}: {check.acceptance} ({check.source})")


if __name__ == "__main__":
    main()
