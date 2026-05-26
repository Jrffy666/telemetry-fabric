"""Deterministic report output helpers."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from analytics.exceptions import OptionalDependencyError


@dataclass(frozen=True)
class ReportManifest:
    path: str
    format: str
    row_count: int
    generated_at: str
    metadata: Mapping[str, Any]


def write_report(
    rows: Iterable[Mapping[str, Any]],
    output_path: str | Path,
    *,
    metadata: Mapping[str, Any] | None = None,
) -> ReportManifest:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    materialized = [dict(row) for row in rows]
    suffix = path.suffix.lower().lstrip(".")

    if suffix == "json":
        path.write_text(
            json.dumps(materialized, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
    elif suffix == "jsonl":
        with path.open("w", encoding="utf-8") as handle:
            for row in materialized:
                handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")
    elif suffix == "parquet":
        try:
            import pandas as pd
        except ModuleNotFoundError as exc:
            raise OptionalDependencyError("pandas", "Parquet report output") from exc
        pd.DataFrame(materialized).to_parquet(path, index=False)
    else:
        raise ValueError("Report output extension must be .json, .jsonl, or .parquet")

    manifest = ReportManifest(
        path=str(path),
        format=suffix,
        row_count=len(materialized),
        generated_at=datetime.now(timezone.utc).isoformat(),
        metadata=metadata or {},
    )
    path.with_suffix(path.suffix + ".manifest.json").write_text(
        json.dumps(asdict(manifest), indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    return manifest
