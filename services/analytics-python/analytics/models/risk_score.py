"""Risk score prototype."""

from __future__ import annotations

from dataclasses import dataclass, field
from math import log10
from typing import Mapping


@dataclass(frozen=True)
class RiskScoreBreakdown:
    score: float
    label: str
    components: dict[str, float] = field(default_factory=dict)


def score_address(features: Mapping[str, float]) -> RiskScoreBreakdown:
    """Produce a deterministic 0-100 prototype score from extracted features."""

    components = {
        "blacklist": _blacklist_component(features),
        "large_transfer": _scale_log(features.get("largest_transfer_usd", 0.0), cap=25.0),
        "counterparty": min(features.get("unique_counterparties", 0.0) / 50.0 * 15.0, 15.0),
        "anomaly": min(max(features.get("anomaly_score", 0.0), 0.0) * 20.0, 20.0),
        "velocity": min(features.get("tx_count", 0.0) / 1000.0 * 10.0, 10.0),
    }
    score = min(sum(components.values()), 100.0)
    return RiskScoreBreakdown(score=round(score, 2), label=_label(score), components=components)


def _blacklist_component(features: Mapping[str, float]) -> float:
    if features.get("blacklist_counterparty_count", 0.0) > 0:
        return 30.0
    hops = features.get("blacklist_hops", 99.0)
    if hops <= 1:
        return 25.0
    if hops == 2:
        return 15.0
    if hops == 3:
        return 8.0
    return 0.0


def _scale_log(value: float, cap: float) -> float:
    if value <= 0:
        return 0.0
    return min(log10(value + 1.0) / 7.0 * cap, cap)


def _label(score: float) -> str:
    if score >= 75:
        return "critical"
    if score >= 50:
        return "high"
    if score >= 25:
        return "medium"
    return "low"
