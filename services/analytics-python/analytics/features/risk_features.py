"""Feature extraction for risk scoring prototypes."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

from analytics.features.address_profile import AddressProfile
from analytics.features.token_flow import TokenFlowSummary


def extract_risk_features(
    profile: AddressProfile,
    token_flow: TokenFlowSummary | None = None,
    blacklist_hops: int | None = None,
    anomaly_score: float | None = None,
) -> dict[str, float]:
    """Convert domain summaries into numeric model features."""

    flow = token_flow or TokenFlowSummary(address=profile.address)
    return {
        "tx_count": float(profile.tx_count),
        "sent_count": float(profile.sent_count),
        "received_count": float(profile.received_count),
        "unique_counterparties": float(profile.unique_counterparties),
        "unique_tokens": float(profile.unique_tokens),
        "total_sent_usd": _as_float(profile.total_sent_usd),
        "total_received_usd": _as_float(profile.total_received_usd),
        "net_usd": _as_float(profile.net_usd),
        "largest_transfer_usd": _as_float(profile.largest_transfer_usd),
        "blacklist_counterparty_count": float(profile.blacklist_counterparty_count),
        "token_flow_transfer_count": float(flow.transfer_count),
        "token_flow_net_usd": _as_float(flow.net_usd),
        "blacklist_hops": float(blacklist_hops if blacklist_hops is not None else 99),
        "anomaly_score": float(anomaly_score if anomaly_score is not None else 0.0),
    }


def build_feature_matrix(feature_rows: list[Mapping[str, Any]]) -> tuple[list[str], list[list[float]]]:
    """Build a stable in-memory feature matrix for prototypes/tests."""

    feature_names = sorted({key for row in feature_rows for key in row})
    matrix = [
        [float(row.get(feature_name, 0.0) or 0.0) for feature_name in feature_names]
        for row in feature_rows
    ]
    return feature_names, matrix


def _as_float(value: Decimal | int | float | str) -> float:
    return float(value)
