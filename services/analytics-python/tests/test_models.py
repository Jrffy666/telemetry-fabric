from __future__ import annotations

from analytics.models.anomaly_detection import robust_zscore_anomalies
from analytics.models.risk_score import score_address


def test_robust_zscore_anomalies_flags_outlier():
    rows = [{"value": 10}, {"value": 11}, {"value": 9}, {"value": 1000}]

    anomalies = robust_zscore_anomalies(rows, "value", threshold=3.5)

    assert [row["value"] for row in anomalies] == [1000]


def test_risk_score_uses_blacklist_signal():
    result = score_address(
        {
            "blacklist_counterparty_count": 1,
            "largest_transfer_usd": 1_000_000,
            "unique_counterparties": 20,
            "tx_count": 50,
            "anomaly_score": 0.2,
        }
    )

    assert result.score > 30
    assert result.label in {"medium", "high", "critical"}
    assert result.components["blacklist"] == 30.0
