"""Anomaly detection prototypes for offline feature tables."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import median
from typing import Any, Mapping, Sequence

from analytics.exceptions import OptionalDependencyError


@dataclass
class AnomalyDetector:
    """IsolationForest wrapper for offline experiments."""

    contamination: float = 0.01
    random_state: int = 42

    def __post_init__(self) -> None:
        self._model: Any | None = None

    def fit(self, matrix: Sequence[Sequence[float]]) -> "AnomalyDetector":
        try:
            from sklearn.ensemble import IsolationForest
        except ModuleNotFoundError as exc:
            raise OptionalDependencyError("scikit-learn", "IsolationForest anomaly detection") from exc

        self._model = IsolationForest(
            contamination=self.contamination,
            random_state=self.random_state,
        )
        self._model.fit(matrix)
        return self

    def score(self, matrix: Sequence[Sequence[float]]) -> list[float]:
        if self._model is None:
            raise RuntimeError("AnomalyDetector must be fitted before scoring")
        return [float(value) for value in self._model.decision_function(matrix)]

    def predict(self, matrix: Sequence[Sequence[float]]) -> list[int]:
        if self._model is None:
            raise RuntimeError("AnomalyDetector must be fitted before prediction")
        return [int(value) for value in self._model.predict(matrix)]


def robust_zscore_anomalies(
    rows: Sequence[Mapping[str, Any]],
    field: str,
    threshold: float = 3.5,
) -> list[dict[str, Any]]:
    """Dependency-free anomaly prototype based on median absolute deviation."""

    values = [float(row[field]) for row in rows if row.get(field) is not None]
    if not values:
        return []

    med = median(values)
    deviations = [abs(value - med) for value in values]
    mad = median(deviations)
    if mad == 0:
        return []

    anomalies: list[dict[str, Any]] = []
    for row in rows:
        if row.get(field) is None:
            continue
        value = float(row[field])
        robust_z = 0.6745 * (value - med) / mad
        if abs(robust_z) >= threshold:
            output = dict(row)
            output["robust_zscore"] = robust_z
            anomalies.append(output)
    return anomalies
