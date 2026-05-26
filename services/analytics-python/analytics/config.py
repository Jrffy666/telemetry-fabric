"""Configuration loading for offline analytics jobs."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from analytics.clients.clickhouse import ClickHouseConfig, QuerySafetyConfig
from analytics.clients.kafka import KafkaConsumerConfig
from analytics.clients.s3 import S3ParquetConfig
from analytics.exceptions import OptionalDependencyError


@dataclass(frozen=True)
class AnalyticsConfig:
    clickhouse: ClickHouseConfig
    kafka: KafkaConsumerConfig
    s3: S3ParquetConfig
    query_safety: QuerySafetyConfig
    output_dir: str = "reports/out"

    @classmethod
    def from_env(cls) -> "AnalyticsConfig":
        return cls(
            clickhouse=ClickHouseConfig.from_env(),
            kafka=KafkaConsumerConfig.from_env(),
            s3=S3ParquetConfig.from_env(),
            query_safety=QuerySafetyConfig.from_env(),
            output_dir=os.getenv("ANALYTICS_OUTPUT_DIR", "reports/out"),
        )

    @classmethod
    def from_yaml(cls, path: str | Path) -> "AnalyticsConfig":
        raw = _load_yaml(path)
        clickhouse = raw.get("clickhouse", {})
        kafka = raw.get("kafka", {})
        s3 = raw.get("s3", {})
        safety = raw.get("query_safety", {})

        return cls(
            clickhouse=ClickHouseConfig.from_mapping(clickhouse),
            kafka=KafkaConsumerConfig.from_mapping(kafka),
            s3=S3ParquetConfig.from_mapping(s3),
            query_safety=QuerySafetyConfig.from_mapping(safety),
            output_dir=str(raw.get("output_dir", "reports/out")),
        )


def load_config(path: str | Path | None = None) -> AnalyticsConfig:
    if path:
        return AnalyticsConfig.from_yaml(path)
    env_path = os.getenv("ANALYTICS_CONFIG")
    if env_path:
        return AnalyticsConfig.from_yaml(env_path)
    return AnalyticsConfig.from_env()


def _load_yaml(path: str | Path) -> Mapping[str, Any]:
    try:
        import yaml
    except ModuleNotFoundError as exc:
        raise OptionalDependencyError("PyYAML", "YAML configuration loading") from exc

    with Path(path).open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, Mapping):
        raise ValueError("analytics config YAML must contain a mapping at the top level")
    return loaded
