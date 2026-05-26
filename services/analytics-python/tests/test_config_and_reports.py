from __future__ import annotations

import json

import pytest

from analytics.config import AnalyticsConfig
from analytics.reports import write_report


def test_yaml_config_loading(tmp_path):
    pytest.importorskip("yaml")
    config_path = tmp_path / "analytics.yaml"
    config_path.write_text(
        """
clickhouse:
  host: clickhouse.internal
  port: 8443
query_safety:
  default_limit: 123
kafka:
  topics:
    - replay.topic
s3:
  batch_size: 2048
output_dir: reports/test
""",
        encoding="utf-8",
    )

    config = AnalyticsConfig.from_yaml(config_path)

    assert config.clickhouse.host == "clickhouse.internal"
    assert config.clickhouse.port == 8443
    assert config.query_safety.default_limit == 123
    assert config.kafka.topics == ("replay.topic",)
    assert config.s3.batch_size == 2048


def test_write_jsonl_report_with_manifest(tmp_path):
    report_path = tmp_path / "report.jsonl"

    manifest = write_report(
        [{"address": "0xaaa", "score": 42}],
        report_path,
        metadata={"job": "unit_test"},
    )

    assert manifest.row_count == 1
    assert report_path.read_text(encoding="utf-8").strip() == '{"address": "0xaaa", "score": 42}'
    manifest_payload = json.loads(
        report_path.with_suffix(".jsonl.manifest.json").read_text(encoding="utf-8")
    )
    assert manifest_payload["metadata"]["job"] == "unit_test"
