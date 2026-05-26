from __future__ import annotations

import pytest

from analytics.clients.clickhouse import ClickHouseConfig, QuerySafetyConfig
from analytics.clients.kafka import KafkaConsumerClient, KafkaConsumerConfig
from analytics.clients.s3 import S3ParquetConfig


def test_clickhouse_config_from_env(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_HOST", "ch.example")
    monkeypatch.setenv("CLICKHOUSE_PORT", "9440")
    monkeypatch.setenv("CLICKHOUSE_SECURE", "true")

    config = ClickHouseConfig.from_env()

    assert config.host == "ch.example"
    assert config.port == 9440
    assert config.secure is True


def test_clickhouse_config_rejects_invalid_port():
    with pytest.raises(ValueError, match="port"):
        ClickHouseConfig(port=70000)


def test_query_safety_rejects_unbounded_defaults():
    with pytest.raises(ValueError, match="default_limit"):
        QuerySafetyConfig(default_limit=100, max_limit=10)


def test_query_safety_rejects_non_positive_chunk_size():
    with pytest.raises(ValueError, match="chunk_size"):
        QuerySafetyConfig(chunk_size=0)


def test_kafka_message_decode():
    payload = KafkaConsumerClient._decode_message(b'{"event_type":"transfer","value":1}')

    assert payload == {"event_type": "transfer", "value": 1}


def test_s3_config_from_env(monkeypatch):
    monkeypatch.setenv("S3_REGION", "us-east-1")
    monkeypatch.setenv("S3_ENDPOINT_URL", "https://s3.example")

    config = S3ParquetConfig.from_env()

    assert config.region == "us-east-1"
    assert config.endpoint_url == "https://s3.example"


def test_kafka_config_topics_from_env(monkeypatch):
    monkeypatch.setenv("KAFKA_TOPICS", "a,b, c ")

    config = KafkaConsumerConfig.from_env()

    assert config.topics == ("a", "b", "c")
