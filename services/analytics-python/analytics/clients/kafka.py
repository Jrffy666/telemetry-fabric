"""Kafka consumer client for offline replay and feature jobs."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from analytics.exceptions import OptionalDependencyError


@dataclass(frozen=True)
class KafkaRecord:
    payload: dict[str, Any]
    topic: str
    partition: int
    offset: int
    key: str | None = None
    timestamp_ms: int | None = None


@dataclass(frozen=True)
class KafkaConsumerConfig:
    bootstrap_servers: str = "localhost:9092"
    group_id: str = "analytics-python"
    topics: Sequence[str] = field(default_factory=tuple)
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    enable_auto_offset_store: bool = False
    max_poll_records: int = 500
    poll_timeout_seconds: float = 1.0
    extra_config: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_env(cls, prefix: str = "KAFKA_") -> "KafkaConsumerConfig":
        topics = tuple(
            item.strip()
            for item in os.getenv(f"{prefix}TOPICS", "").split(",")
            if item.strip()
        )
        return cls(
            bootstrap_servers=os.getenv(f"{prefix}BOOTSTRAP_SERVERS", cls.bootstrap_servers),
            group_id=os.getenv(f"{prefix}GROUP_ID", cls.group_id),
            topics=topics,
            auto_offset_reset=os.getenv(f"{prefix}AUTO_OFFSET_RESET", cls.auto_offset_reset),
            enable_auto_commit=_env_bool(f"{prefix}ENABLE_AUTO_COMMIT", cls.enable_auto_commit),
            enable_auto_offset_store=_env_bool(
                f"{prefix}ENABLE_AUTO_OFFSET_STORE",
                cls.enable_auto_offset_store,
            ),
            max_poll_records=int(os.getenv(f"{prefix}MAX_POLL_RECORDS", str(cls.max_poll_records))),
            poll_timeout_seconds=float(
                os.getenv(f"{prefix}POLL_TIMEOUT_SECONDS", str(cls.poll_timeout_seconds))
            ),
        )

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any] | None) -> "KafkaConsumerConfig":
        values = values or {}
        topics = values.get("topics", ())
        if isinstance(topics, str):
            topics = tuple(item.strip() for item in topics.split(",") if item.strip())
        return cls(
            bootstrap_servers=str(values.get("bootstrap_servers", cls.bootstrap_servers)),
            group_id=str(values.get("group_id", cls.group_id)),
            topics=tuple(topics),
            auto_offset_reset=str(values.get("auto_offset_reset", cls.auto_offset_reset)),
            enable_auto_commit=_bool_value(
                values.get("enable_auto_commit", cls.enable_auto_commit)
            ),
            enable_auto_offset_store=_bool_value(
                values.get("enable_auto_offset_store", cls.enable_auto_offset_store)
            ),
            max_poll_records=int(values.get("max_poll_records", cls.max_poll_records)),
            poll_timeout_seconds=float(
                values.get("poll_timeout_seconds", cls.poll_timeout_seconds)
            ),
            extra_config=dict(values.get("extra_config", {})),
        )


class KafkaConsumerClient:
    """Small wrapper around confluent-kafka Consumer.

    This is for asynchronous analytics consumption only. It should not be used
    by crawler workers or any latency-sensitive ingest path.
    """

    def __init__(self, config: KafkaConsumerConfig | None = None) -> None:
        self.config = config or KafkaConsumerConfig.from_env()
        self._consumer: Any | None = None

    def connect(self) -> Any:
        if self._consumer is None:
            try:
                from confluent_kafka import Consumer
            except ModuleNotFoundError as exc:
                raise OptionalDependencyError("confluent-kafka", "Kafka consumption") from exc

            settings = {
                "bootstrap.servers": self.config.bootstrap_servers,
                "group.id": self.config.group_id,
                "auto.offset.reset": self.config.auto_offset_reset,
                "enable.auto.commit": self.config.enable_auto_commit,
                "enable.auto.offset.store": self.config.enable_auto_offset_store,
                **dict(self.config.extra_config),
            }
            self._consumer = Consumer(settings)
            if self.config.topics:
                self._consumer.subscribe(list(self.config.topics))
        return self._consumer

    def poll_batch(
        self,
        max_records: int | None = None,
        timeout_seconds: float | None = None,
    ) -> list[dict[str, Any]]:
        return [
            record.payload
            for record in self.poll_records(
                max_records=max_records,
                timeout_seconds=timeout_seconds,
            )
        ]

    def poll_records(
        self,
        max_records: int | None = None,
        timeout_seconds: float | None = None,
    ) -> list[KafkaRecord]:
        consumer = self.connect()
        record_limit = max_records or self.config.max_poll_records
        timeout = timeout_seconds if timeout_seconds is not None else self.config.poll_timeout_seconds
        records: list[KafkaRecord] = []

        for _ in range(record_limit):
            msg = consumer.poll(timeout)
            if msg is None:
                break
            if msg.error():
                raise RuntimeError(str(msg.error()))
            records.append(self._decode_record(msg))
        return records

    def batches(
        self,
        max_records: int | None = None,
        timeout_seconds: float | None = None,
    ) -> Iterable[list[dict[str, Any]]]:
        while True:
            batch = self.poll_batch(max_records=max_records, timeout_seconds=timeout_seconds)
            if not batch:
                break
            yield batch

    def record_batches(
        self,
        max_records: int | None = None,
        timeout_seconds: float | None = None,
    ) -> Iterable[list[KafkaRecord]]:
        while True:
            batch = self.poll_records(max_records=max_records, timeout_seconds=timeout_seconds)
            if not batch:
                break
            yield batch

    def store_offsets(self, records: Sequence[KafkaRecord]) -> None:
        """Store offsets after processing but before committing.

        This supports the manual strategy:
        poll -> process -> store_offsets -> commit.
        """

        if not records:
            return
        try:
            from confluent_kafka import TopicPartition
        except ModuleNotFoundError as exc:
            raise OptionalDependencyError("confluent-kafka", "Kafka offset management") from exc

        offsets = [
            TopicPartition(record.topic, record.partition, record.offset + 1)
            for record in records
        ]
        self.connect().store_offsets(offsets=offsets)

    def commit(self, records: Sequence[KafkaRecord] | None = None) -> None:
        if records is not None:
            self.store_offsets(records)
        self.connect().commit(asynchronous=False)

    def close(self) -> None:
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None

    @staticmethod
    def _decode_message(value: bytes | str | None) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        payload = json.loads(value)
        if not isinstance(payload, dict):
            raise ValueError("Kafka message payload must decode to a JSON object")
        return payload

    @classmethod
    def _decode_record(cls, msg: Any) -> KafkaRecord:
        key = msg.key()
        if isinstance(key, bytes):
            key = key.decode("utf-8")
        timestamp_type, timestamp_ms = msg.timestamp()
        return KafkaRecord(
            payload=cls._decode_message(msg.value()),
            topic=msg.topic(),
            partition=int(msg.partition()),
            offset=int(msg.offset()),
            key=key,
            timestamp_ms=int(timestamp_ms) if timestamp_type else None,
        )


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return _bool_value(value)


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes"}
    return bool(value)
