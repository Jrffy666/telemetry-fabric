# Python Consumer Skeleton

This is a wiring guide for Python analytics or stream jobs consuming contract
envelopes from Kafka.

```python
from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class KafkaMessage:
    topic: str
    partition: int
    offset: int
    key: bytes
    value: bytes
    headers: Mapping[str, bytes]


class DeadLetterWriter:
    def write(self, original: KafkaMessage, error_class: str, error: Exception) -> None:
        raise NotImplementedError


def consume_one(message: KafkaMessage, handle_envelope, dlq: DeadLetterWriter) -> None:
    if not message.value:
        dlq.write(message, "missing_required_field", ValueError("empty kafka message value"))
        return

    try:
        handle_envelope(message.value, message.headers)
    except UnsupportedSchemaVersion as exc:
        dlq.write(message, "unsupported_schema_version", exc)
    except DecodeFailed as exc:
        dlq.write(message, "decode_failed", exc)


class UnsupportedSchemaVersion(Exception):
    pass


class DecodeFailed(Exception):
    pass
```

Python consumers should keep schema handling explicit. Treat Kafka offsets as
stream positions and use envelope checkpoints for chain replay semantics.
