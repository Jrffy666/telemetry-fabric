# Dead Letter Policy

`chain.events.dead_letter` stores messages that cannot be safely processed.
DLQ records are operational signals and repair inputs, not a replacement for
normal retry behavior.

## When To Dead-Letter

Send a message to the DLQ when:

- Envelope schema validation fails.
- Domain payload cannot be decoded.
- Required routing fields are missing.
- `schema_version` is unsupported by the consumer.
- A consumer detects an invariant violation and cannot safely continue.

Do not dead-letter transient broker, network, or downstream availability errors
until normal producer or consumer retries are exhausted.

## Required Context

DLQ messages should include these attributes in the envelope or payload:

- `original_topic`
- `original_partition`
- `original_offset`
- `original_key`
- `consumer_group`
- `error_class`
- `error_message`
- `failed_at`
- `schema_version`
- `event_type`

The original event should be preserved in the DLQ payload when size limits
allow. If preserving the full payload would exceed `max_message_bytes`, include
a durable object reference instead.

## Replay From DLQ

DLQ replay is manual by default:

1. Inspect and classify the failure.
2. Fix schema mapping, decoder logic, or bad source data handling.
3. Re-publish corrected records to the original topic or a dedicated repair job.
4. Keep the original DLQ record for audit until retention expires.

DLQ consumers must be idempotent. Replaying a DLQ record can duplicate a
previous partially processed message.

## Error Classes

Use stable error classes so operators can group failures:

- `schema_validation_failed`
- `decode_failed`
- `missing_required_field`
- `unsupported_schema_version`
- `partition_key_failed`
- `consumer_invariant_failed`
