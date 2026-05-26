# Go Producer Skeleton

This is a wiring guide for a Go service that publishes blockchain events to
Kafka. Keep generated protobuf types in a shared contracts package and publish
only `platform.Envelope` values.

```go
package producer

import (
	"context"
)

type KafkaWriter interface {
	WriteMessages(ctx context.Context, messages ...Message) error
}

type Message struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string][]byte
}

type Envelope struct {
	TenantID      string
	EventType     string
	SchemaVersion string
	Priority      string
	DedupeKey     string
}

func PublishEnvelope(ctx context.Context, writer KafkaWriter, topic string, key []byte, encodedEnvelope []byte, envelope Envelope) error {
	return writer.WriteMessages(ctx, Message{
		Topic: topic,
		Key:   key,
		Value: encodedEnvelope,
		Headers: map[string][]byte{
			"tenant_id":      []byte(envelope.TenantID),
			"event_type":     []byte(envelope.EventType),
			"schema_version": []byte(envelope.SchemaVersion),
			"priority":       []byte(envelope.Priority),
			"dedupe_key":     []byte(envelope.DedupeKey),
		},
	})
}
```

Recommended partition keys:

- `chain:network:block_number` for raw and important chain events.
- `chain:network:address` for address analytics streams.
- `priority` or `priority:rule_id` for alert routing.

Do not publish raw adapter structs directly. Map them to contract messages,
encode the envelope, then write to Kafka.
