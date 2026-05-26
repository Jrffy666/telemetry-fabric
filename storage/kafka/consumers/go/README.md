# Go Consumer Skeleton

This is a wiring guide for consuming Kafka messages that contain contract
envelopes.

```go
package consumer

import "context"

type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string][]byte
}

type Handler interface {
	HandleEnvelope(ctx context.Context, message Message, encodedEnvelope []byte) error
}

type DeadLetterWriter interface {
	WriteDeadLetter(ctx context.Context, original Message, errorClass string, err error) error
}

func ConsumeOne(ctx context.Context, message Message, handler Handler, dlq DeadLetterWriter) error {
	if len(message.Value) == 0 {
		return dlq.WriteDeadLetter(ctx, message, "missing_required_field", ErrEmptyValue)
	}

	if err := handler.HandleEnvelope(ctx, message, message.Value); err != nil {
		return dlq.WriteDeadLetter(ctx, message, "consumer_invariant_failed", err)
	}
	return nil
}

var ErrEmptyValue = consumerError("empty kafka message value")

type consumerError string

func (e consumerError) Error() string { return string(e) }
```

Consumers should decode `platform.Envelope`, validate `event_type` and
`schema_version`, then decode the domain payload. Offset commits should happen
only after processing or DLQ write succeeds.
