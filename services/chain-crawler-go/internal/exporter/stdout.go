package exporter

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"sync"

	"telemetry-fabric/services/chain-crawler-go/pkg/event"
)

type StdoutExporter struct {
	mu sync.Mutex
	w  io.Writer
}

func NewStdoutExporter() *StdoutExporter {
	return &StdoutExporter{w: os.Stdout}
}

func NewWriterExporter(w io.Writer) *StdoutExporter {
	return &StdoutExporter{w: w}
}

func (e *StdoutExporter) Export(ctx context.Context, events []event.NormalizedEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	encoder := json.NewEncoder(e.w)
	for _, evt := range events {
		if err := encoder.Encode(evt); err != nil {
			return err
		}
	}
	return nil
}

func (e *StdoutExporter) Close(ctx context.Context) error {
	return ctx.Err()
}

func (e *StdoutExporter) HealthCheck(ctx context.Context) error {
	return ctx.Err()
}
