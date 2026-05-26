package exporter

import (
	"context"
	"encoding/json"
	"os"
	"sync"

	"telemetry-fabric/services/chain-crawler-go/pkg/event"
)

type FileExporter struct {
	mu   sync.Mutex
	file *os.File
}

func NewFileExporter(path string) (*FileExporter, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileExporter{file: file}, nil
}

func (e *FileExporter) Export(ctx context.Context, events []event.NormalizedEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	encoder := json.NewEncoder(e.file)
	for _, evt := range events {
		if err := encoder.Encode(evt); err != nil {
			return err
		}
	}
	return e.file.Sync()
}

func (e *FileExporter) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	return e.file.Close()
}

func (e *FileExporter) HealthCheck(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	_, err := e.file.Stat()
	return err
}
