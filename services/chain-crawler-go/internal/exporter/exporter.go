package exporter

import (
	"context"

	"telemetry-fabric/services/chain-crawler-go/pkg/event"
)

type Exporter interface {
	Export(ctx context.Context, events []event.NormalizedEvent) error
	Close(ctx context.Context) error
}
