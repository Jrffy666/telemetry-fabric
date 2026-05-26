package adapter

import (
	"context"

	"telemetry-fabric/services/chain-crawler-go/pkg/event"
)

type NormalizedEvent = event.NormalizedEvent
type BlockHeader = event.BlockHeader
type NodeHealth = event.NodeHealth

type ChainAdapter interface {
	Name() string
	Chain() string
	Network() string
	LatestHeight(ctx context.Context) (uint64, error)
	FetchRange(ctx context.Context, from uint64, to uint64) ([]NormalizedEvent, error)
	SubscribeHeads(ctx context.Context) (<-chan BlockHeader, error)
	HealthCheck(ctx context.Context) (*NodeHealth, error)
	FinalityDepth() uint64
}
