package mock

import (
	"context"
	"fmt"
	"sync"
	"time"

	"telemetry-fabric/services/chain-crawler-go/pkg/event"
)

type Config struct {
	Chain         string
	Network       string
	ChainID       uint64
	StartHeight   uint64
	FinalityDepth uint64
	HeadInterval  time.Duration
	SourceRPC     string
}

type Adapter struct {
	mu     sync.Mutex
	cfg    Config
	latest uint64
}

func New(cfg Config) *Adapter {
	if cfg.Chain == "" {
		cfg.Chain = "mockchain"
	}
	if cfg.Network == "" {
		cfg.Network = "local"
	}
	if cfg.ChainID == 0 {
		cfg.ChainID = 31337
	}
	if cfg.FinalityDepth == 0 {
		cfg.FinalityDepth = 6
	}
	if cfg.HeadInterval <= 0 {
		cfg.HeadInterval = time.Second
	}
	if cfg.SourceRPC == "" {
		cfg.SourceRPC = "mock-rpc-1"
	}

	return &Adapter{
		cfg:    cfg,
		latest: cfg.StartHeight + cfg.FinalityDepth + 10,
	}
}

func (a *Adapter) Name() string {
	return "mock"
}

func (a *Adapter) Chain() string {
	return a.cfg.Chain
}

func (a *Adapter) Network() string {
	return a.cfg.Network
}

func (a *Adapter) LatestHeight(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.latest++
	return a.latest, nil
}

func (a *Adapter) FetchRange(ctx context.Context, from uint64, to uint64) ([]event.NormalizedEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if from > to {
		return nil, nil
	}

	events := make([]event.NormalizedEvent, 0, to-from+1)
	for height := from; height <= to; height++ {
		events = append(events, a.mockEvent(height))
	}
	return events, nil
}

func (a *Adapter) SubscribeHeads(ctx context.Context) (<-chan event.BlockHeader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	heads := make(chan event.BlockHeader, 1)
	go func() {
		defer close(heads)
		ticker := time.NewTicker(a.cfg.HeadInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.mu.Lock()
				a.latest++
				height := a.latest
				a.mu.Unlock()

				header := event.BlockHeader{
					Chain:      a.cfg.Chain,
					Network:    a.cfg.Network,
					Height:     height,
					Hash:       blockHash(height),
					ParentHash: blockHash(height - 1),
					Timestamp:  time.Now().UTC(),
					SourceRPC:  a.cfg.SourceRPC,
				}

				select {
				case <-ctx.Done():
					return
				case heads <- header:
				}
			}
		}
	}()
	return heads, nil
}

func (a *Adapter) HealthCheck(ctx context.Context) (*event.NodeHealth, error) {
	latest, err := a.LatestHeight(ctx)
	if err != nil {
		return nil, err
	}

	finalized := uint64(0)
	if latest > a.cfg.FinalityDepth {
		finalized = latest - a.cfg.FinalityDepth
	}

	return &event.NodeHealth{
		NodeID:         "mock-node-1",
		Chain:          a.cfg.Chain,
		Network:        a.cfg.Network,
		ChainID:        a.cfg.ChainID,
		RPCEndpointID:  a.cfg.SourceRPC,
		SourceRPC:      a.cfg.SourceRPC,
		Status:         event.NodeStatusHealthy,
		Synced:         true,
		LatestBlock:    latest,
		FinalizedBlock: finalized,
		BlockLag:       0,
		PeerCount:      1,
		LatencyMS:      1,
		ErrorRate:      0,
		ClientVersion:  "mock-adapter/0.1.0",
		CheckedAt:      time.Now().UTC(),
		Attributes: map[string]string{
			"adapter": "mock",
		},
	}, nil
}

func (a *Adapter) FinalityDepth() uint64 {
	return a.cfg.FinalityDepth
}

func (a *Adapter) mockEvent(height uint64) event.NormalizedEvent {
	confirmations := uint32(a.cfg.FinalityDepth)
	return event.NormalizedEvent{
		Chain:           a.cfg.Chain,
		Network:         a.cfg.Network,
		ChainID:         a.cfg.ChainID,
		BlockNumber:     height,
		BlockHash:       blockHash(height),
		BlockTimestamp:  time.Now().UTC().Add(-time.Duration(a.cfg.FinalityDepth) * time.Second),
		TxHash:          fmt.Sprintf("0xmocktx%064x", height),
		TxIndex:         0,
		LogIndex:        0,
		EventType:       "transfer",
		FromAddress:     "0x0000000000000000000000000000000000000001",
		ToAddress:       "0x0000000000000000000000000000000000000002",
		ContractAddress: "0x0000000000000000000000000000000000000003",
		TokenAddress:    "0x0000000000000000000000000000000000000003",
		TokenSymbol:     "MOCK",
		TokenStandard:   event.TokenStandardERC20,
		AmountRaw:       "1000000000000000000",
		AmountDecimal:   "1.000000000000000000",
		AmountUSD:       "1.00",
		GasUsed:         21000,
		GasPrice:        "1000000000",
		Success:         true,
		FinalityStatus:  event.FinalityStatusConfirmed,
		Confirmations:   confirmations,
		SourceRPC:       a.cfg.SourceRPC,
		Reorged:         false,
	}
}

func blockHash(height uint64) string {
	return fmt.Sprintf("0xmockblock%064x", height)
}
