package scheduler

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"telemetry-fabric/services/chain-crawler-go/internal/checkpoint"
	"telemetry-fabric/services/chain-crawler-go/internal/exporter"
	"telemetry-fabric/services/chain-crawler-go/internal/filter"
	"telemetry-fabric/services/chain-crawler-go/internal/limiter"
	"telemetry-fabric/services/chain-crawler-go/internal/metrics"
	"telemetry-fabric/services/chain-crawler-go/internal/rpcpool"
	"telemetry-fabric/services/chain-crawler-go/pkg/event"
)

func TestSchedulerStopWaitsForInFlightCycleAndPreventsNewCycles(t *testing.T) {
	adapter := &blockingAdapter{
		latest:       20,
		block:        make(chan struct{}),
		fetchStarted: make(chan struct{}),
	}
	exp := &collectingExporter{}
	registry := metrics.NewRegistry()
	store := checkpoint.NewMemoryStore()
	s := New(
		adapter,
		store,
		exp,
		filter.AllowAll{},
		limiter.New(0),
		rpcpool.RetryPolicy{Attempts: 1, RetryBudget: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond},
		registry,
		Config{StartHeight: 10, BatchSize: 2, PollInterval: 10 * time.Millisecond},
	)

	done := make(chan error, 1)
	go func() {
		done <- s.Run(context.Background())
	}()

	select {
	case <-adapter.fetchStarted:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not enter fetch")
	}

	s.Stop()
	select {
	case <-done:
		t.Fatal("scheduler exited before in-flight fetch completed")
	case <-time.After(30 * time.Millisecond):
	}

	close(adapter.block)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop after in-flight cycle completed")
	}

	if got := exp.count(); got != 2 {
		t.Fatalf("exported events = %d, want 2", got)
	}
	cp, ok, err := store.Load(context.Background(), "mockchain", "local")
	if err != nil || !ok {
		t.Fatalf("load checkpoint: ok=%v err=%v", ok, err)
	}
	if cp.Height != 11 || cp.FinalizedHeight != 11 || cp.FinalityStatus != checkpoint.FinalityStatusFinalized {
		t.Fatalf("checkpoint not finalized for processed range: %+v", cp)
	}
	if err := s.RunOnce(context.Background()); !errors.Is(err, ErrStopped) {
		t.Fatalf("run once after stop = %v, want %v", err, ErrStopped)
	}
}

func TestSchedulerDoesNotAdvanceCheckpointWhenExportFails(t *testing.T) {
	block := make(chan struct{})
	close(block)
	adapter := &blockingAdapter{
		latest:       20,
		block:        block,
		fetchStarted: make(chan struct{}),
	}
	store := checkpoint.NewMemoryStore()
	exportErr := errors.New("export sink unavailable")
	s := New(
		adapter,
		store,
		&failingExporter{err: exportErr},
		filter.AllowAll{},
		limiter.New(0),
		rpcpool.RetryPolicy{Attempts: 1, RetryBudget: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond},
		metrics.NewRegistry(),
		Config{StartHeight: 10, BatchSize: 2, PollInterval: 10 * time.Millisecond},
	)

	err := s.RunOnce(context.Background())
	if !errors.Is(err, exportErr) {
		t.Fatalf("run once error = %v, want %v", err, exportErr)
	}

	if cp, ok, err := store.Load(context.Background(), "mockchain", "local"); err != nil || ok {
		t.Fatalf("checkpoint advanced after failed export: checkpoint=%+v ok=%v err=%v", cp, ok, err)
	}
}

func TestSchedulerDetectsFetchedEventBlockGapsAndDoesNotAdvanceCheckpoint(t *testing.T) {
	ctx := context.Background()
	block := make(chan struct{})
	close(block)
	adapter := &blockingAdapter{
		latest:          13,
		block:           block,
		fetchStarted:    make(chan struct{}),
		observedHeights: []uint64{10, 12, 13},
	}
	store := checkpoint.NewMemoryStore()
	if err := store.Save(ctx, checkpoint.Checkpoint{
		Chain:           "mockchain",
		Network:         "local",
		Height:          9,
		PendingHeight:   9,
		FinalizedHeight: 9,
		FinalityStatus:  checkpoint.FinalityStatusFinalized,
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	registry := metrics.NewRegistry()
	exp := &collectingExporter{}
	s := New(
		adapter,
		store,
		exp,
		filter.AllowAll{},
		limiter.New(0),
		rpcpool.RetryPolicy{Attempts: 1, RetryBudget: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond},
		registry,
		Config{BatchSize: 4, PollInterval: 10 * time.Millisecond},
	)

	if err := s.RunOnce(ctx); err != nil {
		t.Fatalf("run once: %v", err)
	}

	if got := exp.count(); got != 0 {
		t.Fatalf("exported events across gapped range = %d, want 0", got)
	}
	cp, ok, err := store.Load(ctx, "mockchain", "local")
	if err != nil || !ok {
		t.Fatalf("load checkpoint: ok=%v err=%v", ok, err)
	}
	if cp.Height != 9 || cp.FinalizedHeight != 9 {
		t.Fatalf("checkpoint advanced despite fetched block gap: %+v", cp)
	}

	var out bytes.Buffer
	if err := registry.WritePrometheus(&out); err != nil {
		t.Fatalf("write metrics: %v", err)
	}
	if !strings.Contains(out.String(), `crawler_block_gap_total{chain="mockchain",network="local"} 1`) {
		t.Fatalf("missing block gap metric in:\n%s", out.String())
	}
}

func TestEventKeyPrefersEVMIdentityAndFallsBackForMissingFields(t *testing.T) {
	evt := event.NormalizedEvent{
		Chain:       "ethereum",
		Network:     "mainnet",
		ChainID:     1,
		BlockNumber: 100,
		BlockHash:   "0xABCDEF",
		TxHash:      "0xFEDCBA",
		TxIndex:     7,
		LogIndex:    2,
	}

	key := eventKey(evt)
	for _, want := range []string{"chain_id:1", "block_hash:0xabcdef", "tx_hash:0xfedcba", "log_index:2"} {
		if !strings.Contains(key, want) {
			t.Fatalf("event key %q missing %q", key, want)
		}
	}
	if strings.Contains(key, "block_number:100") {
		t.Fatalf("event key used block number fallback despite block hash: %q", key)
	}

	otherBlock := evt
	otherBlock.BlockHash = "0x1234"
	if eventKey(otherBlock) == key {
		t.Fatalf("event key did not distinguish different block hashes: %q", key)
	}
	otherChain := evt
	otherChain.ChainID = 2
	if eventKey(otherChain) == key {
		t.Fatalf("event key did not distinguish different chain IDs: %q", key)
	}

	fallback := evt
	fallback.ChainID = 0
	fallback.BlockHash = ""
	fallback.TxHash = ""
	fallbackKey := eventKey(fallback)
	for _, want := range []string{"chain:ethereum:network:mainnet", "block_number:100", "tx_index:7", "log_index:2"} {
		if !strings.Contains(fallbackKey, want) {
			t.Fatalf("fallback event key %q missing %q", fallbackKey, want)
		}
	}
}

type blockingAdapter struct {
	latest          uint64
	block           chan struct{}
	fetchOnce       sync.Once
	fetchStarted    chan struct{}
	observedHeights []uint64
}

func (a *blockingAdapter) Name() string    { return "blocking" }
func (a *blockingAdapter) Chain() string   { return "mockchain" }
func (a *blockingAdapter) Network() string { return "local" }
func (a *blockingAdapter) FinalityDepth() uint64 {
	return 0
}

func (a *blockingAdapter) LatestHeight(ctx context.Context) (uint64, error) {
	return a.latest, ctx.Err()
}

func (a *blockingAdapter) FetchRange(ctx context.Context, from uint64, to uint64) ([]event.NormalizedEvent, error) {
	a.fetchOnce.Do(func() {
		close(a.fetchStarted)
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-a.block:
	}
	heights := a.observedHeights
	if len(heights) == 0 {
		heights = make([]uint64, 0, to-from+1)
		for height := from; height <= to; height++ {
			heights = append(heights, height)
		}
	}
	events := make([]event.NormalizedEvent, 0, len(heights))
	for _, height := range heights {
		events = append(events, event.NormalizedEvent{
			Chain:          a.Chain(),
			Network:        a.Network(),
			BlockNumber:    height,
			TxHash:         "0xmock",
			LogIndex:       0,
			EventType:      "transfer",
			FinalityStatus: event.FinalityStatusConfirmed,
		})
	}
	return events, nil
}

func (a *blockingAdapter) SubscribeHeads(ctx context.Context) (<-chan event.BlockHeader, error) {
	ch := make(chan event.BlockHeader)
	close(ch)
	return ch, ctx.Err()
}

func (a *blockingAdapter) HealthCheck(ctx context.Context) (*event.NodeHealth, error) {
	return &event.NodeHealth{Status: event.NodeStatusHealthy}, ctx.Err()
}

type collectingExporter struct {
	mu     sync.Mutex
	events []event.NormalizedEvent
}

func (e *collectingExporter) Export(ctx context.Context, events []event.NormalizedEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, events...)
	return nil
}

func (e *collectingExporter) Close(ctx context.Context) error {
	return ctx.Err()
}

func (e *collectingExporter) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.events)
}

var _ exporter.Exporter = (*collectingExporter)(nil)

type failingExporter struct {
	err error
}

func (e *failingExporter) Export(ctx context.Context, _ []event.NormalizedEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return e.err
}

func (e *failingExporter) Close(ctx context.Context) error {
	return ctx.Err()
}

var _ exporter.Exporter = (*failingExporter)(nil)
