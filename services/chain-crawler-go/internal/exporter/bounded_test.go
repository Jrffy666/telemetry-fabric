package exporter

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"telemetry-fabric/services/chain-crawler-go/internal/metrics"
	"telemetry-fabric/services/chain-crawler-go/pkg/event"
)

func TestBoundedExporterPropagatesInnerExportErrorsBeforeAck(t *testing.T) {
	sinkErr := errors.New("sink failed")
	bounded := NewBoundedExporter(&failingExporter{err: sinkErr}, BoundedOptions{
		Capacity: 1,
		Policy:   BackpressureSlowDown,
	})
	bounded.Start(context.Background())
	defer func() { _ = bounded.Close(context.Background()) }()

	err := bounded.Export(context.Background(), []event.NormalizedEvent{testEvent("transfer")})
	if !errors.Is(err, sinkErr) {
		t.Fatalf("export error = %v, want %v", err, sinkErr)
	}
}

func TestBoundedExporterDropPolicyDropsOnlyDegradableEventsWhenQueueIsFull(t *testing.T) {
	registry := metrics.NewRegistry()
	inner := newBlockingExporter()
	bounded := NewBoundedExporter(inner, BoundedOptions{
		Capacity: 1,
		Policy:   BackpressureDrop,
		Metrics:  registry,
	})
	bounded.Start(context.Background())
	defer func() {
		inner.release()
		if err := bounded.Close(context.Background()); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()

	firstErr := exportAsync(bounded, testEvent("transfer"))
	inner.waitStarted(t)
	secondErr := exportAsync(bounded, testEvent("transfer"))
	waitQueueLen(t, bounded.normal, 1)

	if err := bounded.Export(context.Background(), []event.NormalizedEvent{testEvent("aggregate")}); err != nil {
		t.Fatalf("drop degradable event should not fail: %v", err)
	}
	inner.release()
	requireAsyncExport(t, firstErr)
	requireAsyncExport(t, secondErr)

	var output bytes.Buffer
	if err := registry.WritePrometheus(&output); err != nil {
		t.Fatalf("write metrics: %v", err)
	}
	if !strings.Contains(output.String(), `crawler_exporter_backpressure_total{action="drop",chain="mockchain",network="local",policy="drop"} 1`) {
		t.Fatalf("missing drop backpressure metric:\n%s", output.String())
	}
	if got := inner.count(); got != 2 {
		t.Fatalf("exported events = %d, want 2", got)
	}
}

func TestBoundedExporterPausePolicyReturnsBackpressure(t *testing.T) {
	inner := newBlockingExporter()
	bounded := NewBoundedExporter(inner, BoundedOptions{
		Capacity: 1,
		Policy:   BackpressurePause,
	})
	bounded.Start(context.Background())
	defer func() {
		inner.release()
		_ = bounded.Close(context.Background())
	}()

	firstErr := exportAsync(bounded, testEvent("transfer"))
	inner.waitStarted(t)
	secondErr := exportAsync(bounded, testEvent("transfer"))
	waitQueueLen(t, bounded.normal, 1)

	err := bounded.Export(context.Background(), []event.NormalizedEvent{testEvent("transfer")})
	if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("pause policy error = %v, want %v", err, ErrBackpressure)
	}
	inner.release()
	requireAsyncExport(t, firstErr)
	requireAsyncExport(t, secondErr)
}

func testEvent(eventType string) event.NormalizedEvent {
	return event.NormalizedEvent{
		Chain:          "mockchain",
		Network:        "local",
		BlockNumber:    1,
		TxHash:         "0x1",
		LogIndex:       1,
		EventType:      eventType,
		FinalityStatus: event.FinalityStatusConfirmed,
	}
}

func criticalEvent() event.NormalizedEvent {
	evt := testEvent("critical")
	evt.Reorged = true
	evt.FinalityStatus = event.FinalityStatusReorged
	return evt
}

type failingExporter struct {
	err error
}

func (e *failingExporter) Export(context.Context, []event.NormalizedEvent) error {
	return e.err
}

func (e *failingExporter) Close(context.Context) error {
	return nil
}

type blockingExporter struct {
	started   chan struct{}
	releaseCh chan struct{}
	once      sync.Once
	mu        sync.Mutex
	events    []event.NormalizedEvent
}

func newBlockingExporter() *blockingExporter {
	return &blockingExporter{
		started:   make(chan struct{}),
		releaseCh: make(chan struct{}),
	}
}

func (e *blockingExporter) Export(ctx context.Context, events []event.NormalizedEvent) error {
	e.once.Do(func() {
		close(e.started)
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.releaseCh:
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, events...)
	return nil
}

func (e *blockingExporter) Close(context.Context) error {
	return nil
}

func (e *blockingExporter) waitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-e.started:
	case <-time.After(time.Second):
		t.Fatal("inner exporter did not receive first batch")
	}
}

func (e *blockingExporter) release() {
	select {
	case <-e.releaseCh:
	default:
		close(e.releaseCh)
	}
}

func (e *blockingExporter) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.events)
}

func exportAsync(bounded *BoundedExporter, evt event.NormalizedEvent) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- bounded.Export(context.Background(), []event.NormalizedEvent{evt})
	}()
	return errCh
}

func requireAsyncExport(t *testing.T, errCh <-chan error) {
	t.Helper()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("async export: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("async export did not complete")
	}
}

func waitQueueLen(t *testing.T, queue chan exportBatch, want int) {
	t.Helper()
	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if len(queue) == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("queue length = %d, want %d", len(queue), want)
		case <-ticker.C:
		}
	}
}
