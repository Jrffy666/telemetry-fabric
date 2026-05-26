package exporter

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"telemetry-fabric/services/chain-crawler-go/internal/metrics"
	"telemetry-fabric/services/chain-crawler-go/pkg/event"
)

var ErrBackpressure = errors.New("exporter: backpressure")

type BackpressurePolicy string

const (
	BackpressureSlowDown BackpressurePolicy = "slowdown"
	BackpressureDrop     BackpressurePolicy = "drop"
	BackpressurePause    BackpressurePolicy = "pause"
)

type BoundedOptions struct {
	Capacity int
	Policy   BackpressurePolicy
	Metrics  *metrics.Registry
}

type BoundedExporter struct {
	inner    Exporter
	policy   BackpressurePolicy
	metrics  *metrics.Registry
	critical chan exportBatch
	normal   chan exportBatch

	start    sync.Once
	close    sync.Once
	submitMu sync.Mutex
	submitWG sync.WaitGroup
	done     chan struct{}

	started atomic.Bool
	closed  atomic.Bool
}

type exportBatch struct {
	events []event.NormalizedEvent
	result chan error
}

func NewBoundedExporter(inner Exporter, options BoundedOptions) *BoundedExporter {
	if options.Capacity <= 0 {
		options.Capacity = 1
	}
	if options.Policy == "" {
		options.Policy = BackpressureSlowDown
	}
	return &BoundedExporter{
		inner:    inner,
		policy:   options.Policy,
		metrics:  options.Metrics,
		critical: make(chan exportBatch, options.Capacity),
		normal:   make(chan exportBatch, options.Capacity),
		done:     make(chan struct{}),
	}
}

func (e *BoundedExporter) Start(ctx context.Context) {
	e.start.Do(func() {
		e.started.Store(true)
		go e.run(ctx)
	})
}

func (e *BoundedExporter) Export(ctx context.Context, events []event.NormalizedEvent) error {
	if len(events) == 0 {
		return ctx.Err()
	}
	if e.closed.Load() {
		return ErrBackpressure
	}
	e.submitMu.Lock()
	if e.closed.Load() {
		e.submitMu.Unlock()
		return ErrBackpressure
	}
	e.submitWG.Add(1)
	e.submitMu.Unlock()
	defer e.submitWG.Done()

	critical, requiredNormal, degradable := splitEvents(events)
	if len(critical) > 0 {
		if err := e.submit(ctx, e.critical, critical, true); err != nil {
			return err
		}
	}
	normal := append(append([]event.NormalizedEvent{}, requiredNormal...), degradable...)
	if len(normal) == 0 {
		return nil
	}

	switch e.policy {
	case BackpressurePause:
		if err := e.submit(ctx, e.normal, normal, false); err != nil {
			e.observe(normal, "pause")
			return err
		}
		return nil
	case BackpressureDrop:
		if err := e.submit(ctx, e.normal, normal, false); err == nil {
			return nil
		} else if !errors.Is(err, ErrBackpressure) {
			return err
		}

		if len(degradable) > 0 {
			e.observe(degradable, "drop")
			if len(requiredNormal) == 0 {
				return nil
			}
			e.observe(requiredNormal, "slowdown")
			return e.submit(ctx, e.normal, requiredNormal, true)
		}
		if len(normal) > 0 {
			e.observe(normal, "slowdown")
		}
		return e.submit(ctx, e.normal, normal, true)
	default:
		if len(e.normal) == cap(e.normal) {
			e.observe(normal, "slowdown")
		}
		return e.submit(ctx, e.normal, normal, true)
	}
}

func (e *BoundedExporter) Close(ctx context.Context) error {
	e.submitMu.Lock()
	e.closed.Store(true)
	e.submitMu.Unlock()
	e.submitWG.Wait()
	e.close.Do(func() {
		close(e.critical)
		close(e.normal)
		if !e.started.Load() {
			close(e.done)
		}
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.done:
	}

	if e.inner == nil {
		return nil
	}
	return e.inner.Close(ctx)
}

func (e *BoundedExporter) HealthCheck(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.closed.Load() {
		return errors.New("exporter: closed")
	}
	if checker, ok := e.inner.(interface{ HealthCheck(context.Context) error }); ok {
		return checker.HealthCheck(ctx)
	}
	return nil
}

func (e *BoundedExporter) run(ctx context.Context) {
	defer close(e.done)
	for {
		select {
		case <-ctx.Done():
			e.drain(context.Background())
			return
		default:
		}

		select {
		case batch, ok := <-e.critical:
			if !ok {
				e.drain(ctx)
				return
			}
			e.export(ctx, batch)
		default:
			select {
			case batch, ok := <-e.critical:
				if !ok {
					e.drain(ctx)
					return
				}
				e.export(ctx, batch)
			case batch, ok := <-e.normal:
				if !ok {
					e.drain(ctx)
					return
				}
				e.export(ctx, batch)
			case <-ctx.Done():
				e.drain(context.Background())
				return
			}
		}
	}
}

func (e *BoundedExporter) drain(ctx context.Context) {
	for {
		select {
		case batch, ok := <-e.critical:
			if ok {
				e.export(ctx, batch)
				continue
			}
		default:
		}
		break
	}

	for {
		select {
		case batch, ok := <-e.normal:
			if ok {
				e.export(ctx, batch)
				continue
			}
		default:
		}
		break
	}
}

func (e *BoundedExporter) submit(ctx context.Context, queue chan exportBatch, events []event.NormalizedEvent, block bool) error {
	if e.closed.Load() {
		return ErrBackpressure
	}
	if !e.started.Load() {
		if e.inner == nil {
			return errors.New("exporter: inner exporter is not configured")
		}
		return e.inner.Export(ctx, events)
	}

	batch := exportBatch{
		events: events,
		result: make(chan error, 1),
	}
	if !block {
		select {
		case queue <- batch:
			return e.wait(ctx, batch)
		default:
			return ErrBackpressure
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case queue <- batch:
		return e.wait(ctx, batch)
	}
}

func (e *BoundedExporter) wait(ctx context.Context, batch exportBatch) error {
	select {
	case err := <-batch.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *BoundedExporter) export(ctx context.Context, batch exportBatch) {
	if e.inner == nil {
		batch.result <- errors.New("exporter: inner exporter is not configured")
		return
	}
	batch.result <- e.inner.Export(ctx, batch.events)
}

func (e *BoundedExporter) observe(events []event.NormalizedEvent, action string) {
	if e.metrics == nil || len(events) == 0 {
		return
	}
	labels := metrics.ChainLabels(events[0].Chain, events[0].Network)
	labels["policy"] = string(e.policy)
	labels["action"] = action
	e.metrics.IncCounter("crawler_exporter_backpressure_total", labels, float64(len(events)))
}

func splitEvents(events []event.NormalizedEvent) ([]event.NormalizedEvent, []event.NormalizedEvent, []event.NormalizedEvent) {
	critical := make([]event.NormalizedEvent, 0)
	requiredNormal := make([]event.NormalizedEvent, 0, len(events))
	degradable := make([]event.NormalizedEvent, 0)
	for _, evt := range events {
		if IsCritical(evt) {
			critical = append(critical, evt)
			continue
		}
		if IsDegradable(evt) {
			degradable = append(degradable, evt)
		} else {
			requiredNormal = append(requiredNormal, evt)
		}
	}
	return critical, requiredNormal, degradable
}

func IsCritical(evt event.NormalizedEvent) bool {
	return evt.Reorged ||
		evt.FinalityStatus == event.FinalityStatusReorged ||
		strings.EqualFold(evt.EventType, "critical")
}

func IsDegradable(evt event.NormalizedEvent) bool {
	eventType := strings.ToLower(evt.EventType)
	return eventType == "drop" ||
		eventType == "aggregate" ||
		strings.Contains(eventType, "aggregate")
}
