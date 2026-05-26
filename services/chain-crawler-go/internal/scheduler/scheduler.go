package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"telemetry-fabric/services/chain-crawler-go/internal/checkpoint"
	"telemetry-fabric/services/chain-crawler-go/internal/exporter"
	"telemetry-fabric/services/chain-crawler-go/internal/filter"
	"telemetry-fabric/services/chain-crawler-go/internal/limiter"
	"telemetry-fabric/services/chain-crawler-go/internal/metrics"
	"telemetry-fabric/services/chain-crawler-go/internal/rpcpool"
	"telemetry-fabric/services/chain-crawler-go/pkg/adapter"
	"telemetry-fabric/services/chain-crawler-go/pkg/event"
)

var ErrStopped = errors.New("scheduler: stopped")

type Config struct {
	StartHeight  uint64
	BatchSize    uint64
	PollInterval time.Duration
}

type Scheduler struct {
	adapter    adapter.ChainAdapter
	checkpoint checkpoint.Store
	exporter   exporter.Exporter
	filter     filter.Filter
	limiter    *limiter.Limiter
	retry      rpcpool.RetryPolicy
	metrics    *metrics.Registry
	cfg        Config
	stopOnce   sync.Once
	stopCh     chan struct{}
}

func New(
	chainAdapter adapter.ChainAdapter,
	checkpointStore checkpoint.Store,
	eventExporter exporter.Exporter,
	eventFilter filter.Filter,
	rateLimiter *limiter.Limiter,
	retryPolicy rpcpool.RetryPolicy,
	registry *metrics.Registry,
	cfg Config,
) *Scheduler {
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if eventFilter == nil {
		eventFilter = filter.AllowAll{}
	}
	if registry == nil {
		registry = metrics.NewRegistry()
	}

	return &Scheduler{
		adapter:    chainAdapter,
		checkpoint: checkpointStore,
		exporter:   eventExporter,
		filter:     eventFilter,
		limiter:    rateLimiter,
		retry:      retryPolicy,
		metrics:    registry,
		cfg:        cfg,
		stopCh:     make(chan struct{}),
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	select {
	case <-s.stopCh:
		return nil
	default:
	}

	if err := s.RunOnce(ctx); err != nil {
		return err
	}

	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stopCh:
			return nil
		case <-ticker.C:
			if err := s.RunOnce(ctx); err != nil {
				if errors.Is(err, ErrStopped) {
					return nil
				}
				log.Printf("crawler scheduler cycle failed: %v", err)
			}
		}
	}
}

func (s *Scheduler) RunOnce(ctx context.Context) error {
	select {
	case <-s.stopCh:
		return ErrStopped
	default:
	}

	labels := metrics.ChainLabels(s.adapter.Chain(), s.adapter.Network())
	s.metrics.SetGauge("crawler_worker_inflight", labels, 1)
	defer s.metrics.SetGauge("crawler_worker_inflight", labels, 0)

	var latest uint64
	started := time.Now()
	err := rpcpool.Retry(ctx, s.retry, func(ctx context.Context) error {
		if s.limiter != nil {
			if err := s.limiter.Wait(ctx); err != nil {
				return err
			}
		}
		var err error
		latest, err = s.adapter.LatestHeight(ctx)
		return err
	})
	s.metrics.SetGauge("crawler_rpc_latency_ms", labels, float64(time.Since(started).Milliseconds()))
	if err != nil {
		s.metrics.IncCounter("crawler_rpc_errors_total", labels, 1)
		if rpcpool.IsRateLimited(err) {
			s.metrics.IncCounter("crawler_rpc_rate_limited_total", labels, 1)
		}
		return err
	}
	s.metrics.SetGauge("crawler_chain_head_height", labels, float64(latest))

	target := finalizedTarget(latest, s.adapter.FinalityDepth())
	cp, ok, err := s.checkpoint.Load(ctx, s.adapter.Chain(), s.adapter.Network())
	if err != nil {
		return err
	}
	if !ok {
		height := uint64(0)
		if s.cfg.StartHeight > 0 {
			height = s.cfg.StartHeight - 1
		}
		cp = checkpoint.Checkpoint{
			Chain:   s.adapter.Chain(),
			Network: s.adapter.Network(),
			Height:  height,
		}
	}

	if target <= cp.Height {
		s.recordLag(labels, latest, cp.Height)
		return nil
	}

	from := cp.Height + 1
	to := min(from+s.cfg.BatchSize-1, target)

	var fetched []event.NormalizedEvent
	started = time.Now()
	err = rpcpool.Retry(ctx, s.retry, func(ctx context.Context) error {
		if s.limiter != nil {
			if err := s.limiter.Wait(ctx); err != nil {
				return err
			}
		}
		var err error
		fetched, err = s.adapter.FetchRange(ctx, from, to)
		return err
	})
	s.metrics.SetGauge("crawler_rpc_latency_ms", labels, float64(time.Since(started).Milliseconds()))
	if err != nil {
		s.metrics.IncCounter("crawler_rpc_errors_total", labels, 1)
		if rpcpool.IsRateLimited(err) {
			s.metrics.IncCounter("crawler_rpc_rate_limited_total", labels, 1)
		}
		return fmt.Errorf("fetch range %d-%d: %w", from, to, err)
	}
	gaps := checkpoint.DetectBlockGaps(from, to, observedBlockHeights(fetched))
	if len(gaps) > 0 {
		s.metrics.IncCounter("crawler_block_gap_total", labels, float64(len(gaps)))
		s.recordLag(labels, latest, cp.Height)
		return nil
	}

	seen := make(map[string]struct{}, len(fetched))
	kept := make([]event.NormalizedEvent, 0, len(fetched))
	for _, evt := range fetched {
		key := eventKey(evt)
		if _, exists := seen[key]; exists {
			s.metrics.IncCounter("crawler_duplicate_events_total", labels, 1)
			continue
		}
		seen[key] = struct{}{}

		decision := s.filter.Apply(evt)
		if !decision.Keep {
			s.metrics.IncCounter("crawler_discarded_events_total", labels, 1)
			continue
		}
		kept = append(kept, evt)
		s.metrics.IncCounter("crawler_kept_events_total", labels, 1)
	}

	if len(kept) > 0 {
		if err := s.exporter.Export(ctx, kept); err != nil {
			if errors.Is(err, exporter.ErrBackpressure) {
				s.metrics.IncCounter("crawler_exporter_backpressure_total", backpressureLabels(labels, "pause"), float64(len(kept)))
			}
			return err
		}
	}

	cp.Height = to
	cp.Chain = s.adapter.Chain()
	cp.Network = s.adapter.Network()
	cp.FinalizedHeight = to
	cp.PendingHeight = to
	cp.FinalityStatus = checkpoint.FinalityStatusFinalized
	if err := s.checkpoint.Save(ctx, cp); err != nil {
		return err
	}

	s.metrics.SetGauge("crawler_processed_height", labels, float64(to))
	s.metrics.SetGauge("crawler_checkpoint_height", labels, float64(to))
	s.recordLag(labels, latest, to)
	return nil
}

func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
}

func (s *Scheduler) recordLag(labels map[string]string, latest uint64, processed uint64) {
	lag := uint64(0)
	if latest > processed {
		lag = latest - processed
	}
	s.metrics.SetGauge("crawler_lag_blocks", labels, float64(lag))
	s.metrics.SetGauge("crawler_lag_seconds", labels, float64(lag*12))
}

func observedBlockHeights(events []event.NormalizedEvent) []uint64 {
	heights := make([]uint64, 0, len(events))
	for _, evt := range events {
		heights = append(heights, evt.BlockNumber)
	}
	return heights
}

func eventKey(evt event.NormalizedEvent) string {
	chainScope := fmt.Sprintf("chain:%s:network:%s", evt.Chain, evt.Network)
	if evt.ChainID != 0 {
		chainScope = fmt.Sprintf("chain_id:%d", evt.ChainID)
	}

	blockScope := fmt.Sprintf("block_number:%d", evt.BlockNumber)
	if evt.BlockHash != "" {
		blockScope = "block_hash:" + strings.ToLower(evt.BlockHash)
	}

	txScope := "tx_hash:" + strings.ToLower(evt.TxHash)
	if evt.TxHash == "" {
		txScope = fmt.Sprintf("tx_index:%d", evt.TxIndex)
	}

	return strings.Join([]string{
		chainScope,
		blockScope,
		txScope,
		fmt.Sprintf("log_index:%d", evt.LogIndex),
	}, ":")
}

func backpressureLabels(base map[string]string, action string) map[string]string {
	labels := make(map[string]string, len(base)+2)
	for key, value := range base {
		labels[key] = value
	}
	labels["policy"] = "pause"
	labels["action"] = action
	return labels
}

func finalizedTarget(latest uint64, finalityDepth uint64) uint64 {
	if latest <= finalityDepth {
		return 0
	}
	return latest - finalityDepth
}

func min(a uint64, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
