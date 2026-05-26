package rpcpool

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

var ErrNoEndpoints = errors.New("rpcpool: no endpoints configured")
var ErrNoAvailableEndpoints = errors.New("rpcpool: no available endpoints")

type CircuitState string

const (
	CircuitClosed   CircuitState = "closed"
	CircuitOpen     CircuitState = "open"
	CircuitHalfOpen CircuitState = "half_open"
)

type Endpoint struct {
	ID                  string        `json:"id"`
	URL                 string        `json:"url"`
	Healthy             bool          `json:"healthy"`
	LastError           string        `json:"last_error,omitempty"`
	Latency             time.Duration `json:"latency"`
	Score               float64       `json:"score"`
	ErrorRate           float64       `json:"error_rate"`
	RateLimited         bool          `json:"rate_limited"`
	State               CircuitState  `json:"state"`
	ConsecutiveFailures int           `json:"consecutive_failures"`
	LastObservedAt      time.Time     `json:"last_observed_at"`
	OpenedAt            time.Time     `json:"opened_at,omitempty"`
}

type Pool struct {
	mu  sync.Mutex
	cfg Config
	now func() time.Time

	endpoints []Endpoint
	next      int
}

type Config struct {
	CircuitOpenAfter int
	HalfOpenAfter    time.Duration
	LatencyWeight    float64
	ErrorWeight      float64
}

func New(endpoints []Endpoint) *Pool {
	return NewWithConfig(endpoints, Config{})
}

func NewWithConfig(endpoints []Endpoint, cfg Config) *Pool {
	if cfg.CircuitOpenAfter <= 0 {
		cfg.CircuitOpenAfter = 3
	}
	if cfg.HalfOpenAfter <= 0 {
		cfg.HalfOpenAfter = 30 * time.Second
	}
	if cfg.LatencyWeight <= 0 {
		cfg.LatencyWeight = 0.2
	}
	if cfg.ErrorWeight <= 0 {
		cfg.ErrorWeight = 0.3
	}

	copied := make([]Endpoint, len(endpoints))
	copy(copied, endpoints)
	now := time.Now
	for i := range copied {
		copied[i].Healthy = true
		copied[i].Score = 1
		copied[i].State = CircuitClosed
		copied[i].LastObservedAt = now().UTC()
	}
	return &Pool{
		cfg:       cfg,
		now:       now,
		endpoints: copied,
	}
}

func (p *Pool) Next(ctx context.Context) (Endpoint, error) {
	if err := ctx.Err(); err != nil {
		return Endpoint{}, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.endpoints) == 0 {
		return Endpoint{}, ErrNoEndpoints
	}

	p.promoteHalfOpenLocked()

	bestIdx := -1
	bestScore := -1.0
	for i := 0; i < len(p.endpoints); i++ {
		idx := (p.next + i) % len(p.endpoints)
		endpoint := p.endpoints[idx]
		if endpoint.State == CircuitOpen {
			continue
		}
		score := endpoint.Score
		if endpoint.State == CircuitHalfOpen {
			score += 2
		}
		if score > bestScore {
			bestScore = score
			bestIdx = idx
		}
	}

	if bestIdx < 0 {
		return Endpoint{}, ErrNoAvailableEndpoints
	}

	p.next = (bestIdx + 1) % len(p.endpoints)
	return p.endpoints[bestIdx], nil
}

func (p *Pool) Report(ctx context.Context, endpointID string, latency time.Duration, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for i := range p.endpoints {
		if p.endpoints[i].ID != endpointID {
			continue
		}
		p.reportLocked(i, latency, err)
		return nil
	}
	return nil
}

func (p *Pool) Snapshot() []Endpoint {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.promoteHalfOpenLocked()
	copied := make([]Endpoint, len(p.endpoints))
	copy(copied, p.endpoints)
	return copied
}

func (p *Pool) reportLocked(i int, latency time.Duration, err error) {
	now := p.now().UTC()
	endpoint := &p.endpoints[i]
	endpoint.Latency = latency
	endpoint.LastObservedAt = now
	endpoint.RateLimited = IsRateLimited(err)

	if err != nil {
		endpoint.ConsecutiveFailures++
		endpoint.Healthy = false
		endpoint.LastError = sanitizeError(err)
		endpoint.ErrorRate = movingAverage(endpoint.ErrorRate, 1, p.cfg.ErrorWeight)
		endpoint.Score = clampScore(endpoint.Score - scorePenalty(latency, endpoint.ErrorRate, endpoint.RateLimited))
		if endpoint.ConsecutiveFailures >= p.cfg.CircuitOpenAfter || endpoint.RateLimited {
			endpoint.State = CircuitOpen
			endpoint.OpenedAt = now
		}
		return
	}

	endpoint.Healthy = true
	endpoint.LastError = ""
	endpoint.ConsecutiveFailures = 0
	endpoint.ErrorRate = movingAverage(endpoint.ErrorRate, 0, p.cfg.ErrorWeight)
	endpoint.Score = clampScore(endpoint.Score + scoreReward(latency))
	if endpoint.State == CircuitHalfOpen || endpoint.State == CircuitOpen {
		endpoint.State = CircuitClosed
		endpoint.OpenedAt = time.Time{}
	}
}

func (p *Pool) promoteHalfOpenLocked() {
	now := p.now().UTC()
	for i := range p.endpoints {
		if p.endpoints[i].State == CircuitOpen && now.Sub(p.endpoints[i].OpenedAt) >= p.cfg.HalfOpenAfter {
			p.endpoints[i].State = CircuitHalfOpen
		}
	}
}

func movingAverage(current float64, sample float64, weight float64) float64 {
	if current == 0 {
		return sample
	}
	return current*(1-weight) + sample*weight
}

func scorePenalty(latency time.Duration, errorRate float64, rateLimited bool) float64 {
	penalty := 0.2 + math.Min(errorRate, 1)*0.5
	if latency > time.Second {
		penalty += 0.1
	}
	if rateLimited {
		penalty += 0.4
	}
	return penalty
}

func scoreReward(latency time.Duration) float64 {
	if latency <= 0 {
		return 0.1
	}
	if latency < 250*time.Millisecond {
		return 0.2
	}
	if latency < time.Second {
		return 0.1
	}
	return 0.05
}

func clampScore(score float64) float64 {
	if score < 0 {
		return 0
	}
	if score > 1 {
		return 1
	}
	return score
}

func sanitizeError(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	if len(text) > 256 {
		text = text[:256]
	}
	return text
}

func (p *Pool) setClockForTest(now func() time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if now != nil {
		p.now = now
	}
}
