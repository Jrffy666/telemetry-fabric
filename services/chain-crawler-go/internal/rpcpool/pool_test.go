package rpcpool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestEndpointCircuitBreakerHalfOpenAndRecovery(t *testing.T) {
	now := time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC)
	pool := NewWithConfig([]Endpoint{
		{ID: "a", URL: "https://a.example"},
		{ID: "b", URL: "https://b.example"},
	}, Config{CircuitOpenAfter: 2, HalfOpenAfter: time.Minute})
	pool.setClockForTest(func() time.Time { return now })

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := pool.Report(ctx, "a", 2*time.Second, errors.New("http 500")); err != nil {
			t.Fatalf("report failure: %v", err)
		}
	}

	snapshot := pool.Snapshot()
	if snapshot[0].State != CircuitOpen {
		t.Fatalf("endpoint state = %s, want %s", snapshot[0].State, CircuitOpen)
	}

	next, err := pool.Next(ctx)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if next.ID != "b" {
		t.Fatalf("next endpoint = %s, want b", next.ID)
	}

	now = now.Add(time.Minute + time.Second)
	next, err = pool.Next(ctx)
	if err != nil {
		t.Fatalf("next half-open: %v", err)
	}
	if next.ID != "a" {
		t.Fatalf("half-open endpoint = %s, want a", next.ID)
	}
	if err := pool.Report(ctx, "a", 50*time.Millisecond, nil); err != nil {
		t.Fatalf("report recovery: %v", err)
	}
	if got := pool.Snapshot()[0].State; got != CircuitClosed {
		t.Fatalf("recovered endpoint state = %s, want %s", got, CircuitClosed)
	}
}

func TestPoolConcurrentNextAndReport(t *testing.T) {
	pool := New([]Endpoint{
		{ID: "a", URL: "https://a.example"},
		{ID: "b", URL: "https://b.example"},
		{ID: "c", URL: "https://c.example"},
	})
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			endpoint, err := pool.Next(ctx)
			if err != nil {
				t.Errorf("next: %v", err)
				return
			}
			var reportErr error
			if endpoint.ID == "a" && i%5 == 0 {
				reportErr = RateLimited(errors.New("http 429"))
			}
			if err := pool.Report(ctx, endpoint.ID, time.Duration(i+1)*time.Millisecond, reportErr); err != nil {
				t.Errorf("report: %v", err)
			}
		}(i)
	}
	wg.Wait()
}
