package evm

import (
	"context"
	"sync"
	"time"
)

type rateLimiter struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time
}

func newRateLimiter(perSecond int) *rateLimiter {
	if perSecond <= 0 {
		return nil
	}
	return &rateLimiter{interval: time.Second / time.Duration(perSecond)}
}

func (l *rateLimiter) wait(ctx context.Context) error {
	if l == nil {
		return nil
	}

	l.mu.Lock()
	now := time.Now()
	waitUntil := l.next
	if waitUntil.Before(now) {
		waitUntil = now
	}
	l.next = waitUntil.Add(l.interval)
	l.mu.Unlock()

	timer := time.NewTimer(time.Until(waitUntil))
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
