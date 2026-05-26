package limiter

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestLimiterConcurrentWaitHonorsCancellation(t *testing.T) {
	lim := New(1)
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- lim.Wait(ctx)
		}()
	}

	time.Sleep(20 * time.Millisecond)
	cancel()
	wg.Wait()
	close(errs)

	var canceled int
	for err := range errs {
		if err == context.Canceled {
			canceled++
		}
	}
	if canceled == 0 {
		t.Fatal("expected at least one waiter to observe context cancellation")
	}
}
