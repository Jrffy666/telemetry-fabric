package workerpool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestShutdownDrainsQueuedJobsAndRejectsNewSubmissions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := New(2, 4)
	pool.Start(ctx)

	var ran atomic.Int64
	for i := 0; i < 4; i++ {
		if err := pool.Submit(ctx, func(context.Context) error {
			time.Sleep(10 * time.Millisecond)
			ran.Add(1)
			return nil
		}); err != nil {
			t.Fatalf("submit job: %v", err)
		}
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
	defer shutdownCancel()
	if err := pool.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if got := ran.Load(); got != 4 {
		t.Fatalf("drained jobs = %d, want 4", got)
	}
	if err := pool.Submit(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrStopped) {
		t.Fatalf("submit after shutdown error = %v, want %v", err, ErrStopped)
	}
}

func TestSubmitUnblocksOnShutdownWhenQueueFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := New(1, 1)
	block := make(chan struct{})
	pool.Start(ctx)

	if err := pool.Submit(ctx, func(context.Context) error {
		<-block
		return nil
	}); err != nil {
		t.Fatalf("submit blocking job: %v", err)
	}
	if err := pool.Submit(ctx, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("submit queued job: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- pool.Submit(ctx, func(context.Context) error { return nil })
	}()

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		shutdownDone <- pool.Shutdown(shutdownCtx)
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrStopped) {
			t.Fatalf("blocked submit error = %v, want %v", err, ErrStopped)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked submit did not unblock on shutdown")
	}

	close(block)
	if err := <-shutdownDone; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}
