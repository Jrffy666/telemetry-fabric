package rpcpool

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryStopsOnPermanentErrorAndHonorsBudget(t *testing.T) {
	var attempts int
	err := Retry(context.Background(), RetryPolicy{
		Attempts:       5,
		RetryBudget:    5,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
	}, func(context.Context) error {
		attempts++
		return Permanent(errors.New("invalid params"))
	})
	if err == nil {
		t.Fatal("expected permanent error")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}

	attempts = 0
	err = Retry(context.Background(), RetryPolicy{
		Attempts:       5,
		RetryBudget:    2,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
	}, func(context.Context) error {
		attempts++
		return errors.New("temporary")
	})
	if err == nil {
		t.Fatal("expected final retry error")
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want retry budget 2", attempts)
	}
}
