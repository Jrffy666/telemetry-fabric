package rpcpool

import (
	"context"
	"errors"
	"math/rand"
	"net"
	"strings"
	"time"
)

var (
	ErrRetryBudgetExhausted = errors.New("rpcpool: retry budget exhausted")
	ErrPermanent            = errors.New("rpcpool: permanent error")
	ErrRateLimited          = errors.New("rpcpool: rate limited")
)

type permanentError struct {
	err error
}

func (e permanentError) Error() string {
	return e.err.Error()
}

func (e permanentError) Unwrap() error {
	return e.err
}

type rateLimitError struct {
	err error
}

func (e rateLimitError) Error() string {
	return e.err.Error()
}

func (e rateLimitError) Unwrap() error {
	return e.err
}

type RetryPolicy struct {
	Attempts       int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	RetryBudget    int
	Jitter         float64
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		Attempts:       3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		RetryBudget:    3,
		Jitter:         0.2,
	}
}

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

func RateLimited(err error) error {
	if err == nil {
		return nil
	}
	return rateLimitError{err: err}
}

func IsPermanent(err error) bool {
	if errors.Is(err, ErrPermanent) || errors.As(err, new(permanentError)) {
		return true
	}
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "invalid_params") ||
		strings.Contains(text, "invalid params") ||
		strings.Contains(text, "missing_block") ||
		strings.Contains(text, "pruned_data") ||
		strings.Contains(text, "execution_reverted") ||
		strings.Contains(text, "block_range_too_large")
}

func IsRateLimited(err error) bool {
	if errors.Is(err, ErrRateLimited) || errors.As(err, new(rateLimitError)) {
		return true
	}
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "rate limit") ||
		strings.Contains(text, "rate_limited") ||
		strings.Contains(text, "too many requests") ||
		strings.Contains(text, "http 429") ||
		strings.Contains(text, " 429")
}

func IsTransient(err error) bool {
	if err == nil || IsPermanent(err) {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) || IsRateLimited(err)
}

func Retry(ctx context.Context, policy RetryPolicy, op func(context.Context) error) error {
	if policy.Attempts <= 0 {
		policy.Attempts = 1
	}
	if policy.RetryBudget <= 0 || policy.RetryBudget > policy.Attempts {
		policy.RetryBudget = policy.Attempts
	}
	if policy.InitialBackoff <= 0 {
		policy.InitialBackoff = 100 * time.Millisecond
	}
	if policy.MaxBackoff <= 0 {
		policy.MaxBackoff = policy.InitialBackoff
	}
	if policy.Jitter < 0 {
		policy.Jitter = 0
	}
	if policy.Jitter > 1 {
		policy.Jitter = 1
	}

	var lastErr error
	backoff := policy.InitialBackoff
	for attempt := 0; attempt < policy.Attempts && attempt < policy.RetryBudget; attempt++ {
		if err := op(ctx); err != nil {
			lastErr = err
		} else {
			return nil
		}

		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if IsPermanent(lastErr) {
			break
		}
		if attempt == policy.Attempts-1 || attempt == policy.RetryBudget-1 {
			break
		}

		timer := time.NewTimer(withJitter(backoff, policy.Jitter))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		backoff *= 2
		if backoff > policy.MaxBackoff {
			backoff = policy.MaxBackoff
		}
	}
	if lastErr == nil {
		return ErrRetryBudgetExhausted
	}
	return lastErr
}

func withJitter(duration time.Duration, jitter float64) time.Duration {
	if duration <= 0 || jitter <= 0 {
		return duration
	}
	delta := int64(float64(duration) * jitter)
	if delta <= 0 {
		return duration
	}
	offset := rand.Int63n(delta*2+1) - delta
	return duration + time.Duration(offset)
}
