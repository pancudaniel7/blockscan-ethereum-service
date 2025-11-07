package pattern

import (
	"context"
	"math"
	"math/rand"
	"time"
)

type RetryConfig struct {
	Attempts     int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
	Jitter       float64
	ShouldRetry  func(error) bool
}

type RetryOption func(*RetryConfig)

func WithMaxAttempts(n int) RetryOption { return func(c *RetryConfig) { c.Attempts = n } }
func WithInfiniteAttempts() RetryOption { return func(c *RetryConfig) { c.Attempts = 0 } }
func WithInitialDelay(d time.Duration) RetryOption {
	return func(c *RetryConfig) { c.InitialDelay = d }
}
func WithMaxDelay(d time.Duration) RetryOption { return func(c *RetryConfig) { c.MaxDelay = d } }
func WithMultiplier(m float64) RetryOption     { return func(c *RetryConfig) { c.Multiplier = m } }
func WithJitter(j float64) RetryOption         { return func(c *RetryConfig) { c.Jitter = j } }
func WithShouldRetry(f func(error) bool) RetryOption {
	return func(c *RetryConfig) { c.ShouldRetry = f }
}

func Retry(ctx context.Context, fn func(attempt int) error, opts ...RetryOption) error {
	cfg := RetryConfig{
		Attempts:     5,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     10 * time.Second,
		Multiplier:   2.0,
		Jitter:       0.2,
	}

	for _, o := range opts {
		o(&cfg)
	}

	if cfg.InitialDelay <= 0 {
		cfg.InitialDelay = 100 * time.Millisecond
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = cfg.InitialDelay
	}
	if cfg.Multiplier < 1 {
		cfg.Multiplier = 1
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	var lastErr error
	for attempt := 1; cfg.Attempts <= 0 || attempt <= cfg.Attempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := fn(attempt); err == nil {
			return nil
		} else {
			if cfg.ShouldRetry != nil && !cfg.ShouldRetry(err) {
				return err
			}
			lastErr = err
		}
		base := float64(cfg.InitialDelay) * math.Pow(cfg.Multiplier, float64(attempt-1))
		if base > float64(cfg.MaxDelay) {
			base = float64(cfg.MaxDelay)
		}
		delay := time.Duration(base)
		if cfg.Jitter > 0 {
			f := 1 + (rng.Float64()*2-1)*cfg.Jitter
			if f < 0 {
				f = 0
			}
			delay = time.Duration(float64(delay) * f)
		}

		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	return lastErr
}
