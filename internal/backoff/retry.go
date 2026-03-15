package backoff

import (
	"context"
	"time"

	"vectis/internal/interfaces"
)

type RetryConfig struct {
	MaxTries  int
	BaseDelay time.Duration
	Clock     interfaces.Clock
}

type Retryer struct {
	config RetryConfig
}

func NewRetryer(config RetryConfig) *Retryer {
	if config.Clock == nil {
		config.Clock = interfaces.SystemClock{}
	}
	return &Retryer{config: config}
}

func (r *Retryer) Do(ctx context.Context, operation func() error, onRetry func(attempt int, nextDelay time.Duration, err error)) error {
	var lastErr error

	for attempt := range r.config.MaxTries {
		lastErr = operation()
		if lastErr == nil {
			return nil
		}

		if attempt == r.config.MaxTries-1 {
			break
		}

		delay := r.CalculateDelay(attempt)
		if onRetry != nil {
			onRetry(attempt+1, delay, lastErr)
		}

		if err := r.config.Clock.Sleep(ctx, delay); err != nil {
			return err
		}
	}

	return lastErr
}

func (r *Retryer) CalculateDelay(attempt int) time.Duration {
	return r.config.BaseDelay * time.Duration(1<<attempt)
}

func DefaultRetryer(clock interfaces.Clock) *Retryer {
	return NewRetryer(RetryConfig{
		MaxTries:  5,
		BaseDelay: 500 * time.Millisecond,
		Clock:     clock,
	})
}
