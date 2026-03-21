package backoff_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"vectis/internal/backoff"
	"vectis/internal/interfaces/mocks"
)

func TestExponentialDelay_Capped(t *testing.T) {
	got := backoff.ExponentialDelay(500*time.Millisecond, 10, 30*time.Second)
	if got != 30*time.Second {
		t.Errorf("expected cap 30s, got %v", got)
	}
	if backoff.ExponentialDelay(500*time.Millisecond, 0, 30*time.Second) != 500*time.Millisecond {
		t.Errorf("attempt 0 should be base delay")
	}
}

func TestRetryer_CalculateDelay(t *testing.T) {
	mockClock := mocks.NewMockClock()
	retryer := backoff.NewRetryer(backoff.RetryConfig{
		MaxTries:  5,
		BaseDelay: 500 * time.Millisecond,
		Clock:     mockClock,
	})

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 500 * time.Millisecond}, // NOTE(garrett): 500ms * 2^0 = 500ms
		{1, 1 * time.Second},        // NOTE(garrett): 500ms * 2^1 = 1s
		{2, 2 * time.Second},        // NOTE(garrett): 500ms * 2^2 = 2s
		{3, 4 * time.Second},        // NOTE(garrett): 500ms * 2^3 = 4s
		{4, 8 * time.Second},        // NOTE(garrett): 500ms * 2^4 = 8s
		{5, 16 * time.Second},       // NOTE(garrett): 500ms * 2^5 = 16s
	}

	for _, tt := range tests {
		delay := retryer.CalculateDelay(tt.attempt)
		if delay != tt.expected {
			t.Errorf("attempt %d: expected delay %v, got %v", tt.attempt, tt.expected, delay)
		}
	}
}

func TestRetryer_Do_SuccessOnFirstAttempt(t *testing.T) {
	mockClock := mocks.NewMockClock()
	retryer := backoff.NewRetryer(backoff.RetryConfig{
		MaxTries:  5,
		BaseDelay: 500 * time.Millisecond,
		Clock:     mockClock,
	})

	callCount := 0
	err := retryer.Do(context.Background(), func() error {
		callCount++
		return nil
	}, nil)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if callCount != 1 {
		t.Errorf("expected 1 call, got %d", callCount)
	}

	sleeps := mockClock.GetSleeps()
	if len(sleeps) != 0 {
		t.Errorf("expected 0 sleeps on success, got %d", len(sleeps))
	}
}

func TestRetryer_Do_SuccessAfterRetries(t *testing.T) {
	mockClock := mocks.NewMockClock()
	retryer := backoff.NewRetryer(backoff.RetryConfig{
		MaxTries:  5,
		BaseDelay: 500 * time.Millisecond,
		Clock:     mockClock,
	})

	callCount := 0
	targetErr := errors.New("temporary error")

	err := retryer.Do(context.Background(), func() error {
		callCount++
		if callCount < 3 {
			return targetErr
		}
		return nil
	}, nil)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if callCount != 3 {
		t.Errorf("expected 3 calls, got %d", callCount)
	}

	sleeps := mockClock.GetSleeps()
	if len(sleeps) != 2 {
		t.Errorf("expected 2 sleeps (after attempts 1 and 2), got %d", len(sleeps))
	}

	if sleeps[0] != 500*time.Millisecond {
		t.Errorf("expected first sleep 500ms, got %v", sleeps[0])
	}
	if sleeps[1] != 1*time.Second {
		t.Errorf("expected second sleep 1s, got %v", sleeps[1])
	}
}

func TestRetryer_Do_MaxRetriesExceeded(t *testing.T) {
	mockClock := mocks.NewMockClock()
	retryer := backoff.NewRetryer(backoff.RetryConfig{
		MaxTries:  3,
		BaseDelay: 100 * time.Millisecond,
		Clock:     mockClock,
	})

	callCount := 0
	finalErr := errors.New("persistent error")

	err := retryer.Do(context.Background(), func() error {
		callCount++
		return finalErr
	}, nil)

	if err != finalErr {
		t.Errorf("expected error %v, got %v", finalErr, err)
	}

	if callCount != 3 {
		t.Errorf("expected 3 calls (max retries), got %d", callCount)
	}

	sleeps := mockClock.GetSleeps()
	if len(sleeps) != 2 {
		t.Errorf("expected 2 sleeps, got %d", len(sleeps))
	}
}

func TestRetryer_Do_OnRetryCallback(t *testing.T) {
	mockClock := mocks.NewMockClock()
	retryer := backoff.NewRetryer(backoff.RetryConfig{
		MaxTries:  3,
		BaseDelay: 100 * time.Millisecond,
		Clock:     mockClock,
	})

	var callbacks []struct {
		attempt int
		delay   time.Duration
		err     error
	}

	targetErr := errors.New("test error")

	_ = retryer.Do(context.Background(), func() error {
		return targetErr
	}, func(attempt int, delay time.Duration, err error) {
		callbacks = append(callbacks, struct {
			attempt int
			delay   time.Duration
			err     error
		}{attempt, delay, err})
	})

	if len(callbacks) != 2 {
		t.Errorf("expected 2 callbacks, got %d", len(callbacks))
	}

	for i, cb := range callbacks {
		if cb.attempt != i+1 {
			t.Errorf("callback %d: expected attempt %d, got %d", i, i+1, cb.attempt)
		}
		if cb.err != targetErr {
			t.Errorf("callback %d: expected error %v, got %v", i, targetErr, cb.err)
		}
	}

	if callbacks[0].delay != 100*time.Millisecond {
		t.Errorf("first callback: expected delay 100ms, got %v", callbacks[0].delay)
	}

	if callbacks[1].delay != 200*time.Millisecond {
		t.Errorf("second callback: expected delay 200ms, got %v", callbacks[1].delay)
	}
}

func TestRetryer_Do_ContextCancellation(t *testing.T) {
	mockClock := mocks.NewMockClock()
	retryer := backoff.NewRetryer(backoff.RetryConfig{
		MaxTries:  5,
		BaseDelay: 1 * time.Hour,
		Clock:     mockClock,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := retryer.Do(ctx, func() error {
		return errors.New("will retry")
	}, nil)

	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestRetryer_Do_ContextCancellationDuringSleep(t *testing.T) {
	mockClock := mocks.NewMockClock()
	mockClock.SetSleepError(context.Canceled)

	retryer := backoff.NewRetryer(backoff.RetryConfig{
		MaxTries:  5,
		BaseDelay: 100 * time.Millisecond,
		Clock:     mockClock,
	})

	ctx := context.Background()
	callCount := 0
	err := retryer.Do(ctx, func() error {
		callCount++
		return errors.New("retry me")
	}, nil)

	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestRetryer_Do_ZeroMaxTries(t *testing.T) {
	mockClock := mocks.NewMockClock()
	retryer := backoff.NewRetryer(backoff.RetryConfig{
		MaxTries:  0,
		BaseDelay: 100 * time.Millisecond,
		Clock:     mockClock,
	})

	callCount := 0
	err := retryer.Do(context.Background(), func() error {
		callCount++
		return nil
	}, nil)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if callCount != 0 {
		t.Errorf("expected 0 calls with MaxTries=0, got %d", callCount)
	}
}

func TestRetryer_Do_ZeroBaseDelay(t *testing.T) {
	mockClock := mocks.NewMockClock()
	retryer := backoff.NewRetryer(backoff.RetryConfig{
		MaxTries:  3,
		BaseDelay: 0,
		Clock:     mockClock,
	})

	callCount := 0
	err := retryer.Do(context.Background(), func() error {
		callCount++
		if callCount < 3 {
			return errors.New("retry")
		}
		return nil
	}, nil)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if callCount != 3 {
		t.Errorf("expected 3 calls, got %d", callCount)
	}

	sleeps := mockClock.GetSleeps()
	for i, sleep := range sleeps {
		if sleep != 0 {
			t.Errorf("sleep %d: expected 0 delay, got %v", i, sleep)
		}
	}
}

func TestDefaultRetryer(t *testing.T) {
	mockClock := mocks.NewMockClock()
	retryer := backoff.DefaultRetryer(mockClock)

	err := retryer.Do(context.Background(), func() error {
		return nil
	}, nil)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRetryer_DefaultClock(t *testing.T) {
	retryer := backoff.NewRetryer(backoff.RetryConfig{
		MaxTries:  1,
		BaseDelay: 1 * time.Millisecond,
		Clock:     nil,
	})

	err := retryer.Do(context.Background(), func() error {
		return nil
	}, nil)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
