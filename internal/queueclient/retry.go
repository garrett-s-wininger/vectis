package queueclient

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	EnqueueMaxAttempts = 6
	enqueueBaseDelay   = 80 * time.Millisecond
	enqueueMaxDelay    = 2 * time.Second
)

type EnqueueRetryOptions struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Jitter      func(time.Duration) time.Duration
	Sleep       func(context.Context, time.Duration) error
}

func DefaultEnqueueRetryOptions() EnqueueRetryOptions {
	return EnqueueRetryOptions{
		MaxAttempts: EnqueueMaxAttempts,
		BaseDelay:   enqueueBaseDelay,
		MaxDelay:    enqueueMaxDelay,
		Jitter:      fullJitter,
		Sleep:       sleepContext,
	}
}

func fullJitter(delay time.Duration) time.Duration {
	if delay <= 0 {
		return 0
	}

	return time.Duration(rand.Int64N(int64(delay) + 1))
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func normalizeEnqueueRetryOptions(opts EnqueueRetryOptions) EnqueueRetryOptions {
	defaults := DefaultEnqueueRetryOptions()
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = defaults.MaxAttempts
	}

	if opts.BaseDelay <= 0 {
		opts.BaseDelay = defaults.BaseDelay
	}

	if opts.MaxDelay <= 0 {
		opts.MaxDelay = defaults.MaxDelay
	}

	if opts.Jitter == nil {
		opts.Jitter = defaults.Jitter
	}

	if opts.Sleep == nil {
		opts.Sleep = defaults.Sleep
	}

	return opts
}

func IsTransientRPCError(err error) bool {
	if err == nil {
		return false
	}

	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

func IsTransientDequeueError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) {
		return false
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	if IsTransientRPCError(err) {
		return true
	}

	switch status.Code(err) {
	case codes.Internal, codes.Unknown, codes.Aborted:
		return true
	default:
		return false
	}
}

func IsTransientEnqueueError(err error) bool {
	return IsTransientRPCError(err)
}

func EnqueueWithRetry(ctx context.Context, q interfaces.QueueService, req *api.JobRequest, log interfaces.Logger) error {
	_, err := EnqueueWithRetryResult(ctx, q, req, log)
	return err
}

func EnqueueWithRetryResult(ctx context.Context, q interfaces.QueueService, req *api.JobRequest, log interfaces.Logger) (*api.Empty, error) {
	return EnqueueWithRetryResultOptions(ctx, q, req, log, EnqueueRetryOptions{})
}

func EnqueueWithRetryResultOptions(ctx context.Context, q interfaces.QueueService, req *api.JobRequest, log interfaces.Logger, opts EnqueueRetryOptions) (*api.Empty, error) {
	if q == nil {
		return nil, fmt.Errorf("queue service not available")
	}

	opts = normalizeEnqueueRetryOptions(opts)

	var lastErr error
	for attempt := 1; attempt <= opts.MaxAttempts; attempt++ {
		span := trace.SpanFromContext(ctx)
		span.AddEvent("queue.enqueue.attempt", trace.WithAttributes(
			attribute.Int("attempt", attempt),
			attribute.Int("max_attempts", opts.MaxAttempts),
		))

		empty, err := q.Enqueue(ctx, req)
		if err == nil {
			span.AddEvent("queue.enqueue.success", trace.WithAttributes(
				attribute.Int("attempt", attempt),
			))
			return empty, nil
		}

		lastErr = err
		span.AddEvent("queue.enqueue.error", trace.WithAttributes(
			attribute.Int("attempt", attempt),
			attribute.String("error", err.Error()),
		))

		if attempt == opts.MaxAttempts || !IsTransientEnqueueError(err) {
			return nil, err
		}

		delay := min(opts.BaseDelay*time.Duration(uint(1)<<uint(attempt-1)), opts.MaxDelay)
		delay = opts.Jitter(delay)

		log.Debug("enqueue: transient error (attempt %d/%d): %v; retrying in %v", attempt, opts.MaxAttempts, err, delay)

		if err := opts.Sleep(ctx, delay); err != nil {
			return nil, err
		}
	}

	return nil, lastErr
}
