package queueclient

import (
	"context"
	"errors"
	"fmt"
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
	if q == nil {
		return nil, fmt.Errorf("queue service not available")
	}

	var lastErr error
	for attempt := 1; attempt <= EnqueueMaxAttempts; attempt++ {
		span := trace.SpanFromContext(ctx)
		span.AddEvent("queue.enqueue.attempt", trace.WithAttributes(
			attribute.Int("attempt", attempt),
			attribute.Int("max_attempts", EnqueueMaxAttempts),
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
		if attempt == EnqueueMaxAttempts || !IsTransientEnqueueError(err) {
			return nil, err
		}

		delay := min(enqueueBaseDelay*time.Duration(uint(1)<<uint(attempt-1)), enqueueMaxDelay)

		log.Debug("enqueue: transient error (attempt %d/%d): %v; retrying in %v", attempt, EnqueueMaxAttempts, err, delay)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}

	return nil, lastErr
}
