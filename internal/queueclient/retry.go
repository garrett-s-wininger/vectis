package queueclient

import (
	"context"
	"errors"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"

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

func EnqueueWithRetry(ctx context.Context, q interfaces.QueueService, job *api.Job, log interfaces.Logger) error {
	_, err := EnqueueWithRetryResult(ctx, q, job, log)
	return err
}

func EnqueueWithRetryResult(ctx context.Context, q interfaces.QueueService, job *api.Job, log interfaces.Logger) (*api.Empty, error) {
	var lastErr error
	for attempt := 1; attempt <= EnqueueMaxAttempts; attempt++ {
		empty, err := q.Enqueue(ctx, job)
		if err == nil {
			return empty, nil
		}

		lastErr = err
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
