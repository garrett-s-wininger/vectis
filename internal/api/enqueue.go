package api

import (
	"context"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	enqueueMaxAttempts = 6
	enqueueBaseDelay   = 80 * time.Millisecond
	enqueueMaxDelay    = 2 * time.Second
)

func enqueueWithRetry(ctx context.Context, q interfaces.QueueService, job *api.Job, log interfaces.Logger) error {
	var lastErr error
	for attempt := 1; attempt <= enqueueMaxAttempts; attempt++ {
		_, err := q.Enqueue(ctx, job)
		if err == nil {
			return nil
		}

		lastErr = err
		if attempt == enqueueMaxAttempts || !isTransientEnqueueError(err) {
			return err
		}

		delay := min(enqueueBaseDelay*time.Duration(uint(1)<<uint(attempt-1)), enqueueMaxDelay)

		log.Debug("enqueue: transient error (attempt %d/%d): %v; retrying in %v", attempt, enqueueMaxAttempts, err, delay)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return lastErr
}

func isTransientEnqueueError(err error) bool {
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
