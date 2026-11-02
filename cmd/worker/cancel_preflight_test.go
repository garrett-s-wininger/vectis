package main

import (
	"context"
	"errors"
	"testing"

	"vectis/internal/interfaces/mocks"
)

func TestQueuedCancellationStopsBeforeWorkerCore(t *testing.T) {
	w := &worker{store: &mocks.MockRunsRepository{CancelRequested: true}}
	err := w.executeWithLeaseRenewal(context.Background(), "queued-run", newExecutionClaimState("claim"), nil, nil)
	if !errors.Is(err, errRunCancelled) {
		t.Fatalf("expected cancellation before execution, got %v", err)
	}
}
