package queueclient_test

import (
	"context"
	"errors"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/queueclient"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type flakyQueue struct {
	calls int
}

func (f *flakyQueue) Enqueue(ctx context.Context, job *api.Job) (*api.Empty, error) {
	f.calls++
	if f.calls < 2 {
		return nil, status.Error(codes.Unavailable, "try again")
	}
	return &api.Empty{}, nil
}

var _ interfaces.QueueService = (*flakyQueue)(nil)

func TestIsTransientRPCError(t *testing.T) {
	t.Parallel()

	if queueclient.IsTransientRPCError(nil) {
		t.Error("nil should not be transient")
	}

	if queueclient.IsTransientRPCError(errors.New("plain")) {
		t.Error("plain error should not be transient")
	}

	if !queueclient.IsTransientRPCError(status.Error(codes.Unavailable, "x")) {
		t.Error("Unavailable should be transient")
	}

	if !queueclient.IsTransientRPCError(status.Error(codes.DeadlineExceeded, "x")) {
		t.Error("DeadlineExceeded should be transient")
	}

	if !queueclient.IsTransientRPCError(status.Error(codes.ResourceExhausted, "x")) {
		t.Error("ResourceExhausted should be transient")
	}

	if queueclient.IsTransientRPCError(status.Error(codes.InvalidArgument, "x")) {
		t.Error("InvalidArgument should not be transient")
	}

	u := status.Error(codes.Unavailable, "x")
	if queueclient.IsTransientEnqueueError(u) != queueclient.IsTransientRPCError(u) {
		t.Error("IsTransientEnqueueError should match IsTransientRPCError")
	}
}

func TestEnqueueWithRetry_SucceedsAfterUnavailable(t *testing.T) {
	t.Parallel()

	q := &flakyQueue{}
	log := mocks.NewMockLogger()
	jobID := "j1"
	runID := "r1"
	job := &api.Job{Id: &jobID, RunId: &runID}

	if err := queueclient.EnqueueWithRetry(context.Background(), q, job, log); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if q.calls != 2 {
		t.Fatalf("expected 2 Enqueue calls, got %d", q.calls)
	}
}
