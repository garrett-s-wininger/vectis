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

func (f *flakyQueue) Enqueue(ctx context.Context, req *api.JobRequest) (*api.Empty, error) {
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

	if !queueclient.IsTransientDequeueError(status.Error(codes.Unknown, "x")) {
		t.Error("Unknown should be transient for dequeue")
	}

	if !queueclient.IsTransientDequeueError(status.Error(codes.Internal, "x")) {
		t.Error("Internal should be transient for dequeue")
	}

	if !queueclient.IsTransientDequeueError(status.Error(codes.Aborted, "x")) {
		t.Error("Aborted should be transient for dequeue")
	}

	if queueclient.IsTransientDequeueError(status.Error(codes.InvalidArgument, "x")) {
		t.Error("InvalidArgument should not be transient for dequeue")
	}

	if queueclient.IsTransientDequeueError(context.Canceled) {
		t.Error("context.Canceled should not be transient for dequeue")
	}

	if queueclient.IsTransientDequeueError(context.DeadlineExceeded) {
		t.Error("context.DeadlineExceeded should not be transient for dequeue")
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
	req := &api.JobRequest{Job: &api.Job{Id: &jobID, RunId: &runID}}

	if err := queueclient.EnqueueWithRetry(context.Background(), q, req, log); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if q.calls != 2 {
		t.Fatalf("expected 2 Enqueue calls, got %d", q.calls)
	}
}
