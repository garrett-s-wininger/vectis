package cell

import (
	"context"
	"errors"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewExecutionSubmissionTargetsEnvelopeCell(t *testing.T) {
	req := validJobRequest(t)
	submission, err := NewExecutionSubmission(req)
	if err != nil {
		t.Fatalf("NewExecutionSubmission: %v", err)
	}

	if submission.TargetCellID() != "iad-a" {
		t.Fatalf("target cell id: got %q, want iad-a", submission.TargetCellID())
	}

	if submission.Envelope == nil {
		t.Fatal("expected envelope to be decoded")
	}
}

func TestNewExecutionSubmissionDefaultsTargetCell(t *testing.T) {
	req := &api.JobRequest{Job: validExecutionEnvelope().Job}
	submission, err := NewExecutionSubmission(req)
	if err != nil {
		t.Fatalf("NewExecutionSubmission: %v", err)
	}

	if submission.TargetCellID() != dal.DefaultCellID {
		t.Fatalf("target cell id: got %q, want %q", submission.TargetCellID(), dal.DefaultCellID)
	}

	if submission.Envelope != nil {
		t.Fatalf("expected no envelope, got %+v", submission.Envelope)
	}
}

func TestSubmitToQueueEnqueuesRequest(t *testing.T) {
	queue := mocks.NewMockQueueService()
	req := validJobRequest(t)

	if err := SubmitToQueue(context.Background(), queue, req, mocks.NewMockLogger()); err != nil {
		t.Fatalf("SubmitToQueue: %v", err)
	}

	reqs := queue.GetJobRequests()
	if len(reqs) != 1 {
		t.Fatalf("expected one enqueued request, got %d", len(reqs))
	}

	if reqs[0] != req {
		t.Fatal("expected ingress to enqueue original request")
	}
}

func TestSubmitToQueueRejectsInvalidEnvelope(t *testing.T) {
	queue := mocks.NewMockQueueService()
	req := &api.JobRequest{
		Job: validExecutionEnvelope().Job,
		Metadata: map[string]string{
			ExecutionEnvelopeMetadataKey: "{",
		},
	}

	if err := SubmitToQueue(context.Background(), queue, req, mocks.NewMockLogger()); err == nil {
		t.Fatal("SubmitToQueue succeeded, want error")
	}

	if got := len(queue.GetJobRequests()); got != 0 {
		t.Fatalf("expected no enqueued requests, got %d", got)
	}
}

func TestQueueExecutionIngressRetriesTransientEnqueue(t *testing.T) {
	req := validJobRequest(t)
	queue := &transientQueue{failuresLeft: 1}

	if err := SubmitToQueue(context.Background(), queue, req, mocks.NewMockLogger()); err != nil {
		t.Fatalf("SubmitToQueue: %v", err)
	}

	if queue.calls != 2 {
		t.Fatalf("expected two enqueue attempts, got %d", queue.calls)
	}
}

func validJobRequest(t *testing.T) *api.JobRequest {
	t.Helper()

	env := validExecutionEnvelope()
	req := &api.JobRequest{Job: env.Job}
	if _, err := AttachExecutionEnvelope(req, dal.ExecutionDispatchRecord{
		RunID:             env.RunID,
		JobID:             env.Job.GetId(),
		SegmentID:         env.SegmentID,
		ExecutionID:       env.ExecutionID,
		CellID:            env.CellID,
		Attempt:           1,
		DefinitionVersion: env.DefinitionVersion,
		DefinitionHash:    env.DefinitionHash,
	}, env.CreatedAtUnixNano); err != nil {
		t.Fatalf("AttachExecutionEnvelope: %v", err)
	}

	return req
}

type transientQueue struct {
	failuresLeft int
	calls        int
}

func (q *transientQueue) Enqueue(ctx context.Context, req *api.JobRequest) (*api.Empty, error) {
	q.calls++
	if q.failuresLeft > 0 {
		q.failuresLeft--
		return nil, status.Error(codes.Unavailable, "try again")
	}

	return &api.Empty{}, nil
}

var _ interfaces.QueueService = (*transientQueue)(nil)

func TestSubmitToQueueReturnsQueueError(t *testing.T) {
	queue := mocks.NewMockQueueService()
	queue.SetEnqueueError(errors.New("nope"))

	if err := SubmitToQueue(context.Background(), queue, validJobRequest(t), mocks.NewMockLogger()); err == nil {
		t.Fatal("SubmitToQueue succeeded, want error")
	}
}
