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

func TestStaticExecutionRouterRoutesByTargetCell(t *testing.T) {
	iadIngress := &recordingIngress{}
	pdxIngress := &recordingIngress{}
	router := NewStaticExecutionRouter(map[string]ExecutionIngress{
		"iad-a": iadIngress,
		"pdx-b": pdxIngress,
	})

	iadSubmission, err := NewExecutionSubmission(validJobRequestForCell(t, "iad-a"))
	if err != nil {
		t.Fatalf("NewExecutionSubmission iad-a: %v", err)
	}

	pdxSubmission, err := NewExecutionSubmission(validJobRequestForCell(t, "pdx-b"))
	if err != nil {
		t.Fatalf("NewExecutionSubmission pdx-b: %v", err)
	}

	if err := router.SubmitExecution(context.Background(), iadSubmission); err != nil {
		t.Fatalf("SubmitExecution iad-a: %v", err)
	}

	if err := router.SubmitExecution(context.Background(), pdxSubmission); err != nil {
		t.Fatalf("SubmitExecution pdx-b: %v", err)
	}

	if len(iadIngress.submissions) != 1 {
		t.Fatalf("iad ingress submissions: got %d, want 1", len(iadIngress.submissions))
	}

	if len(pdxIngress.submissions) != 1 {
		t.Fatalf("pdx ingress submissions: got %d, want 1", len(pdxIngress.submissions))
	}

	if iadIngress.submissions[0].TargetCellID() != "iad-a" {
		t.Fatalf("iad target: got %q, want iad-a", iadIngress.submissions[0].TargetCellID())
	}

	if pdxIngress.submissions[0].TargetCellID() != "pdx-b" {
		t.Fatalf("pdx target: got %q, want pdx-b", pdxIngress.submissions[0].TargetCellID())
	}
}

func TestStaticExecutionRouterReturnsMissingRoute(t *testing.T) {
	router := NewStaticExecutionRouter(map[string]ExecutionIngress{})
	submission, err := NewExecutionSubmission(validJobRequestForCell(t, "iad-a"))
	if err != nil {
		t.Fatalf("NewExecutionSubmission: %v", err)
	}

	if err := router.SubmitExecution(context.Background(), submission); !IsCellNotRoutable(err) {
		t.Fatalf("expected ErrCellNotRoutable, got %v", err)
	}
}

func TestStaticExecutionRouterDefaultsBlankRouteToLocal(t *testing.T) {
	ingress := &recordingIngress{}
	router := NewStaticExecutionRouter(map[string]ExecutionIngress{
		"": ingress,
	})

	submission, err := NewExecutionSubmission(&api.JobRequest{Job: validExecutionEnvelope().Job})
	if err != nil {
		t.Fatalf("NewExecutionSubmission: %v", err)
	}

	if err := router.SubmitExecution(context.Background(), submission); err != nil {
		t.Fatalf("SubmitExecution: %v", err)
	}

	if len(ingress.submissions) != 1 {
		t.Fatalf("ingress submissions: got %d, want 1", len(ingress.submissions))
	}

	if ingress.submissions[0].TargetCellID() != dal.DefaultCellID {
		t.Fatalf("target cell: got %q, want %q", ingress.submissions[0].TargetCellID(), dal.DefaultCellID)
	}
}

func TestSubmitToLocalQueueEnqueuesMatchingCellRequest(t *testing.T) {
	queue := mocks.NewMockQueueService()
	req := validJobRequest(t)

	if err := SubmitToLocalQueue(context.Background(), "iad-a", queue, req, mocks.NewMockLogger()); err != nil {
		t.Fatalf("SubmitToLocalQueue: %v", err)
	}

	reqs := queue.GetJobRequests()
	if len(reqs) != 1 {
		t.Fatalf("expected one enqueued request, got %d", len(reqs))
	}

	if reqs[0] != req {
		t.Fatal("expected ingress to enqueue original request")
	}
}

func TestSubmitToLocalQueueRejectsRemoteCellRequest(t *testing.T) {
	queue := mocks.NewMockQueueService()
	req := validJobRequestForCell(t, "pdx-b")

	if err := SubmitToLocalQueue(context.Background(), "iad-a", queue, req, mocks.NewMockLogger()); !IsCellNotRoutable(err) {
		t.Fatalf("expected ErrCellNotRoutable, got %v", err)
	}

	if got := len(queue.GetJobRequests()); got != 0 {
		t.Fatalf("expected no enqueued requests, got %d", got)
	}
}

func TestSubmitToLocalQueueRejectsInvalidEnvelope(t *testing.T) {
	queue := mocks.NewMockQueueService()
	req := &api.JobRequest{
		Job: validExecutionEnvelope().Job,
		Metadata: map[string]string{
			ExecutionEnvelopeMetadataKey: "{",
		},
	}

	if err := SubmitToLocalQueue(context.Background(), "iad-a", queue, req, mocks.NewMockLogger()); err == nil {
		t.Fatal("SubmitToLocalQueue succeeded, want error")
	}

	if got := len(queue.GetJobRequests()); got != 0 {
		t.Fatalf("expected no enqueued requests, got %d", got)
	}
}

func TestQueueExecutionIngressRetriesTransientEnqueue(t *testing.T) {
	req := validJobRequest(t)
	queue := &transientQueue{failuresLeft: 1}

	if err := SubmitToLocalQueue(context.Background(), "iad-a", queue, req, mocks.NewMockLogger()); err != nil {
		t.Fatalf("SubmitToLocalQueue: %v", err)
	}

	if queue.calls != 2 {
		t.Fatalf("expected two enqueue attempts, got %d", queue.calls)
	}
}

func validJobRequest(t *testing.T) *api.JobRequest {
	t.Helper()
	return validJobRequestForCell(t, "iad-a")
}

func validJobRequestForCell(t *testing.T, cellID string) *api.JobRequest {
	t.Helper()

	env := validExecutionEnvelope()
	req := &api.JobRequest{Job: env.Job}
	if _, err := AttachExecutionEnvelope(req, dal.ExecutionDispatchRecord{
		RunID:             env.RunID,
		JobID:             env.Job.GetId(),
		SegmentID:         env.SegmentID,
		ExecutionID:       env.ExecutionID,
		CellID:            cellID,
		Attempt:           1,
		DefinitionVersion: env.DefinitionVersion,
		DefinitionHash:    env.DefinitionHash,
	}, env.CreatedAtUnixNano); err != nil {
		t.Fatalf("AttachExecutionEnvelope: %v", err)
	}

	return req
}

type recordingIngress struct {
	submissions []ExecutionSubmission
	err         error
}

func (i *recordingIngress) SubmitExecution(ctx context.Context, submission ExecutionSubmission) error {
	if i.err != nil {
		return i.err
	}

	i.submissions = append(i.submissions, submission)
	return nil
}

var _ ExecutionIngress = (*recordingIngress)(nil)

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

func TestSubmitToLocalQueueReturnsQueueError(t *testing.T) {
	queue := mocks.NewMockQueueService()
	queue.SetEnqueueError(errors.New("nope"))

	if err := SubmitToLocalQueue(context.Background(), "iad-a", queue, validJobRequest(t), mocks.NewMockLogger()); err == nil {
		t.Fatal("SubmitToLocalQueue succeeded, want error")
	}
}
