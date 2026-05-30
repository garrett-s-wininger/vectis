package cellingress

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"

	"google.golang.org/protobuf/encoding/protojson"
)

func TestSubmitExecutionAcceptsLocalEnvelope(t *testing.T) {
	queue := mocks.NewMockQueueService()
	srv := NewQueueServer("iad-a", queue, mocks.NewMockLogger())
	req := httptest.NewRequest(http.MethodPost, "/cell/v1/executions", executionBody(t, validJobRequestForCell(t, "iad-a")))
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: got %d, want %d; body=%s", rr.Code, http.StatusAccepted, rr.Body.String())
	}

	var resp submitExecutionResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.Status != "accepted" {
		t.Fatalf("response status: got %q, want accepted", resp.Status)
	}

	if resp.CellID != "iad-a" {
		t.Fatalf("response cell_id: got %q, want iad-a", resp.CellID)
	}

	reqs := queue.GetJobRequests()
	if len(reqs) != 1 {
		t.Fatalf("queued requests: got %d, want 1", len(reqs))
	}

	if reqs[0].GetJob().GetRunId() != "run-1" {
		t.Fatalf("queued run id: got %q, want run-1", reqs[0].GetJob().GetRunId())
	}
}

func TestSubmitExecutionRequiresJobRequest(t *testing.T) {
	srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())
	req := httptest.NewRequest(http.MethodPost, "/cell/v1/executions", strings.NewReader(`{}`))
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	assertErrorCode(t, rr, http.StatusBadRequest, "missing_job_request")
}

func TestSubmitExecutionRequiresEnvelope(t *testing.T) {
	queue := mocks.NewMockQueueService()
	srv := NewQueueServer("iad-a", queue, mocks.NewMockLogger())
	jobReq := validJobRequestForCell(t, "iad-a")
	delete(jobReq.Metadata, cell.ExecutionEnvelopeMetadataKey)
	req := httptest.NewRequest(http.MethodPost, "/cell/v1/executions", executionBody(t, jobReq))
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	assertErrorCode(t, rr, http.StatusBadRequest, "missing_execution_envelope")
	if got := len(queue.GetJobRequests()); got != 0 {
		t.Fatalf("queued requests: got %d, want 0", got)
	}
}

func TestSubmitExecutionRejectsWrongCell(t *testing.T) {
	queue := mocks.NewMockQueueService()
	srv := NewQueueServer("iad-a", queue, mocks.NewMockLogger())
	req := httptest.NewRequest(http.MethodPost, "/cell/v1/executions", executionBody(t, validJobRequestForCell(t, "pdx-b")))
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	assertErrorCode(t, rr, http.StatusConflict, "wrong_cell")
	if got := len(queue.GetJobRequests()); got != 0 {
		t.Fatalf("queued requests: got %d, want 0", got)
	}
}

func TestSubmitExecutionReturnsQueueUnavailable(t *testing.T) {
	queue := mocks.NewMockQueueService()
	queue.SetEnqueueError(errors.New("queue closed"))
	srv := NewQueueServer("iad-a", queue, mocks.NewMockLogger())
	req := httptest.NewRequest(http.MethodPost, "/cell/v1/executions", executionBody(t, validJobRequestForCell(t, "iad-a")))
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	assertErrorCode(t, rr, http.StatusServiceUnavailable, "queue_unavailable")
}

func TestSubmitExecutionDurablyAcceptsBeforeReturningQueueUnavailable(t *testing.T) {
	queue := mocks.NewMockQueueService()
	queue.SetEnqueueError(errors.New("queue closed"))
	acceptances := &recordingAcceptanceStore{}
	srv := NewQueueServer("iad-a", queue, mocks.NewMockLogger())
	srv.SetAcceptanceStore(acceptances)
	req := httptest.NewRequest(http.MethodPost, "/cell/v1/executions", executionBody(t, validJobRequestForCell(t, "iad-a")))
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	assertErrorCode(t, rr, http.StatusServiceUnavailable, "queue_unavailable")

	got := acceptances.snapshot()
	if got.ExecutionID != "execution-1" {
		t.Fatalf("accepted execution: got %q, want execution-1", got.ExecutionID)
	}

	if got.RunID != "run-1" || got.JobID != "job-1" || got.CellID != "iad-a" {
		t.Fatalf("unexpected acceptance: %+v", got)
	}

	if got.DefinitionJSON == "" || got.RequestJSON == "" {
		t.Fatalf("acceptance should include definition and request JSON: %+v", got)
	}
}

func TestSubmitExecutionReturnsConflictWhenAcceptanceConflicts(t *testing.T) {
	queue := mocks.NewMockQueueService()
	acceptances := &recordingAcceptanceStore{err: dal.ErrConflict}
	srv := NewQueueServer("iad-a", queue, mocks.NewMockLogger())
	srv.SetAcceptanceStore(acceptances)
	req := httptest.NewRequest(http.MethodPost, "/cell/v1/executions", executionBody(t, validJobRequestForCell(t, "iad-a")))
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	assertErrorCode(t, rr, http.StatusConflict, "execution_conflict")
	if got := len(queue.GetJobRequests()); got != 0 {
		t.Fatalf("queued requests: got %d, want 0", got)
	}
}

func TestHealthEndpoints(t *testing.T) {
	srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())

	for _, path := range []string{"/health/live", "/health/ready"} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rr := httptest.NewRecorder()

			srv.ServeHTTP(rr, req)

			if rr.Code != http.StatusOK {
				t.Fatalf("status: got %d, want %d; body=%s", rr.Code, http.StatusOK, rr.Body.String())
			}
		})
	}
}

func executionBody(t *testing.T, req *api.JobRequest) *bytes.Reader {
	t.Helper()

	payload, err := protojson.Marshal(req)
	if err != nil {
		t.Fatalf("marshal job request: %v", err)
	}

	body, err := json.Marshal(submitExecutionRequest{JobRequest: payload})
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}

	return bytes.NewReader(body)
}

func validJobRequestForCell(t *testing.T, cellID string) *api.JobRequest {
	t.Helper()

	jobID := "job-1"
	runID := "run-1"
	action := "builtins/shell"
	req := &api.JobRequest{
		Job: &api.Job{
			Id:    &jobID,
			RunId: &runID,
			Root: &api.Node{
				Uses: &action,
				With: map[string]string{"command": "echo hi"},
			},
		},
	}

	if _, err := cell.AttachExecutionEnvelope(req, dal.ExecutionDispatchRecord{
		RunID:             runID,
		JobID:             jobID,
		SegmentID:         "segment-1",
		ExecutionID:       "execution-1",
		CellID:            cellID,
		Attempt:           1,
		DefinitionVersion: 3,
		DefinitionHash:    "sha256:abc123",
		RunIndex:          5,
	}, 123); err != nil {
		t.Fatalf("AttachExecutionEnvelope: %v", err)
	}

	return req
}

func assertErrorCode(t *testing.T, rr *httptest.ResponseRecorder, status int, code string) {
	t.Helper()

	if rr.Code != status {
		t.Fatalf("status: got %d, want %d; body=%s", rr.Code, status, rr.Body.String())
	}

	var resp errorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}

	if resp.Code != code {
		t.Fatalf("error code: got %q, want %q; body=%s", resp.Code, code, rr.Body.String())
	}
}

type recordingAcceptanceStore struct {
	acceptance dal.CellExecutionAcceptance
	err        error
}

func (s *recordingAcceptanceStore) AcceptExecution(ctx context.Context, acceptance dal.CellExecutionAcceptance) (bool, error) {
	if s.err != nil {
		return false, s.err
	}

	s.acceptance = acceptance
	return true, nil
}

func (s *recordingAcceptanceStore) snapshot() dal.CellExecutionAcceptance {
	return s.acceptance
}
