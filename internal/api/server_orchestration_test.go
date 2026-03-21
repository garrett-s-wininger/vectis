package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"vectis/internal/api"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
)

func TestAPIServer_TriggerJob_OrchestrationUsesRepositories(t *testing.T) {
	jobs := mocks.NewMockJobsRepository()
	jobs.Definitions["job-1"] = `{"id":"job-1","root":{"uses":"builtins/shell","with":{"command":"echo hi"}}}`

	runs := mocks.NewMockRunsRepository()
	runs.CreateRunID = "run-1"
	runs.CreateRunIndex = 7

	logger := mocks.NewMockLogger()
	queue := mocks.NewMockQueueService()
	server := api.NewAPIServerWithRepositories(logger, jobs, runs)
	server.SetQueueClient(queue)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/job-1", bytes.NewReader(nil))
	req.SetPathValue("id", "job-1")
	rec := httptest.NewRecorder()

	server.TriggerJob(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d", http.StatusAccepted, rec.Code)
	}

	if runs.LastCreateJobID != "job-1" {
		t.Fatalf("expected create run for job-1, got %q", runs.LastCreateJobID)
	}

	if len(runs.TouchedRunIDs) != 1 || runs.TouchedRunIDs[0] != "run-1" {
		t.Fatalf("expected touch for run-1, got %+v", runs.TouchedRunIDs)
	}

	enqueued := queue.GetJobs()
	if len(enqueued) != 1 {
		t.Fatalf("expected one enqueued job, got %d", len(enqueued))
	}

	if enqueued[0].GetId() != "job-1" || enqueued[0].GetRunId() != "run-1" {
		t.Fatalf("unexpected queued payload: id=%q run=%q", enqueued[0].GetId(), enqueued[0].GetRunId())
	}
}

func TestAPIServer_GetJobRuns_OrchestrationUsesRunsRepository(t *testing.T) {
	jobs := mocks.NewMockJobsRepository()
	runs := mocks.NewMockRunsRepository()
	runs.ListByJobResults = []dal.RunRecord{{
		RunID:    "run-2",
		RunIndex: 2,
		Status:   "failed",
	}}

	server := api.NewAPIServerWithRepositories(mocks.NewMockLogger(), jobs, runs)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-1/runs?since=1", nil)
	req.SetPathValue("id", "job-1")
	rec := httptest.NewRecorder()

	server.GetJobRuns(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	if runs.LastListSince == nil || *runs.LastListSince != 1 {
		t.Fatalf("expected since=1 to be passed to repository, got %+v", runs.LastListSince)
	}

	var body []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(body) != 1 {
		t.Fatalf("expected one run in response, got %d", len(body))
	}

	if body[0]["run_id"] != "run-2" {
		t.Fatalf("expected run_id run-2, got %v", body[0]["run_id"])
	}
}
