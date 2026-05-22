package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"vectis/internal/api"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
)

func TestAPIServer_TriggerJob_OrchestrationUsesRepositories(t *testing.T) {
	jobs := mocks.NewMockJobsRepository()
	jobs.Definitions["job-1"] = `{"id":"job-1","root":{"uses":"builtins/shell","with":{"command":"echo hi"}}}`
	jobs.DefinitionVersions["job-1"] = 3

	runs := mocks.NewMockRunsRepository()
	runs.CreateRunID = "run-1"
	runs.CreateRunIndex = 7

	logger := mocks.NewMockLogger()
	queue := mocks.NewMockQueueService()
	server := api.NewAPIServerWithRepositories(logger, jobs, runs, mocks.StubEphemeralRunStarter{})
	server.SetQueueClient(queue)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/job-1", bytes.NewReader(nil))
	req.SetPathValue("id", "job-1")
	rec := httptest.NewRecorder()

	server.TriggerJob(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d", http.StatusAccepted, rec.Code)
	}

	lastCreateJobID, lastDefVersion := runs.SnapshotLastCreate()
	if lastCreateJobID != "job-1" {
		t.Fatalf("expected create run for job-1, got %q", lastCreateJobID)
	}

	if lastDefVersion != 3 {
		t.Fatalf("expected stored trigger to use current definition_version 3, got %d", lastDefVersion)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(queue.GetJobs()) >= 1 && len(runs.SnapshotTouchedRunIDs()) >= 1 {
			break
		}

		time.Sleep(5 * time.Millisecond)
	}

	touched := runs.SnapshotTouchedRunIDs()
	if len(touched) != 1 || touched[0] != "run-1" {
		t.Fatalf("expected touch for run-1, got %+v", touched)
	}

	enqueued := queue.GetJobs()
	if len(enqueued) != 1 {
		t.Fatalf("expected one enqueued job, got %d", len(enqueued))
	}

	if enqueued[0].GetId() != "job-1" || enqueued[0].GetRunId() != "run-1" {
		t.Fatalf("unexpected queued payload: id=%q run=%q", enqueued[0].GetId(), enqueued[0].GetRunId())
	}
}

func TestAPIServer_TriggerJob_OrchestrationUsesTargetCell(t *testing.T) {
	jobs := mocks.NewMockJobsRepository()
	jobs.Definitions["job-target-cell"] = `{"id":"job-target-cell","root":{"uses":"builtins/shell","with":{"command":"echo hi"}}}`
	jobs.DefinitionVersions["job-target-cell"] = 4

	runs := mocks.NewMockRunsRepository()
	runs.CreateRunID = "run-target-cell"
	runs.CreateRunIndex = 1

	server := api.NewAPIServerWithRepositories(mocks.NewMockLogger(), jobs, runs, mocks.StubEphemeralRunStarter{})
	server.SetQueueClient(mocks.NewMockQueueService())

	body := bytes.NewBufferString(`{"cell_id":"iad-a"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/job-target-cell", body)
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "job-target-cell")
	rec := httptest.NewRecorder()

	server.TriggerJob(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d: %s", http.StatusAccepted, rec.Code, rec.Body.String())
	}

	if got := runs.SnapshotLastCreateTargetCell(); got != "iad-a" {
		t.Fatalf("expected target cell iad-a, got %q", got)
	}
}

func TestAPIServer_GetJobRuns_OrchestrationUsesRunsRepository(t *testing.T) {
	jobs := mocks.NewMockJobsRepository()
	jobs.Definitions["job-1"] = `{"id":"job-1"}`
	runs := mocks.NewMockRunsRepository()
	runs.ListByJobResults = []dal.RunRecord{{
		RunID:    "run-2",
		RunIndex: 2,
		Status:   "failed",
	}}

	server := api.NewAPIServerWithRepositories(mocks.NewMockLogger(), jobs, runs, mocks.StubEphemeralRunStarter{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-1/runs?after_index=1", nil)
	req.SetPathValue("id", "job-1")
	rec := httptest.NewRecorder()

	server.GetJobRuns(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	lastAfterIndex := runs.SnapshotLastListAfterIndex()
	if lastAfterIndex == nil || *lastAfterIndex != 1 {
		t.Fatalf("expected after_index=1 to be passed to repository, got %+v", lastAfterIndex)
	}

	var resp struct {
		Data       []map[string]any `json:"data"`
		NextCursor *int64           `json:"next_cursor,omitempty"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	body := resp.Data
	if len(body) != 1 {
		t.Fatalf("expected one run in response, got %d", len(body))
	}

	if body[0]["run_id"] != "run-2" {
		t.Fatalf("expected run_id run-2, got %v", body[0]["run_id"])
	}
}

func TestAPIServer_GetJobRuns_OrchestrationParsesSinceTime(t *testing.T) {
	jobs := mocks.NewMockJobsRepository()
	jobs.Definitions["job-1"] = `{"id":"job-1"}`
	runs := mocks.NewMockRunsRepository()

	server := api.NewAPIServerWithRepositories(mocks.NewMockLogger(), jobs, runs, mocks.StubEphemeralRunStarter{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-1/runs?since=2026-05-15T12:30:00Z", nil)
	req.SetPathValue("id", "job-1")
	rec := httptest.NewRecorder()

	server.GetJobRuns(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	lastSince := runs.SnapshotLastListSinceTime()
	want := time.Date(2026, 5, 15, 12, 30, 0, 0, time.UTC)
	if lastSince == nil || !lastSince.Equal(want) {
		t.Fatalf("expected since timestamp %s, got %+v", want.Format(time.RFC3339), lastSince)
	}
}
