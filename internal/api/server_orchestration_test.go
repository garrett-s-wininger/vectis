package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"vectis/internal/api"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
)

func TestAPIServer_TriggerJob_OrchestrationUsesRepositories(t *testing.T) {
	jobs := mocks.NewMockJobsRepository()
	definition := `{"id":"job-1","root":{"uses":"builtins/shell","with":{"command":"echo hi"}}}`
	jobs.Definitions["job-1"] = definition
	jobs.DefinitionVersions["job-1"] = 3

	runs := mocks.NewMockRunsRepository()
	runs.CreateRunID = "run-1"
	runs.CreateRunIndex = 7
	runs.PendingExecution = dal.ExecutionDispatchRecord{
		DefinitionVersion: 3,
		DefinitionHash:    dal.DefinitionHash(definition),
	}

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
	definition := `{"id":"job-target-cell","root":{"uses":"builtins/shell","with":{"command":"echo hi"}}}`
	jobs.Definitions["job-target-cell"] = definition
	jobs.DefinitionVersions["job-target-cell"] = 4

	runs := mocks.NewMockRunsRepository()
	runs.CreateRunID = "run-target-cell"
	runs.CreateRunIndex = 1
	runs.PendingExecution = dal.ExecutionDispatchRecord{
		DefinitionVersion: 4,
		DefinitionHash:    dal.DefinitionHash(definition),
	}

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

func TestAPIServer_TriggerJob_OrchestrationUsesTargetCells(t *testing.T) {
	jobs := mocks.NewMockJobsRepository()
	definition := `{"id":"job-target-cells","root":{"uses":"builtins/shell","with":{"command":"echo hi"}}}`
	jobs.Definitions["job-target-cells"] = definition
	jobs.DefinitionVersions["job-target-cells"] = 5

	runs := mocks.NewMockRunsRepository()
	runs.CreateRunID = "run-target-cells"
	runs.CreateRunIndex = 11
	runs.PendingExecution = dal.ExecutionDispatchRecord{
		DefinitionVersion: 5,
		DefinitionHash:    dal.DefinitionHash(definition),
	}

	queue := mocks.NewMockQueueService()
	logger := mocks.NewMockLogger()
	server := api.NewAPIServerWithRepositories(logger, jobs, runs, mocks.StubEphemeralRunStarter{})
	server.SetQueueClient(queue)
	server.SetExecutionIngress(cell.NewStaticExecutionRouter(map[string]cell.ExecutionIngress{
		"local": cell.NewQueueExecutionIngress(queue, logger),
		"iad-a": cell.NewQueueExecutionIngress(queue, logger),
		"pdx-b": cell.NewQueueExecutionIngress(queue, logger),
	}))

	body := bytes.NewBufferString(`{"cell_ids":["iad-a","pdx-b","iad-a"]}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/job-target-cells", body)
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "job-target-cells")
	rec := httptest.NewRecorder()

	server.TriggerJob(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d: %s", http.StatusAccepted, rec.Code, rec.Body.String())
	}

	if got, want := runs.SnapshotLastCreateTargetCells(), []string{"iad-a", "pdx-b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("expected target cells %+v, got %+v", want, got)
	}

	var resp struct {
		JobID    string `json:"job_id"`
		RunID    string `json:"run_id,omitempty"`
		RunIndex int    `json:"run_index,omitempty"`
		Runs     []struct {
			RunID    string `json:"run_id"`
			RunIndex int    `json:"run_index"`
			CellID   string `json:"cell_id"`
		} `json:"runs"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.JobID != "job-target-cells" {
		t.Fatalf("expected job_id job-target-cells, got %q", resp.JobID)
	}

	if resp.RunID != "" || resp.RunIndex != 0 {
		t.Fatalf("expected multi-cell response to omit single-run fields, got run_id=%q run_index=%d", resp.RunID, resp.RunIndex)
	}

	if len(resp.Runs) != 2 {
		t.Fatalf("expected two response runs, got %d", len(resp.Runs))
	}

	if resp.Runs[0].RunID != "run-target-cells-1" || resp.Runs[0].RunIndex != 11 || resp.Runs[0].CellID != "iad-a" {
		t.Fatalf("unexpected first run response: %+v", resp.Runs[0])
	}

	if resp.Runs[1].RunID != "run-target-cells-2" || resp.Runs[1].RunIndex != 12 || resp.Runs[1].CellID != "pdx-b" {
		t.Fatalf("unexpected second run response: %+v", resp.Runs[1])
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(queue.GetJobs()) >= 2 && len(runs.SnapshotTouchedRunIDs()) >= 2 {
			break
		}

		time.Sleep(5 * time.Millisecond)
	}

	touched := map[string]bool{}
	for _, runID := range runs.SnapshotTouchedRunIDs() {
		touched[runID] = true
	}

	for _, wantRunID := range []string{"run-target-cells-1", "run-target-cells-2"} {
		if !touched[wantRunID] {
			t.Fatalf("expected touched run %q, got %+v", wantRunID, touched)
		}
	}

	enqueued := queue.GetJobs()
	if len(enqueued) != 2 {
		t.Fatalf("expected two enqueued jobs, got %d", len(enqueued))
	}

	enqueuedRunIDs := map[string]bool{}
	for _, job := range enqueued {
		enqueuedRunIDs[job.GetRunId()] = true
	}

	for _, wantRunID := range []string{"run-target-cells-1", "run-target-cells-2"} {
		if !enqueuedRunIDs[wantRunID] {
			t.Fatalf("expected enqueued run %q, got %+v", wantRunID, enqueuedRunIDs)
		}
	}
}

func TestAPIServer_TriggerJob_DispatchesTargetCellThroughExecutionIngress(t *testing.T) {
	jobs := mocks.NewMockJobsRepository()
	definition := `{"id":"job-remote-cell","root":{"uses":"builtins/shell","with":{"command":"echo hi"}}}`
	jobs.Definitions["job-remote-cell"] = definition
	jobs.DefinitionVersions["job-remote-cell"] = 6

	runs := mocks.NewMockRunsRepository()
	runs.CreateRunID = "run-remote-cell"
	runs.CreateRunIndex = 1
	runs.PendingExecution = dal.ExecutionDispatchRecord{
		RunID:             "run-remote-cell",
		JobID:             "job-remote-cell",
		RunIndex:          1,
		SegmentID:         "segment-1",
		ExecutionID:       "execution-1",
		CellID:            "iad-a",
		Attempt:           1,
		DefinitionVersion: 6,
		DefinitionHash:    dal.DefinitionHash(definition),
	}

	ingress := &recordingExecutionIngress{done: make(chan struct{})}
	server := api.NewAPIServerWithRepositories(mocks.NewMockLogger(), jobs, runs, mocks.StubEphemeralRunStarter{})
	server.SetExecutionIngress(cell.NewStaticExecutionRouter(map[string]cell.ExecutionIngress{
		"iad-a": ingress,
	}))

	body := bytes.NewBufferString(`{"cell_id":"iad-a"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/job-remote-cell", body)
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "job-remote-cell")
	rec := httptest.NewRecorder()

	server.TriggerJob(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d: %s", http.StatusAccepted, rec.Code, rec.Body.String())
	}

	select {
	case <-ingress.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for execution ingress submission")
	}

	submission := ingress.snapshot()
	if submission.TargetCellID() != "iad-a" {
		t.Fatalf("target cell: got %q, want iad-a", submission.TargetCellID())
	}

	if submission.Request.GetJob().GetRunId() != "run-remote-cell" {
		t.Fatalf("submitted run id: got %q, want run-remote-cell", submission.Request.GetJob().GetRunId())
	}

	touched := runs.SnapshotTouchedRunIDs()
	if len(touched) != 1 || touched[0] != "run-remote-cell" {
		t.Fatalf("expected touch for run-remote-cell, got %+v", touched)
	}
}

func TestAPIServer_GetJobRuns_OrchestrationUsesRunsRepository(t *testing.T) {
	jobs := mocks.NewMockJobsRepository()
	jobs.Definitions["job-1"] = `{"id":"job-1"}`
	runs := mocks.NewMockRunsRepository()
	runs.ListByJobResults = []dal.RunRecord{{
		RunID:      "run-2",
		RunIndex:   2,
		Status:     "failed",
		OwningCell: "pdx-b",
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

	if body[0]["owning_cell"] != "pdx-b" {
		t.Fatalf("expected owning_cell pdx-b, got %v", body[0]["owning_cell"])
	}
}

type recordingExecutionIngress struct {
	submission cell.ExecutionSubmission
	done       chan struct{}
}

func (i *recordingExecutionIngress) SubmitExecution(ctx context.Context, submission cell.ExecutionSubmission) error {
	i.submission = submission
	close(i.done)
	return nil
}

func (i *recordingExecutionIngress) snapshot() cell.ExecutionSubmission {
	return i.submission
}

func TestAPIServer_GetJobRuns_OrchestrationParsesOwningCell(t *testing.T) {
	jobs := mocks.NewMockJobsRepository()
	jobs.Definitions["job-1"] = `{"id":"job-1"}`
	runs := mocks.NewMockRunsRepository()

	server := api.NewAPIServerWithRepositories(mocks.NewMockLogger(), jobs, runs, mocks.StubEphemeralRunStarter{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-1/runs?cell_id=pdx-b", nil)
	req.SetPathValue("id", "job-1")
	rec := httptest.NewRecorder()

	server.GetJobRuns(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	if got := runs.SnapshotLastListOwningCell(); got != "pdx-b" {
		t.Fatalf("expected owning cell pdx-b, got %q", got)
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
