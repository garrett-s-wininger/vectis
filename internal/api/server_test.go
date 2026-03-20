package api_test

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"vectis/internal/api"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/runstore"
	"vectis/internal/testutil/dbtest"

	"github.com/google/uuid"
)

func setupTestServer(t *testing.T) (*api.APIServer, *mocks.MockLogger, *mocks.MockQueueService, *sql.DB) {
	db := dbtest.NewTestDB(t)
	logger := mocks.NewMockLogger()
	queueService := mocks.NewMockQueueService()

	server := api.NewAPIServer(logger, db)
	server.SetQueueClient(queueService)

	return server, logger, queueService, db
}

func TestAPIServer_CreateJob_Success(t *testing.T) {
	server, logger, _, db := setupTestServer(t)

	jobDef := map[string]any{
		"id": "test-job-1",
		"root": map[string]any{
			"id":   "node-1",
			"uses": "builtins/shell",
			"with": map[string]string{
				"command": "echo hello",
			},
		},
	}

	body, _ := json.Marshal(jobDef)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.CreateJob(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("expected status %d, got %d", http.StatusCreated, rec.Code)
	}

	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM stored_jobs WHERE job_id = ?", "test-job-1").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query db: %v", err)
	}

	if count != 1 {
		t.Errorf("expected 1 job in db, got %d", count)
	}

	infoCalls := logger.GetInfoCalls()
	hasStoredMsg := false
	for _, msg := range infoCalls {
		if strings.Contains(msg, "Stored job: test-job-1") {
			hasStoredMsg = true
			break
		}
	}

	if !hasStoredMsg {
		t.Errorf("expected logger to contain 'Stored job: test-job-1', got: %v", infoCalls)
	}
}

func TestAPIServer_CreateJob_InvalidContentType(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()

	server.CreateJob(rec, req)

	if rec.Code != http.StatusUnsupportedMediaType {
		t.Errorf("expected status %d, got %d", http.StatusUnsupportedMediaType, rec.Code)
	}
}

func TestAPIServer_CreateJob_InvalidJSON(t *testing.T) {
	server, _, _, db := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader("not json"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.CreateJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM stored_jobs").Scan(&count)
	if count != 0 {
		t.Errorf("expected 0 jobs in db, got %d", count)
	}
}

func TestAPIServer_CreateJob_DuplicateJobID(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	jobDef := map[string]any{
		"id": "duplicate-job",
		"root": map[string]any{
			"uses": "builtins/shell",
			"with": map[string]string{
				"command": "echo hello",
			},
		},
	}

	body, _ := json.Marshal(jobDef)
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewReader(body))
	req1.Header.Set("Content-Type", "application/json")
	rec1 := httptest.NewRecorder()
	server.CreateJob(rec1, req1)

	if rec1.Code != http.StatusCreated {
		t.Fatalf("first job creation failed: %d", rec1.Code)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	server.CreateJob(rec2, req2)

	if rec2.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d for duplicate, got %d", http.StatusInternalServerError, rec2.Code)
	}
}

func TestAPIServer_GetJobs_Empty(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	rec := httptest.NewRecorder()

	server.GetJobs(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	var jobs []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &jobs); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if len(jobs) != 0 {
		t.Errorf("expected empty array, got %v", jobs)
	}

	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", ct)
	}
}

func TestAPIServer_GetJobs_WithJobs(t *testing.T) {
	server, _, _, db := setupTestServer(t)

	job1 := `{"id": "job-1", "root": {"uses": "builtins/shell"}}`
	job2 := `{"id": "job-2", "root": {"uses": "builtins/shell"}}`
	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", "job-1", job1)
	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", "job-2", job2)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	rec := httptest.NewRecorder()

	server.GetJobs(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	var jobs []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &jobs); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if len(jobs) != 2 {
		t.Errorf("expected 2 jobs, got %d", len(jobs))
	}

	if jobs[0]["name"] != "job-1" && jobs[0]["name"] != "job-2" {
		t.Errorf("unexpected job name: %v", jobs[0]["name"])
	}

	if jobs[0]["definition"] == nil {
		t.Error("expected definition to be present")
	}
}

func TestAPIServer_GetJob_Success(t *testing.T) {
	server, _, _, db := setupTestServer(t)

	jobDef := `{"id": "job-get-1", "root": {"uses": "builtins/shell"}}`
	if _, err := db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", "job-get-1", jobDef); err != nil {
		t.Fatalf("insert job: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-get-1", nil)
	req.SetPathValue("id", "job-get-1")
	rec := httptest.NewRecorder()

	server.GetJob(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected Content-Type application/json, got %s", ct)
	}

	if strings.TrimSpace(rec.Body.String()) != jobDef {
		t.Fatalf("expected body %s, got %s", jobDef, rec.Body.String())
	}
}

func TestAPIServer_GetJob_NotFound(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/nonexistent", nil)
	req.SetPathValue("id", "nonexistent")
	rec := httptest.NewRecorder()

	server.GetJob(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, rec.Code)
	}
}

func TestAPIServer_DeleteJob_Success(t *testing.T) {
	server, logger, _, db := setupTestServer(t)
	jobDef := `{"id": "job-to-delete", "root": {"uses": "builtins/shell"}}`
	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", "job-to-delete", jobDef)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/jobs/job-to-delete", nil)
	req.SetPathValue("id", "job-to-delete")
	rec := httptest.NewRecorder()

	server.DeleteJob(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("expected status %d, got %d", http.StatusNoContent, rec.Code)
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM stored_jobs WHERE job_id = ?", "job-to-delete").Scan(&count)
	if count != 0 {
		t.Errorf("expected 0 jobs with id 'job-to-delete', got %d", count)
	}

	infoCalls := logger.GetInfoCalls()
	hasDeletedMsg := false
	for _, msg := range infoCalls {
		if strings.Contains(msg, "Deleted job: job-to-delete") {
			hasDeletedMsg = true
			break
		}
	}

	if !hasDeletedMsg {
		t.Errorf("expected logger to contain 'Deleted job: job-to-delete', got: %v", infoCalls)
	}
}

func TestAPIServer_DeleteJob_MissingID(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/jobs/", nil)
	rec := httptest.NewRecorder()

	server.DeleteJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestAPIServer_TriggerJob_Success(t *testing.T) {
	server, logger, queueService, db := setupTestServer(t)
	jobDef := `{"id": "job-to-trigger", "root": {"uses": "builtins/shell", "with": {"command": "echo test"}}}`
	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", "job-to-trigger", jobDef)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/job-to-trigger", nil)
	req.SetPathValue("id", "job-to-trigger")
	rec := httptest.NewRecorder()

	server.TriggerJob(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, rec.Code)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 1 {
		t.Errorf("expected 1 job enqueued, got %d", len(jobs))
	}

	if jobs[0].GetId() != "job-to-trigger" {
		t.Errorf("expected job id 'job-to-trigger', got %s", jobs[0].GetId())
	}

	runID := jobs[0].GetRunId()
	if runID == "" {
		t.Errorf("expected run id to be set on enqueued job")
	}

	var dbStatus string
	var runIndex int
	err := db.QueryRow("SELECT status, run_index FROM job_runs WHERE job_id = ? AND run_id = ?", "job-to-trigger", runID).Scan(&dbStatus, &runIndex)
	if err != nil {
		t.Fatalf("expected job_runs row for triggered job: %v", err)
	}

	if dbStatus != "queued" {
		t.Errorf("expected job_runs.status queued, got %s", dbStatus)
	}

	if runIndex != 1 {
		t.Errorf("expected run_index 1, got %d", runIndex)
	}

	infoCalls := logger.GetInfoCalls()
	hasTriggeredMsg := false
	for _, msg := range infoCalls {
		if strings.Contains(msg, "Triggered job: job-to-trigger") {
			hasTriggeredMsg = true
			break
		}
	}

	if !hasTriggeredMsg {
		t.Errorf("expected logger to contain 'Triggered job: job-to-trigger', got: %v", infoCalls)
	}
}

func TestAPIServer_TriggerJob_NotFound(t *testing.T) {
	server, _, queueService, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/nonexistent-job", nil)
	req.SetPathValue("id", "nonexistent-job")
	rec := httptest.NewRecorder()

	server.TriggerJob(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, rec.Code)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 0 {
		t.Errorf("expected 0 jobs enqueued, got %d", len(jobs))
	}
}

func TestAPIServer_TriggerJob_MissingID(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/", nil)
	rec := httptest.NewRecorder()

	server.TriggerJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestAPIServer_TriggerJob_QueueError(t *testing.T) {
	server, logger, queueService, db := setupTestServer(t)
	jobDef := `{"id": "job-trigger-fail", "root": {"uses": "builtins/shell"}}`
	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", "job-trigger-fail", jobDef)

	queueService.SetEnqueueError(errors.New("queue unavailable"))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/job-trigger-fail", nil)
	req.SetPathValue("id", "job-trigger-fail")
	rec := httptest.NewRecorder()

	server.TriggerJob(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, rec.Code)
	}

	errorCalls := logger.GetErrorCalls()
	hasQueueError := false
	for _, msg := range errorCalls {
		if strings.Contains(msg, "Failed to enqueue job") {
			hasQueueError = true
			break
		}
	}

	if !hasQueueError {
		t.Errorf("expected logger error 'Failed to enqueue job', got: %v", errorCalls)
	}
}

func TestAPIServer_GetJobRuns_ReturnsStatusAndFailureReasonAfterStatusTransitions(t *testing.T) {
	server, _, queueService, db := setupTestServer(t)
	jobDef := `{"id": "job-runs-status", "root": {"uses": "builtins/shell", "with": {"command": "echo test"}}}`
	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", "job-runs-status", jobDef)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/job-runs-status", nil)
	req.SetPathValue("id", "job-runs-status")
	rec := httptest.NewRecorder()
	server.TriggerJob(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("trigger: expected 202, got %d", rec.Code)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job enqueued, got %d", len(jobs))
	}

	runID := jobs[0].GetRunId()
	store := runstore.NewStore(db)
	ctx := req.Context()

	if err := store.MarkRunRunning(ctx, runID); err != nil {
		t.Fatalf("MarkRunRunning: %v", err)
	}

	if err := store.MarkRunFailed(ctx, runID, "step failed: exit code 1"); err != nil {
		t.Fatalf("MarkRunFailed: %v", err)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-runs-status/runs", nil)
	getReq.SetPathValue("id", "job-runs-status")
	getRec := httptest.NewRecorder()
	server.GetJobRuns(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetJobRuns: expected 200, got %d", getRec.Code)
	}

	var runs []struct {
		RunID         string  `json:"run_id"`
		RunIndex      int     `json:"run_index"`
		Status        string  `json:"status"`
		StartedAt     *string `json:"started_at,omitempty"`
		FinishedAt    *string `json:"finished_at,omitempty"`
		FailureReason *string `json:"failure_reason,omitempty"`
	}

	if err := json.NewDecoder(getRec.Body).Decode(&runs); err != nil {
		t.Fatalf("decode runs: %v", err)
	}

	if len(runs) != 1 {
		t.Fatalf("expected 1 run, got %d", len(runs))
	}

	if runs[0].Status != "failed" {
		t.Errorf("status want failed, got %s", runs[0].Status)
	}

	if runs[0].FailureReason == nil || *runs[0].FailureReason != "step failed: exit code 1" {
		t.Errorf("failure_reason want %q, got %v", "step failed: exit code 1", runs[0].FailureReason)
	}

	if runs[0].StartedAt == nil {
		t.Error("started_at should be set after MarkRunRunning")
	}

	if runs[0].FinishedAt == nil {
		t.Error("finished_at should be set after MarkRunFailed")
	}
}

func TestAPIServer_UpdateJobDefinition_Success(t *testing.T) {
	server, logger, _, db := setupTestServer(t)
	initialDef := `{"id": "job-to-update", "root": {"uses": "builtins/shell", "with": {"command": "echo old"}}}`
	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", "job-to-update", initialDef)

	newDef := map[string]any{
		"id": "job-to-update",
		"root": map[string]any{
			"uses": "builtins/shell",
			"with": map[string]string{
				"command": "echo new",
			},
		},
	}

	body, _ := json.Marshal(newDef)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/jobs/job-to-update", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "job-to-update")
	rec := httptest.NewRecorder()

	server.UpdateJobDefinition(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("expected status %d, got %d", http.StatusNoContent, rec.Code)
	}

	var updatedDef string
	db.QueryRow("SELECT definition_json FROM stored_jobs WHERE job_id = ?", "job-to-update").Scan(&updatedDef)
	if !strings.Contains(updatedDef, "echo new") {
		t.Errorf("expected updated definition to contain 'echo new', got: %s", updatedDef)
	}

	infoCalls := logger.GetInfoCalls()
	hasUpdatedMsg := false
	for _, msg := range infoCalls {
		if strings.Contains(msg, "Updated job definition: job-to-update") {
			hasUpdatedMsg = true
			break
		}
	}

	if !hasUpdatedMsg {
		t.Errorf("expected logger to contain 'Updated job definition: job-to-update', got: %v", infoCalls)
	}
}

func TestAPIServer_UpdateJobDefinition_MissingID(t *testing.T) {
	server, _, _, _ := setupTestServer(t)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/jobs/", nil)
	rec := httptest.NewRecorder()

	server.UpdateJobDefinition(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestAPIServer_UpdateJobDefinition_InvalidContentType(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/jobs/job-1", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "text/plain")
	req.SetPathValue("id", "job-1")
	rec := httptest.NewRecorder()

	server.UpdateJobDefinition(rec, req)

	if rec.Code != http.StatusUnsupportedMediaType {
		t.Errorf("expected status %d, got %d", http.StatusUnsupportedMediaType, rec.Code)
	}
}

func TestAPIServer_UpdateJobDefinition_IDMismatch(t *testing.T) {
	server, _, _, _ := setupTestServer(t)
	newDef := map[string]any{
		"id": "different-id",
		"root": map[string]any{
			"uses": "builtins/shell",
		},
	}

	body, _ := json.Marshal(newDef)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/jobs/job-1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "job-1")
	rec := httptest.NewRecorder()

	server.UpdateJobDefinition(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestAPIServer_UpdateJobDefinition_InvalidJSON(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/jobs/job-1", strings.NewReader("not json"))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "job-1")
	rec := httptest.NewRecorder()

	server.UpdateJobDefinition(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestAPIServer_RunJob_Success(t *testing.T) {
	server, logger, queueService, db := setupTestServer(t)

	jobDef := map[string]any{
		"root": map[string]any{
			"id":   "node-1",
			"uses": "builtins/shell",
			"with": map[string]string{
				"command": "echo hello",
			},
		},
	}

	body, _ := json.Marshal(jobDef)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/run", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.RunJob(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, rec.Code)
	}

	var resp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if resp.ID == "" {
		t.Error("expected non-empty id in response")
	}

	if _, err := uuid.Parse(resp.ID); err != nil {
		t.Errorf("expected id to be a valid UUID, got %q: %v", resp.ID, err)
	}

	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM stored_jobs WHERE job_id = ?", resp.ID).Scan(&count)
	if err != nil {
		t.Fatalf("failed to query db: %v", err)
	}

	if count != 0 {
		t.Errorf("expected 0 rows in stored_jobs for ephemeral id %q, got %d", resp.ID, count)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 1 {
		t.Errorf("expected 1 job enqueued, got %d", len(jobs))
	}

	if jobs[0].GetId() != resp.ID {
		t.Errorf("expected enqueued job id %q, got %q", resp.ID, jobs[0].GetId())
	}

	infoCalls := logger.GetInfoCalls()
	hasEnqueuedMsg := false
	for _, msg := range infoCalls {
		if strings.Contains(msg, "Enqueued ephemeral job: "+resp.ID) {
			hasEnqueuedMsg = true
			break
		}
	}

	if !hasEnqueuedMsg {
		t.Errorf("expected logger to contain 'Enqueued ephemeral job: %s', got: %v", resp.ID, infoCalls)
	}
}

func TestAPIServer_RunJob_OverwritesClientID(t *testing.T) {
	server, _, queueService, db := setupTestServer(t)

	jobDef := map[string]any{
		"id": "client-provided-id",
		"root": map[string]any{
			"uses": "builtins/shell",
			"with": map[string]string{"command": "echo test"},
		},
	}

	body, _ := json.Marshal(jobDef)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/run", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.RunJob(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, rec.Code)
	}

	var resp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if resp.ID == "client-provided-id" {
		t.Error("expected server to overwrite client id with generated UUID")
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM stored_jobs WHERE job_id = ?", "client-provided-id").Scan(&count)
	if count != 0 {
		t.Errorf("expected no stored job with client id, got %d", count)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 1 || jobs[0].GetId() != resp.ID {
		t.Errorf("enqueued job id should be %q, got %v", resp.ID, jobs)
	}
}

func TestAPIServer_RunJob_InvalidContentType(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/run", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()

	server.RunJob(rec, req)

	if rec.Code != http.StatusUnsupportedMediaType {
		t.Errorf("expected status %d, got %d", http.StatusUnsupportedMediaType, rec.Code)
	}
}

func TestAPIServer_RunJob_InvalidJSON(t *testing.T) {
	server, _, queueService, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/run", strings.NewReader("not json"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.RunJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}

	if len(queueService.GetJobs()) != 0 {
		t.Error("expected no job enqueued on invalid JSON")
	}
}

func TestAPIServer_RunJob_MissingRoot(t *testing.T) {
	server, _, queueService, _ := setupTestServer(t)

	jobDef := map[string]any{"id": "no-root"}
	body, _ := json.Marshal(jobDef)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/run", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.RunJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}

	if len(queueService.GetJobs()) != 0 {
		t.Error("expected no job enqueued when root is missing")
	}
}

func TestAPIServer_RunJob_QueueError(t *testing.T) {
	server, logger, queueService, _ := setupTestServer(t)

	queueService.SetEnqueueError(errors.New("queue unavailable"))
	jobDef := map[string]any{
		"root": map[string]any{"uses": "builtins/shell", "with": map[string]string{"command": "echo"}},
	}
	body, _ := json.Marshal(jobDef)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/run", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.RunJob(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, rec.Code)
	}

	errorCalls := logger.GetErrorCalls()
	hasQueueError := false
	for _, msg := range errorCalls {
		if strings.Contains(msg, "Failed to enqueue job") {
			hasQueueError = true
			break
		}
	}

	if !hasQueueError {
		t.Errorf("expected logger error 'Failed to enqueue job', got: %v", errorCalls)
	}
}

func TestAPIServer_SSEJobRuns_ReceivesRunOnTrigger(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	jobID := "job-sse-test"
	jobDef := `{"id": "job-sse-test", "root": {"uses": "builtins/shell", "with": {"command": "echo test"}}}`
	_, err := db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", jobID, jobDef)
	if err != nil {
		t.Fatalf("insert job: %v", err)
	}

	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	sseURL := httpServer.URL + "/api/v1/sse/jobs/" + jobID + "/runs"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sseURL, nil)
	if err != nil {
		t.Fatalf("create sse request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	sseResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("connect sse: %v", err)
	}
	defer sseResp.Body.Close()

	triggerResp, err := http.Post(httpServer.URL+"/api/v1/jobs/trigger/"+jobID, "application/json", nil)
	if err != nil {
		t.Fatalf("trigger job: %v", err)
	}
	triggerResp.Body.Close()
	if triggerResp.StatusCode != http.StatusAccepted {
		t.Fatalf("trigger: expected 202, got %d", triggerResp.StatusCode)
	}

	reader := bufio.NewReader(sseResp.Body)
	var dataBuf strings.Builder
	var ev struct {
		RunID    string `json:"run_id"`
		RunIndex int    `json:"run_index"`
	}

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read sse line: %v", err)
		}

		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if dataBuf.Len() == 0 {
				continue
			}
			message := []byte(dataBuf.String())
			dataBuf.Reset()

			if err := json.Unmarshal(message, &ev); err != nil {
				t.Fatalf("unmarshal run event: %v", err)
			}
			break
		}

		if strings.HasPrefix(line, "data:") {
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			dataBuf.WriteString(data)
		}
	}

	if ev.RunID == "" {
		t.Error("expected non-empty run_id")
	}

	if ev.RunIndex != 1 {
		t.Errorf("expected run_index 1, got %d", ev.RunIndex)
	}
}
