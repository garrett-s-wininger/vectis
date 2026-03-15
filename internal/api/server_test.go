package api_test

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"vectis/internal/api"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
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

	if rec.Code != http.StatusNoContent {
		t.Errorf("expected status %d, got %d", http.StatusNoContent, rec.Code)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 1 {
		t.Errorf("expected 1 job enqueued, got %d", len(jobs))
	}

	if jobs[0].GetId() != "job-to-trigger" {
		t.Errorf("expected job id 'job-to-trigger', got %s", jobs[0].GetId())
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
