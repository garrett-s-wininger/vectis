package api_test

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"vectis/internal/api"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
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

func waitForNEnqueuedJobs(t *testing.T, q *mocks.MockQueueService, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		if len(q.GetJobs()) >= n {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for %d enqueued jobs, got %d", n, len(q.GetJobs()))
}

func waitForLoggerErrorContaining(t *testing.T, log *mocks.MockLogger, sub string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		for _, msg := range log.GetErrorCalls() {
			if strings.Contains(msg, sub) {
				return
			}
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for logger error containing %q", sub)
}

func waitForNDispatchEvents(t *testing.T, db *sql.DB, runID string, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM run_dispatch_events WHERE run_id = ?`, runID).Scan(&count); err != nil {
			t.Fatalf("count dispatch events: %v", err)
		}

		if count >= n {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for %d dispatch events for run %s", n, runID)
}

func assertAPIError(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()

	if rec.Code != status {
		t.Fatalf("expected status %d, got %d: %s", status, rec.Code, rec.Body.String())
	}

	if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Fatalf("expected JSON content type, got %q", ct)
	}

	var body struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode api error: %v; body=%s", err, rec.Body.String())
	}

	if body.Code != code {
		t.Fatalf("expected error code %q, got %q; body=%s", code, body.Code, rec.Body.String())
	}

	if body.Message == "" {
		t.Fatalf("expected non-empty message; body=%s", rec.Body.String())
	}
}

func apiErrorValidationFields(t *testing.T, rec *httptest.ResponseRecorder) []struct {
	Field   string `json:"field"`
	Message string `json:"message"`
} {
	t.Helper()

	var body struct {
		Details struct {
			Fields []struct {
				Field   string `json:"field"`
				Message string `json:"message"`
			} `json:"fields"`
		} `json:"details"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode api validation fields: %v; body=%s", err, rec.Body.String())
	}

	if len(body.Details.Fields) == 0 {
		t.Fatalf("expected validation fields in body=%s", rec.Body.String())
	}

	return body.Details.Fields
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
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
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

	assertAPIError(t, rec, http.StatusUnsupportedMediaType, "unsupported_media_type")
}

func TestAPIServer_CreateJob_DBUnavailable(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	jobDef := map[string]any{
		"id": "test-job-db-down",
		"root": map[string]any{
			"id":   "root",
			"uses": "builtins/shell",
			"with": map[string]string{"command": "echo hi"},
		},
	}

	body, _ := json.Marshal(jobDef)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.CreateJob(rec, req)

	assertAPIError(t, rec, http.StatusServiceUnavailable, "database_unavailable")
}

func TestAPIServer_GetJobs_DBUnavailable(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	rec := httptest.NewRecorder()
	server.GetJobs(rec, req)

	assertAPIError(t, rec, http.StatusServiceUnavailable, "database_unavailable")
}

func TestAPIServer_GetJobs_ListError_ClassifiedUnavailable(t *testing.T) {
	jobs := mocks.NewMockJobsRepository()
	jobs.ListErr = fmt.Errorf("dial: %w", syscall.ECONNREFUSED)
	runs := mocks.NewMockRunsRepository()
	server := api.NewAPIServerWithRepositories(mocks.NewMockLogger(), jobs, runs, mocks.StubEphemeralRunStarter{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	rec := httptest.NewRecorder()
	server.GetJobs(rec, req)

	assertAPIError(t, rec, http.StatusServiceUnavailable, "database_unavailable")
}

func TestAPIServer_GetStuckRunsIncludesCellBreakdown(t *testing.T) {
	runs := mocks.NewMockRunsRepository()
	runs.CountStuckResult = 3
	runs.CountStuckByCell = []dal.RunCountByCell{
		{CellID: "iad-a", Count: 2},
		{CellID: "pdx-b", Count: 1},
	}

	server := api.NewAPIServerWithRepositories(mocks.NewMockLogger(), mocks.NewMockJobsRepository(), runs, mocks.StubEphemeralRunStarter{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/reconciler/stuck-runs", nil)
	rec := httptest.NewRecorder()
	server.GetStuckRuns(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var body struct {
		Stuck int64 `json:"stuck"`
		Cells []struct {
			CellID string `json:"cell_id"`
			Stuck  int64  `json:"stuck"`
		} `json:"cells"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, rec.Body.String())
	}

	if body.Stuck != 3 {
		t.Fatalf("stuck total: got %d, want 3", body.Stuck)
	}

	if len(body.Cells) != 2 {
		t.Fatalf("cells len: got %d, want 2 (%+v)", len(body.Cells), body.Cells)
	}

	if body.Cells[0].CellID != "iad-a" || body.Cells[0].Stuck != 2 {
		t.Fatalf("first cell: got %+v", body.Cells[0])
	}

	if body.Cells[1].CellID != "pdx-b" || body.Cells[1].Stuck != 1 {
		t.Fatalf("second cell: got %+v", body.Cells[1])
	}
}

func TestAPIServer_GetJobRuns_DBUnavailable(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/some-job/runs", nil)
	req.SetPathValue("id", "some-job")
	rec := httptest.NewRecorder()
	server.GetJobRuns(rec, req)

	assertAPIError(t, rec, http.StatusServiceUnavailable, "database_unavailable")
}

func TestAPIServer_GetJobRuns_NotFound(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/missing/runs", nil)
	req.SetPathValue("id", "missing")
	rec := httptest.NewRecorder()
	server.GetJobRuns(rec, req)

	assertAPIError(t, rec, http.StatusNotFound, "job_not_found")
}

func TestAPIServer_TriggerJob_DBUnavailableOnGetDefinition(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	jobDef := `{"id": "job-trig-db", "root": {"uses": "builtins/shell"}}`
	if _, err := db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", "job-trig-db", jobDef); err != nil {
		t.Fatalf("insert job: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/job-trig-db", nil)
	req.SetPathValue("id", "job-trig-db")
	rec := httptest.NewRecorder()
	server.TriggerJob(rec, req)

	assertAPIError(t, rec, http.StatusServiceUnavailable, "database_unavailable")
}

func TestAPIServer_PostCellCatalogEvent_RecordAndReplay(t *testing.T) {
	server, _, _, db := setupTestServer(t)

	body := []byte(`{
		"event_key": "event-1",
		"event_type": "run.status",
		"payload": {"run_id": "run-1", "status": "running"}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/cells/iad-a/catalog-events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("cell_id", "iad-a")
	rec := httptest.NewRecorder()

	server.PostCellCatalogEvent(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d: %s", http.StatusAccepted, rec.Code, rec.Body.String())
	}

	var resp struct {
		ID         int64  `json:"id"`
		SourceCell string `json:"source_cell"`
		EventKey   string `json:"event_key"`
		EventType  string `json:"event_type"`
		Status     string `json:"status"`
		Created    bool   `json:"created"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.ID == 0 || resp.SourceCell != "iad-a" || resp.EventKey != "event-1" || resp.EventType != "run.status" || resp.Status != dal.CatalogEventStatusPending || !resp.Created {
		t.Fatalf("unexpected response: %+v", resp)
	}

	var payload string
	if err := db.QueryRow("SELECT payload_json FROM cell_catalog_events WHERE id = ?", resp.ID).Scan(&payload); err != nil {
		t.Fatalf("query catalog event: %v", err)
	}

	var storedPayload map[string]string
	if err := json.Unmarshal([]byte(payload), &storedPayload); err != nil {
		t.Fatalf("decode stored payload: %v", err)
	}

	if storedPayload["run_id"] != "run-1" || storedPayload["status"] != "running" {
		t.Fatalf("unexpected payload: %+v", storedPayload)
	}

	dupReq := httptest.NewRequest(http.MethodPost, "/api/v1/cells/iad-a/catalog-events", bytes.NewReader(body))
	dupReq.Header.Set("Content-Type", "application/json")
	dupReq.SetPathValue("cell_id", "iad-a")
	dupRec := httptest.NewRecorder()
	server.PostCellCatalogEvent(dupRec, dupReq)

	if dupRec.Code != http.StatusAccepted {
		t.Fatalf("duplicate expected status %d, got %d: %s", http.StatusAccepted, dupRec.Code, dupRec.Body.String())
	}

	var dupResp struct {
		ID      int64 `json:"id"`
		Created bool  `json:"created"`
	}

	if err := json.Unmarshal(dupRec.Body.Bytes(), &dupResp); err != nil {
		t.Fatalf("decode duplicate response: %v", err)
	}

	if dupResp.ID != resp.ID || dupResp.Created {
		t.Fatalf("unexpected duplicate response: %+v", dupResp)
	}
}

func TestAPIServer_PostCellCatalogEvent_InvalidEvent(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/cells/iad-a/catalog-events", strings.NewReader(`{
		"event_key": "event-bad",
		"event_type": "not-real",
		"payload": {}
	}`))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("cell_id", "iad-a")
	rec := httptest.NewRecorder()

	server.PostCellCatalogEvent(rec, req)

	assertAPIError(t, rec, http.StatusBadRequest, "invalid_catalog_event")
}

func TestAPIServer_GetCatalogStatus(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	events := dal.NewSQLRepositories(db).CatalogEvents()

	first, _, err := events.Record(ctx, "iad-a", "event-1", "run.status", []byte(`{"run_id":"run-1","status":"running"}`))
	if err != nil {
		t.Fatalf("record first event: %v", err)
	}

	second, _, err := events.Record(ctx, "iad-a", "event-2", "execution.status", []byte(`{"execution_id":"execution-1","status":"accepted"}`))
	if err != nil {
		t.Fatalf("record second event: %v", err)
	}

	if err := events.MarkApplied(ctx, first.ID); err != nil {
		t.Fatalf("mark first event applied: %v", err)
	}

	if err := events.MarkFailed(ctx, second.ID, "bad payload"); err != nil {
		t.Fatalf("mark second event failed: %v", err)
	}

	if _, _, err := events.Record(ctx, "iad-b", "event-3", "run.status", []byte(`{"run_id":"run-2","status":"queued"}`)); err != nil {
		t.Fatalf("record third event: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/catalog/status", http.NoBody)
	server.GetCatalogStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Pending          int64  `json:"pending"`
		Applied          int64  `json:"applied"`
		Failed           int64  `json:"failed"`
		Total            int64  `json:"total"`
		LastReceivedUnix *int64 `json:"last_received_unix,omitempty"`
		LastAppliedUnix  *int64 `json:"last_applied_unix,omitempty"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.Pending != 1 || resp.Applied != 1 || resp.Failed != 1 || resp.Total != 3 {
		t.Fatalf("unexpected catalog status: %+v", resp)
	}

	if resp.LastReceivedUnix == nil {
		t.Fatal("expected last_received_unix")
	}

	if resp.LastAppliedUnix == nil {
		t.Fatal("expected last_applied_unix")
	}
}

func TestAPIServer_CreateJob_InvalidJSON(t *testing.T) {
	server, _, _, db := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader("not json"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.CreateJob(rec, req)

	assertAPIError(t, rec, http.StatusBadRequest, "invalid_job_definition")

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
			"id":   "root",
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

	assertAPIError(t, rec2, http.StatusConflict, "job_already_exists")
}

func TestAPIServer_CreateJob_ValidationError(t *testing.T) {
	server, _, _, db := setupTestServer(t)

	jobDef := map[string]any{
		"id": "bad-job",
		"root": map[string]any{
			"uses": "builtins/not-real",
		},
	}

	body, _ := json.Marshal(jobDef)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.CreateJob(rec, req)

	assertAPIError(t, rec, http.StatusBadRequest, "invalid_job_definition")

	fields := apiErrorValidationFields(t, rec)
	wantFields := map[string]string{
		"root.id":   "is required",
		"root.uses": `unknown action "builtins/not-real"`,
	}

	for _, field := range fields {
		delete(wantFields, field.Field)
	}

	if len(wantFields) != 0 {
		t.Fatalf("missing structured validation fields %v in body=%s", wantFields, rec.Body.String())
	}

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM stored_jobs WHERE job_id = ?", "bad-job").Scan(&count); err != nil {
		t.Fatalf("count jobs: %v", err)
	}

	if count != 0 {
		t.Fatalf("expected invalid job not to persist, got count %d", count)
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

	var resp struct {
		Data       []map[string]any `json:"data"`
		NextCursor *int64           `json:"next_cursor,omitempty"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if len(resp.Data) != 0 {
		t.Errorf("expected empty array, got %v", resp.Data)
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

	var resp struct {
		Data       []map[string]any `json:"data"`
		NextCursor *int64           `json:"next_cursor,omitempty"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	jobs := resp.Data
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

	assertAPIError(t, rec, http.StatusNotFound, "job_not_found")
}

func TestAPIServer_GetJob_DBUnavailable(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	jobDef := `{"id": "job-db-down", "root": {"uses": "builtins/shell"}}`
	if _, err := db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", "job-db-down", jobDef); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-db-down", nil)
	req.SetPathValue("id", "job-db-down")
	rec := httptest.NewRecorder()
	server.GetJob(rec, req)

	assertAPIError(t, rec, http.StatusServiceUnavailable, "database_unavailable")
}

func TestAPIServer_GetJob_InvalidVersion(t *testing.T) {
	server, _, _, db := setupTestServer(t)

	jobDef := `{"id": "job-version", "root": {"uses": "builtins/shell"}}`
	if _, err := db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", "job-version", jobDef); err != nil {
		t.Fatalf("insert job: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-version?version=abc", nil)
	req.SetPathValue("id", "job-version")
	rec := httptest.NewRecorder()
	server.GetJob(rec, req)

	assertAPIError(t, rec, http.StatusBadRequest, "invalid_version")
}

func TestAPIServer_GetJob_VersionNotFound(t *testing.T) {
	server, _, _, db := setupTestServer(t)

	jobDef := `{"id": "job-version-missing", "root": {"uses": "builtins/shell"}}`
	if _, err := db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", "job-version-missing", jobDef); err != nil {
		t.Fatalf("insert job: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-version-missing?version=99", nil)
	req.SetPathValue("id", "job-version-missing")
	rec := httptest.NewRecorder()
	server.GetJob(rec, req)

	assertAPIError(t, rec, http.StatusNotFound, "job_version_not_found")
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

	assertAPIError(t, rec, http.StatusBadRequest, "missing_id")
}

func TestAPIServer_DeleteJob_NotFound(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/jobs/missing", nil)
	req.SetPathValue("id", "missing")
	rec := httptest.NewRecorder()
	server.DeleteJob(rec, req)

	assertAPIError(t, rec, http.StatusNotFound, "job_not_found")
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

	waitForNEnqueuedJobs(t, queueService, 1)
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

	reqs := queueService.GetJobRequests()
	envelopeJSON := reqs[0].GetMetadata()[cell.ExecutionEnvelopeMetadataKey]
	if envelopeJSON == "" {
		t.Fatal("expected execution envelope metadata")
	}

	env, err := cell.DecodeExecutionEnvelope([]byte(envelopeJSON))
	if err != nil {
		t.Fatalf("decode execution envelope: %v", err)
	}

	if env.Job.GetId() != "job-to-trigger" || env.RunID != runID {
		t.Fatalf("unexpected envelope identity: job=%q run=%q", env.Job.GetId(), env.RunID)
	}

	if env.ExecutionID == "" || env.SegmentID == "" || env.CellID != dal.DefaultCellID {
		t.Fatalf("unexpected envelope target: execution=%q segment=%q cell=%q", env.ExecutionID, env.SegmentID, env.CellID)
	}

	var dbStatus string
	var runIndex int
	err = db.QueryRow("SELECT status, run_index FROM job_runs WHERE job_id = ? AND run_id = ?", "job-to-trigger", runID).Scan(&dbStatus, &runIndex)
	if err != nil {
		t.Fatalf("expected job_runs row for triggered job: %v", err)
	}

	if dbStatus != "queued" {
		t.Errorf("expected job_runs.status queued, got %s", dbStatus)
	}

	if runIndex != 1 {
		t.Errorf("expected run_index 1, got %d", runIndex)
	}

	deadline := time.Now().Add(2 * time.Second)
	hasTriggeredMsg := false
	for time.Now().Before(deadline) && !hasTriggeredMsg {
		for _, msg := range logger.GetInfoCalls() {
			if strings.Contains(msg, "Triggered job: job-to-trigger") {
				hasTriggeredMsg = true
				break
			}
		}
		if !hasTriggeredMsg {
			time.Sleep(5 * time.Millisecond)
		}
	}

	if !hasTriggeredMsg {
		t.Errorf("expected logger to contain 'Triggered job: job-to-trigger', got: %v", logger.GetInfoCalls())
	}
}

func TestAPIServer_TriggerJob_IdempotencyKeyReplaysRun(t *testing.T) {
	server, _, queueService, db := setupTestServer(t)
	jobDef := `{"id": "job-idempotent", "root": {"id": "root", "uses": "builtins/shell", "with": {"command": "echo test"}}}`
	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", "job-idempotent", jobDef)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/job-idempotent", nil)
	req.SetPathValue("id", "job-idempotent")
	req.Header.Set("Idempotency-Key", "trigger-key-1")
	rec := httptest.NewRecorder()
	server.TriggerJob(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("first trigger: expected %d, got %d: %s", http.StatusAccepted, rec.Code, rec.Body.String())
	}

	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/job-idempotent", nil)
	req2.SetPathValue("id", "job-idempotent")
	req2.Header.Set("Idempotency-Key", "trigger-key-1")
	rec2 := httptest.NewRecorder()
	server.TriggerJob(rec2, req2)
	if rec2.Code != http.StatusAccepted {
		t.Fatalf("second trigger: expected %d, got %d: %s", http.StatusAccepted, rec2.Code, rec2.Body.String())
	}

	if rec.Body.String() != rec2.Body.String() {
		t.Fatalf("expected replayed response %q, got %q", rec.Body.String(), rec2.Body.String())
	}

	waitForNEnqueuedJobs(t, queueService, 1)
	if got := len(queueService.GetJobs()); got != 1 {
		t.Fatalf("expected one enqueued job after replay, got %d", got)
	}

	var runCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM job_runs WHERE job_id = ?", "job-idempotent").Scan(&runCount); err != nil {
		t.Fatalf("count runs: %v", err)
	}

	if runCount != 1 {
		t.Fatalf("expected one run row after replay, got %d", runCount)
	}
}

func TestAPIServer_TriggerJob_IdempotencyScopeIncludesJob(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", "job-a", `{"id":"job-a","root":{"id":"root","uses":"builtins/shell"}}`)
	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", "job-b", `{"id":"job-b","root":{"id":"root","uses":"builtins/shell"}}`)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/job-a", nil)
	req.SetPathValue("id", "job-a")
	req.Header.Set("Idempotency-Key", "same-key")
	rec := httptest.NewRecorder()
	server.TriggerJob(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("first trigger: expected %d, got %d", http.StatusAccepted, rec.Code)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/job-b", nil)
	req2.SetPathValue("id", "job-b")
	req2.Header.Set("Idempotency-Key", "same-key")
	rec2 := httptest.NewRecorder()
	server.TriggerJob(rec2, req2)
	if rec2.Code != http.StatusAccepted {
		t.Fatalf("separate job should have separate idempotency scope: got %d", rec2.Code)
	}
}

func TestAPIServer_TriggerJob_NotFound(t *testing.T) {
	server, _, queueService, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/nonexistent-job", nil)
	req.SetPathValue("id", "nonexistent-job")
	rec := httptest.NewRecorder()

	server.TriggerJob(rec, req)

	assertAPIError(t, rec, http.StatusNotFound, "job_not_found")

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

	assertAPIError(t, rec, http.StatusBadRequest, "missing_id")
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

	if rec.Code != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, rec.Code)
	}

	waitForLoggerErrorContaining(t, logger, "Failed to enqueue job")
	if len(queueService.GetJobs()) != 0 {
		t.Errorf("expected 0 jobs enqueued after queue error, got %d", len(queueService.GetJobs()))
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

	waitForNEnqueuedJobs(t, queueService, 1)
	jobs := queueService.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job enqueued, got %d", len(jobs))
	}

	runID := jobs[0].GetRunId()
	store := dal.NewSQLRepositories(db).Runs()
	ctx := req.Context()

	if err := store.MarkRunRunning(ctx, runID); err != nil {
		t.Fatalf("MarkRunRunning: %v", err)
	}

	if err := store.MarkRunFailed(ctx, runID, "", dal.FailureCodeExecution, "step failed: exit code 1"); err != nil {
		t.Fatalf("MarkRunFailed: %v", err)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-runs-status/runs", nil)
	getReq.SetPathValue("id", "job-runs-status")
	getRec := httptest.NewRecorder()
	server.GetJobRuns(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetJobRuns: expected 200, got %d", getRec.Code)
	}

	var resp struct {
		Data []struct {
			RunID         string  `json:"run_id"`
			RunIndex      int     `json:"run_index"`
			Status        string  `json:"status"`
			FailureCode   *string `json:"failure_code,omitempty"`
			StartedAt     *string `json:"started_at,omitempty"`
			FinishedAt    *string `json:"finished_at,omitempty"`
			FailureReason *string `json:"failure_reason,omitempty"`
		} `json:"data"`
	}

	if err := json.NewDecoder(getRec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode runs: %v", err)
	}

	runs := resp.Data
	if len(runs) != 1 {
		t.Fatalf("expected 1 run, got %d", len(runs))
	}

	if runs[0].Status != "failed" {
		t.Errorf("status want failed, got %s", runs[0].Status)
	}

	if runs[0].FailureReason == nil || *runs[0].FailureReason != "step failed: exit code 1" {
		t.Errorf("failure_reason want %q, got %v", "step failed: exit code 1", runs[0].FailureReason)
	}

	if runs[0].FailureCode == nil || *runs[0].FailureCode != dal.FailureCodeExecution {
		t.Errorf("failure_code want %q, got %v", dal.FailureCodeExecution, runs[0].FailureCode)
	}

	if runs[0].StartedAt == nil {
		t.Error("started_at should be set after MarkRunRunning")
	}

	if runs[0].FinishedAt == nil {
		t.Error("finished_at should be set after MarkRunFailed")
	}
}

func TestAPIServer_GetJobRuns_InvalidSince(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-runs-status/runs?since=not-a-date", nil)
	req.SetPathValue("id", "job-runs-status")
	rec := httptest.NewRecorder()
	server.GetJobRuns(rec, req)

	assertAPIError(t, rec, http.StatusBadRequest, "invalid_since")
}

func TestAPIServer_UpdateJobDefinition_Success(t *testing.T) {
	server, logger, _, db := setupTestServer(t)
	initialDef := `{"id": "job-to-update", "root": {"uses": "builtins/shell", "with": {"command": "echo old"}}}`
	db.Exec("INSERT INTO stored_jobs (job_id, definition_json, version) VALUES (?, ?, 1)", "job-to-update", initialDef)

	newDef := map[string]any{
		"id": "job-to-update",
		"root": map[string]any{
			"id":   "root",
			"uses": "builtins/shell",
			"with": map[string]string{
				"command": "echo new",
			},
		},
	}

	body, _ := json.Marshal(newDef)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/jobs/job-to-update", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.SetPathValue("id", "job-to-update")
	rec := httptest.NewRecorder()

	server.UpdateJobDefinition(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("expected status %d, got %d", http.StatusNoContent, rec.Code)
	}

	if rec.Header().Get("X-Vectis-Version") != "2" {
		t.Errorf("expected X-Vectis-Version header 2, got %s", rec.Header().Get("X-Vectis-Version"))
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

	assertAPIError(t, rec, http.StatusBadRequest, "missing_id")
}

func TestAPIServer_UpdateJobDefinition_InvalidContentType(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/jobs/job-1", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "text/plain")
	req.SetPathValue("id", "job-1")
	rec := httptest.NewRecorder()

	server.UpdateJobDefinition(rec, req)

	assertAPIError(t, rec, http.StatusUnsupportedMediaType, "unsupported_media_type")
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

	assertAPIError(t, rec, http.StatusBadRequest, "job_id_mismatch")
}

func TestAPIServer_UpdateJobDefinition_InvalidJSON(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/jobs/job-1", strings.NewReader("not json"))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "job-1")
	rec := httptest.NewRecorder()

	server.UpdateJobDefinition(rec, req)

	assertAPIError(t, rec, http.StatusBadRequest, "invalid_job_definition")
}

func TestAPIServer_UpdateJobDefinition_ValidationErrorDoesNotPersist(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	initialDef := `{"id": "job-validation-update", "root": {"id": "root", "uses": "builtins/shell", "with": {"command": "echo old"}}}`
	if _, err := db.Exec("INSERT INTO stored_jobs (job_id, definition_json, version) VALUES (?, ?, 1)", "job-validation-update", initialDef); err != nil {
		t.Fatalf("insert job: %v", err)
	}

	newDef := map[string]any{
		"id": "job-validation-update",
		"root": map[string]any{
			"id":   "root",
			"uses": "builtins/not-real",
		},
	}

	body, _ := json.Marshal(newDef)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/jobs/job-validation-update", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "job-validation-update")
	rec := httptest.NewRecorder()

	server.UpdateJobDefinition(rec, req)

	assertAPIError(t, rec, http.StatusBadRequest, "invalid_job_definition")

	fields := apiErrorValidationFields(t, rec)
	var found bool
	for _, field := range fields {
		if field.Field == "root.uses" && field.Message == `unknown action "builtins/not-real"` {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("expected unknown action validation field, got %q", rec.Body.String())
	}

	var gotDef string
	var gotVersion int
	if err := db.QueryRow("SELECT definition_json, version FROM stored_jobs WHERE job_id = ?", "job-validation-update").Scan(&gotDef, &gotVersion); err != nil {
		t.Fatalf("select job: %v", err)
	}

	if gotDef != initialDef || gotVersion != 1 {
		t.Fatalf("expected original definition/version to remain, got version=%d def=%s", gotVersion, gotDef)
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
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
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

	var jdCount int
	err = db.QueryRow("SELECT COUNT(*) FROM job_definitions WHERE job_id = ? AND version = 1", resp.ID).Scan(&jdCount)
	if err != nil {
		t.Fatalf("failed to query job_definitions: %v", err)
	}

	if jdCount != 1 {
		t.Errorf("expected 1 row in job_definitions for ephemeral id %q, got %d", resp.ID, jdCount)
	}

	waitForNEnqueuedJobs(t, queueService, 1)
	jobs := queueService.GetJobs()
	if len(jobs) != 1 {
		t.Errorf("expected 1 job enqueued, got %d", len(jobs))
	}

	if jobs[0].GetId() != resp.ID {
		t.Errorf("expected enqueued job id %q, got %q", resp.ID, jobs[0].GetId())
	}

	deadline := time.Now().Add(2 * time.Second)
	hasEnqueuedMsg := false
	for time.Now().Before(deadline) && !hasEnqueuedMsg {
		for _, msg := range logger.GetInfoCalls() {
			if strings.Contains(msg, "Enqueued ephemeral job: "+resp.ID) {
				hasEnqueuedMsg = true
				break
			}
		}
		if !hasEnqueuedMsg {
			time.Sleep(5 * time.Millisecond)
		}
	}

	if !hasEnqueuedMsg {
		t.Errorf("expected logger to contain 'Enqueued ephemeral job: %s', got: %v", resp.ID, logger.GetInfoCalls())
	}
}

func TestAPIServer_RunJob_TargetCellPersistsExecutionTarget(t *testing.T) {
	server, _, _, db := setupTestServer(t)

	jobDef := map[string]any{
		"root": map[string]any{
			"id":   "node-1",
			"uses": "builtins/shell",
			"with": map[string]string{
				"command": "echo target",
			},
		},
	}

	body, _ := json.Marshal(map[string]any{
		"cell_id": "pdx-b",
		"job":     jobDef,
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/run", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.RunJob(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d: %s", http.StatusAccepted, rec.Code, rec.Body.String())
	}

	var resp struct {
		ID    string `json:"id"`
		RunID string `json:"run_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if resp.RunID == "" {
		t.Fatal("expected run_id in response")
	}

	var owningCell, executionCell string
	if err := db.QueryRow(`
		SELECT jr.owning_cell, se.cell_id
		FROM job_runs jr
		JOIN segment_executions se ON se.run_id = jr.run_id
		WHERE jr.run_id = ?
	`, resp.RunID).Scan(&owningCell, &executionCell); err != nil {
		t.Fatalf("query target cell: %v", err)
	}

	if owningCell != "pdx-b" {
		t.Fatalf("owning cell: got %q, want pdx-b", owningCell)
	}

	if executionCell != "pdx-b" {
		t.Fatalf("execution cell: got %q, want pdx-b", executionCell)
	}
}

func TestAPIServer_GetRun_EphemeralRun(t *testing.T) {
	server, _, queueService, db := setupTestServer(t)

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
		t.Fatalf("RunJob: expected status %d, got %d: %s", http.StatusAccepted, rec.Code, rec.Body.String())
	}

	var runResp struct {
		RunID string `json:"run_id"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &runResp); err != nil {
		t.Fatalf("parse run response: %v", err)
	}

	if runResp.RunID == "" {
		t.Fatal("expected run_id")
	}

	waitForNEnqueuedJobs(t, queueService, 1)
	waitForNDispatchEvents(t, db, runResp.RunID, 2)

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runResp.RunID, nil)
	getReq.SetPathValue("id", runResp.RunID)
	getRec := httptest.NewRecorder()

	server.GetRun(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("GetRun: expected status %d, got %d: %s", http.StatusOK, getRec.Code, getRec.Body.String())
	}

	var got struct {
		RunID          string `json:"run_id"`
		Status         string `json:"status"`
		DispatchEvents []struct {
			Source    string `json:"source"`
			EventType string `json:"event_type"`
		} `json:"dispatch_events"`
	}

	if err := json.Unmarshal(getRec.Body.Bytes(), &got); err != nil {
		t.Fatalf("parse get run response: %v", err)
	}

	if got.RunID != runResp.RunID {
		t.Fatalf("run_id: want %q, got %q", runResp.RunID, got.RunID)
	}

	if got.Status != "queued" {
		t.Fatalf("status: want queued, got %q", got.Status)
	}

	if len(got.DispatchEvents) != 2 {
		t.Fatalf("expected dispatch trail in run response, got %+v", got.DispatchEvents)
	}

	if got.DispatchEvents[0].Source != dal.DispatchSourceAPI || got.DispatchEvents[0].EventType != dal.DispatchEventAttempt {
		t.Fatalf("unexpected first dispatch event: %+v", got.DispatchEvents[0])
	}

	if got.DispatchEvents[1].Source != dal.DispatchSourceAPI || got.DispatchEvents[1].EventType != dal.DispatchEventSuccess {
		t.Fatalf("unexpected second dispatch event: %+v", got.DispatchEvents[1])
	}
}

func TestAPIServer_RunJob_IdempotencyKeyReplaysRun(t *testing.T) {
	server, _, queueService, db := setupTestServer(t)

	jobDef := map[string]any{
		"root": map[string]any{
			"id":   "root",
			"uses": "builtins/shell",
			"with": map[string]string{"command": "echo hello"},
		},
	}
	body, _ := json.Marshal(jobDef)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/run", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "run-key-1")
	rec := httptest.NewRecorder()
	server.RunJob(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("first run: expected %d, got %d: %s", http.StatusAccepted, rec.Code, rec.Body.String())
	}

	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/run", bytes.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Idempotency-Key", "run-key-1")
	rec2 := httptest.NewRecorder()
	server.RunJob(rec2, req2)
	if rec2.Code != http.StatusAccepted {
		t.Fatalf("second run: expected %d, got %d: %s", http.StatusAccepted, rec2.Code, rec2.Body.String())
	}

	if rec.Body.String() != rec2.Body.String() {
		t.Fatalf("expected replayed response %q, got %q", rec.Body.String(), rec2.Body.String())
	}

	waitForNEnqueuedJobs(t, queueService, 1)
	if got := len(queueService.GetJobs()); got != 1 {
		t.Fatalf("expected one enqueued job after replay, got %d", got)
	}

	var runCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM job_runs").Scan(&runCount); err != nil {
		t.Fatalf("count runs: %v", err)
	}

	if runCount != 1 {
		t.Fatalf("expected one run row after replay, got %d", runCount)
	}
}

func TestAPIServer_RunJob_IdempotencyKeyConflict(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	bodyA, _ := json.Marshal(map[string]any{
		"root": map[string]any{
			"id":   "root",
			"uses": "builtins/shell",
			"with": map[string]string{"command": "echo a"},
		},
	})

	bodyB, _ := json.Marshal(map[string]any{
		"root": map[string]any{
			"id":   "root",
			"uses": "builtins/shell",
			"with": map[string]string{"command": "echo b"},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/run", bytes.NewReader(bodyA))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "run-key-conflict")
	rec := httptest.NewRecorder()
	server.RunJob(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("first run: expected %d, got %d: %s", http.StatusAccepted, rec.Code, rec.Body.String())
	}

	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/run", bytes.NewReader(bodyB))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Idempotency-Key", "run-key-conflict")
	rec2 := httptest.NewRecorder()
	server.RunJob(rec2, req2)
	assertAPIError(t, rec2, http.StatusConflict, "idempotency_key_reused")
}

func TestAPIServer_RunJob_OverwritesClientID(t *testing.T) {
	server, _, queueService, db := setupTestServer(t)

	jobDef := map[string]any{
		"id": "client-provided-id",
		"root": map[string]any{
			"id":   "root",
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

	waitForNEnqueuedJobs(t, queueService, 1)
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

	assertAPIError(t, rec, http.StatusUnsupportedMediaType, "unsupported_media_type")
}

func TestAPIServer_RunJob_InvalidJSON(t *testing.T) {
	server, _, queueService, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/run", strings.NewReader("not json"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.RunJob(rec, req)

	assertAPIError(t, rec, http.StatusBadRequest, "invalid_job_definition")

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

	assertAPIError(t, rec, http.StatusBadRequest, "invalid_job_definition")

	if len(queueService.GetJobs()) != 0 {
		t.Error("expected no job enqueued when root is missing")
	}
}

func TestAPIServer_RunJob_QueueError(t *testing.T) {
	server, logger, queueService, _ := setupTestServer(t)

	queueService.SetEnqueueError(errors.New("queue unavailable"))
	jobDef := map[string]any{
		"root": map[string]any{"id": "root", "uses": "builtins/shell", "with": map[string]string{"command": "echo"}},
	}

	body, _ := json.Marshal(jobDef)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/run", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.RunJob(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, rec.Code)
	}

	waitForLoggerErrorContaining(t, logger, "Failed to enqueue job")
	if len(queueService.GetJobs()) != 0 {
		t.Errorf("expected 0 jobs enqueued after queue error, got %d", len(queueService.GetJobs()))
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

		if after, ok := strings.CutPrefix(line, "data:"); ok {
			data := strings.TrimSpace(after)
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

func TestAPIServer_ForceFailRun_Success(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `INSERT INTO stored_jobs (job_id, namespace_id, definition_json) VALUES (?, ?, ?)`, "job-force-fail", 1, `{"id":"job-force-fail"}`); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	runs := dal.NewSQLRepositories(db).Runs()

	runID, _, err := runs.CreateRun(ctx, "job-force-fail", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+runID+"/force-fail", strings.NewReader(`{"reason":"manual intervention"}`))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", runID)
	rec := httptest.NewRecorder()

	server.ForceFailRun(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d", http.StatusNoContent, rec.Code)
	}

	var status string
	var failureCode string
	var reason sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT status, failure_code, failure_reason FROM job_runs WHERE run_id = ?`, runID).Scan(&status, &failureCode, &reason); err != nil {
		t.Fatalf("query run: %v", err)
	}

	if status != "failed" {
		t.Fatalf("expected status failed, got %q", status)
	}

	if !reason.Valid || reason.String != "manual intervention" {
		t.Fatalf("expected failure reason set, got %v", reason)
	}
	if failureCode != dal.FailureCodeForceFailed {
		t.Fatalf("expected failure_code %q, got %q", dal.FailureCodeForceFailed, failureCode)
	}
}

func TestAPIServer_ForceFailRun_NotFound(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/missing/force-fail", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "missing")
	rec := httptest.NewRecorder()

	server.ForceFailRun(rec, req)

	assertAPIError(t, rec, http.StatusNotFound, "run_not_found")
}

func TestAPIServer_RepairMarkRun_ResolvesOrphanedRun(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `INSERT INTO stored_jobs (job_id, namespace_id, definition_json) VALUES (?, ?, ?)`, "job-repair-mark", 1, `{"id":"job-repair-mark"}`); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	runs := dal.NewSQLRepositories(db).Runs()

	runID, _, err := runs.CreateRun(ctx, "job-repair-mark", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	claimed, token, err := runs.TryClaim(ctx, runID, "worker-a", time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatalf("TryClaim: %v", err)
	}

	if !claimed || token == "" {
		t.Fatalf("expected claim token, got claimed=%v token=%q", claimed, token)
	}

	if err := runs.MarkRunOrphaned(ctx, runID, token, dal.OrphanReasonLeaseExpired); err != nil {
		t.Fatalf("MarkRunOrphaned: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+runID+"/repair/mark-abandoned", strings.NewReader(`{"reason":"worker deleted"}`))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", runID)
	rec := httptest.NewRecorder()

	server.RepairMarkRunAbandoned(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusNoContent, rec.Code, rec.Body.String())
	}

	var status string
	var reason sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT status, failure_reason FROM job_runs WHERE run_id = ?`, runID).Scan(&status, &reason); err != nil {
		t.Fatalf("query run: %v", err)
	}

	if status != dal.RunStatusAbandoned {
		t.Fatalf("expected abandoned status, got %q", status)
	}

	if !reason.Valid || reason.String != "worker deleted" {
		t.Fatalf("expected repair reason, got %v", reason)
	}
}

func TestAPIServer_RepairMarkRun_RunningConflict(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `INSERT INTO stored_jobs (job_id, namespace_id, definition_json) VALUES (?, ?, ?)`, "job-repair-running", 1, `{"id":"job-repair-running"}`); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	runs := dal.NewSQLRepositories(db).Runs()

	runID, _, err := runs.CreateRun(ctx, "job-repair-running", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	if claimed, _, err := runs.TryClaim(ctx, runID, "worker-a", time.Now().Add(time.Minute)); err != nil || !claimed {
		t.Fatalf("TryClaim claimed=%v err=%v", claimed, err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+runID+"/repair/mark-failed", strings.NewReader(`{}`))
	req.SetPathValue("id", runID)
	rec := httptest.NewRecorder()

	server.RepairMarkRunFailed(rec, req)

	assertAPIError(t, rec, http.StatusConflict, "run_repair_conflict")
}

func TestAPIServer_ForceRequeueRun_Success(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `INSERT INTO stored_jobs (job_id, namespace_id, definition_json) VALUES (?, ?, ?)`, "job-force-requeue", 1, `{"id":"job-force-requeue"}`); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	runs := dal.NewSQLRepositories(db).Runs()

	runID, _, err := runs.CreateRun(ctx, "job-force-requeue", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	claimed, token, err := runs.TryClaim(ctx, runID, "worker-a", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("TryClaim: %v", err)
	}
	if !claimed || token == "" {
		t.Fatalf("expected claim token, got claimed=%v token=%q", claimed, token)
	}

	if err := runs.MarkRunFailed(ctx, runID, token, dal.FailureCodeExecution, "transient failure"); err != nil {
		t.Fatalf("MarkRunFailed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+runID+"/force-requeue", nil)
	req.SetPathValue("id", runID)
	rec := httptest.NewRecorder()

	server.ForceRequeueRun(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d", http.StatusNoContent, rec.Code)
	}

	var status string
	var failureCode string
	var failure sql.NullString
	var claimToken sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT status, failure_code, failure_reason, claim_token FROM job_runs WHERE run_id = ?`, runID).
		Scan(&status, &failureCode, &failure, &claimToken); err != nil {
		t.Fatalf("query run: %v", err)
	}

	if status != "queued" {
		t.Fatalf("expected queued status, got %q", status)
	}

	if failureCode != "" || failure.Valid || claimToken.Valid {
		t.Fatalf("expected cleared failure fields/token, got failure_code=%q failure=%v claim_token=%v", failureCode, failure, claimToken)
	}
}

func TestAPIServer_ForceRequeueRun_SucceededConflict(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `INSERT INTO stored_jobs (job_id, namespace_id, definition_json) VALUES (?, ?, ?)`, "job-force-requeue-succeeded", 1, `{"id":"job-force-requeue-succeeded"}`); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	runs := dal.NewSQLRepositories(db).Runs()

	runID, _, err := runs.CreateRun(ctx, "job-force-requeue-succeeded", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	if err := runs.MarkRunSucceeded(ctx, runID, ""); err != nil {
		t.Fatalf("MarkRunSucceeded: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+runID+"/force-requeue", nil)
	req.SetPathValue("id", runID)
	rec := httptest.NewRecorder()

	server.ForceRequeueRun(rec, req)

	assertAPIError(t, rec, http.StatusConflict, "run_requeue_forbidden")
}

func TestAPIServer_ForceRequeueRun_RunningConflict(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `INSERT INTO stored_jobs (job_id, namespace_id, definition_json) VALUES (?, ?, ?)`, "job-force-requeue-running", 1, `{"id":"job-force-requeue-running"}`); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	runs := dal.NewSQLRepositories(db).Runs()

	runID, _, err := runs.CreateRun(ctx, "job-force-requeue-running", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	claimed, _, err := runs.TryClaim(ctx, runID, "worker-a", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("TryClaim: %v", err)
	}
	if !claimed {
		t.Fatal("expected TryClaim to succeed")
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+runID+"/force-requeue", nil)
	req.SetPathValue("id", runID)
	rec := httptest.NewRecorder()

	server.ForceRequeueRun(rec, req)

	assertAPIError(t, rec, http.StatusConflict, "run_requeue_conflict")
}

func TestAPIServer_CancelRun_NotExecutingConflict(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `INSERT INTO stored_jobs (job_id, namespace_id, definition_json) VALUES (?, ?, ?)`, "job-cancel-queued", 1, `{"id":"job-cancel-queued"}`); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	runs := dal.NewSQLRepositories(db).Runs()

	runID, _, err := runs.CreateRun(ctx, "job-cancel-queued", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+runID+"/cancel", nil)
	req.SetPathValue("id", runID)
	rec := httptest.NewRecorder()

	server.CancelRun(rec, req)

	assertAPIError(t, rec, http.StatusConflict, "run_not_executing")
}

func TestAPIServer_CancelRun_ResolverUsesRequestBoundContext(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `INSERT INTO stored_jobs (job_id, namespace_id, definition_json) VALUES (?, ?, ?)`, "job-cancel-context", 1, `{"id":"job-cancel-context"}`); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	runs := dal.NewSQLRepositories(db).Runs()

	runID, _, err := runs.CreateRun(ctx, "job-cancel-context", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	claimed, token, err := runs.TryClaim(ctx, runID, "worker-a", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("TryClaim: %v", err)
	}

	if !claimed || token == "" {
		t.Fatalf("expected claim token, got claimed=%v token=%q", claimed, token)
	}

	resolverErr := errors.New("resolver stopped before worker RPC")
	var sawDeadline bool
	server.ResolveWorkerAddress = func(ctx context.Context, workerID string) (string, error) {
		if workerID != "worker-a" {
			t.Fatalf("workerID = %q, want worker-a", workerID)
		}

		_, sawDeadline = ctx.Deadline()
		return "", resolverErr
	}

	reqCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+runID+"/cancel", nil).WithContext(reqCtx)
	req.SetPathValue("id", runID)
	rec := httptest.NewRecorder()

	server.CancelRun(rec, req)

	assertAPIError(t, rec, http.StatusBadGateway, "worker_not_reachable")
	if !sawDeadline {
		t.Fatal("expected resolver to receive request-bound context with deadline")
	}
}

func TestAPIServer_Handler_SetsXRequestID(t *testing.T) {
	t.Parallel()
	srv, _, _, _ := setupTestServer(t)
	h := srv.Handler()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	h.ServeHTTP(rec, req)

	if rec.Header().Get("X-Request-ID") == "" {
		t.Fatal("expected X-Request-ID on response")
	}
}

func TestAPIServer_Handler_AccessLogJSON(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	srv, _, _, _ := setupTestServer(t)
	srv.AccessLogger = slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	h := srv.Handler()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}

	var payload struct {
		Msg           string `json:"msg"`
		CorrelationID string `json:"correlation_id"`
		Method        string `json:"method"`
		Status        int    `json:"status"`
		HTTPRoute     string `json:"http_route"`
	}

	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatalf("parse access log: %v, raw=%q", err, buf.String())
	}

	if payload.Msg != "http_request" {
		t.Fatalf("msg=%q", payload.Msg)
	}

	if payload.CorrelationID == "" || payload.Method != "GET" || payload.Status != http.StatusOK {
		t.Fatalf("%+v", payload)
	}

	if payload.HTTPRoute != "GET /api/v1/jobs" {
		t.Fatalf("http_route=%q", payload.HTTPRoute)
	}
}
