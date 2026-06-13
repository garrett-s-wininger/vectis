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
	"vectis/internal/testutil/runfixture"

	"github.com/google/uuid"
	"github.com/spf13/viper"
)

func setupTestServer(t *testing.T) (*api.APIServer, *mocks.MockLogger, *mocks.MockQueueService, *sql.DB) {
	db := dbtest.NewTestDB(t)
	logger := mocks.NewMockLogger()
	queueService := mocks.NewMockQueueService()

	server := api.NewAPIServer(logger, db)
	server.SetQueueClient(queueService)

	return server, logger, queueService, db
}

func insertStoredJobForTest(t *testing.T, db *sql.DB, jobID, definitionJSON string, namespaceIDs ...int64) {
	t.Helper()

	namespaceID := int64(1)
	if len(namespaceIDs) > 0 {
		namespaceID = namespaceIDs[0]
	}

	if err := dal.NewSQLRepositories(db).Jobs().Create(context.Background(), jobID, definitionJSON, namespaceID); err != nil {
		t.Fatalf("insert stored job %s: %v", jobID, err)
	}
}

func claimPendingRunExecutionForAPITest(t *testing.T, runs dal.RunsRepository, runID, owner string, leaseUntil time.Time) dal.ExecutionClaimResult {
	t.Helper()

	dispatch, err := runs.GetPendingExecution(context.Background(), runID)
	if err != nil {
		t.Fatalf("get pending execution for run %s: %v", runID, err)
	}

	claim, err := runs.TryClaimExecution(context.Background(), dispatch.ExecutionID, owner, leaseUntil)
	if err != nil {
		t.Fatalf("claim execution %s: %v", dispatch.ExecutionID, err)
	}

	if !claim.Claimed || claim.ClaimToken == "" {
		t.Fatalf("expected execution %s to be claimable, claim=%+v", dispatch.ExecutionID, claim)
	}

	return claim
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

func clearIdempotencyResponseForAPITest(t *testing.T, db *sql.DB, scope, key string) (resourceType, resourceID string) {
	t.Helper()

	var response sql.NullString
	if err := db.QueryRow(`
		SELECT response_json, resource_type, resource_id
		FROM idempotency_keys
		WHERE scope = ? AND key = ?
	`, scope, key).Scan(&response, &resourceType, &resourceID); err != nil {
		t.Fatalf("query idempotency key: %v", err)
	}

	if !response.Valid {
		t.Fatal("expected idempotency response to be cached before simulating crash")
	}

	if resourceType == "" || resourceID == "" {
		t.Fatalf("expected idempotency resource pointer, got type=%q id=%q", resourceType, resourceID)
	}

	if _, err := db.Exec(`
		UPDATE idempotency_keys
		SET response_json = NULL
		WHERE scope = ? AND key = ?
	`, scope, key); err != nil {
		t.Fatalf("clear idempotency response: %v", err)
	}

	return resourceType, resourceID
}

func assertIdempotencyResponseCachedForAPITest(t *testing.T, db *sql.DB, scope, key string) {
	t.Helper()

	var response sql.NullString
	if err := db.QueryRow(`
		SELECT response_json
		FROM idempotency_keys
		WHERE scope = ? AND key = ?
	`, scope, key).Scan(&response); err != nil {
		t.Fatalf("query idempotency response: %v", err)
	}

	if !response.Valid || response.String == "" {
		t.Fatal("expected idempotency response to be cached")
	}
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

	server.Handler().ServeHTTP(rec, req)

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

func TestAPIServer_GetQueueBacklogIncludesCellBreakdown(t *testing.T) {
	runs := mocks.NewMockRunsRepository()
	runs.CountByStatusResult = 3
	runs.CountByStatusByCellResult = []dal.RunCountByCell{
		{CellID: "iad-a", Count: 2},
		{CellID: "pdx-b", Count: 1},
	}

	server := api.NewAPIServerWithRepositories(mocks.NewMockLogger(), mocks.NewMockJobsRepository(), runs, mocks.StubEphemeralRunStarter{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/queue/backlog", nil)
	rec := httptest.NewRecorder()
	server.GetQueueBacklog(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var body struct {
		Queued int64 `json:"queued"`
		Cells  []struct {
			CellID string `json:"cell_id"`
			Queued int64  `json:"queued"`
		} `json:"cells"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, rec.Body.String())
	}

	if body.Queued != 3 {
		t.Fatalf("queued total: got %d, want 3", body.Queued)
	}

	if len(body.Cells) != 2 {
		t.Fatalf("cells len: got %d, want 2 (%+v)", len(body.Cells), body.Cells)
	}

	if body.Cells[0].CellID != "iad-a" || body.Cells[0].Queued != 2 {
		t.Fatalf("first cell: got %+v", body.Cells[0])
	}

	if body.Cells[1].CellID != "pdx-b" || body.Cells[1].Queued != 1 {
		t.Fatalf("second cell: got %+v", body.Cells[1])
	}
}

func TestAPIServer_GetCellsStatusChecksConfiguredIngress(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	ready := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health/ready" {
			t.Errorf("ready server path: got %s, want /health/ready", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ready.Close()

	unhealthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health/ready" {
			t.Errorf("unhealthy server path: got %s, want /health/ready", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer unhealthy.Close()

	viper.Set("cell_ingress_endpoints", []string{
		"pdx-b=" + unhealthy.URL,
		"iad-a=" + ready.URL,
	})

	runs := mocks.NewMockRunsRepository()
	runs.CountByStatusByCellResult = []dal.RunCountByCell{{CellID: "sjc-c", Count: 4}}
	runs.CountStuckByCell = []dal.RunCountByCell{{CellID: "sjc-c", Count: 2}}
	runs.CountTaskContinuationByCell = []dal.RunCountByCell{{CellID: "sjc-c", Count: 3}}
	runs.CountTaskFinalizeByCell = []dal.RunCountByCell{{CellID: "sjc-c", Count: 1}}

	server := api.NewAPIServerWithRepositories(mocks.NewMockLogger(), mocks.NewMockJobsRepository(), runs, mocks.StubEphemeralRunStarter{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/cells/status", nil)
	rec := httptest.NewRecorder()
	server.GetCellsStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var body struct {
		Cells []struct {
			CellID                  string `json:"cell_id"`
			Ready                   bool   `json:"ready"`
			IngressRequired         bool   `json:"ingress_required"`
			IngressConfigured       bool   `json:"ingress_configured"`
			IngressReachable        bool   `json:"ingress_reachable"`
			Status                  string `json:"status"`
			HTTPStatus              int    `json:"http_status"`
			Error                   string `json:"error"`
			Queued                  int64  `json:"queued"`
			Stuck                   int64  `json:"stuck"`
			TaskContinuationPending int64  `json:"task_continuation_pending"`
			TaskFinalizationPending int64  `json:"task_finalization_pending"`
			Checks                  []struct {
				ID      string `json:"id"`
				Status  string `json:"status"`
				Summary string `json:"summary"`
			} `json:"checks"`
		} `json:"cells"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, rec.Body.String())
	}

	if len(body.Cells) != 3 {
		t.Fatalf("cells len: got %d, want 3 (%+v)", len(body.Cells), body.Cells)
	}

	if body.Cells[0].CellID != "iad-a" || !body.Cells[0].Ready || !body.Cells[0].IngressRequired || !body.Cells[0].IngressConfigured || !body.Cells[0].IngressReachable || body.Cells[0].Status != "ready" || body.Cells[0].HTTPStatus != http.StatusOK {
		t.Fatalf("ready cell: got %+v", body.Cells[0])
	}

	if len(body.Cells[0].Checks) != 3 || body.Cells[0].Checks[0].ID != "ingress" || body.Cells[0].Checks[0].Status != "pass" {
		t.Fatalf("ready cell checks: got %+v", body.Cells[0].Checks)
	}

	if body.Cells[1].CellID != "pdx-b" || body.Cells[1].Ready || !body.Cells[1].IngressRequired || !body.Cells[1].IngressConfigured || body.Cells[1].IngressReachable || body.Cells[1].Status != "unhealthy" || body.Cells[1].HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("unhealthy cell: got %+v", body.Cells[1])
	}

	if len(body.Cells[1].Checks) != 3 || body.Cells[1].Checks[0].ID != "ingress" || body.Cells[1].Checks[0].Status != "fail" {
		t.Fatalf("unhealthy cell checks: got %+v", body.Cells[1].Checks)
	}

	if body.Cells[2].CellID != "sjc-c" || body.Cells[2].Ready || !body.Cells[2].IngressRequired || body.Cells[2].IngressConfigured || body.Cells[2].IngressReachable || body.Cells[2].Status != "missing_route" || body.Cells[2].Queued != 4 || body.Cells[2].Stuck != 2 || body.Cells[2].TaskContinuationPending != 3 || body.Cells[2].TaskFinalizationPending != 1 {
		t.Fatalf("missing route cell: got %+v", body.Cells[2])
	}

	if len(body.Cells[2].Checks) != 3 || body.Cells[2].Checks[0].Status != "fail" || body.Cells[2].Checks[1].Status != "warn" {
		t.Fatalf("missing route cell checks: got %+v", body.Cells[2].Checks)
	}

	for _, want := range []string{"2 queued runs", "3 task continuations", "1 orphaned task finalization"} {
		if !strings.Contains(body.Cells[2].Checks[1].Summary, want) {
			t.Fatalf("missing route dispatch summary: got %q, want %q", body.Cells[2].Checks[1].Summary, want)
		}
	}
}

func TestAPIServer_GetCellsStatusIncludesCatalogSourceCounts(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	events := dal.NewSQLRepositories(db).CatalogEvents()

	failed, _, err := events.Record(ctx, "pdx-b", "failed-event", "run.status", []byte(`{"run_id":"run-1","status":"failed"}`))
	if err != nil {
		t.Fatalf("record failed event: %v", err)
	}
	if _, _, err := events.Record(ctx, "pdx-b", "pending-event", "execution.status", []byte(`{"execution_id":"execution-1","status":"running"}`)); err != nil {
		t.Fatalf("record pending event: %v", err)
	}
	if err := events.MarkFailed(ctx, failed.ID, "apply failed"); err != nil {
		t.Fatalf("mark failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/cells/status", nil)
	rec := httptest.NewRecorder()
	server.GetCellsStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var body struct {
		Cells []struct {
			CellID         string `json:"cell_id"`
			Ready          bool   `json:"ready"`
			Status         string `json:"status"`
			CatalogPending int64  `json:"catalog_pending"`
			CatalogFailed  int64  `json:"catalog_failed"`
			CatalogTotal   int64  `json:"catalog_total"`
			Checks         []struct {
				ID     string `json:"id"`
				Status string `json:"status"`
			} `json:"checks"`
		} `json:"cells"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, rec.Body.String())
	}

	if len(body.Cells) != 1 {
		t.Fatalf("cells len: got %d, want 1 (%+v)", len(body.Cells), body.Cells)
	}

	if body.Cells[0].CellID != "pdx-b" || body.Cells[0].Ready || body.Cells[0].Status != "missing_route" || body.Cells[0].CatalogPending != 1 || body.Cells[0].CatalogFailed != 1 || body.Cells[0].CatalogTotal != 2 {
		t.Fatalf("unexpected catalog source cell: %+v", body.Cells[0])
	}

	if len(body.Cells[0].Checks) != 3 || body.Cells[0].Checks[2].ID != "catalog" || body.Cells[0].Checks[2].Status != "fail" {
		t.Fatalf("unexpected catalog source checks: %+v", body.Cells[0].Checks)
	}
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

func TestAPIServer_GetStuckRunsIncludesTaskFinalizationPending(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "global-a")
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-stuck-task-finalization", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-stuck-task-finalization"
	def := `{"id":"job-stuck-task-finalization","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRunInCell(ctx, jobID, nil, 1, "pdx-b")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	runfixture.FinalizeExecutionByClaim(t, ctx, repos, dispatch.ExecutionID, dal.ExecutionStatusSucceeded)

	if err := repos.Runs().MarkRunOrphaned(ctx, runID, "lease expired"); err != nil {
		t.Fatalf("mark orphaned: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/reconciler/stuck-runs", nil)
	rec := httptest.NewRecorder()
	server.GetStuckRuns(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var body struct {
		Stuck                   int64 `json:"stuck"`
		TaskFinalizationPending int64 `json:"task_finalization_pending"`
		TaskFinalizationCells   []struct {
			CellID  string `json:"cell_id"`
			Pending int64  `json:"pending"`
		} `json:"task_finalization_cells"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, rec.Body.String())
	}

	if body.Stuck != 0 {
		t.Fatalf("stuck total: got %d, want 0", body.Stuck)
	}

	if body.TaskFinalizationPending != 1 {
		t.Fatalf("task finalization pending: got %d, want 1", body.TaskFinalizationPending)
	}

	if len(body.TaskFinalizationCells) != 1 || body.TaskFinalizationCells[0].CellID != "pdx-b" || body.TaskFinalizationCells[0].Pending != 1 {
		t.Fatalf("task finalization cells: %+v", body.TaskFinalizationCells)
	}
}

func TestAPIServer_GetStuckRunsIncludesTaskContinuationPending(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "global-a")
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-stuck-task-continuation", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-stuck-task-continuation"
	def := `{"id":"job-stuck-task-continuation","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRunInCell(ctx, jobID, nil, 1, "pdx-b")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	if _, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: dispatch.TaskID,
		TaskKey:      "child",
		Name:         "child",
		SpecHash:     "sha256:child",
		TargetCellID: "pdx-b",
	}); err != nil {
		t.Fatalf("ensure child task: %v", err)
	}

	result := runfixture.FinalizeExecutionByClaim(t, ctx, repos, dispatch.ExecutionID, dal.ExecutionStatusSucceeded)
	if result.Outcome != dal.ExecutionFinalizationOutcomeContinued {
		t.Fatalf("expected continuation outcome, got %+v", result)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/reconciler/stuck-runs", nil)
	rec := httptest.NewRecorder()
	server.GetStuckRuns(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var body struct {
		TaskContinuationPending int64 `json:"task_continuation_pending"`
		TaskContinuationCells   []struct {
			CellID  string `json:"cell_id"`
			Pending int64  `json:"pending"`
		} `json:"task_continuation_cells"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, rec.Body.String())
	}

	if body.TaskContinuationPending != 1 {
		t.Fatalf("task continuation pending: got %d, want 1", body.TaskContinuationPending)
	}

	if len(body.TaskContinuationCells) != 1 || body.TaskContinuationCells[0].CellID != "pdx-b" || body.TaskContinuationCells[0].Pending != 1 {
		t.Fatalf("task continuation cells: %+v", body.TaskContinuationCells)
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
	insertStoredJobForTest(t, db, "job-trig-db", jobDef)

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
		Sources          []struct {
			SourceCell string `json:"source_cell"`
			Pending    int64  `json:"pending"`
			Applied    int64  `json:"applied"`
			Failed     int64  `json:"failed"`
			Total      int64  `json:"total"`
		} `json:"sources"`
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

	if len(resp.Sources) != 2 {
		t.Fatalf("sources len: got %d want 2 (%+v)", len(resp.Sources), resp.Sources)
	}

	if resp.Sources[0].SourceCell != "iad-a" || resp.Sources[0].Pending != 0 || resp.Sources[0].Applied != 1 || resp.Sources[0].Failed != 1 || resp.Sources[0].Total != 2 {
		t.Fatalf("unexpected iad source summary: %+v", resp.Sources[0])
	}

	if resp.Sources[1].SourceCell != "iad-b" || resp.Sources[1].Pending != 1 || resp.Sources[1].Applied != 0 || resp.Sources[1].Failed != 0 || resp.Sources[1].Total != 1 {
		t.Fatalf("unexpected iad-b source summary: %+v", resp.Sources[1])
	}
}

func TestAPIServer_GetCronStatusIncludesDueAndClaimedSchedules(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-cron-status", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-cron-status"
	if err := repos.Jobs().Create(ctx, jobID, `{"id":"job-cron-status","root":{"uses":"builtins/shell"}}`, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	oldestDue := now.Add(-10 * time.Minute)
	insertCronStatusTestSchedule(t, ctx, db, jobID, "* * * * *", oldestDue, "cron-a", now.Add(5*time.Minute))
	insertCronStatusTestSchedule(t, ctx, db, jobID, "*/2 * * * *", now.Add(-5*time.Minute), "", time.Time{})
	insertCronStatusTestSchedule(t, ctx, db, jobID, "*/5 * * * *", now.Add(5*time.Minute), "", time.Time{})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/cron/status", http.NoBody)
	server.GetCronStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		ScheduleCount int64  `json:"schedule_count"`
		DueCount      int64  `json:"due_count"`
		ClaimedCount  int64  `json:"claimed_count"`
		OldestDueUnix *int64 `json:"oldest_due_unix"`
		Active        bool   `json:"active"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.ScheduleCount != 3 || resp.DueCount != 1 || resp.ClaimedCount != 1 || !resp.Active {
		t.Fatalf("unexpected cron status: %+v", resp)
	}

	if resp.OldestDueUnix == nil || *resp.OldestDueUnix != oldestDue.Unix() {
		t.Fatalf("oldest due: got %+v, want %d", resp.OldestDueUnix, oldestDue.Unix())
	}
}

func insertCronStatusTestSchedule(t *testing.T, ctx context.Context, db *sql.DB, jobID, cronSpec string, nextRunAt time.Time, claimToken string, claimedUntil time.Time) {
	t.Helper()

	result, err := db.ExecContext(ctx, "INSERT INTO job_triggers (job_id, trigger_type) VALUES (?, ?)", jobID, "cron")
	if err != nil {
		t.Fatalf("insert cron trigger: %v", err)
	}
	triggerID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("cron trigger id: %v", err)
	}

	if claimToken == "" {
		if _, err := db.ExecContext(ctx, "INSERT INTO cron_trigger_specs (trigger_id, cron_spec, next_run_at) VALUES (?, ?, ?)", triggerID, cronSpec, nextRunAt.Format(time.RFC3339)); err != nil {
			t.Fatalf("insert cron trigger spec: %v", err)
		}
		return
	}

	if _, err := db.ExecContext(ctx, "INSERT INTO cron_trigger_specs (trigger_id, cron_spec, next_run_at, claim_token, claimed_until) VALUES (?, ?, ?, ?, ?)", triggerID, cronSpec, nextRunAt.Format(time.RFC3339), claimToken, claimedUntil.Format(time.RFC3339)); err != nil {
		t.Fatalf("insert claimed cron trigger spec: %v", err)
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
	insertStoredJobForTest(t, db, "job-1", job1)
	insertStoredJobForTest(t, db, "job-2", job2)

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
	insertStoredJobForTest(t, db, "job-get-1", jobDef)

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
	insertStoredJobForTest(t, db, "job-db-down", jobDef)
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
	insertStoredJobForTest(t, db, "job-version", jobDef)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-version?version=abc", nil)
	req.SetPathValue("id", "job-version")
	rec := httptest.NewRecorder()
	server.GetJob(rec, req)

	assertAPIError(t, rec, http.StatusBadRequest, "invalid_version")
}

func TestAPIServer_GetJob_VersionNotFound(t *testing.T) {
	server, _, _, db := setupTestServer(t)

	jobDef := `{"id": "job-version-missing", "root": {"uses": "builtins/shell"}}`
	insertStoredJobForTest(t, db, "job-version-missing", jobDef)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-version-missing?version=99", nil)
	req.SetPathValue("id", "job-version-missing")
	rec := httptest.NewRecorder()
	server.GetJob(rec, req)

	assertAPIError(t, rec, http.StatusNotFound, "job_version_not_found")
}

func TestAPIServer_DeleteJob_Success(t *testing.T) {
	server, logger, _, db := setupTestServer(t)
	jobDef := `{"id": "job-to-delete", "root": {"uses": "builtins/shell"}}`
	insertStoredJobForTest(t, db, "job-to-delete", jobDef)

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
	insertStoredJobForTest(t, db, "job-to-trigger", jobDef)

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

	var invocationID, payloadHash string
	if err := db.QueryRow("SELECT trigger_invocation_id, execution_payload_hash FROM job_runs WHERE run_id = ?", runID).Scan(&invocationID, &payloadHash); err != nil {
		t.Fatalf("query run audit linkage: %v", err)
	}

	if invocationID == "" {
		t.Fatal("expected trigger_invocation_id to be recorded")
	}

	if payloadHash == "" {
		t.Fatal("expected execution_payload_hash to be recorded")
	}

	var triggerType, requestedCells string
	if err := db.QueryRow("SELECT trigger_type, requested_cells FROM trigger_invocations WHERE invocation_id = ?", invocationID).Scan(&triggerType, &requestedCells); err != nil {
		t.Fatalf("query trigger invocation: %v", err)
	}

	if triggerType != dal.TriggerTypeManual {
		t.Fatalf("trigger type: got %q want %q", triggerType, dal.TriggerTypeManual)
	}

	if !strings.Contains(requestedCells, dal.DefaultCellID) {
		t.Fatalf("expected requested cells to include default cell, got %s", requestedCells)
	}

	var executionPayload string
	if err := db.QueryRow("SELECT payload_json FROM execution_payloads WHERE payload_hash = ?", payloadHash).Scan(&executionPayload); err != nil {
		t.Fatalf("query execution payload: %v", err)
	}

	if !strings.Contains(executionPayload, runID) || !strings.Contains(executionPayload, "job-to-trigger") {
		t.Fatalf("execution payload should contain run/job identity, got %s", executionPayload)
	}

	getRunsReq := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-to-trigger/runs", nil)
	getRunsReq.SetPathValue("id", "job-to-trigger")
	getRunsRec := httptest.NewRecorder()
	server.GetJobRuns(getRunsRec, getRunsReq)
	if getRunsRec.Code != http.StatusOK {
		t.Fatalf("GetJobRuns: expected status %d, got %d: %s", http.StatusOK, getRunsRec.Code, getRunsRec.Body.String())
	}

	var runsResp struct {
		Data []struct {
			RunID                string   `json:"run_id"`
			TriggerInvocationID  *string  `json:"trigger_invocation_id,omitempty"`
			TriggerType          *string  `json:"trigger_type,omitempty"`
			RequestedCells       []string `json:"requested_cells,omitempty"`
			ExecutionPayloadHash string   `json:"execution_payload_hash,omitempty"`
		} `json:"data"`
	}

	if err := json.NewDecoder(getRunsRec.Body).Decode(&runsResp); err != nil {
		t.Fatalf("decode runs response: %v", err)
	}

	if len(runsResp.Data) != 1 {
		t.Fatalf("expected one run row, got %d", len(runsResp.Data))
	}

	listedRun := runsResp.Data[0]
	if listedRun.RunID != runID || listedRun.ExecutionPayloadHash != payloadHash {
		t.Fatalf("unexpected run audit row: %+v", listedRun)
	}

	if listedRun.TriggerInvocationID == nil || *listedRun.TriggerInvocationID != invocationID {
		t.Fatalf("trigger invocation id from run list: got %+v want %q", listedRun.TriggerInvocationID, invocationID)
	}

	if listedRun.TriggerType == nil || *listedRun.TriggerType != dal.TriggerTypeManual {
		t.Fatalf("trigger type from run list: got %+v want %q", listedRun.TriggerType, dal.TriggerTypeManual)
	}

	if len(listedRun.RequestedCells) != 1 || listedRun.RequestedCells[0] != dal.DefaultCellID {
		t.Fatalf("requested cells from run list: got %+v", listedRun.RequestedCells)
	}

	getRunReq := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID, nil)
	getRunReq.SetPathValue("id", runID)
	getRunRec := httptest.NewRecorder()
	server.GetRun(getRunRec, getRunReq)
	if getRunRec.Code != http.StatusOK {
		t.Fatalf("GetRun: expected status %d, got %d: %s", http.StatusOK, getRunRec.Code, getRunRec.Body.String())
	}

	var runResp struct {
		RunID                string  `json:"run_id"`
		TriggerInvocationID  *string `json:"trigger_invocation_id,omitempty"`
		ExecutionPayloadHash string  `json:"execution_payload_hash,omitempty"`
	}

	if err := json.NewDecoder(getRunRec.Body).Decode(&runResp); err != nil {
		t.Fatalf("decode run response: %v", err)
	}

	if runResp.RunID != runID || runResp.ExecutionPayloadHash != payloadHash {
		t.Fatalf("unexpected run detail audit fields: %+v", runResp)
	}

	if runResp.TriggerInvocationID == nil || *runResp.TriggerInvocationID != invocationID {
		t.Fatalf("trigger invocation id from run detail: got %+v want %q", runResp.TriggerInvocationID, invocationID)
	}

	leaseUntil := time.Now().Add(time.Minute)
	executionClaim, err := dal.NewSQLRepositories(db).Runs().TryClaimExecution(context.Background(), env.ExecutionID, "worker-api-task-list", leaseUntil)
	if err != nil {
		t.Fatalf("claim execution for task list: %v", err)
	}

	if !executionClaim.Claimed || executionClaim.ClaimToken == "" {
		t.Fatalf("expected execution claim before task list, claim=%+v", executionClaim)
	}

	secretCount := 1
	fileCount := 1
	if err := dal.NewSQLRepositories(db).Runs().RecordExecutionSecurityEvent(context.Background(), dal.RecordExecutionSecurityEventParams{
		RunID:         runID,
		TaskID:        env.TaskID,
		TaskAttemptID: env.TaskAttemptID,
		ExecutionID:   env.ExecutionID,
		EventType:     dal.ExecutionSecurityEventSecretResolution,
		Outcome:       "success",
		Reason:        "ok",
		Provider:      "encryptedfs",
		SecretCount:   &secretCount,
		FileCount:     &fileCount,
	}); err != nil {
		t.Fatalf("record execution security event: %v", err)
	}

	tasksReq := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID+"/tasks", nil)
	tasksReq.SetPathValue("id", runID)
	tasksRec := httptest.NewRecorder()
	server.GetRunTasks(tasksRec, tasksReq)
	if tasksRec.Code != http.StatusOK {
		t.Fatalf("GetRunTasks: expected status %d, got %d: %s", http.StatusOK, tasksRec.Code, tasksRec.Body.String())
	}

	var tasksResp struct {
		Data []struct {
			TaskID   string `json:"task_id"`
			RunID    string `json:"run_id"`
			TaskKey  string `json:"task_key"`
			Name     string `json:"name"`
			Status   string `json:"status"`
			Attempts []struct {
				AttemptID       string  `json:"attempt_id"`
				TaskID          string  `json:"task_id"`
				RunID           string  `json:"run_id"`
				ExecutionID     string  `json:"execution_id"`
				ExecutionStatus string  `json:"execution_status"`
				CellID          string  `json:"cell_id"`
				LeaseOwner      *string `json:"lease_owner"`
				LeaseUntil      *int64  `json:"lease_until"`
				Attempt         int     `json:"attempt"`
				Status          string  `json:"status"`
				SecurityEvents  []struct {
					EventType   string  `json:"event_type"`
					Outcome     string  `json:"outcome"`
					Reason      string  `json:"reason"`
					Provider    *string `json:"provider"`
					SecretCount *int    `json:"secret_count"`
					FileCount   *int    `json:"file_count"`
				} `json:"security_events"`
			} `json:"attempts"`
		} `json:"data"`
	}

	if err := json.NewDecoder(tasksRec.Body).Decode(&tasksResp); err != nil {
		t.Fatalf("decode run tasks response: %v", err)
	}

	if len(tasksResp.Data) != 1 {
		t.Fatalf("expected one root task, got %d", len(tasksResp.Data))
	}

	rootTask := tasksResp.Data[0]
	if rootTask.TaskID != runID+":root" || rootTask.RunID != runID || rootTask.TaskKey != dal.RootTaskKey || rootTask.Name != dal.RootTaskKey || rootTask.Status != dal.TaskStatusAccepted {
		t.Fatalf("unexpected root task response: %+v", rootTask)
	}

	if len(rootTask.Attempts) != 1 {
		t.Fatalf("expected one root task attempt, got %d", len(rootTask.Attempts))
	}

	rootAttempt := rootTask.Attempts[0]
	if rootAttempt.AttemptID != runID+":root:attempt:1" || rootAttempt.TaskID != rootTask.TaskID || rootAttempt.RunID != runID || rootAttempt.CellID != dal.DefaultCellID || rootAttempt.Attempt != 1 || rootAttempt.Status != dal.TaskStatusAccepted {
		t.Fatalf("unexpected root task attempt response: %+v", rootAttempt)
	}

	if rootAttempt.ExecutionID != env.ExecutionID || rootAttempt.ExecutionStatus != dal.ExecutionStatusAccepted {
		t.Fatalf("unexpected root task execution response: %+v", rootAttempt)
	}

	if rootAttempt.LeaseOwner == nil || *rootAttempt.LeaseOwner != "worker-api-task-list" || rootAttempt.LeaseUntil == nil || *rootAttempt.LeaseUntil != leaseUntil.Unix() {
		t.Fatalf("unexpected root task lease response: %+v", rootAttempt)
	}

	if len(rootAttempt.SecurityEvents) != 1 {
		t.Fatalf("expected one root task security event, got %+v", rootAttempt.SecurityEvents)
	}

	securityEvent := rootAttempt.SecurityEvents[0]
	if securityEvent.EventType != dal.ExecutionSecurityEventSecretResolution || securityEvent.Outcome != "success" || securityEvent.Reason != "ok" {
		t.Fatalf("unexpected root task security event: %+v", securityEvent)
	}

	if securityEvent.Provider == nil || *securityEvent.Provider != "encryptedfs" || securityEvent.SecretCount == nil || *securityEvent.SecretCount != 1 || securityEvent.FileCount == nil || *securityEvent.FileCount != 1 {
		t.Fatalf("unexpected root task security event details: %+v", securityEvent)
	}

	payloadReq := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID+"/execution-payload", nil)
	payloadReq.SetPathValue("id", runID)
	payloadRec := httptest.NewRecorder()
	server.GetRunExecutionPayload(payloadRec, payloadReq)
	if payloadRec.Code != http.StatusOK {
		t.Fatalf("GetRunExecutionPayload: expected status %d, got %d: %s", http.StatusOK, payloadRec.Code, payloadRec.Body.String())
	}

	var payloadResp struct {
		RunID       string         `json:"run_id"`
		PayloadHash string         `json:"payload_hash"`
		Payload     map[string]any `json:"payload"`
	}
	if err := json.NewDecoder(payloadRec.Body).Decode(&payloadResp); err != nil {
		t.Fatalf("decode execution payload response: %v", err)
	}

	if payloadResp.RunID != runID || payloadResp.PayloadHash != payloadHash {
		t.Fatalf("unexpected execution payload response: %+v", payloadResp)
	}

	payloadJob, ok := payloadResp.Payload["job"].(map[string]any)
	if !ok || payloadJob["id"] != "job-to-trigger" || payloadJob["runId"] != runID {
		t.Fatalf("unexpected execution payload job identity: %+v", payloadResp.Payload["job"])
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

func TestAPIServer_ReplayRun_CreatesNewRunFromSourceDefinition(t *testing.T) {
	server, _, queueService, db := setupTestServer(t)
	jobID := "job-replay"
	defV1 := `{"id": "job-replay", "root": {"id": "root", "uses": "builtins/shell", "with": {"command": "echo old"}}}`
	defV2 := `{"id": "job-replay", "root": {"id": "root", "uses": "builtins/shell", "with": {"command": "echo new"}}}`
	insertStoredJobForTest(t, db, jobID, defV1)

	triggerReq := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/"+jobID, nil)
	triggerReq.SetPathValue("id", jobID)
	triggerRec := httptest.NewRecorder()
	server.TriggerJob(triggerRec, triggerReq)
	if triggerRec.Code != http.StatusAccepted {
		t.Fatalf("trigger: expected status %d, got %d: %s", http.StatusAccepted, triggerRec.Code, triggerRec.Body.String())
	}

	waitForNEnqueuedJobs(t, queueService, 1)
	sourceRunID := queueService.GetJobs()[0].GetRunId()
	if sourceRunID == "" {
		t.Fatal("expected source run id")
	}

	repos := dal.NewSQLRepositories(db)
	if err := repos.Runs().MarkRunFailed(context.Background(), sourceRunID, dal.FailureCodeExecution, "environment failed"); err != nil {
		t.Fatalf("mark source failed: %v", err)
	}

	updateReq := httptest.NewRequest(http.MethodPut, "/api/v1/jobs/"+jobID, strings.NewReader(defV2))
	updateReq.Header.Set("Content-Type", "application/json")
	updateReq.SetPathValue("id", jobID)
	updateRec := httptest.NewRecorder()
	server.UpdateJobDefinition(updateRec, updateReq)
	if updateRec.Code != http.StatusNoContent {
		t.Fatalf("update definition: expected status %d, got %d: %s", http.StatusNoContent, updateRec.Code, updateRec.Body.String())
	}

	replayReq := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+sourceRunID+"/replay", nil)
	replayReq.SetPathValue("id", sourceRunID)
	replayRec := httptest.NewRecorder()
	server.ReplayRun(replayRec, replayReq)
	if replayRec.Code != http.StatusAccepted {
		t.Fatalf("replay: expected status %d, got %d: %s", http.StatusAccepted, replayRec.Code, replayRec.Body.String())
	}

	var replayResp struct {
		JobID         string `json:"job_id"`
		RunID         string `json:"run_id"`
		RunIndex      int    `json:"run_index"`
		ReplayOfRunID string `json:"replay_of_run_id"`
	}

	if err := json.Unmarshal(replayRec.Body.Bytes(), &replayResp); err != nil {
		t.Fatalf("decode replay response: %v", err)
	}

	if replayResp.JobID != jobID || replayResp.RunID == "" || replayResp.RunID == sourceRunID || replayResp.ReplayOfRunID != sourceRunID {
		t.Fatalf("unexpected replay response: %+v", replayResp)
	}

	waitForNEnqueuedJobs(t, queueService, 2)
	replayedJob := queueService.GetJobs()[1]
	if replayedJob.GetRunId() != replayResp.RunID {
		t.Fatalf("replayed job run id: got %q want %q", replayedJob.GetRunId(), replayResp.RunID)
	}

	if got := replayedJob.GetRoot().GetWith()["command"]; got != "echo old" {
		t.Fatalf("replay should use source definition version, command=%q", got)
	}

	var replayOfRunID, invocationID, payloadHash string
	var definitionVersion int
	if err := db.QueryRow("SELECT replay_of_run_id, definition_version, trigger_invocation_id, execution_payload_hash FROM job_runs WHERE run_id = ?", replayResp.RunID).Scan(&replayOfRunID, &definitionVersion, &invocationID, &payloadHash); err != nil {
		t.Fatalf("query replay run audit fields: %v", err)
	}

	if replayOfRunID != sourceRunID || definitionVersion != 1 {
		t.Fatalf("replay audit row: replay_of=%q version=%d", replayOfRunID, definitionVersion)
	}

	if invocationID == "" || payloadHash == "" {
		t.Fatalf("expected invocation and payload hash, got invocation=%q payload=%q", invocationID, payloadHash)
	}

	var triggerType string
	if err := db.QueryRow("SELECT trigger_type FROM trigger_invocations WHERE invocation_id = ?", invocationID).Scan(&triggerType); err != nil {
		t.Fatalf("query replay trigger invocation: %v", err)
	}

	if triggerType != dal.TriggerTypeReplay {
		t.Fatalf("trigger type: got %q want %q", triggerType, dal.TriggerTypeReplay)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+replayResp.RunID, nil)
	getReq.SetPathValue("id", replayResp.RunID)
	getRec := httptest.NewRecorder()
	server.GetRun(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetRun replay: expected status %d, got %d: %s", http.StatusOK, getRec.Code, getRec.Body.String())
	}

	var gotRun struct {
		ReplayOfRunID *string `json:"replay_of_run_id,omitempty"`
		TriggerType   *string `json:"trigger_type,omitempty"`
	}

	if err := json.Unmarshal(getRec.Body.Bytes(), &gotRun); err != nil {
		t.Fatalf("decode replay run detail: %v", err)
	}

	if gotRun.ReplayOfRunID == nil || *gotRun.ReplayOfRunID != sourceRunID {
		t.Fatalf("run detail replay source: got %+v want %q", gotRun.ReplayOfRunID, sourceRunID)
	}

	if gotRun.TriggerType == nil || *gotRun.TriggerType != dal.TriggerTypeReplay {
		t.Fatalf("run detail trigger type: got %+v want %q", gotRun.TriggerType, dal.TriggerTypeReplay)
	}
}

func TestAPIServer_ReplayRun_IdempotencyCrashRecoveryReplaysRun(t *testing.T) {
	server, _, queueService, db := setupTestServer(t)
	jobID := "job-replay-idempotent-crash"
	key := "replay-key-crash"
	definitionJSON := `{"id": "job-replay-idempotent-crash", "root": {"id": "root", "uses": "builtins/shell", "with": {"command": "echo replay"}}}`
	insertStoredJobForTest(t, db, jobID, definitionJSON)

	triggerReq := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/"+jobID, nil)
	triggerReq.SetPathValue("id", jobID)
	triggerRec := httptest.NewRecorder()
	server.TriggerJob(triggerRec, triggerReq)
	if triggerRec.Code != http.StatusAccepted {
		t.Fatalf("trigger source: expected status %d, got %d: %s", http.StatusAccepted, triggerRec.Code, triggerRec.Body.String())
	}

	waitForNEnqueuedJobs(t, queueService, 1)
	sourceRunID := queueService.GetJobs()[0].GetRunId()
	if sourceRunID == "" {
		t.Fatal("expected source run id")
	}

	repos := dal.NewSQLRepositories(db)
	if err := repos.Runs().MarkRunFailed(context.Background(), sourceRunID, dal.FailureCodeExecution, "source failed"); err != nil {
		t.Fatalf("mark source failed: %v", err)
	}

	scope := "replay:" + sourceRunID + ":anonymous"
	replayReq := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+sourceRunID+"/replay", nil)
	replayReq.SetPathValue("id", sourceRunID)
	replayReq.Header.Set("Idempotency-Key", key)
	replayRec := httptest.NewRecorder()
	server.ReplayRun(replayRec, replayReq)
	if replayRec.Code != http.StatusAccepted {
		t.Fatalf("first replay: expected status %d, got %d: %s", http.StatusAccepted, replayRec.Code, replayRec.Body.String())
	}

	var firstResp struct {
		JobID         string `json:"job_id"`
		RunID         string `json:"run_id"`
		RunIndex      int    `json:"run_index"`
		CellID        string `json:"cell_id,omitempty"`
		ReplayOfRunID string `json:"replay_of_run_id"`
	}

	if err := json.Unmarshal(replayRec.Body.Bytes(), &firstResp); err != nil {
		t.Fatalf("decode first replay response: %v", err)
	}

	if firstResp.JobID != jobID || firstResp.RunID == "" || firstResp.RunID == sourceRunID || firstResp.ReplayOfRunID != sourceRunID {
		t.Fatalf("unexpected first replay response: %+v", firstResp)
	}

	waitForNEnqueuedJobs(t, queueService, 2)
	resourceType, _ := clearIdempotencyResponseForAPITest(t, db, scope, key)
	if resourceType != "trigger_invocation" {
		t.Fatalf("idempotency resource type: got %q, want trigger_invocation", resourceType)
	}

	replayReq2 := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+sourceRunID+"/replay", nil)
	replayReq2.SetPathValue("id", sourceRunID)
	replayReq2.Header.Set("Idempotency-Key", key)
	replayRec2 := httptest.NewRecorder()
	server.ReplayRun(replayRec2, replayReq2)
	if replayRec2.Code != http.StatusAccepted {
		t.Fatalf("recovered replay: expected status %d, got %d: %s", http.StatusAccepted, replayRec2.Code, replayRec2.Body.String())
	}

	if replayRec.Body.String() != replayRec2.Body.String() {
		t.Fatalf("expected recovered response %q, got %q", replayRec.Body.String(), replayRec2.Body.String())
	}

	assertIdempotencyResponseCachedForAPITest(t, db, scope, key)
	if got := len(queueService.GetJobs()); got != 2 {
		t.Fatalf("expected recovered replay not to enqueue another job, got %d queued jobs", got)
	}

	var replayCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM job_runs WHERE replay_of_run_id = ?", sourceRunID).Scan(&replayCount); err != nil {
		t.Fatalf("count replay runs: %v", err)
	}

	if replayCount != 1 {
		t.Fatalf("expected one replay run after recovery, got %d", replayCount)
	}
}

func TestAPIServer_ReplayRun_RejectsActiveSourceRun(t *testing.T) {
	server, _, queueService, db := setupTestServer(t)
	jobID := "job-replay-active"
	definition := `{"id": "job-replay-active", "root": {"uses": "builtins/shell", "with": {"command": "echo active"}}}`
	insertStoredJobForTest(t, db, jobID, definition)

	triggerReq := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/"+jobID, nil)
	triggerReq.SetPathValue("id", jobID)
	triggerRec := httptest.NewRecorder()
	server.TriggerJob(triggerRec, triggerReq)
	if triggerRec.Code != http.StatusAccepted {
		t.Fatalf("trigger: expected status %d, got %d: %s", http.StatusAccepted, triggerRec.Code, triggerRec.Body.String())
	}

	waitForNEnqueuedJobs(t, queueService, 1)
	sourceRunID := queueService.GetJobs()[0].GetRunId()

	replayReq := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+sourceRunID+"/replay", nil)
	replayReq.SetPathValue("id", sourceRunID)
	replayRec := httptest.NewRecorder()
	server.ReplayRun(replayRec, replayReq)

	assertAPIError(t, replayRec, http.StatusConflict, "source_run_not_replayable")
}

func TestAPIServer_TriggerJob_IdempotencyKeyReplaysRun(t *testing.T) {
	server, _, queueService, db := setupTestServer(t)
	jobDef := `{"id": "job-idempotent", "root": {"id": "root", "uses": "builtins/shell", "with": {"command": "echo test"}}}`
	insertStoredJobForTest(t, db, "job-idempotent", jobDef)

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

func TestAPIServer_TriggerJob_IdempotencyCrashRecoveryReplaysInvocationRuns(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	jobID := "job-idempotent-crash"
	key := "trigger-key-crash"
	scope := "trigger:" + jobID + ":anonymous"
	jobDef := `{"id": "job-idempotent-crash", "root": {"id": "root", "uses": "builtins/shell", "with": {"command": "echo test"}}}`
	insertStoredJobForTest(t, db, jobID, jobDef)

	body := []byte(`{"cell_ids":["iad-a","pdx-b"]}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/"+jobID, bytes.NewReader(body))
	req.SetPathValue("id", jobID)
	req.Header.Set("Idempotency-Key", key)
	rec := httptest.NewRecorder()
	server.TriggerJob(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("first trigger: expected %d, got %d: %s", http.StatusAccepted, rec.Code, rec.Body.String())
	}

	resourceType, _ := clearIdempotencyResponseForAPITest(t, db, scope, key)
	if resourceType != "trigger_invocation" {
		t.Fatalf("idempotency resource type: got %q, want trigger_invocation", resourceType)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/"+jobID, bytes.NewReader(body))
	req2.SetPathValue("id", jobID)
	req2.Header.Set("Idempotency-Key", key)
	rec2 := httptest.NewRecorder()
	server.TriggerJob(rec2, req2)
	if rec2.Code != http.StatusAccepted {
		t.Fatalf("recovered trigger: expected %d, got %d: %s", http.StatusAccepted, rec2.Code, rec2.Body.String())
	}

	if rec.Body.String() != rec2.Body.String() {
		t.Fatalf("expected recovered response %q, got %q", rec.Body.String(), rec2.Body.String())
	}

	assertIdempotencyResponseCachedForAPITest(t, db, scope, key)

	var runCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM job_runs WHERE job_id = ?", jobID).Scan(&runCount); err != nil {
		t.Fatalf("count runs: %v", err)
	}

	if runCount != 2 {
		t.Fatalf("expected original two run rows after recovery, got %d", runCount)
	}
}

func TestAPIServer_TriggerJob_IdempotencyScopeIncludesJob(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	insertStoredJobForTest(t, db, "job-a", `{"id":"job-a","root":{"id":"root","uses":"builtins/shell"}}`)
	insertStoredJobForTest(t, db, "job-b", `{"id":"job-b","root":{"id":"root","uses":"builtins/shell"}}`)

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
	insertStoredJobForTest(t, db, "job-trigger-fail", jobDef)

	queueService.SetEnqueueError(errors.New("queue unavailable"))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/job-trigger-fail", nil)
	req.SetPathValue("id", "job-trigger-fail")
	rec := httptest.NewRecorder()

	server.TriggerJob(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, rec.Code)
	}

	var triggerResp struct {
		RunID string `json:"run_id"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &triggerResp); err != nil {
		t.Fatalf("parse trigger response: %v", err)
	}

	waitForLoggerErrorContaining(t, logger, "Failed to enqueue job")
	waitForNDispatchEvents(t, db, triggerResp.RunID, 3)
	events, err := dal.NewSQLRepositories(db).DispatchEvents().ListByRun(context.Background(), triggerResp.RunID)
	if err != nil {
		t.Fatalf("list dispatch events: %v", err)
	}

	if events[0].EventType != dal.DispatchEventAccepted || events[1].EventType != dal.DispatchEventAttempt || events[2].EventType != dal.DispatchEventFailure {
		t.Fatalf("unexpected dispatch events after queue error: %+v", events)
	}

	if len(queueService.GetJobs()) != 0 {
		t.Errorf("expected 0 jobs enqueued after queue error, got %d", len(queueService.GetJobs()))
	}
}

func TestAPIServer_GetJobRuns_ReturnsStatusAndFailureReasonAfterStatusTransitions(t *testing.T) {
	server, _, queueService, db := setupTestServer(t)
	jobDef := `{"id": "job-runs-status", "root": {"uses": "builtins/shell", "with": {"command": "echo test"}}}`
	insertStoredJobForTest(t, db, "job-runs-status", jobDef)

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

	if err := store.MarkRunFailed(ctx, runID, dal.FailureCodeExecution, "step failed: exit code 1"); err != nil {
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
	insertStoredJobForTest(t, db, "job-to-update", initialDef)

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

	updatedDef, _, err := dal.NewSQLRepositories(db).Jobs().GetDefinition(context.Background(), "job-to-update")
	if err != nil {
		t.Fatalf("get updated definition: %v", err)
	}
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
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

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
	insertStoredJobForTest(t, db, "job-validation-update", initialDef)

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
	gotDef, gotVersion, err := dal.NewSQLRepositories(db).Jobs().GetDefinition(context.Background(), "job-validation-update")
	if err != nil {
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
	waitForNDispatchEvents(t, db, runResp.RunID, 3)

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runResp.RunID, nil)
	getReq.SetPathValue("id", runResp.RunID)
	getRec := httptest.NewRecorder()

	server.GetRun(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("GetRun: expected status %d, got %d: %s", http.StatusOK, getRec.Code, getRec.Body.String())
	}

	var got struct {
		RunID           string `json:"run_id"`
		Status          string `json:"status"`
		DispatchSummary []struct {
			Source        string `json:"source"`
			Accepted      int    `json:"accepted"`
			Attempts      int    `json:"attempts"`
			Successes     int    `json:"successes"`
			Failures      int    `json:"failures"`
			FirstEventAt  int64  `json:"first_event_at"`
			LastEventAt   int64  `json:"last_event_at"`
			LastEventType string `json:"last_event_type"`
		} `json:"dispatch_summary"`
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

	if len(got.DispatchEvents) != 3 {
		t.Fatalf("expected dispatch trail in run response, got %+v", got.DispatchEvents)
	}

	if len(got.DispatchSummary) != 1 {
		t.Fatalf("expected dispatch summary in run response, got %+v", got.DispatchSummary)
	}

	if got.DispatchSummary[0].Source != dal.DispatchSourceAPI || got.DispatchSummary[0].Accepted != 1 || got.DispatchSummary[0].Attempts != 1 || got.DispatchSummary[0].Successes != 1 || got.DispatchSummary[0].Failures != 0 || got.DispatchSummary[0].LastEventType != dal.DispatchEventSuccess {
		t.Fatalf("unexpected dispatch summary: %+v", got.DispatchSummary[0])
	}

	if got.DispatchEvents[0].Source != dal.DispatchSourceAPI || got.DispatchEvents[0].EventType != dal.DispatchEventAccepted {
		t.Fatalf("unexpected first dispatch event: %+v", got.DispatchEvents[0])
	}

	if got.DispatchEvents[1].Source != dal.DispatchSourceAPI || got.DispatchEvents[1].EventType != dal.DispatchEventAttempt {
		t.Fatalf("unexpected second dispatch event: %+v", got.DispatchEvents[1])
	}

	if got.DispatchEvents[2].Source != dal.DispatchSourceAPI || got.DispatchEvents[2].EventType != dal.DispatchEventSuccess {
		t.Fatalf("unexpected third dispatch event: %+v", got.DispatchEvents[2])
	}
}

func TestAPIServer_GetRun_IncludesTaskFinalizationRepairNextAction(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	ctx := context.Background()

	insertStoredJobForTest(t, db, "job-task-finalization-detail", `{"id":"job-task-finalization-detail","root":{"uses":"builtins/shell"}}`)
	runID, _, err := repos.Runs().CreateRun(ctx, "job-task-finalization-detail", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	runfixture.FinalizeExecutionByClaim(t, ctx, repos, dispatch.ExecutionID, dal.ExecutionStatusSucceeded)

	if err := repos.Runs().MarkRunOrphaned(ctx, runID, dal.OrphanReasonLeaseExpired); err != nil {
		t.Fatalf("mark run orphaned: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID, nil)
	req.SetPathValue("id", runID)
	rec := httptest.NewRecorder()
	server.GetRun(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GetRun: expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var got struct {
		RunID          string  `json:"run_id"`
		Status         string  `json:"status"`
		NextAction     *string `json:"next_action"`
		TaskCompletion *struct {
			Total          int `json:"total"`
			Succeeded      int `json:"succeeded"`
			TerminalFailed int `json:"terminal_failed"`
			Incomplete     int `json:"incomplete"`
		} `json:"task_completion"`
	}

	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode run detail: %v", err)
	}

	if got.RunID != runID {
		t.Fatalf("run_id: got %q, want %q", got.RunID, runID)
	}

	if got.Status != dal.RunStatusOrphaned {
		t.Fatalf("status: got %q, want %q", got.Status, dal.RunStatusOrphaned)
	}

	if got.NextAction == nil || *got.NextAction != "task_finalization_repair_pending" {
		t.Fatalf("next_action: got %+v, want task_finalization_repair_pending", got.NextAction)
	}

	if got.TaskCompletion == nil {
		t.Fatal("missing task_completion in response")
	}

	if got.TaskCompletion.Total != 1 || got.TaskCompletion.Succeeded != 1 || got.TaskCompletion.TerminalFailed != 0 || got.TaskCompletion.Incomplete != 0 {
		t.Fatalf("task completion summary mismatch: %+v", got.TaskCompletion)
	}
}

func TestAPIServer_GetRun_IncludesTaskContinuationNextAction(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	ctx := context.Background()

	insertStoredJobForTest(t, db, "job-task-continuation-detail", `{"id":"job-task-continuation-detail","root":{"uses":"builtins/shell"}}`)
	runID, _, err := repos.Runs().CreateRun(ctx, "job-task-continuation-detail", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	if _, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: dispatch.TaskID,
		TaskKey:      "child",
		Name:         "child",
		SpecHash:     "sha256:child",
		TargetCellID: "local",
	}); err != nil {
		t.Fatalf("ensure child task: %v", err)
	}

	result := runfixture.FinalizeExecutionByClaim(t, ctx, repos, dispatch.ExecutionID, dal.ExecutionStatusSucceeded)
	if result.Outcome != dal.ExecutionFinalizationOutcomeContinued {
		t.Fatalf("expected continuation outcome, got %+v", result)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID, nil)
	req.SetPathValue("id", runID)
	rec := httptest.NewRecorder()
	server.GetRun(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GetRun: expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var got struct {
		RunID          string  `json:"run_id"`
		Status         string  `json:"status"`
		NextAction     *string `json:"next_action"`
		TaskCompletion *struct {
			Total          int `json:"total"`
			Succeeded      int `json:"succeeded"`
			TerminalFailed int `json:"terminal_failed"`
			Incomplete     int `json:"incomplete"`
		} `json:"task_completion"`
	}

	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode run detail: %v", err)
	}

	if got.RunID != runID {
		t.Fatalf("run_id: got %q, want %q", got.RunID, runID)
	}

	if got.Status != dal.RunStatusQueued {
		t.Fatalf("status: got %q, want %q", got.Status, dal.RunStatusQueued)
	}

	if got.NextAction == nil || *got.NextAction != "task_continuation_pending" {
		t.Fatalf("next_action: got %+v, want task_continuation_pending", got.NextAction)
	}

	if got.TaskCompletion == nil {
		t.Fatal("missing task_completion in response")
	}

	if got.TaskCompletion.Total != 2 || got.TaskCompletion.Succeeded != 1 || got.TaskCompletion.TerminalFailed != 0 || got.TaskCompletion.Incomplete != 1 {
		t.Fatalf("task completion summary mismatch: %+v", got.TaskCompletion)
	}
}

func TestAPIServer_GetRun_IncludesSecurityGateNextAction(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	ctx := context.Background()

	insertStoredJobForTest(t, db, "job-security-gate-detail", `{"id":"job-security-gate-detail","root":{"uses":"builtins/shell"}}`)
	runID, _, err := repos.Runs().CreateRun(ctx, "job-security-gate-detail", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	if err := repos.Runs().RecordExecutionSecurityEvent(ctx, dal.RecordExecutionSecurityEventParams{
		RunID:         runID,
		TaskID:        dispatch.TaskID,
		TaskAttemptID: dispatch.TaskAttemptID,
		ExecutionID:   dispatch.ExecutionID,
		EventType:     dal.ExecutionSecurityEventSecretResolution,
		Outcome:       "denied",
		Reason:        "authorization_denied",
		Provider:      "encryptedfs",
		CreatedAt:     123,
	}); err != nil {
		t.Fatalf("record security event: %v", err)
	}

	if err := repos.Runs().MarkRunFailed(ctx, runID, dal.FailureCodeExecution, "resolve execution secrets: denied"); err != nil {
		t.Fatalf("mark run failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID, nil)
	req.SetPathValue("id", runID)
	rec := httptest.NewRecorder()
	server.GetRun(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GetRun: expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var got struct {
		RunID                     string  `json:"run_id"`
		Status                    string  `json:"status"`
		NextAction                *string `json:"next_action"`
		LatestFailedSecurityEvent *struct {
			EventType string  `json:"event_type"`
			Outcome   string  `json:"outcome"`
			Reason    string  `json:"reason"`
			Provider  *string `json:"provider"`
		} `json:"latest_failed_security_event"`
	}

	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode run detail: %v", err)
	}

	if got.RunID != runID || got.Status != dal.RunStatusFailed {
		t.Fatalf("unexpected run detail: %+v", got)
	}

	if got.NextAction == nil || *got.NextAction != "security_gate_failed" {
		t.Fatalf("next_action: got %+v, want security_gate_failed", got.NextAction)
	}

	if got.LatestFailedSecurityEvent == nil || got.LatestFailedSecurityEvent.EventType != dal.ExecutionSecurityEventSecretResolution || got.LatestFailedSecurityEvent.Outcome != "denied" || got.LatestFailedSecurityEvent.Provider == nil || *got.LatestFailedSecurityEvent.Provider != "encryptedfs" {
		t.Fatalf("latest_failed_security_event: %+v", got.LatestFailedSecurityEvent)
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

func TestAPIServer_RunJob_IdempotencyCrashRecoveryReplaysGeneratedJob(t *testing.T) {
	server, _, queueService, db := setupTestServer(t)
	key := "run-key-crash"
	scope := "run:/:anonymous"

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
	req.Header.Set("Idempotency-Key", key)
	rec := httptest.NewRecorder()
	server.RunJob(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("first run: expected %d, got %d: %s", http.StatusAccepted, rec.Code, rec.Body.String())
	}

	var firstResp struct {
		ID    string `json:"id"`
		RunID string `json:"run_id"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &firstResp); err != nil {
		t.Fatalf("decode first response: %v", err)
	}

	if firstResp.ID == "" || firstResp.RunID == "" {
		t.Fatalf("first response missing generated ids: %+v", firstResp)
	}

	resourceType, _ := clearIdempotencyResponseForAPITest(t, db, scope, key)
	if resourceType != "trigger_invocation" {
		t.Fatalf("idempotency resource type: got %q, want trigger_invocation", resourceType)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/run", bytes.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Idempotency-Key", key)
	rec2 := httptest.NewRecorder()
	server.RunJob(rec2, req2)
	if rec2.Code != http.StatusAccepted {
		t.Fatalf("recovered run: expected %d, got %d: %s", http.StatusAccepted, rec2.Code, rec2.Body.String())
	}

	if rec.Body.String() != rec2.Body.String() {
		t.Fatalf("expected recovered response %q, got %q", rec.Body.String(), rec2.Body.String())
	}

	assertIdempotencyResponseCachedForAPITest(t, db, scope, key)
	waitForNEnqueuedJobs(t, queueService, 1)
	if got := len(queueService.GetJobs()); got != 1 {
		t.Fatalf("expected one enqueued job after recovered replay, got %d", got)
	}

	var runCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM job_runs").Scan(&runCount); err != nil {
		t.Fatalf("count runs: %v", err)
	}

	if runCount != 1 {
		t.Fatalf("expected original run row after recovery, got %d", runCount)
	}

	var persistedJobID string
	if err := db.QueryRow("SELECT job_id FROM job_runs WHERE run_id = ?", firstResp.RunID).Scan(&persistedJobID); err != nil {
		t.Fatalf("query recovered run job id: %v", err)
	}

	if persistedJobID != firstResp.ID {
		t.Fatalf("persisted job id: got %q, want recovered id %q", persistedJobID, firstResp.ID)
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

	server.Handler().ServeHTTP(rec, req)

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
	server, logger, queueService, db := setupTestServer(t)

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

	var runResp struct {
		RunID string `json:"run_id"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &runResp); err != nil {
		t.Fatalf("parse run response: %v", err)
	}

	waitForLoggerErrorContaining(t, logger, "Failed to enqueue job")
	waitForNDispatchEvents(t, db, runResp.RunID, 3)
	events, err := dal.NewSQLRepositories(db).DispatchEvents().ListByRun(context.Background(), runResp.RunID)
	if err != nil {
		t.Fatalf("list dispatch events: %v", err)
	}

	if events[0].EventType != dal.DispatchEventAccepted || events[1].EventType != dal.DispatchEventAttempt || events[2].EventType != dal.DispatchEventFailure {
		t.Fatalf("unexpected dispatch events after queue error: %+v", events)
	}

	if len(queueService.GetJobs()) != 0 {
		t.Errorf("expected 0 jobs enqueued after queue error, got %d", len(queueService.GetJobs()))
	}
}

func TestAPIServer_SSEJobRuns_ReceivesRunOnTrigger(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	jobID := "job-sse-test"
	jobDef := `{"id": "job-sse-test", "root": {"uses": "builtins/shell", "with": {"command": "echo test"}}}`
	insertStoredJobForTest(t, db, jobID, jobDef)

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
	insertStoredJobForTest(t, db, "job-force-fail", `{"id":"job-force-fail"}`, 1)
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
	insertStoredJobForTest(t, db, "job-repair-mark", `{"id":"job-repair-mark"}`, 1)
	runs := dal.NewSQLRepositories(db).Runs()

	runID, _, err := runs.CreateRun(ctx, "job-repair-mark", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	claimPendingRunExecutionForAPITest(t, runs, runID, "worker-a", time.Now().Add(-time.Minute))

	if err := runs.MarkRunOrphaned(ctx, runID, dal.OrphanReasonLeaseExpired); err != nil {
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
	insertStoredJobForTest(t, db, "job-repair-running", `{"id":"job-repair-running"}`, 1)
	runs := dal.NewSQLRepositories(db).Runs()

	runID, _, err := runs.CreateRun(ctx, "job-repair-running", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	claimPendingRunExecutionForAPITest(t, runs, runID, "worker-a", time.Now().Add(time.Minute))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+runID+"/repair/mark-failed", strings.NewReader(`{}`))
	req.SetPathValue("id", runID)
	rec := httptest.NewRecorder()

	server.RepairMarkRunFailed(rec, req)

	assertAPIError(t, rec, http.StatusConflict, "run_repair_conflict")
}

func TestAPIServer_ForceRequeueRun_Success(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	insertStoredJobForTest(t, db, "job-force-requeue", `{"id":"job-force-requeue"}`, 1)
	runs := dal.NewSQLRepositories(db).Runs()

	runID, _, err := runs.CreateRun(ctx, "job-force-requeue", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	claimPendingRunExecutionForAPITest(t, runs, runID, "worker-a", time.Now().Add(time.Minute))

	if err := runs.MarkRunFailed(ctx, runID, dal.FailureCodeExecution, "transient failure"); err != nil {
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
	if err := db.QueryRowContext(ctx, `SELECT status, failure_code, failure_reason FROM job_runs WHERE run_id = ?`, runID).
		Scan(&status, &failureCode, &failure); err != nil {
		t.Fatalf("query run: %v", err)
	}

	if status != "queued" {
		t.Fatalf("expected queued status, got %q", status)
	}

	if failureCode != "" || failure.Valid {
		t.Fatalf("expected cleared failure fields, got failure_code=%q failure=%v", failureCode, failure)
	}
}

func TestAPIServer_ForceRequeueRun_SucceededConflict(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	insertStoredJobForTest(t, db, "job-force-requeue-succeeded", `{"id":"job-force-requeue-succeeded"}`, 1)
	runs := dal.NewSQLRepositories(db).Runs()

	runID, _, err := runs.CreateRun(ctx, "job-force-requeue-succeeded", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	if err := runs.MarkRunSucceeded(ctx, runID); err != nil {
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
	insertStoredJobForTest(t, db, "job-force-requeue-running", `{"id":"job-force-requeue-running"}`, 1)
	runs := dal.NewSQLRepositories(db).Runs()

	runID, _, err := runs.CreateRun(ctx, "job-force-requeue-running", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	claimPendingRunExecutionForAPITest(t, runs, runID, "worker-a", time.Now().Add(time.Minute))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+runID+"/force-requeue", nil)
	req.SetPathValue("id", runID)
	rec := httptest.NewRecorder()

	server.ForceRequeueRun(rec, req)

	assertAPIError(t, rec, http.StatusConflict, "run_requeue_conflict")
}

func TestAPIServer_CancelRun_NotExecutingConflict(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	insertStoredJobForTest(t, db, "job-cancel-queued", `{"id":"job-cancel-queued"}`, 1)
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
	insertStoredJobForTest(t, db, "job-cancel-context", `{"id":"job-cancel-context"}`, 1)
	runs := dal.NewSQLRepositories(db).Runs()

	runID, _, err := runs.CreateRun(ctx, "job-cancel-context", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	claimPendingRunExecutionForAPITest(t, runs, runID, "worker-a", time.Now().Add(time.Minute))

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

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected durable cancel acceptance, got status=%d body=%s", rec.Code, rec.Body.String())
	}
	if !sawDeadline {
		t.Fatal("expected resolver to receive request-bound context with deadline")
	}

	var cancelRequestedAt sql.NullInt64
	var cancelReason sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT cancel_requested_at, cancel_reason
		FROM job_runs
		WHERE run_id = ?
	`, runID).Scan(&cancelRequestedAt, &cancelReason); err != nil {
		t.Fatalf("query cancel request: %v", err)
	}

	if !cancelRequestedAt.Valid || cancelRequestedAt.Int64 <= 0 {
		t.Fatalf("expected durable cancel timestamp, got %v", cancelRequestedAt)
	}

	if !cancelReason.Valid || cancelReason.String != dal.CancelReasonAPI {
		t.Fatalf("expected cancel reason %q, got %v", dal.CancelReasonAPI, cancelReason)
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
