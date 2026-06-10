//go:build cgo && !nosqlite

package observability

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/prometheus/common/expfmt"

	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"
)

func TestRegisterSQLDBPoolMetrics_appearsInScrape(t *testing.T) {
	ctx := context.Background()
	h, shutdown, err := InitAPIMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	if err := RegisterSQLDBPoolMetrics(db); err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
	req.Header.Set("Accept", string(expfmt.NewFormat(expfmt.TypeOpenMetrics)))
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}

	names, err := metricFamilyNames(rr.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"db_client_connections_open", "db_client_connections_in_use"} {
		if _, ok := names[want]; !ok {
			t.Fatalf("missing metric %q; got: %v", want, sortedFamilyNames(names))
		}
	}
}

func TestRegisterRetentionStorageMetrics_appearsInScrape(t *testing.T) {
	ctx := context.Background()
	h, shutdown, err := InitAPIMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	db := dbtest.NewTestDB(t)
	old := time.Now().Add(-48 * time.Hour).UTC().Format("2006-01-02 15:04:05")
	if _, err := db.Exec(`
		INSERT INTO job_runs (run_id, job_id, run_index, status, started_at, finished_at)
		VALUES ('metric-run', 'metric-job', 1, 'succeeded', ?, ?)
	`, old, old); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`
		INSERT INTO audit_log (event_type, metadata, created_at)
		VALUES ('metric.event', '{}', ?)
	`, old); err != nil {
		t.Fatal(err)
	}

	if err := RegisterRetentionStorageMetrics(db); err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
	req.Header.Set("Accept", string(expfmt.NewFormat(expfmt.TypeOpenMetrics)))
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}

	names, err := metricFamilyNames(rr.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"vectis_storage_records", "vectis_storage_oldest_record_age_seconds"} {
		if _, ok := names[want]; !ok {
			t.Fatalf("missing metric %q; got: %v", want, sortedFamilyNames(names))
		}
	}

	families, err := metricFamilies(rr.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	if !metricFamilyHasLabels(families["vectis_storage_records"], map[string]string{"surface": "task_dispatch_intents"}) {
		t.Fatalf("storage records metric missing task_dispatch_intents surface: %v", families["vectis_storage_records"])
	}

	if !metricFamilyHasLabels(families["vectis_storage_records"], map[string]string{"surface": "run_artifacts"}) {
		t.Fatalf("storage records metric missing run_artifacts surface: %v", families["vectis_storage_records"])
	}
}

func TestRegisterTaskDispatchBacklogMetrics_appearsInScrape(t *testing.T) {
	ctx := context.Background()
	h, shutdown, err := InitAPIMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "global-a")

	ns, err := repos.Namespaces().Create(ctx, "team-task-dispatch-metrics", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-task-dispatch-metrics"
	def := `{"id":"job-task-dispatch-metrics","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRunInCell(ctx, jobID, nil, 1, "iad-a")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	if _, _, err := repos.TaskDispatchIntents().Ensure(ctx, dal.TaskDispatchIntentCreate{
		ExecutionID:       dispatch.ExecutionID,
		RunID:             dispatch.RunID,
		TaskID:            dispatch.TaskID,
		TaskAttemptID:     dispatch.TaskAttemptID,
		SourceExecutionID: "source-execution",
		CellID:            dispatch.CellID,
	}); err != nil {
		t.Fatalf("ensure task dispatch intent: %v", err)
	}

	if err := RegisterTaskDispatchBacklogMetrics(db); err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
	req.Header.Set("Accept", string(expfmt.NewFormat(expfmt.TypeOpenMetrics)))
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}

	families, err := metricFamilies(rr.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	if !metricFamilyHasLabels(families["vectis_task_dispatch_pending_intents"], map[string]string{"cell_id": "iad-a"}) {
		t.Fatalf("task dispatch backlog metric missing cell labels: %v", families["vectis_task_dispatch_pending_intents"])
	}
}
