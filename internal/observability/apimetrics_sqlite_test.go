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
}
