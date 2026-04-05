package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
)

func TestNewWorkerMetrics_appearsOnScrape(t *testing.T) {
	ctx := context.Background()
	h, shutdown, err := InitServiceMetrics(ctx, "vectis-worker")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	wm, err := NewWorkerMetrics()
	if err != nil {
		t.Fatal(err)
	}

	wm.RecordJobReceived(ctx)
	wm.RecordJobFinished(ctx, WorkerOutcomeSuccess, 50*time.Millisecond)

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

	for _, want := range []string{"vectis_worker_jobs_received_total", "vectis_worker_job_duration_seconds"} {
		if _, ok := names[want]; !ok {
			t.Fatalf("missing metric %q; got: %v", want, sortedFamilyNames(names))
		}
	}
}
