package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/common/expfmt"
)

func TestDispatchMetrics_AppearsOnScrape(t *testing.T) {
	ctx := context.Background()
	h, shutdown, err := InitServiceMetrics(ctx, "vectis-test-dispatch")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	dm, err := NewDispatchMetrics()
	if err != nil {
		t.Fatal(err)
	}

	dm.RecordDispatchEvent(ctx, "api", "attempt", "iad-a")
	dm.RecordDispatchEvent(ctx, "reconciler", "success", "iad-a")

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

	if _, ok := names["vectis_run_dispatch_events_total"]; !ok {
		t.Fatalf("missing metric vectis_run_dispatch_events_total; got: %v", sortedFamilyNames(names))
	}
}
