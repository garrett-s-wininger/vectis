package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/common/expfmt"
)

func TestRegisterQueueGauges_appearsOnScrape(t *testing.T) {
	ctx := context.Background()
	h, shutdown, err := InitServiceMetrics(ctx, "vectis-queue")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	var pending, inflight int64 = 3, 2
	if err := RegisterQueueGauges(func() (int64, int64) {
		return pending, inflight
	}); err != nil {
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
	for _, want := range []string{"vectis_queue_jobs_pending", "vectis_queue_deliveries_inflight"} {
		if _, ok := names[want]; !ok {
			t.Fatalf("missing metric %q; got: %v", want, sortedFamilyNames(names))
		}
	}
}
