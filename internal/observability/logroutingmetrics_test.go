package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/common/expfmt"
)

func TestLogRoutingMetrics_ExposeStableLabels(t *testing.T) {
	ctx := context.Background()
	h, shutdown, err := InitServiceMetrics(ctx, "vectis-worker")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	metrics, err := NewLogRoutingMetrics()
	if err != nil {
		t.Fatal(err)
	}

	metrics.RecordShardAssignment(ctx, "new")
	metrics.RecordShardRouteFailure(ctx, "write", "assigned_unavailable")

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

	if !metricFamilyHasLabels(families["vectis_log_shard_assignments_total"], map[string]string{"outcome": "new"}) {
		t.Fatal("missing log shard assignment labels")
	}

	if !metricFamilyHasLabels(families["vectis_log_shard_route_failures_total"], map[string]string{
		"operation": "write",
		"reason":    "assigned_unavailable",
	}) {
		t.Fatal("missing log shard route failure labels")
	}
}
