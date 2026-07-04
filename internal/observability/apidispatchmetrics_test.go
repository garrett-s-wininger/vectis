package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/common/expfmt"
)

func TestAPIDispatchMetrics_RecordRunEnqueue(t *testing.T) {
	ctx := context.Background()
	metricsHandler, shutdown, err := InitAPIMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	m, err := NewAPIDispatchMetrics()
	if err != nil {
		t.Fatal(err)
	}

	m.RecordRunEnqueue(ctx, APIEnqueueRunKindReplay, APIEnqueueOutcomeAccepted)
	m.RecordRunEnqueue(ctx, APIEnqueueRunKindEphemeral, APIEnqueueOutcomeFailedEnqueue)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
	req.Header.Set("Accept", string(expfmt.NewFormat(expfmt.TypeOpenMetrics)))
	metricsHandler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}

	families, err := metricFamilies(rr.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	family := families["vectis_api_run_enqueue_total"]
	if family == nil {
		t.Fatal("missing vectis_api_run_enqueue_total")
	}

	if !metricFamilyHasLabels(family, map[string]string{
		"run_kind": "replay",
		"outcome":  "accepted",
	}) {
		t.Fatalf("metric missing replay accepted labels: %v", family)
	}

	if !metricFamilyHasLabels(family, map[string]string{
		"run_kind": "ephemeral",
		"outcome":  "failed_enqueue",
	}) {
		t.Fatalf("metric missing ephemeral failure labels: %v", family)
	}
}
