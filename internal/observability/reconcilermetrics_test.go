package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/common/expfmt"
)

func TestReconcilerCounters_AppearOnScrape(t *testing.T) {
	ctx := context.Background()
	h, shutdown, err := InitServiceMetrics(ctx, "vectis-reconciler")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	rm, err := NewReconcilerMetrics()
	if err != nil {
		t.Fatal(err)
	}

	rm.RecordRunsScanned(ctx, 2)
	rm.RecordReenqueueOutcome(ctx, ReconcilerOutcomeSuccess)
	rm.RecordReenqueueOutcome(ctx, ReconcilerOutcomeFailedEnqueue)
	rm.RecordTaskFinalizationRepair(ctx, ReconcilerTaskFinalizationOutcomeSuccess, "failed")
	rm.RecordTaskFinalizationRepair(ctx, ReconcilerTaskFinalizationOutcomeError, ReconcilerTaskFinalizationReduceOutcomeUnknown)

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

	for _, want := range []string{
		"vectis_reconciler_runs_scanned_total",
		"vectis_reconciler_reenqueue_total",
		"vectis_reconciler_task_finalization_repairs_total",
	} {
		if _, ok := names[want]; !ok {
			t.Fatalf("missing metric %q; got: %v", want, sortedFamilyNames(names))
		}
	}

	families, err := metricFamilies(rr.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	if !metricFamilyHasLabels(families["vectis_reconciler_task_finalization_repairs_total"], map[string]string{
		"outcome":        ReconcilerTaskFinalizationOutcomeSuccess,
		"reduce_outcome": "failed",
	}) {
		t.Fatalf("task finalization metric missing success labels: %v", families["vectis_reconciler_task_finalization_repairs_total"])
	}

	if !metricFamilyHasLabels(families["vectis_reconciler_task_finalization_repairs_total"], map[string]string{
		"outcome":        ReconcilerTaskFinalizationOutcomeError,
		"reduce_outcome": ReconcilerTaskFinalizationReduceOutcomeUnknown,
	}) {
		t.Fatalf("task finalization metric missing error labels: %v", families["vectis_reconciler_task_finalization_repairs_total"])
	}
}
