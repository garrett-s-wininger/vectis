package observability

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"vectis/internal/taskdispatch"

	"github.com/prometheus/common/expfmt"
)

func TestTaskDispatchMetrics_RecordDrain(t *testing.T) {
	ctx := context.Background()
	metricsHandler, shutdown, err := InitServiceMetrics(ctx, "vectis-worker")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	m, err := NewTaskDispatchMetrics()
	if err != nil {
		t.Fatal(err)
	}

	m.RecordDrain(ctx,
		taskdispatch.DrainOptions{CellID: "iad-a", RunID: "run-1"},
		taskdispatch.DrainResult{Listed: 2, Enqueued: 1, Failed: 1},
		nil)

	m.RecordDrain(ctx,
		taskdispatch.DrainOptions{CellID: "iad-a"},
		taskdispatch.DrainResult{},
		errors.New("db unavailable"))

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

	if !metricFamilyHasLabels(families["vectis_task_dispatch_drains_total"], map[string]string{
		"scope":       "run",
		"target_cell": "iad-a",
		"outcome":     TaskDispatchDrainOutcomePartialFailure,
	}) {
		t.Fatalf("drain metric missing partial-failure labels: %v", families["vectis_task_dispatch_drains_total"])
	}

	if !metricFamilyHasLabels(families["vectis_task_dispatch_drains_total"], map[string]string{
		"scope":       "cell",
		"target_cell": "iad-a",
		"outcome":     TaskDispatchDrainOutcomeError,
	}) {
		t.Fatalf("drain metric missing error labels: %v", families["vectis_task_dispatch_drains_total"])
	}

	if !metricFamilyHasLabels(families["vectis_task_dispatch_intents_total"], map[string]string{
		"scope":       "run",
		"target_cell": "iad-a",
		"outcome":     TaskDispatchIntentOutcomeListed,
	}) {
		t.Fatalf("intent metric missing listed labels: %v", families["vectis_task_dispatch_intents_total"])
	}

	if !metricFamilyHasLabels(families["vectis_task_dispatch_intents_total"], map[string]string{
		"scope":       "run",
		"target_cell": "iad-a",
		"outcome":     TaskDispatchIntentOutcomeFailed,
	}) {
		t.Fatalf("intent metric missing failed labels: %v", families["vectis_task_dispatch_intents_total"])
	}
}
