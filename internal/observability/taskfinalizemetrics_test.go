package observability

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/taskfinalize"
	"vectis/internal/taskreduce"

	"github.com/prometheus/common/expfmt"
)

func TestTaskFinalizeMetrics_RecordDecisions(t *testing.T) {
	ctx := context.Background()
	metricsHandler, shutdown, err := InitServiceMetrics(ctx, "vectis-worker")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	m, err := NewTaskFinalizeMetrics()
	if err != nil {
		t.Fatal(err)
	}

	reduceFailed := taskreduce.Decision{
		Outcome: taskreduce.OutcomeFailed,
		Summary: dal.RunTaskCompletion{RunID: "run-1", Total: 2, Succeeded: 1, TerminalFailed: 1},
	}

	m.RecordReduce(ctx, reduceFailed, nil)
	m.RecordReduce(ctx, taskreduce.Decision{}, errors.New("db unavailable"))
	m.RecordFinalize(ctx, taskfinalize.Decide(false, reduceFailed))
	m.RecordFinalize(ctx, taskfinalize.ExecutionAborted())

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

	if !metricFamilyHasLabels(families["vectis_task_reduce_decisions_total"], map[string]string{
		"outcome": string(taskreduce.OutcomeFailed),
	}) {
		t.Fatalf("reduce metric missing failed labels: %v", families["vectis_task_reduce_decisions_total"])
	}

	if !metricFamilyHasLabels(families["vectis_task_reduce_decisions_total"], map[string]string{
		"outcome": TaskReduceOutcomeError,
	}) {
		t.Fatalf("reduce metric missing error labels: %v", families["vectis_task_reduce_decisions_total"])
	}

	if !metricFamilyHasLabels(families["vectis_task_finalize_decisions_total"], map[string]string{
		"outcome":        string(taskfinalize.OutcomeReduceFailed),
		"reduce_outcome": string(taskreduce.OutcomeFailed),
	}) {
		t.Fatalf("finalize metric missing reduce-failed labels: %v", families["vectis_task_finalize_decisions_total"])
	}

	if !metricFamilyHasLabels(families["vectis_task_finalize_decisions_total"], map[string]string{
		"outcome":        string(taskfinalize.OutcomeExecutionAborted),
		"reduce_outcome": taskFinalizeReduceOutcomeNone,
	}) {
		t.Fatalf("finalize metric missing aborted labels: %v", families["vectis_task_finalize_decisions_total"])
	}
}
