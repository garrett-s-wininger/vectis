package observability

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
)

func TestOrchestratorRetentionMetrics(t *testing.T) {
	h, shutdown, err := InitServiceMetrics(t.Context(), "vectis-orchestrator")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = shutdown(context.Background()) })
	var runs, tasks, pending, reclaimed int64 = 3, 12, 2, 7
	unregister, err := RegisterOrchestratorRetentionMetrics(func() (int64, int64, int64, int64) {
		return runs, tasks, pending, reclaimed
	})

	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = unregister() })
	for range 2 {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
		if rr.Code != http.StatusOK {
			t.Fatalf("scrape status %d", rr.Code)
		}

		for name, want := range map[string]int64{
			"vectis_orchestrator_retained_runs":             runs,
			"vectis_orchestrator_retained_tasks":            tasks,
			"vectis_orchestrator_runs_awaiting_persistence": pending,
			"vectis_orchestrator_reclaimed_runs_total":      reclaimed,
		} {
			if !regexp.MustCompile(fmt.Sprintf(`(?m)^%s(\{[^\n]*\})? %d$`, name, want)).Match(rr.Body.Bytes()) {
				t.Fatalf("missing %s=%d in scrape: %s", name, want, rr.Body.String())
			}
		}

		runs, tasks, pending, reclaimed = 0, 0, 0, 10
	}
}
