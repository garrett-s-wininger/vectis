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
	wm.RecordSPIRESVIDCheck(ctx, WorkerSPIRESVIDOutcomeSuccess, WorkerSPIRESVIDReasonMatched)
	wm.RecordSPIRESVIDCheck(ctx, WorkerSPIRESVIDOutcomeFailed, WorkerSPIRESVIDReasonMismatch)
	wm.SetLifecyclePhase(WorkerPhaseExecuting)
	wm.SetDraining(true)
	wm.SetDBUnavailable(true)

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
		"vectis_worker_jobs_received_total",
		"vectis_worker_job_duration_seconds",
		"vectis_worker_spire_svid_checks_total",
		"vectis_worker_lifecycle_state",
		"vectis_worker_draining",
		"vectis_worker_db_unavailable",
	} {
		if _, ok := names[want]; !ok {
			t.Fatalf("missing metric %q; got: %v", want, sortedFamilyNames(names))
		}
	}

	families, err := metricFamilies(rr.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	if !metricFamilyHasLabels(families["vectis_worker_lifecycle_state"], map[string]string{"state": WorkerPhaseExecuting}) {
		t.Fatalf("lifecycle metric missing executing state: %v", families["vectis_worker_lifecycle_state"])
	}

	if !metricFamilyHasLabels(families["vectis_worker_spire_svid_checks_total"], map[string]string{
		"outcome": WorkerSPIRESVIDOutcomeSuccess,
		"reason":  WorkerSPIRESVIDReasonMatched,
	}) {
		t.Fatalf("SPIRE SVID metric missing success labels: %v", families["vectis_worker_spire_svid_checks_total"])
	}

	if !metricFamilyHasLabels(families["vectis_worker_spire_svid_checks_total"], map[string]string{
		"outcome": WorkerSPIRESVIDOutcomeFailed,
		"reason":  WorkerSPIRESVIDReasonMismatch,
	}) {
		t.Fatalf("SPIRE SVID metric missing failure labels: %v", families["vectis_worker_spire_svid_checks_total"])
	}
}
