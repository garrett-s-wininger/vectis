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
	wm.RecordSPIFFESVIDCheck(ctx, WorkerSPIFFESVIDOutcomeSuccess, WorkerSPIFFESVIDReasonMatched)
	wm.RecordSPIFFESVIDCheck(ctx, WorkerSPIFFESVIDOutcomeFailed, WorkerSPIFFESVIDReasonMismatch)
	wm.RecordCheckoutCacheWarm(ctx, WorkerCheckoutCacheWarmOutcomePartial, WorkerCheckoutCacheWarmReasonRemoteFailures, 2, 1, 125*time.Millisecond)
	wm.RecordOrchestratorRecovery(ctx, "complete")
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
		"vectis_worker_orchestrator_recoveries_total",
		"vectis_worker_job_duration_seconds",
		"vectis_worker_spiffe_svid_checks_total",
		"vectis_worker_checkout_cache_warm_passes_total",
		"vectis_worker_checkout_cache_warm_duration_seconds",
		"vectis_worker_checkout_cache_warm_remotes_total",
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

	if !metricFamilyHasLabels(families["vectis_worker_spiffe_svid_checks_total"], map[string]string{
		"outcome": WorkerSPIFFESVIDOutcomeSuccess,
		"reason":  WorkerSPIFFESVIDReasonMatched,
	}) {
		t.Fatalf("SPIFFE SVID metric missing success labels: %v", families["vectis_worker_spiffe_svid_checks_total"])
	}

	if !metricFamilyHasLabels(families["vectis_worker_spiffe_svid_checks_total"], map[string]string{
		"outcome": WorkerSPIFFESVIDOutcomeFailed,
		"reason":  WorkerSPIFFESVIDReasonMismatch,
	}) {
		t.Fatalf("SPIFFE SVID metric missing failure labels: %v", families["vectis_worker_spiffe_svid_checks_total"])
	}

	if !metricFamilyHasLabels(families["vectis_worker_orchestrator_recoveries_total"], map[string]string{"stage": "complete"}) {
		t.Fatalf("orchestrator recovery metric missing stage label: %v", families["vectis_worker_orchestrator_recoveries_total"])
	}

	if !metricFamilyHasLabels(families["vectis_worker_checkout_cache_warm_passes_total"], map[string]string{
		"outcome": WorkerCheckoutCacheWarmOutcomePartial,
		"reason":  WorkerCheckoutCacheWarmReasonRemoteFailures,
	}) {
		t.Fatalf("checkout cache warm pass metric missing labels: %v", families["vectis_worker_checkout_cache_warm_passes_total"])
	}

	if !metricFamilyHasLabels(families["vectis_worker_checkout_cache_warm_remotes_total"], map[string]string{
		"outcome": WorkerCheckoutCacheWarmRemoteOutcomeWarmed,
	}) {
		t.Fatalf("checkout cache warm remotes metric missing warmed label: %v", families["vectis_worker_checkout_cache_warm_remotes_total"])
	}

	if !metricFamilyHasLabels(families["vectis_worker_checkout_cache_warm_remotes_total"], map[string]string{
		"outcome": WorkerCheckoutCacheWarmRemoteOutcomeFailed,
	}) {
		t.Fatalf("checkout cache warm remotes metric missing failed label: %v", families["vectis_worker_checkout_cache_warm_remotes_total"])
	}
}
