package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
)

func TestCheckoutActionMetrics_appearsOnScrape(t *testing.T) {
	ctx := context.Background()
	h, shutdown, err := InitServiceMetrics(ctx, "vectis-worker-core")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	metrics, err := NewCheckoutActionMetrics()
	if err != nil {
		t.Fatal(err)
	}

	metrics.RecordCheckoutActionCacheCheck(ctx, CheckoutActionCacheOutcomeHit, CheckoutActionReasonOK, 20*time.Millisecond)
	metrics.RecordCheckoutActionCacheCheck(ctx, CheckoutActionCacheOutcomeMiss, CheckoutActionReasonNoCache, 3*time.Millisecond)
	metrics.RecordCheckoutActionResult(ctx, CheckoutActionStrategyCache, CheckoutActionOutcomeSuccess, CheckoutActionReasonOK, 120*time.Millisecond)
	metrics.RecordCheckoutActionResult(ctx, CheckoutActionStrategyDirect, CheckoutActionOutcomeFailed, CheckoutActionReasonGitCloneFailed, 2*time.Second)

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

	for _, family := range []string{
		"vectis_checkout_action_results_total",
		"vectis_checkout_action_duration_seconds",
		"vectis_checkout_action_cache_checks_total",
		"vectis_checkout_action_cache_check_duration_seconds",
	} {
		if families[family] == nil {
			t.Fatalf("missing metric family %q", family)
		}
	}

	if !metricFamilyHasLabels(families["vectis_checkout_action_results_total"], map[string]string{
		"strategy": CheckoutActionStrategyCache,
		"outcome":  CheckoutActionOutcomeSuccess,
		"reason":   CheckoutActionReasonOK,
	}) {
		t.Fatalf("checkout result metric missing cache success labels: %v", families["vectis_checkout_action_results_total"])
	}

	if !metricFamilyHasLabels(families["vectis_checkout_action_results_total"], map[string]string{
		"strategy": CheckoutActionStrategyDirect,
		"outcome":  CheckoutActionOutcomeFailed,
		"reason":   CheckoutActionReasonGitCloneFailed,
	}) {
		t.Fatalf("checkout result metric missing direct failure labels: %v", families["vectis_checkout_action_results_total"])
	}

	if !metricFamilyHasLabels(families["vectis_checkout_action_cache_checks_total"], map[string]string{
		"outcome": CheckoutActionCacheOutcomeHit,
		"reason":  CheckoutActionReasonOK,
	}) {
		t.Fatalf("checkout cache check metric missing hit labels: %v", families["vectis_checkout_action_cache_checks_total"])
	}

	if !metricFamilyHasLabels(families["vectis_checkout_action_cache_checks_total"], map[string]string{
		"outcome": CheckoutActionCacheOutcomeMiss,
		"reason":  CheckoutActionReasonNoCache,
	}) {
		t.Fatalf("checkout cache check metric missing miss labels: %v", families["vectis_checkout_action_cache_checks_total"])
	}
}
