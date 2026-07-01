package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/common/expfmt"
)

func TestCheckoutCacheMetrics_appearsOnScrape(t *testing.T) {
	ctx := context.Background()
	h, shutdown, err := InitServiceMetrics(ctx, "vectis-worker-core")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	metrics, err := NewCheckoutCacheMetrics()
	if err != nil {
		t.Fatal(err)
	}

	metrics.RecordCheckoutCacheStats(ctx, CheckoutCacheStats{
		Repositories: 2,
		Generations:  5,
		PackFiles:    12,
		PackBytes:    4096,
		ActiveLeases: 1,
	})

	metrics.RecordCheckoutCacheClone(ctx, CheckoutCacheCloneModeHardlink, CheckoutCacheCloneReasonOK)
	metrics.RecordCheckoutCacheClone(ctx, CheckoutCacheCloneModeCopy, CheckoutCacheCloneReasonProbe)

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
		"vectis_checkout_cache_repositories",
		"vectis_checkout_cache_generations",
		"vectis_checkout_cache_pack_files",
		"vectis_checkout_cache_pack_bytes",
		"vectis_checkout_cache_active_leases",
		"vectis_checkout_cache_clones_total",
	} {
		if families[family] == nil {
			t.Fatalf("missing metric family %q", family)
		}
	}

	if !metricFamilyHasLabels(families["vectis_checkout_cache_clones_total"], map[string]string{
		"mode":   CheckoutCacheCloneModeHardlink,
		"reason": CheckoutCacheCloneReasonOK,
	}) {
		t.Fatalf("checkout cache clone metric missing hardlink labels: %v", families["vectis_checkout_cache_clones_total"])
	}

	if !metricFamilyHasLabels(families["vectis_checkout_cache_clones_total"], map[string]string{
		"mode":   CheckoutCacheCloneModeCopy,
		"reason": CheckoutCacheCloneReasonProbe,
	}) {
		t.Fatalf("checkout cache clone metric missing copy/probe labels: %v", families["vectis_checkout_cache_clones_total"])
	}
}
