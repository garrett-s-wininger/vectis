package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/common/expfmt"
)

func TestAPISecurityMetrics_RecordSecurityRejection(t *testing.T) {
	ctx := context.Background()
	metricsHandler, shutdown, err := InitAPIMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	m, err := NewAPISecurityMetrics()
	if err != nil {
		t.Fatal(err)
	}

	m.RecordSecurityRejection(ctx, "invalid_host_header", "unknown", http.StatusBadRequest)
	m.RecordSecurityRejection(ctx, "csrf_token_required", "POST /api/v1/logout", http.StatusForbidden)

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

	family := families["vectis_api_security_rejections_total"]
	if family == nil {
		t.Fatal("missing vectis_api_security_rejections_total")
	}

	if !metricFamilyHasLabels(family, map[string]string{
		"reason": "invalid_host_header",
		"route":  "unknown",
		"status": "400",
	}) {
		t.Fatalf("metric missing invalid host labels: %v", family)
	}

	if !metricFamilyHasLabels(family, map[string]string{
		"reason": "csrf_token_required",
		"route":  "POST /api/v1/logout",
		"status": "403",
	}) {
		t.Fatalf("metric missing csrf labels: %v", family)
	}
}
