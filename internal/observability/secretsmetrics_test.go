package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
)

func TestSecretsMetrics_RecordResolve(t *testing.T) {
	ctx := context.Background()
	metricsHandler, shutdown, err := InitServiceMetrics(ctx, "vectis-secrets")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	m, err := NewSecretsMetrics()
	if err != nil {
		t.Fatal(err)
	}

	m.RecordResolve(ctx, SecretsResolveOutcomeSuccess, SecretsResolveReasonOK, "encryptedfs", 5*time.Millisecond)
	m.RecordResolve(ctx, SecretsResolveOutcomeDenied, SecretsResolveReasonAuthorizationDenied, "encryptedfs", 10*time.Millisecond)

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

	for _, familyName := range []string{"vectis_secrets_resolve_requests_total", "vectis_secrets_resolve_duration_seconds"} {
		family := families[familyName]
		if family == nil {
			t.Fatalf("missing %s", familyName)
		}

		if !metricFamilyHasLabels(family, map[string]string{
			"outcome":  SecretsResolveOutcomeSuccess,
			"reason":   SecretsResolveReasonOK,
			"provider": "encryptedfs",
		}) {
			t.Fatalf("%s missing success labels: %v", familyName, family)
		}

		if !metricFamilyHasLabels(family, map[string]string{
			"outcome":  SecretsResolveOutcomeDenied,
			"reason":   SecretsResolveReasonAuthorizationDenied,
			"provider": "encryptedfs",
		}) {
			t.Fatalf("%s missing denied labels: %v", familyName, family)
		}
	}
}
