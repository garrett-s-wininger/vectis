package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func TestFetchMetadataRejectsCrossSiteWithoutOrigin(t *testing.T) {
	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	metrics := &fakeSecurityRejectionMetrics{}
	s.SetAPISecurityMetrics(metrics)

	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	req.Host = "localhost"
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	rec := httptest.NewRecorder()

	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d, want %d; body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
	}

	var body apiError
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error: %v; body=%s", err, rec.Body.String())
	}

	if body.Code != string(apiErrFetchMetadataForbidden) {
		t.Fatalf("code=%q, want %q", body.Code, apiErrFetchMetadataForbidden)
	}

	requireSecurityRejection(t, metrics, securityReasonFetchMetadataForbidden, securityRejectionUnknownRoute, http.StatusForbidden)
}

func TestFetchMetadataAllowsCrossSiteWithOriginForCORS(t *testing.T) {
	t.Setenv("VECTIS_API_CORS_ALLOWED_ORIGINS", "https://ui.example")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())

	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	req.Host = "localhost"
	req.Header.Set("Origin", "https://ui.example")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	rec := httptest.NewRecorder()

	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	assertCORSAllowed(t, rec, "https://ui.example")
}
