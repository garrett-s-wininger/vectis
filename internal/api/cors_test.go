package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func TestCORSMiddlewareDefaultClosed(t *testing.T) {
	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	metrics := &fakeSecurityRejectionMetrics{}
	s.SetAPISecurityMetrics(metrics)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	req.Host = "localhost"
	req.Header.Set("Origin", "https://ui.example")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("actual cross-origin code = %d, want %d; body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
	}

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want empty", got)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodOptions, "/api/v1/jobs", nil)
	req.Host = "localhost"
	req.Header.Set("Origin", "https://ui.example")
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("preflight code = %d, want %d", rec.Code, http.StatusForbidden)
	}

	requireSecurityRejection(t, metrics, securityReasonCORSOriginForbidden, securityRejectionUnknownRoute, http.StatusForbidden)
	requireSecurityRejection(t, metrics, securityReasonCORSPreflightForbidden, securityRejectionUnknownRoute, http.StatusForbidden)
}

func TestCORSMiddlewareAllowsSameOriginWithoutConfiguredCORS(t *testing.T) {
	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	req.Host = "localhost"
	req.Header.Set("Origin", "http://localhost")
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("same-origin code = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want empty for same-origin", got)
	}
}

func TestCORSMiddlewareAllowsConfiguredOrigin(t *testing.T) {
	t.Setenv("VECTIS_API_CORS_ALLOWED_ORIGINS", "https://ui.example")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	req.Host = "localhost"
	req.Header.Set("Origin", "https://ui.example")
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("configured origin code = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	assertCORSAllowed(t, rec, "https://ui.example")
}

func TestCORSMiddlewarePreflightAllowsConfiguredOriginAndHeaders(t *testing.T) {
	t.Setenv("VECTIS_API_CORS_ALLOWED_ORIGINS", "https://ui.example")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/api/v1/jobs", nil)
	req.Host = "localhost"
	req.Header.Set("Origin", "https://ui.example")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	req.Header.Set("Access-Control-Request-Headers", "Authorization, X-CSRF-Token, Content-Type")
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("preflight code = %d, want %d; body=%s", rec.Code, http.StatusNoContent, rec.Body.String())
	}

	assertCORSAllowed(t, rec, "https://ui.example")
	if got := rec.Header().Get("Access-Control-Allow-Methods"); got != corsAllowMethods {
		t.Fatalf("Access-Control-Allow-Methods = %q, want %q", got, corsAllowMethods)
	}
	if got := rec.Header().Get("Access-Control-Allow-Headers"); got != "Authorization, X-CSRF-Token, Content-Type" {
		t.Fatalf("Access-Control-Allow-Headers = %q", got)
	}
	if got := rec.Header().Get("Access-Control-Max-Age"); got != corsMaxAge {
		t.Fatalf("Access-Control-Max-Age = %q, want %q", got, corsMaxAge)
	}
}

func TestCORSMiddlewarePreflightRejectsUnlistedHeader(t *testing.T) {
	t.Setenv("VECTIS_API_CORS_ALLOWED_ORIGINS", "https://ui.example")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	metrics := &fakeSecurityRejectionMetrics{}
	s.SetAPISecurityMetrics(metrics)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/api/v1/jobs", nil)
	req.Host = "localhost"
	req.Header.Set("Origin", "https://ui.example")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	req.Header.Set("Access-Control-Request-Headers", "X-Not-Allowed")
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("preflight code = %d, want %d", rec.Code, http.StatusForbidden)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want empty on rejected preflight", got)
	}

	requireSecurityRejection(t, metrics, securityReasonCORSPreflightForbidden, securityRejectionUnknownRoute, http.StatusForbidden)
}

func TestCORSAllowedHeaders(t *testing.T) {
	got, ok := corsAllowedHeaders("authorization, authorization, x-request-id")
	if !ok {
		t.Fatal("expected headers to be allowed")
	}
	if got != "Authorization, X-Request-ID" {
		t.Fatalf("headers = %q", got)
	}

	if _, ok := corsAllowedHeaders("x-evil"); ok {
		t.Fatal("expected x-evil to be rejected")
	}
}

func assertCORSAllowed(t *testing.T, rec *httptest.ResponseRecorder, origin string) {
	t.Helper()

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != origin {
		t.Fatalf("Access-Control-Allow-Origin = %q, want %q", got, origin)
	}
	if got := rec.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Fatalf("Access-Control-Allow-Credentials = %q, want true", got)
	}
	for _, want := range []string{"Origin", "Access-Control-Request-Method", "Access-Control-Request-Headers"} {
		if !varyContains(rec.Header().Get("Vary"), want) {
			t.Fatalf("Vary = %q, missing %q", rec.Header().Get("Vary"), want)
		}
	}
}

func varyContains(vary, want string) bool {
	for part := range strings.SplitSeq(vary, ",") {
		if strings.EqualFold(strings.TrimSpace(part), want) {
			return true
		}
	}

	return false
}
