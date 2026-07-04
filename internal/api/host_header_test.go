package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/spf13/viper"
)

func TestHostHeaderMiddlewareRejectsUntrustedHost(t *testing.T) {
	t.Setenv("VECTIS_API_ALLOWED_HOSTS", "api.example")

	handler := hostHeaderMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	req.Host = "evil.example"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusBadRequest)
	}

	var body apiError
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error: %v; body=%s", err, rec.Body.String())
	}

	if body.Code != string(apiErrInvalidHostHeader) {
		t.Fatalf("code=%q, want %q", body.Code, apiErrInvalidHostHeader)
	}
}

func TestHostHeaderMiddlewareAllowsTrustedHost(t *testing.T) {
	t.Setenv("VECTIS_API_ALLOWED_HOSTS", "api.example")

	handler := hostHeaderMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	req.Host = "api.example:8443"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusNoContent)
	}
}

func TestAPIServerHandlerRejectsUntrustedHostWithSecurityHeaders(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_API_ALLOWED_HOSTS", "api.example")

	s := &APIServer{}
	metrics := &fakeSecurityRejectionMetrics{}
	s.SetAPISecurityMetrics(metrics)
	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	req.Host = "evil.example"
	rec := httptest.NewRecorder()

	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusBadRequest)
	}

	assertSecurityHeader(t, rec, "X-Content-Type-Options", "nosniff")
	assertSecurityHeader(t, rec, "X-Frame-Options", "DENY")
	assertSecurityHeader(t, rec, "Referrer-Policy", "no-referrer")

	var body apiError
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error: %v; body=%s", err, rec.Body.String())
	}

	if body.Code != string(apiErrInvalidHostHeader) {
		t.Fatalf("code=%q, want %q", body.Code, apiErrInvalidHostHeader)
	}

	requireSecurityRejection(t, metrics, securityReasonInvalidHostHeader, securityRejectionUnknownRoute, http.StatusBadRequest)
}
