package api

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"

	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func TestHandlerAppliesSecurityHeaders(t *testing.T) {
	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	s.Handler().ServeHTTP(rec, req)

	assertSecurityHeader(t, rec, "X-Content-Type-Options", "nosniff")
	assertSecurityHeader(t, rec, "X-Frame-Options", "DENY")
	assertSecurityHeader(t, rec, "Referrer-Policy", "no-referrer")
	assertSecurityHeader(t, rec, "Permissions-Policy", "camera=(), geolocation=(), microphone=(), payment=(), usb=()")
	assertSecurityHeader(t, rec, "Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
	if got := rec.Header().Get("Strict-Transport-Security"); got != "" {
		t.Fatalf("Strict-Transport-Security over HTTP = %q, want empty", got)
	}
}

func TestHandlerAppliesHSTSForDirectTLS(t *testing.T) {
	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "https://example.test/health/live", nil)
	req.TLS = &tls.ConnectionState{}
	s.Handler().ServeHTTP(rec, req)

	assertSecurityHeader(t, rec, "Strict-Transport-Security", "max-age=31536000")
}

func assertNoStore(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()

	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control=%q, want no-store", got)
	}

	if got := rec.Header().Get("Pragma"); got != "no-cache" {
		t.Fatalf("Pragma=%q, want no-cache", got)
	}

	if got := rec.Header().Get("Expires"); got != "0" {
		t.Fatalf("Expires=%q, want 0", got)
	}
}

func assertSecurityHeader(t *testing.T, rec *httptest.ResponseRecorder, key, want string) {
	t.Helper()

	if got := rec.Header().Get(key); got != want {
		t.Fatalf("%s=%q, want %q", key, got, want)
	}
}
