package api

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
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
	assertSecurityHeader(t, rec, "Cross-Origin-Opener-Policy", "same-origin")
	assertSecurityHeader(t, rec, "Cross-Origin-Resource-Policy", "same-origin")
	assertSecurityHeader(t, rec, "Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")

	if got := rec.Header().Get("Strict-Transport-Security"); got != "" {
		t.Fatalf("Strict-Transport-Security over HTTP = %q, want empty", got)
	}

	assertNoStoreAbsent(t, rec)
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

func TestProtectedRoutesDefaultNoStore(t *testing.T) {
	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/version", nil)
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}

	assertNoStore(t, rec)
}

func TestSensitiveAuthRoutesSetNoStoreBeforeHandlers(t *testing.T) {
	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	h := s.Handler()

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		contentTyp string
		wantStatus int
	}{
		{
			name:       "setup status",
			method:     http.MethodGet,
			path:       "/api/v1/setup/status",
			wantStatus: http.StatusOK,
		},
		{
			name:       "setup complete media type rejection",
			method:     http.MethodPost,
			path:       "/api/v1/setup/complete",
			body:       "{}",
			contentTyp: "text/plain",
			wantStatus: http.StatusUnsupportedMediaType,
		},
		{
			name:       "login media type rejection",
			method:     http.MethodPost,
			path:       "/api/v1/login",
			body:       "{}",
			contentTyp: "text/plain",
			wantStatus: http.StatusUnsupportedMediaType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			if tt.contentTyp != "" {
				req.Header.Set("Content-Type", tt.contentTyp)
			}

			h.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status=%d, want %d; body=%s", rec.Code, tt.wantStatus, rec.Body.String())
			}

			assertNoStore(t, rec)
		})
	}
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

func assertNoStoreAbsent(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()

	for _, key := range []string{"Cache-Control", "Pragma", "Expires"} {
		if got := rec.Header().Get(key); got != "" {
			t.Fatalf("%s=%q, want empty", key, got)
		}
	}
}

func assertSecurityHeader(t *testing.T, rec *httptest.ResponseRecorder, key, want string) {
	t.Helper()

	if got := rec.Header().Get(key); got != want {
		t.Fatalf("%s=%q, want %q", key, got, want)
	}
}
