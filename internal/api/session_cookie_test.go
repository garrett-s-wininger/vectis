package api

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSetSessionCookies_secureConfig(t *testing.T) {
	t.Setenv("VECTIS_API_SESSION_COOKIE_SECURE", "true")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/login", nil)
	expiresAt := time.Now().UTC().Add(time.Hour)
	setSessionCookies(rec, req, "session-token", "csrf-token", expiresAt)

	cookies := rec.Result().Cookies()
	if len(cookies) != 2 {
		t.Fatalf("expected two cookies, got %+v", cookies)
	}

	byName := map[string]http.Cookie{}
	for _, c := range cookies {
		byName[c.Name] = *c
		if !c.Secure {
			t.Fatalf("%s cookie should be Secure when configured", c.Name)
		}

		if c.SameSite != http.SameSiteLaxMode {
			t.Fatalf("%s SameSite=%v, want Lax", c.Name, c.SameSite)
		}

		if c.Path != "/" {
			t.Fatalf("%s Path=%q, want /", c.Name, c.Path)
		}
	}

	sessionCookie, ok := byName[sessionCookieName]
	if !ok {
		t.Fatalf("missing session cookie: %+v", cookies)
	}

	csrfCookie, ok := byName[csrfCookieName]
	if !ok {
		t.Fatalf("missing csrf cookie: %+v", cookies)
	}

	if !sessionCookie.HttpOnly {
		t.Fatal("session cookie must be HttpOnly")
	}

	if csrfCookie.HttpOnly {
		t.Fatal("csrf cookie must be readable by browser clients")
	}
}

func TestSetSessionCookies_secureWhenRequestUsesTLS(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "https://example.test/api/v1/login", nil)
	req.TLS = &tls.ConnectionState{}

	setSessionCookies(rec, req, "session-token", "csrf-token", time.Now().UTC().Add(time.Hour))

	for _, c := range rec.Result().Cookies() {
		if !c.Secure {
			t.Fatalf("%s cookie should be Secure for direct TLS requests", c.Name)
		}
	}
}

func TestSetSessionCookies_internalTLSDoesNotForceSecure(t *testing.T) {
	t.Setenv("VECTIS_GRPC_TLS_INSECURE", "false")
	t.Setenv("VECTIS_METRICS_TLS_INSECURE", "false")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example.test/api/v1/login", nil)

	setSessionCookies(rec, req, "session-token", "csrf-token", time.Now().UTC().Add(time.Hour))

	for _, c := range rec.Result().Cookies() {
		if c.Secure {
			t.Fatalf("%s cookie should not infer Secure from internal gRPC or metrics TLS", c.Name)
		}
	}
}
