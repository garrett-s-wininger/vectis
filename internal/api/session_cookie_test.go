package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSetSessionCookies_usesHostPrefixedSecureCookies(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example.test/api/v1/login", nil)
	expiresAt := time.Now().UTC().Add(time.Hour)

	setSessionCookies(rec, req, "session-token", "csrf-token", expiresAt)

	cookies := rec.Result().Cookies()
	if len(cookies) != 2 {
		t.Fatalf("cookies len=%d, want 2: %+v", len(cookies), cookies)
	}

	byName := cookiesByName(cookies)
	sessionCookie, ok := byName[sessionCookieName]
	if !ok {
		t.Fatalf("missing session cookie %s: %+v", sessionCookieName, cookies)
	}

	csrfCookie, ok := byName[csrfCookieName]
	if !ok {
		t.Fatalf("missing csrf cookie %s: %+v", csrfCookieName, cookies)
	}

	assertActiveSessionCookie(t, sessionCookie, "session-token")
	assertActiveCSRFCookie(t, csrfCookie, "csrf-token")
}

func TestSetSessionCookies_neverIssuesUnprefixedCookies(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example.test/api/v1/login", nil)

	setSessionCookies(rec, req, "session-token", "csrf-token", time.Now().UTC().Add(time.Hour))

	byName := cookiesByName(rec.Result().Cookies())
	if _, ok := byName["vectis_session"]; ok {
		t.Fatalf("must not issue unprefixed session cookie: %+v", byName)
	}

	if _, ok := byName["vectis_csrf"]; ok {
		t.Fatalf("must not issue unprefixed csrf cookie: %+v", byName)
	}
}

func TestClearSessionCookies_clearsOnlyHostPrefixedNames(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/logout", nil)

	clearSessionCookies(rec, req)

	cookies := rec.Result().Cookies()
	if len(cookies) != 2 {
		t.Fatalf("cookies len=%d, want 2: %+v", len(cookies), cookies)
	}

	byName := cookiesByName(cookies)
	sessionCookie := assertClearedCookie(t, byName, sessionCookieName)
	csrfCookie := assertClearedCookie(t, byName, csrfCookieName)

	if !sessionCookie.HttpOnly {
		t.Fatalf("%s must be HttpOnly", sessionCookieName)
	}

	if csrfCookie.HttpOnly {
		t.Fatalf("%s must remain browser-readable", csrfCookieName)
	}

	if _, ok := byName["vectis_session"]; ok {
		t.Fatalf("must not clear unprefixed session cookie: %+v", byName)
	}

	if _, ok := byName["vectis_csrf"]; ok {
		t.Fatalf("must not clear unprefixed csrf cookie: %+v", byName)
	}
}

func assertActiveSessionCookie(t *testing.T, cookie http.Cookie, value string) {
	t.Helper()

	assertHostCookieBase(t, cookie, value)
	if !cookie.HttpOnly {
		t.Fatalf("%s must be HttpOnly", cookie.Name)
	}
}

func assertActiveCSRFCookie(t *testing.T, cookie http.Cookie, value string) {
	t.Helper()

	assertHostCookieBase(t, cookie, value)
	if cookie.HttpOnly {
		t.Fatalf("%s must be readable by browser clients", cookie.Name)
	}
}

func assertHostCookieBase(t *testing.T, cookie http.Cookie, value string) {
	t.Helper()

	if cookie.Value != value {
		t.Fatalf("%s value=%q, want %q", cookie.Name, cookie.Value, value)
	}

	if !cookie.Secure {
		t.Fatalf("%s must be Secure", cookie.Name)
	}

	if cookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("%s SameSite=%v, want Lax", cookie.Name, cookie.SameSite)
	}

	if cookie.Path != "/" {
		t.Fatalf("%s Path=%q, want /", cookie.Name, cookie.Path)
	}

	if cookie.Domain != "" {
		t.Fatalf("%s Domain=%q, want host-only cookie", cookie.Name, cookie.Domain)
	}
}

func assertClearedCookie(t *testing.T, byName map[string]http.Cookie, name string) http.Cookie {
	t.Helper()

	cookie, ok := byName[name]
	if !ok {
		t.Fatalf("missing cleared cookie %s in %+v", name, byName)
	}

	if cookie.MaxAge >= 0 {
		t.Fatalf("%s MaxAge=%d, want negative", name, cookie.MaxAge)
	}

	if !cookie.Secure {
		t.Fatalf("%s must be Secure", name)
	}

	if cookie.Path != "/" {
		t.Fatalf("%s Path=%q, want /", name, cookie.Path)
	}

	if cookie.Domain != "" {
		t.Fatalf("%s Domain=%q, want host-only cookie", name, cookie.Domain)
	}

	return cookie
}

func cookiesByName(cookies []*http.Cookie) map[string]http.Cookie {
	byName := make(map[string]http.Cookie, len(cookies))
	for _, c := range cookies {
		byName[c.Name] = *c
	}

	return byName
}
