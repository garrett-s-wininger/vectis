package main

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"vectis/internal/interfaces"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func testLogger() interfaces.Logger {
	return interfaces.NewLogger("ui-test").WithOutput(&bytes.Buffer{})
}

func writeUIIndex(t *testing.T, dir, body string) {
	t.Helper()

	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func requestUIPath(t *testing.T, handler http.Handler, target string) string {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, target, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	return rec.Body.String()
}

func TestUIHandlerUsesConfiguredDirectory(t *testing.T) {
	dir := t.TempDir()
	writeUIIndex(t, dir, "configured ui")

	handler, source := uiHandlerWithFS(dir, testLogger(), fstest.MapFS{})
	if source != "serving "+dir {
		t.Fatalf("source = %q, want configured dir", source)
	}

	if body := requestUIPath(t, handler, "/"); !strings.Contains(body, "configured ui") {
		t.Fatalf("body did not come from configured dir: %q", body)
	}
}

func TestUIHandlerFallsBackToEnvDirectory(t *testing.T) {
	configuredDir := t.TempDir()
	envDir := t.TempDir()
	writeUIIndex(t, envDir, "env ui")
	t.Setenv("VECTIS_UI_DIR", envDir)

	handler, source := uiHandlerWithFS(configuredDir, testLogger(), fstest.MapFS{})
	if source != "serving "+envDir {
		t.Fatalf("source = %q, want env dir", source)
	}

	if body := requestUIPath(t, handler, "/"); !strings.Contains(body, "env ui") {
		t.Fatalf("body did not come from env dir: %q", body)
	}
}

func TestUIHandlerUsesEmbeddedUI(t *testing.T) {
	handler, source := uiHandlerWithFS("", testLogger(), fstest.MapFS{
		"embedded/index.html": &fstest.MapFile{Data: []byte("embedded ui")},
	})
	if source != "serving embedded UI" {
		t.Fatalf("source = %q, want embedded UI", source)
	}

	if body := requestUIPath(t, handler, "/"); !strings.Contains(body, "embedded ui") {
		t.Fatalf("body did not come from embedded UI: %q", body)
	}
}

func TestUIHandlerFallsBackToIndexForClientRoutes(t *testing.T) {
	handler, _ := uiHandlerWithFS("", testLogger(), fstest.MapFS{
		"embedded/index.html": &fstest.MapFile{Data: []byte("embedded ui")},
	})

	if body := requestUIPath(t, handler, "/settings/users"); !strings.Contains(body, "embedded ui") {
		t.Fatalf("SPA fallback did not return index: %q", body)
	}
}

func TestUIHandlerServesStaticAsset(t *testing.T) {
	handler, _ := uiHandlerWithFS("", testLogger(), fstest.MapFS{
		"embedded/index.html":        &fstest.MapFile{Data: []byte("embedded ui")},
		"embedded/assets/app.js":     &fstest.MapFile{Data: []byte("console.log('ok')")},
		"embedded/assets/app.css":    &fstest.MapFile{Data: []byte("body{}")},
		"embedded/assets/nested.svg": &fstest.MapFile{Data: []byte("<svg></svg>")},
	})

	if body := requestUIPath(t, handler, "/assets/app.js"); !strings.Contains(body, "console.log") {
		t.Fatalf("static asset was not served: %q", body)
	}
}

func TestUIHandlerPlaceholderWhenNoUIAvailable(t *testing.T) {
	handler, source := uiHandlerWithFS("", testLogger(), fstest.MapFS{})
	if source != "embedded UI not available" {
		t.Fatalf("source = %q, want placeholder", source)
	}

	body := requestUIPath(t, handler, "/")
	for _, want := range []string{
		"The UI server is running",
		"does not include an embedded UI build",
		"VECTIS_UI_DIR",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("placeholder body missing %q: %q", want, body)
		}
	}
}

func TestHasUIIndexFSRejectsMissingIndex(t *testing.T) {
	if hasUIIndexFS(fstest.MapFS{}) {
		t.Fatal("hasUIIndexFS returned true for an empty filesystem")
	}

	if hasUIIndexFS(fstest.MapFS{"index.html": &fstest.MapFile{Mode: fs.ModeDir}}) {
		t.Fatal("hasUIIndexFS returned true when index.html is a directory")
	}
}

func TestAPIProxyHandlerRejectsInvalidURL(t *testing.T) {
	if _, err := newUIBackend("localhost:8080"); err == nil {
		t.Fatal("newUIBackend accepted URL without scheme and host")
	}
}

func TestBFFLoginCreatesHTTPSessionAndProxiesBearerToken(t *testing.T) {
	var sawBearer bool
	backend, err := newUIBackend("http://api.test")
	if err != nil {
		t.Fatal(err)
	}
	backend.httpClient.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/api/v1/login":
			return jsonResponse(http.StatusOK, `{"token":"api-token","user_id":7,"expires_at":"2030-01-01T00:00:00Z"}`), nil
		case "/api/v1/jobs":
			if r.Header.Get("Authorization") == "Bearer api-token" {
				sawBearer = true
			}
			return jsonResponse(http.StatusOK, `[]`), nil
		default:
			return jsonResponse(http.StatusNotFound, `{"code":"not_found","message":"not found"}`), nil
		}
	})

	loginBody := strings.NewReader(`{"username":"root","password":"longenough"}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/ui/api/login", loginBody)
	backend.login(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("login code=%d body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "api-token") {
		t.Fatalf("login response leaked API token: %s", rec.Body.String())
	}

	sessionCookie := findCookie(rec.Result().Cookies(), uiSessionCookieName)
	if sessionCookie == nil {
		t.Fatal("login did not set UI session cookie")
	}
	assertUISessionCookie(t, sessionCookie)

	proxyRec := httptest.NewRecorder()
	proxyReq := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	proxyReq.AddCookie(sessionCookie)
	backend.apiProxyHandler().ServeHTTP(proxyRec, proxyReq)
	if proxyRec.Code != http.StatusOK {
		t.Fatalf("proxy code=%d body=%s", proxyRec.Code, proxyRec.Body.String())
	}
	if !sawBearer {
		t.Fatal("proxy did not inject bearer token from UI session")
	}
}

func TestBFFSetupCreatesSessionWithoutLeakingAPIToken(t *testing.T) {
	backend, err := newUIBackend("http://api.test")
	if err != nil {
		t.Fatal(err)
	}
	backend.httpClient.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/api/v1/setup/complete" {
			return jsonResponse(http.StatusNotFound, `{"code":"not_found","message":"not found"}`), nil
		}

		return jsonResponse(http.StatusOK, `{"api_token":"setup-api-token","username":"root"}`), nil
	})

	body := strings.NewReader(`{"bootstrap_token":"bootstrap","admin_username":"root","admin_password":"longenough"}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/ui/api/setup/complete", body)
	backend.completeSetup(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("setup code=%d body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "setup-api-token") {
		t.Fatalf("setup response leaked API token: %s", rec.Body.String())
	}
	if findCookie(rec.Result().Cookies(), uiSessionCookieName) == nil {
		t.Fatal("setup did not set UI session cookie")
	}
}

func TestBFFLogoutClearsSession(t *testing.T) {
	backend, err := newUIBackend("http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}

	sessionID, err := backend.sessions.create(uiSession{
		APIToken:  "api-token",
		Username:  "root",
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/ui/api/logout", nil)
	req.AddCookie(&http.Cookie{Name: uiSessionCookieName, Value: sessionID})
	backend.logout(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("logout code=%d body=%s", rec.Code, rec.Body.String())
	}

	if _, ok := backend.sessions.get(sessionID); ok {
		t.Fatal("session still exists after logout")
	}

	cookie := findCookie(rec.Result().Cookies(), uiSessionCookieName)
	if cookie == nil || cookie.MaxAge != -1 {
		t.Fatalf("logout did not clear cookie: %+v", cookie)
	}
}

func TestBFFBlocksTokenRoutesThroughBrowserProxy(t *testing.T) {
	backend, err := newUIBackend("http://api.test")
	if err != nil {
		t.Fatal(err)
	}
	backend.httpClient.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		t.Fatalf("blocked route reached API: %s", r.URL.Path)
		return nil, nil
	})

	for _, path := range []string{"/api/v1/login", "/api/v1/setup/complete", "/api/v1/tokens"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, path, nil)
		backend.apiProxyHandler().ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s code=%d want 404", path, rec.Code)
		}
	}
}

func TestSPAGateRedirectsToSetupWhenSetupIncomplete(t *testing.T) {
	backend := testBackendWithSetupStatus(t, apiSetupStatusResponse{
		AuthEnabled:   true,
		SetupComplete: false,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/runs/123?tab=logs", nil)
	backend.spaGate(textHandler("shell")).ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}

	location := rec.Header().Get("Location")
	if !strings.HasPrefix(location, "/setup?") || !strings.Contains(location, "next=%2Fruns%2F123%3Ftab%3Dlogs") {
		t.Fatalf("Location = %q, want setup redirect with next", location)
	}
}

func TestSPAGateServesSetupRouteWhenSetupIncomplete(t *testing.T) {
	backend := testBackendWithSetupStatus(t, apiSetupStatusResponse{
		AuthEnabled:   true,
		SetupComplete: false,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/setup", nil)
	backend.spaGate(textHandler("shell")).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "shell" {
		t.Fatalf("code=%d body=%q, want shell", rec.Code, rec.Body.String())
	}
}

func TestSPAGateRedirectsToLoginWhenSetupCompleteWithoutSession(t *testing.T) {
	backend := testBackendWithSetupStatus(t, apiSetupStatusResponse{
		AuthEnabled:   true,
		SetupComplete: true,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/runs/123", nil)
	backend.spaGate(textHandler("shell")).ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}

	location := rec.Header().Get("Location")
	if !strings.HasPrefix(location, "/login?") || !strings.Contains(location, "next=%2Fruns%2F123") {
		t.Fatalf("Location = %q, want login redirect with next", location)
	}
}

func TestSPAGateServesLoginRouteWithoutSession(t *testing.T) {
	backend := testBackendWithSetupStatus(t, apiSetupStatusResponse{
		AuthEnabled:   true,
		SetupComplete: true,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/login", nil)
	backend.spaGate(textHandler("shell")).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "shell" {
		t.Fatalf("code=%d body=%q, want shell", rec.Code, rec.Body.String())
	}
}

func TestSPAGateServesRequestedRouteWithSession(t *testing.T) {
	backend := testBackendWithSetupStatus(t, apiSetupStatusResponse{
		AuthEnabled:   true,
		SetupComplete: true,
	})

	sessionCookie := createTestSessionCookie(t, backend)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/runs/123", nil)
	req.AddCookie(sessionCookie)
	backend.spaGate(textHandler("shell")).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "shell" {
		t.Fatalf("code=%d body=%q, want shell", rec.Code, rec.Body.String())
	}
}

func TestSPAGateRedirectsAuthedLoginToNext(t *testing.T) {
	backend := testBackendWithSetupStatus(t, apiSetupStatusResponse{
		AuthEnabled:   true,
		SetupComplete: true,
	})

	sessionCookie := createTestSessionCookie(t, backend)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/login?next=%2Fruns%2F123", nil)
	req.AddCookie(sessionCookie)
	backend.spaGate(textHandler("shell")).ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
	if location := rec.Header().Get("Location"); location != "/runs/123" {
		t.Fatalf("Location = %q, want /runs/123", location)
	}
}

func TestSPAGateServesShellWhenAuthDisabled(t *testing.T) {
	backend := testBackendWithSetupStatus(t, apiSetupStatusResponse{
		AuthEnabled:   false,
		SetupComplete: false,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/runs/123", nil)
	backend.spaGate(textHandler("shell")).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "shell" {
		t.Fatalf("code=%d body=%q, want shell", rec.Code, rec.Body.String())
	}
}

func TestSPAGateSkipsStaticAssets(t *testing.T) {
	backend, err := newUIBackend("http://api.test")
	if err != nil {
		t.Fatal(err)
	}
	backend.httpClient.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		t.Fatalf("static asset should not call API")
		return nil, nil
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/assets/app.js", nil)
	backend.spaGate(textHandler("asset")).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "asset" {
		t.Fatalf("code=%d body=%q, want asset", rec.Code, rec.Body.String())
	}
}

func testBackendWithSetupStatus(t *testing.T, status apiSetupStatusResponse) *uiBackend {
	t.Helper()

	backend, err := newUIBackend("http://api.test")
	if err != nil {
		t.Fatal(err)
	}

	backend.httpClient.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/api/v1/setup/status" {
			return jsonResponse(http.StatusNotFound, `{"code":"not_found","message":"not found"}`), nil
		}

		body := fmt.Sprintf(`{"setup_complete":%t,"auth_enabled":%t}`, status.SetupComplete, status.AuthEnabled)
		return jsonResponse(http.StatusOK, body), nil
	})

	return backend
}

func createTestSessionCookie(t *testing.T, backend *uiBackend) *http.Cookie {
	t.Helper()

	sessionID, err := backend.sessions.create(uiSession{
		APIToken:  "api-token",
		Username:  "root",
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	return &http.Cookie{Name: uiSessionCookieName, Value: sessionID}
}

func textHandler(body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	})
}

func findCookie(cookies []*http.Cookie, name string) *http.Cookie {
	for _, cookie := range cookies {
		if cookie.Name == name {
			return cookie
		}
	}

	return nil
}

func assertUISessionCookie(t *testing.T, cookie *http.Cookie) {
	t.Helper()

	if !cookie.HttpOnly {
		t.Fatal("session cookie must be HttpOnly")
	}
	if cookie.Path != "/" {
		t.Fatalf("session cookie path = %q, want /", cookie.Path)
	}
	if cookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("session cookie SameSite = %v, want Lax", cookie.SameSite)
	}
	if cookie.Value == "api-token" {
		t.Fatal("session cookie must not contain API token")
	}
}
