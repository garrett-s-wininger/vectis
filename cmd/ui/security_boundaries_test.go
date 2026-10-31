package main

import (
	"context"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
	apiserver "vectis/internal/api"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func realAPIBackend(t *testing.T) *uiBackend {
	t.Helper()
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_SESSION_COOKIE_SECURE", "true")
	db := dbtest.NewTestDB(t)
	hash, err := bcrypt.GenerateFromPassword([]byte("audit-fixture-password"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := dal.NewSQLRepositories(db).Auth().CompleteInitialSetup(t.Context(), "fixture-user", string(hash), "synthetic-setup-token-hash", "audit"); err != nil {
		t.Fatal(err)
	}

	api := apiserver.NewAPIServer(mocks.NewMockLogger(), db)
	upstream := httptest.NewServer(api.Handler())
	t.Cleanup(upstream.Close)

	backend, err := newUIBackend(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}

	return backend
}

func TestBFFRejectsCrossSiteLoginBeforeUpstreamAuthentication(t *testing.T) {
	b := realAPIBackend(t)
	body := `{"username":"fixture-user","password":"audit-fixture-password","return_token":true,"ignored":"="}` + "\r\n"

	r := httptest.NewRequest(http.MethodPost, "http://ui.test/ui/api/login", strings.NewReader(body))
	r.Header.Set("Content-Type", "text/plain")
	r.Header.Set("Origin", "https://untrusted.invalid")
	r.Header.Set("Sec-Fetch-Site", "cross-site")
	r.Header.Set("Sec-Fetch-Mode", "navigate")
	r.Header.Set("Sec-Fetch-Dest", "document")

	w := httptest.NewRecorder()
	b.login(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("cross-site login accepted: %d %s", w.Code, w.Body.String())
	}

	if len(w.Result().Cookies()) != 0 || len(b.sessions.sessions) != 0 {
		t.Fatal("rejected login created a session")
	}
}

func TestBFFLoginRequestsRealAPIUpstreamCredentials(t *testing.T) {
	b := realAPIBackend(t)
	r := httptest.NewRequest(http.MethodPost, "http://ui.test/ui/api/login", strings.NewReader(`{"username":"fixture-user","password":"audit-fixture-password"}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Origin", "http://ui.test")
	w := httptest.NewRecorder()
	b.login(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("login=%d %s", w.Code, w.Body.String())
	}

	cookie := findCookie(w.Result().Cookies(), uiSessionCookieName)
	if cookie == nil {
		t.Fatal("missing session")
	}

	session, ok := b.sessions.get(cookie.Value)
	if !ok || session.APIToken == "" {
		t.Fatal("real API did not supply usable upstream credentials")
	}

	proxy := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "http://ui.test/api/v1/namespaces", nil)
	request.Host = b.apiURL.Host
	request.AddCookie(cookie)
	b.apiProxyHandler().ServeHTTP(proxy, request)

	if proxy.Code != http.StatusOK {
		t.Fatalf("real API rejected UI credentials: %d %s", proxy.Code, proxy.Body.String())
	}

	if strings.Contains(w.Body.String(), session.APIToken) {
		t.Fatal("upstream bearer leaked to browser")
	}

	assertUISessionCookie(t, cookie)
}

func TestUISessionCookiesStaySecureBehindTLSProxy(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "http://ui.internal/ui/api/login", nil)
	r.Header.Set("X-Forwarded-Proto", "https")
	w := httptest.NewRecorder()
	(&uiBackend{}).setUISessionCookie(w, r, "synthetic-session-id", time.Now().Add(time.Hour))
	cookie := findCookie(w.Result().Cookies(), uiSessionCookieName)

	if cookie == nil {
		t.Fatal("missing cookie")
	}

	assertUISessionCookie(t, cookie)

}

func TestUIStaticFilesRejectPrivatePaths(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "index.html"), []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(outside, "private.txt"), []byte("SYNTHETIC_OUTSIDE_DATA"), 0600); err != nil {
		t.Fatal(err)
	}

	linkErr := os.Symlink(filepath.Join(outside, "private.txt"), filepath.Join(root, "public.txt"))
	if err := os.WriteFile(filepath.Join(root, ".env"), []byte("SYNTHETIC_DOTFILE_DATA"), 0600); err != nil {
		t.Fatal(err)
	}

	b, err := newUIBackend("http://unused.invalid")
	if err != nil {
		t.Fatal(err)
	}

	assets, _ := uiHandlerWithFS(root, testLogger(), nil)
	h := b.spaGate(assets)
	for _, tc := range []struct{ path, body string }{{"/public.txt", "SYNTHETIC_OUTSIDE_DATA"}, {"/.env", "SYNTHETIC_DOTFILE_DATA"}} {
		if tc.path == "/public.txt" && linkErr != nil {
			t.Logf("symlink unavailable: %v", linkErr)
			continue
		}

		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, tc.path, nil)
		h.ServeHTTP(w, r)
		if w.Code != http.StatusNotFound || strings.Contains(w.Body.String(), tc.body) {
			t.Fatalf("private path %s status=%d body=%s", tc.path, w.Code, w.Body.String())
		}
	}
}

func TestBrowserMutationBoundary(t *testing.T) {
	for _, route := range []string{"login", "setup", "logout", "proxy"} {
		t.Run(route, func(t *testing.T) {
			b, err := newUIBackend("http://unused.invalid")
			if err != nil {
				t.Fatal(err)
			}

			b.httpClient.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
				t.Error("rejected browser request reached API")
				return jsonResponse(500, "{}"), nil
			})

			handlers := map[string]http.HandlerFunc{"login": b.login, "setup": b.completeSetup, "logout": b.logout, "proxy": b.apiProxyHandler().ServeHTTP}
			for _, tc := range []struct {
				name, origin, site, mode, contentType string
				status                                int
			}{
				{name: "missing origin", contentType: "application/json", status: 403},
				{name: "cross-origin JSON", origin: "https://untrusted.invalid", contentType: "application/json", status: 403},
				{name: "same-site sibling", origin: "http://sibling.example.com", site: "same-site", contentType: "application/json", status: 403},
				{name: "cross-site navigation", origin: "http://example.com", site: "cross-site", mode: "navigate", contentType: "text/plain", status: 403},
				{name: "text plain", origin: "http://example.com", contentType: "text/plain", status: 415},
			} {
				if tc.status == 415 && (route == "proxy" || route == "logout") {
					continue
				}

				t.Run(tc.name, func(t *testing.T) {
					r := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(`{}`))
					r.Header.Set("Origin", tc.origin)
					r.Header.Set("Sec-Fetch-Site", tc.site)
					r.Header.Set("Sec-Fetch-Mode", tc.mode)
					r.Header.Set("Content-Type", tc.contentType)
					w := httptest.NewRecorder()
					handlers[route](w, r)

					if w.Code != tc.status {
						t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
					}

					if len(w.Result().Cookies()) != 0 || len(b.sessions.sessions) != 0 {
						t.Fatal("rejected request changed session state")
					}
				})
			}
		})
	}
}

func TestExplicitProxyOriginAndForwardedHeaderSpoofing(t *testing.T) {
	b, err := newUIBackend("http://unused.invalid")
	if err != nil {
		t.Fatal(err)
	}

	request := func(origin string) *http.Request {
		r := httptest.NewRequest(http.MethodPost, "http://internal.test/ui/api/login", nil)
		r.Header.Set("Origin", origin)
		r.Header.Set("X-Forwarded-Proto", "https")
		r.Header.Set("X-Forwarded-Host", "public.example")

		return r
	}

	w := httptest.NewRecorder()
	if b.browserRequestAllowed(w, request("https://public.example"), false) {
		t.Fatal("untrusted forwarded headers selected external origin")
	}

	if err := b.configureTransport("https://public.example", ""); err != nil {
		t.Fatal(err)
	}

	if !b.browserRequestAllowed(httptest.NewRecorder(), request("https://public.example:443"), false) {
		t.Fatal("configured HTTPS proxy origin rejected")
	}

	if b.browserRequestAllowed(httptest.NewRecorder(), request("http://public.example"), false) {
		t.Fatal("HTTP downgrade accepted")
	}
}

func TestUITransportConfigurationRejectsUnsafeOrigins(t *testing.T) {
	for _, origin := range []string{"http://public.example", "https://user:pass@public.example", "https://public.example/path", "https://public.example?query=1", "https://public.example#fragment", "//public.example", "javascript:alert(1)"} {
		b, err := newUIBackend("http://localhost:8080")
		if err != nil {
			t.Fatal(err)
		}

		if err := b.configureTransport(origin, ""); err == nil {
			t.Fatalf("unsafe public origin accepted: %s", origin)
		}
	}
}

func TestHTTPSBrowserSessionAgainstRealAPI(t *testing.T)     { testBrowserSessionLifecycle(t, false) }
func TestLocalHTTPBrowserSessionAgainstRealAPI(t *testing.T) { testBrowserSessionLifecycle(t, true) }
func TestNetworkHTTPBrowserSessionAgainstRealAPI(t *testing.T) {
	testBrowserSessionLifecycle(t, true, true)
}

func testBrowserSessionLifecycle(t *testing.T, localHTTP bool, networkDemo ...bool) {
	t.Helper()
	backend := realAPIBackend(t)
	var server *httptest.Server
	networkHTTP := len(networkDemo) > 0 && networkDemo[0]
	if networkHTTP {
		backend.allowNetworkHTTP = true
		if err := backend.configureTransport("http://dev-vm.test:8089", ""); err != nil {
			t.Fatal(err)
		}
	}

	if localHTTP {
		bindHost := "localhost"
		if networkHTTP {
			bindHost = "0.0.0.0"
		}

		if err := backend.configureLocalHTTP(bindHost, true); err != nil {
			t.Fatal(err)
		}

		server = httptest.NewServer(backend.handler(http.NotFoundHandler(), false))
	} else {
		server = httptest.NewTLSServer(backend.handler(http.NotFoundHandler(), false))
	}
	defer server.Close()

	browserURL := server.URL
	client := server.Client()
	if networkHTTP {
		browserURL = "http://dev-vm.test:8089"
		endpoint, err := url.Parse(server.URL)
		if err != nil {
			t.Fatal(err)
		}

		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.Proxy = nil
		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", endpoint.Host)
		}

		client.Transport = transport
		t.Cleanup(client.CloseIdleConnections)
	}

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}

	client.Jar = jar
	send := func(method, path, body, origin string) *http.Response {
		t.Helper()
		request, err := http.NewRequestWithContext(t.Context(), method, browserURL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}

		if origin != "" {
			request.Header.Set("Origin", origin)
		}

		if body != "" {
			request.Header.Set("Content-Type", "application/json")
		}

		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}

		return response
	}

	login := send(http.MethodPost, "/ui/api/login", `{"username":"fixture-user","password":"audit-fixture-password"}`, browserURL)
	defer func() { _ = login.Body.Close() }()

	if login.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(login.Body)
		t.Fatalf("login=%d %s", login.StatusCode, body)
	}

	cookieName := uiSessionCookieName
	if localHTTP {
		cookieName = uiLocalSessionCookieName
	}

	cookie := findCookie(login.Cookies(), cookieName)
	if cookie == nil {
		t.Fatal("no browser session cookie")
	}

	if localHTTP {
		if cookie.Secure || !cookie.HttpOnly || cookie.Domain != "" || cookie.Path != "/" || cookie.SameSite != http.SameSiteLaxMode {
			t.Fatalf("invalid local session cookie: %+v", cookie)
		}

		if login.Header.Get("Strict-Transport-Security") != "" {
			t.Fatal("HTTP fallback advertised HSTS")
		}
	} else {
		assertUISessionCookie(t, cookie)
		if login.Header.Get("Strict-Transport-Security") == "" {
			t.Fatal("HTTPS session missing HSTS")
		}
	}

	if login.Header.Get("Cache-Control") != "no-store" {
		t.Fatal("session response missing cache protection")
	}

	read := send(http.MethodGet, "/api/v1/namespaces", "", "")
	defer func() { _ = read.Body.Close() }()

	if read.StatusCode != http.StatusOK {
		t.Fatalf("authenticated read=%d", read.StatusCode)
	}

	write := send(http.MethodPost, "/api/v1/namespaces", `{"name":"approved-ui-write"}`, browserURL)
	defer func() { _ = write.Body.Close() }()

	if write.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(write.Body)
		t.Fatalf("valid write=%d %s", write.StatusCode, body)
	}

	rejected := send(http.MethodPost, "/api/v1/namespaces", `{"name":"rejected-ui-write"}`, "https://untrusted.invalid")
	defer func() { _ = rejected.Body.Close() }()

	if rejected.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin write=%d", rejected.StatusCode)
	}

	logout := send(http.MethodPost, "/ui/api/logout", "", browserURL)
	defer func() { _ = logout.Body.Close() }()

	if logout.StatusCode != http.StatusNoContent || logout.Header.Get("Clear-Site-Data") == "" {
		t.Fatalf("logout=%d headers=%v", logout.StatusCode, logout.Header)
	}

	uiURL, err := url.Parse(browserURL)
	if err != nil {
		t.Fatal(err)
	}

	if len(jar.Cookies(uiURL)) != 0 {
		t.Fatal("logout did not clear browser cookie")
	}

	after := send(http.MethodGet, "/api/v1/namespaces", "", "")
	defer func() { _ = after.Body.Close() }()

	if after.StatusCode != http.StatusUnauthorized {
		t.Fatalf("logged-out proxy still authenticated: %d", after.StatusCode)
	}
}

func TestUIBackendRequiresTrustedUpstreamTLS(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer server.Close()

	backend, err := newUIBackend(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	probe, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	response, err := backend.httpClient.Do(probe)
	if response != nil {
		_ = response.Body.Close()
	}

	if err == nil {
		t.Fatal("untrusted API certificate accepted")
	}

	bundle := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	caFile := filepath.Join(t.TempDir(), "api-ca.pem")
	if err := os.WriteFile(caFile, bundle, 0600); err != nil {
		t.Fatal(err)
	}

	if err := backend.configureTransport("", caFile); err != nil {
		t.Fatal(err)
	}

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	response, err = backend.httpClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("trusted upstream=%d", response.StatusCode)
	}
}

func TestLocalHTTPSetupCreatesUsableBrowserSession(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "local-http-fixture-bootstrap")
	api := apiserver.NewAPIServer(mocks.NewMockLogger(), dbtest.NewTestDB(t))
	upstream := httptest.NewServer(api.Handler())
	defer upstream.Close()

	backend, err := newUIBackend(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}

	if err := backend.configureLocalHTTP("localhost", true); err != nil {
		t.Fatal(err)
	}

	ui := httptest.NewServer(backend.handler(http.NotFoundHandler(), false))
	defer ui.Close()

	client := ui.Client()
	client.Jar, err = cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}

	body := `{"bootstrap_token":"local-http-fixture-bootstrap","admin_username":"local-admin","admin_password":"local-fixture-password"}`
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ui.URL+"/ui/api/setup/complete", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}

	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", ui.URL)
	setup, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = setup.Body.Close() }()

	payload, err := io.ReadAll(setup.Body)
	if err != nil {
		t.Fatal(err)
	}

	if setup.StatusCode != http.StatusOK {
		t.Fatalf("local HTTP setup=%d %s", setup.StatusCode, payload)
	}

	cookie := findCookie(setup.Cookies(), uiLocalSessionCookieName)
	if cookie == nil || cookie.Secure || !cookie.HttpOnly {
		t.Fatal("setup did not issue a usable local session cookie")
	}

	if strings.Contains(string(payload), "api_token") {
		t.Fatal("setup exposed upstream credentials")
	}

	request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, ui.URL+"/api/v1/namespaces", nil)
	if err != nil {
		t.Fatal(err)
	}

	read, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = read.Body.Close() }()

	if read.StatusCode != http.StatusOK {
		t.Fatalf("setup session did not authorize API access: %d", read.StatusCode)
	}
}
