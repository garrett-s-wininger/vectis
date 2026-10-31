package main

import (
	"crypto/tls"
	"github.com/spf13/viper"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestLocalHTTPConfigurationRequiresLoopback(t *testing.T) {
	for _, host := range []string{"0.0.0.0", "::", "public.example", "192.0.2.1", ""} {
		b, err := newUIBackend("http://localhost:8080")
		if err != nil {
			t.Fatal(err)
		}

		if err := b.configureLocalHTTP(host, true); err == nil {
			t.Fatalf("nonlocal exception accepted: %q", host)
		}
	}

	for _, host := range []string{"localhost", "127.0.0.1", "::1"} {
		b, err := newUIBackend("http://localhost:8080")
		if err != nil {
			t.Fatal(err)
		}

		if err := b.configureLocalHTTP(host, true); err != nil {
			t.Fatalf("local exception rejected: %s %v", host, err)
		}
	}

	b, err := newUIBackend("http://localhost:8080")
	if err != nil {
		t.Fatal(err)
	}

	if err := b.configureTransport("https://public.example", ""); err != nil {
		t.Fatal(err)
	}

	if err := b.configureLocalHTTP("localhost", true); err == nil {
		t.Fatal("HTTP exception accepted for a public TLS proxy")
	}
}

func TestLocalHTTPRejectsNonlocalRequestsAndCookieFallbacks(t *testing.T) {
	b, err := newUIBackend("http://localhost:8080")
	if err != nil {
		t.Fatal(err)
	}

	if err := b.configureLocalHTTP("localhost", true); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct{ host, peer string }{
		{"public.example", "127.0.0.1:1234"},
		{"localhost:8089", "192.0.2.1:1234"},
	} {
		r := httptest.NewRequest(http.MethodPost, "http://"+tc.host+"/ui/api/login", strings.NewReader(`{}`))
		r.RemoteAddr = tc.peer
		r.Header.Set("Origin", "http://"+tc.host)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Forwarded-For", "127.0.0.1")
		r.Header.Set("X-Forwarded-Host", "localhost")
		w := httptest.NewRecorder()
		if b.browserRequestAllowed(w, r, true) || w.Code != http.StatusForbidden {
			t.Fatal("nonlocal HTTP request accepted")
		}

		cookie := b.sessionCookie(r)
		if !cookie.Secure || cookie.Name != uiSessionCookieName {
			t.Fatal("nonlocal request selected insecure cookies")
		}
	}

	r := httptest.NewRequest(http.MethodGet, "http://localhost:8089/api/v1/namespaces", nil)
	r.RemoteAddr = "127.0.0.1:1234"
	id, err := b.sessions.create(uiSession{APIToken: "synthetic", ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}

	r.AddCookie(&http.Cookie{Name: uiLocalSessionCookieName, Value: id})
	if _, ok := b.sessionFromRequest(r); !ok {
		t.Fatal("local cookie was not accepted in explicit HTTP mode")
	}

	r.TLS = &tls.ConnectionState{}
	if _, ok := b.sessionFromRequest(r); ok {
		t.Fatal("TLS accepted an insecure fallback cookie")
	}

	r.TLS = nil
	if err := b.configureLocalHTTP("localhost", false); err != nil {
		t.Fatal(err)
	}

	if _, ok := b.sessionFromRequest(r); ok {
		t.Fatal("default mode accepted an insecure fallback cookie")
	}
}

func TestLocalHTTPExceptionDefaultsOffAndBindsExplicitEnvironment(t *testing.T) {
	flag := rootCmd.PersistentFlags().Lookup("session-allow-insecure-cookies")
	if flag == nil || flag.DefValue != "false" {
		t.Fatal("HTTP cookie exception is not opt-in")
	}

	t.Setenv("VECTIS_UI_SESSION_ALLOW_INSECURE_COOKIES", "true")
	if !viper.GetBool("session_allow_insecure_cookies") {
		t.Fatal("local supervisor environment does not enable HTTP sessions")
	}
}

func TestNetworkHTTPDemoRequiresOptInAndFixedOrigin(t *testing.T) {
	b, err := newUIBackend("http://localhost:8080")
	if err != nil {
		t.Fatal(err)
	}

	if err := b.configureTransport("http://dev-vm.test:8089", ""); err == nil {
		t.Fatal("network HTTP enabled without opt-in")
	}

	b.allowNetworkHTTP = true
	if err := b.configureLocalHTTP("0.0.0.0", false); err == nil {
		t.Fatal("network HTTP enabled without an explicit origin")
	}

	if err := b.configureTransport("https://dev-vm.test:8089", ""); err != nil {
		t.Fatal(err)
	}

	if err := b.configureLocalHTTP("0.0.0.0", false); err == nil {
		t.Fatal("HTTP demo configured for a TLS public origin")
	}

	if err := b.configureTransport("http://dev-vm.test:8089", ""); err != nil {
		t.Fatal(err)
	}

	if err := b.configureLocalHTTP("0.0.0.0", false); err != nil {
		t.Fatal(err)
	}

	r := httptest.NewRequest(http.MethodPost, "http://dev-vm.test:8089/ui/api/login", strings.NewReader(`{}`))
	r.RemoteAddr = "192.0.2.10:1234"
	r.Header.Set("Origin", "http://dev-vm.test:8089")
	r.Header.Set("Content-Type", "application/json")
	if !b.browserRequestAllowed(httptest.NewRecorder(), r, true) {
		t.Fatal("explicit network demo rejected a remote browser")
	}

	cookie := b.sessionCookie(r)
	if cookie.Secure || !cookie.HttpOnly || cookie.Name != uiLocalSessionCookieName {
		t.Fatal("HTTP demo cookie unusable")
	}

	r.Header.Set("Origin", "http://another-host.test:8089")
	if b.browserRequestAllowed(httptest.NewRecorder(), r, true) {
		t.Fatal("network demo disabled origin checks")
	}

	r.Header.Set("Origin", "http://dev-vm.test:8089")
	r.Host = "another-host.test:8089"
	if b.browserRequestAllowed(httptest.NewRecorder(), r, true) {
		t.Fatal("network demo accepted a different Host")
	}

	r.Host = "dev-vm.test:8089"
	r.TLS = &tls.ConnectionState{}
	if !b.sessionCookie(r).Secure {
		t.Fatal("network demo downgraded a TLS cookie")
	}
}
