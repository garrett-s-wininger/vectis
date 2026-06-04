package httpsecurity

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHeaderMiddleware_appliesBaselineHeaders(t *testing.T) {
	h := HeaderMiddleware(APIHeaderPolicy(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://example.test/api/v1/jobs", nil)
	h.ServeHTTP(rec, req)

	assertHeader(t, rec, "X-Content-Type-Options", "nosniff")
	assertHeader(t, rec, "X-Frame-Options", "DENY")
	assertHeader(t, rec, "Referrer-Policy", "no-referrer")
	assertHeader(t, rec, "Permissions-Policy", "camera=(), geolocation=(), microphone=(), payment=(), usb=()")
	assertHeader(t, rec, "Cross-Origin-Opener-Policy", defaultCrossOriginOpenerPolicy)
	assertHeader(t, rec, "Cross-Origin-Resource-Policy", defaultCrossOriginResourcePolicy)
	assertHeader(t, rec, "Origin-Agent-Cluster", defaultOriginAgentCluster)
	assertHeader(t, rec, "X-Permitted-Cross-Domain-Policies", defaultCrossDomainPolicies)
	assertHeader(t, rec, "X-Download-Options", defaultDownloadOptions)
	assertHeader(t, rec, "Content-Security-Policy", apiContentSecurityPolicy)

	if got := rec.Header().Get("Strict-Transport-Security"); got != "" {
		t.Fatalf("Strict-Transport-Security on HTTP = %q, want empty", got)
	}
}

func TestDefaultMaxHeaderBytes(t *testing.T) {
	if DefaultMaxHeaderBytes != 32<<10 {
		t.Fatalf("DefaultMaxHeaderBytes = %d, want %d", DefaultMaxHeaderBytes, 32<<10)
	}
}

func TestHeaderMiddleware_setsHSTSForTLS(t *testing.T) {
	h := HeaderMiddleware(APIHeaderPolicy(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "https://example.test/api/v1/jobs", nil)
	req.TLS = &tls.ConnectionState{}
	h.ServeHTTP(rec, req)

	assertHeader(t, rec, "Strict-Transport-Security", defaultHSTS)
}

func TestHeaderMiddleware_usesPolicyRequestSecure(t *testing.T) {
	policy := APIHeaderPolicy()
	policy.RequestSecure = func(*http.Request) bool {
		return true
	}

	h := HeaderMiddleware(policy, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://example.test/api/v1/jobs", nil)
	h.ServeHTTP(rec, req)

	assertHeader(t, rec, "Strict-Transport-Security", defaultHSTS)
}

func TestHeaderMiddleware_preservesExistingHeaders(t *testing.T) {
	h := HeaderMiddleware(APIHeaderPolicy(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'self'")
		w.Header().Set("Cross-Origin-Opener-Policy", "same-origin-allow-popups")
		w.Header().Set("Cross-Origin-Resource-Policy", "same-site")
		w.Header().Set("Origin-Agent-Cluster", "?0")
		w.Header().Set("X-Permitted-Cross-Domain-Policies", "master-only")
		w.Header().Set("X-Download-Options", "open")
		w.Header().Set("Strict-Transport-Security", "max-age=60")
		w.WriteHeader(http.StatusNoContent)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "https://example.test/api/v1/jobs", nil)
	req.TLS = &tls.ConnectionState{}
	h.ServeHTTP(rec, req)

	assertHeader(t, rec, "Content-Security-Policy", "default-src 'self'")
	assertHeader(t, rec, "Cross-Origin-Opener-Policy", "same-origin-allow-popups")
	assertHeader(t, rec, "Cross-Origin-Resource-Policy", "same-site")
	assertHeader(t, rec, "Origin-Agent-Cluster", "?0")
	assertHeader(t, rec, "X-Permitted-Cross-Domain-Policies", "master-only")
	assertHeader(t, rec, "X-Download-Options", "open")
	assertHeader(t, rec, "Strict-Transport-Security", "max-age=60")
}

func TestDocsHeaderPolicy_allowsStaticDocsAssets(t *testing.T) {
	h := HeaderMiddleware(DocsHeaderPolicy(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://example.test/", nil)
	h.ServeHTTP(rec, req)

	csp := rec.Header().Get("Content-Security-Policy")
	for _, want := range []string{"default-src 'self'", "script-src 'self' 'unsafe-inline'", "style-src 'self' 'unsafe-inline'", "frame-ancestors 'none'"} {
		if !strings.Contains(csp, want) {
			t.Fatalf("Content-Security-Policy = %q, missing %q", csp, want)
		}
	}
}

func assertHeader(t *testing.T, rec *httptest.ResponseRecorder, key, want string) {
	t.Helper()

	if got := rec.Header().Get(key); got != want {
		t.Fatalf("%s = %q, want %q", key, got, want)
	}
}
