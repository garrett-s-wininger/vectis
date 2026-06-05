package cli

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"

	"vectis/internal/httpsecurity"
)

func TestNewMetricsHTTPServerSetsMaxHeaderBytes(t *testing.T) {
	srv := newMetricsHTTPServer("127.0.0.1:0", http.NotFoundHandler())
	if srv.MaxHeaderBytes != httpsecurity.DefaultMaxHeaderBytes {
		t.Fatalf("MaxHeaderBytes = %d, want %d", srv.MaxHeaderBytes, httpsecurity.DefaultMaxHeaderBytes)
	}
}

func TestMetricsServerHandlerServesMetricsWithSecurityHeaders(t *testing.T) {
	handler := metricsServerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte("vectis_test_metric 1\n"))
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	if got := rec.Body.String(); got != "vectis_test_metric 1\n" {
		t.Fatalf("body = %q, want metrics body", got)
	}

	assertMetricsNoStore(t, rec)
	assertMetricsHeader(t, rec, "X-Content-Type-Options", "nosniff")
	assertMetricsHeader(t, rec, "X-Frame-Options", "DENY")
	assertMetricsHeader(t, rec, "Cross-Origin-Opener-Policy", "same-origin")
	assertMetricsHeader(t, rec, "Cross-Origin-Resource-Policy", "same-origin")
	assertMetricsHeader(t, rec, "Cross-Origin-Embedder-Policy", "require-corp")
	assertMetricsHeader(t, rec, "Origin-Agent-Cluster", "?1")
	assertMetricsHeader(t, rec, "X-Permitted-Cross-Domain-Policies", "none")
	assertMetricsHeader(t, rec, "X-Download-Options", "noopen")
	assertMetricsHeader(t, rec, "Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
}

func TestMetricsServerHandlerAllowsHEAD(t *testing.T) {
	called := false
	handler := metricsServerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodHead, "/metrics", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	if !called {
		t.Fatal("metrics handler was not called")
	}

	assertMetricsNoStore(t, rec)
}

func TestMetricsServerHandlerRejectsUnknownRoute(t *testing.T) {
	handler := metricsServerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("metrics handler should not be called")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/not-metrics", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusNotFound, rec.Body.String())
	}

	assertMetricsNoStore(t, rec)
	assertMetricsHeader(t, rec, "X-Content-Type-Options", "nosniff")
}

func TestMetricsServerHandlerRejectsUnsafeRequestTargets(t *testing.T) {
	handler := metricsServerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("metrics handler should not be called")
	}))

	tests := []struct {
		name   string
		target string
	}{
		{name: "absolute form", target: "http://example.test/metrics"},
		{name: "encoded path", target: "/%6detrics"},
		{name: "encoded slash", target: "/metrics%2Fextra"},
		{name: "dot segment", target: "/metrics/.."},
		{name: "duplicate slash", target: "//metrics"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tt.target, nil)
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}

			assertMetricsNoStore(t, rec)
			assertMetricsHeader(t, rec, "X-Content-Type-Options", "nosniff")
		})
	}
}

func TestMetricsServerHandlerRejectsNonReadMethods(t *testing.T) {
	handler := metricsServerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("metrics handler should not be called")
	}))

	for _, method := range []string{http.MethodPost, http.MethodTrace, http.MethodConnect, "TRACK"} {
		t.Run(method, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(method, "/metrics", nil)
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusMethodNotAllowed, rec.Body.String())
			}

			if got := rec.Header().Get("Allow"); got != "GET, HEAD" {
				t.Fatalf("Allow = %q, want GET, HEAD", got)
			}

			assertMetricsNoStore(t, rec)
			assertMetricsHeader(t, rec, "X-Content-Type-Options", "nosniff")
		})
	}
}

func TestMetricsServerHandlerRejectsMethodOverrideHeaders(t *testing.T) {
	for _, header := range []string{"X-HTTP-Method", "X-HTTP-Method-Override", "X-Method-Override"} {
		t.Run(header, func(t *testing.T) {
			handler := metricsServerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Fatal("metrics handler should not be called")
			}))

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			req.Header.Set(header, http.MethodDelete)
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}

			assertMetricsNoStore(t, rec)
			assertMetricsHeader(t, rec, "X-Content-Type-Options", "nosniff")
		})
	}
}

func TestMetricsServerHandlerRejectsUnacceptableMediaType(t *testing.T) {
	handler := metricsServerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("metrics handler should not be called")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept", "text/html")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotAcceptable {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusNotAcceptable, rec.Body.String())
	}

	assertMetricsNoStore(t, rec)
	assertMetricsHeader(t, rec, "X-Content-Type-Options", "nosniff")
}

func TestMetricsServerHandlerAllowsPrometheusAcceptHeaders(t *testing.T) {
	for _, accept := range []string{
		"",
		"text/plain; version=0.0.4",
		"application/openmetrics-text; version=1.0.0",
		"application/openmetrics-text; version=1.0.0;q=0.75, text/plain; version=0.0.4;q=0.5, */*;q=0.1",
	} {
		t.Run(accept, func(t *testing.T) {
			called := false
			handler := metricsServerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				w.WriteHeader(http.StatusOK)
			}))

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			req.Header.Set("Accept", accept)
			handler.ServeHTTP(rec, req)

			if !called {
				t.Fatal("metrics handler was not called")
			}

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
			}
		})
	}
}

func TestMetricsServerHandlerAppliesHSTSForDirectTLS(t *testing.T) {
	handler := metricsServerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.TLS = &tls.ConnectionState{}
	handler.ServeHTTP(rec, req)

	assertMetricsHeader(t, rec, "Strict-Transport-Security", "max-age=31536000")
}

func assertMetricsNoStore(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()

	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}

	if got := rec.Header().Get("Pragma"); got != "no-cache" {
		t.Fatalf("Pragma = %q, want no-cache", got)
	}

	if got := rec.Header().Get("Expires"); got != "0" {
		t.Fatalf("Expires = %q, want 0", got)
	}
}

func assertMetricsHeader(t *testing.T, rec *httptest.ResponseRecorder, key, want string) {
	t.Helper()

	if got := rec.Header().Get(key); got != want {
		t.Fatalf("%s = %q, want %q", key, got, want)
	}
}
