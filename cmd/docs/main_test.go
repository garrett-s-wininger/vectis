package main

import (
	"bytes"
	"crypto/tls"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/spf13/viper"

	"vectis/internal/httpsecurity"
	"vectis/internal/interfaces"
	"vectis/internal/localpki"
)

func testLogger() interfaces.Logger {
	return interfaces.NewLogger("docs-test").WithOutput(&bytes.Buffer{})
}

func writeDocsIndex(t *testing.T, dir, body string) {
	t.Helper()

	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func requestDocsIndex(t *testing.T, handler http.Handler) string {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	return rec.Body.String()
}

func TestDocsHandlerUsesConfiguredDirectory(t *testing.T) {
	dir := t.TempDir()
	writeDocsIndex(t, dir, "configured docs")

	handler, source := docsHandlerWithFS(dir, testLogger(), fstest.MapFS{})
	if source != "serving "+dir {
		t.Fatalf("source = %q, want configured dir", source)
	}

	if body := requestDocsIndex(t, handler); !strings.Contains(body, "configured docs") {
		t.Fatalf("body did not come from configured dir: %q", body)
	}
}

func TestDocsHandlerFallsBackToEnvDirectory(t *testing.T) {
	configuredDir := t.TempDir()
	envDir := t.TempDir()
	writeDocsIndex(t, envDir, "env docs")
	t.Setenv("VECTIS_DOCS_DIR", envDir)

	handler, source := docsHandlerWithFS(configuredDir, testLogger(), fstest.MapFS{})
	if source != "serving "+envDir {
		t.Fatalf("source = %q, want env dir", source)
	}

	if body := requestDocsIndex(t, handler); !strings.Contains(body, "env docs") {
		t.Fatalf("body did not come from env dir: %q", body)
	}
}

func TestDocsHandlerUsesEmbeddedDocs(t *testing.T) {
	handler, source := docsHandlerWithFS("", testLogger(), fstest.MapFS{
		"embedded/index.html": &fstest.MapFile{Data: []byte("embedded docs")},
	})
	if source != "serving embedded docs" {
		t.Fatalf("source = %q, want embedded docs", source)
	}

	if body := requestDocsIndex(t, handler); !strings.Contains(body, "embedded docs") {
		t.Fatalf("body did not come from embedded docs: %q", body)
	}
}

func TestDocsHandlerPlaceholderWhenNoDocsAvailable(t *testing.T) {
	handler, source := docsHandlerWithFS("", testLogger(), fstest.MapFS{})
	if source != "embedded docs not available" {
		t.Fatalf("source = %q, want placeholder", source)
	}

	body := requestDocsIndex(t, handler)
	for _, want := range []string{
		"The docs server is running",
		"does not include an embedded docs build",
		"VECTIS_DOCS_DIR",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("placeholder body missing %q: %q", want, body)
		}
	}
}

func TestDocsServerHandlerAppliesSecurityHeaders(t *testing.T) {
	handler := docsServerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("docs"))
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	handler.ServeHTTP(rec, req)

	assertDocsHeader(t, rec, "X-Content-Type-Options", "nosniff")
	assertDocsHeader(t, rec, "X-Frame-Options", "DENY")
	assertDocsHeader(t, rec, "Referrer-Policy", "no-referrer")
	if got := rec.Header().Get("Content-Security-Policy"); !strings.Contains(got, "default-src 'self'") || !strings.Contains(got, "frame-ancestors 'none'") {
		t.Fatalf("Content-Security-Policy = %q, want docs policy", got)
	}
	if got := rec.Header().Get("Strict-Transport-Security"); got != "" {
		t.Fatalf("Strict-Transport-Security over HTTP = %q, want empty", got)
	}
}

func TestDocsHTTPServerSetsMaxHeaderBytes(t *testing.T) {
	srv := docsHTTPServer("127.0.0.1:0", http.NotFoundHandler())
	if srv.MaxHeaderBytes != httpsecurity.DefaultMaxHeaderBytes {
		t.Fatalf("MaxHeaderBytes = %d, want %d", srv.MaxHeaderBytes, httpsecurity.DefaultMaxHeaderBytes)
	}
}

func TestDocsServerHandlerAppliesHSTSForDirectTLS(t *testing.T) {
	handler := docsServerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("docs"))
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "https://example.test/", nil)
	req.TLS = &tls.ConnectionState{}
	handler.ServeHTTP(rec, req)

	assertDocsHeader(t, rec, "Strict-Transport-Security", "max-age=31536000")
}

func TestHasDocsIndexFSRejectsMissingIndex(t *testing.T) {
	if hasDocsIndexFS(fstest.MapFS{}) {
		t.Fatal("hasDocsIndexFS returned true for an empty filesystem")
	}

	if hasDocsIndexFS(fstest.MapFS{"index.html": &fstest.MapFile{Mode: fs.ModeDir}}) {
		t.Fatal("hasDocsIndexFS returned true when index.html is a directory")
	}
}

func TestDocsTLSEnabledAndOptions(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if docsTLSEnabled() {
		t.Fatal("docs TLS should be disabled by default")
	}

	m, err := localpki.Ensure(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	viper.Set("tls_cert_file", m.ServerCert)
	viper.Set("tls_key_file", m.ServerKey)

	if !docsTLSEnabled() {
		t.Fatal("docs TLS should be enabled when cert/key are set")
	}

	opts := docsTLSOptions()
	if opts.ServerCert != m.ServerCert || opts.ServerKey != m.ServerKey {
		t.Fatalf("docs TLS options = %+v, want cert/key from local PKI", opts)
	}
}

func assertDocsHeader(t *testing.T, rec *httptest.ResponseRecorder, key, want string) {
	t.Helper()

	if got := rec.Header().Get(key); got != want {
		t.Fatalf("%s = %q, want %q", key, got, want)
	}
}
