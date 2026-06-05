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

	rec := requestDocsPath(t, handler, "/")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	return rec.Body.String()
}

func requestDocsPath(t *testing.T, handler http.Handler, target string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, target, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
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

func TestDocsHandlerConfiguredDirectoryDoesNotListDirectories(t *testing.T) {
	dir := t.TempDir()
	writeDocsIndex(t, dir, "configured docs")
	if err := os.Mkdir(filepath.Join(dir, "assets"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(dir, "assets", "main.js"), []byte("console.log('secret name')"), 0o644); err != nil {
		t.Fatal(err)
	}

	handler, _ := docsHandlerWithFS(dir, testLogger(), fstest.MapFS{})
	rec := requestDocsPath(t, handler, "/assets/")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusNotFound, rec.Body.String())
	}

	if strings.Contains(rec.Body.String(), "main.js") {
		t.Fatalf("directory listing exposed file name: %q", rec.Body.String())
	}
}

func TestDocsHandlerConfiguredDirectoryServesNestedIndex(t *testing.T) {
	dir := t.TempDir()
	writeDocsIndex(t, dir, "configured docs")
	guideDir := filepath.Join(dir, "guide")
	if err := os.Mkdir(guideDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(guideDir, "index.html"), []byte("guide docs"), 0o644); err != nil {
		t.Fatal(err)
	}

	handler, _ := docsHandlerWithFS(dir, testLogger(), fstest.MapFS{})
	rec := requestDocsPath(t, handler, "/guide/")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	if !strings.Contains(rec.Body.String(), "guide docs") {
		t.Fatalf("body did not come from nested index: %q", rec.Body.String())
	}
}

func TestDocsHandlerConfiguredDirectoryHidesDotfiles(t *testing.T) {
	dir := t.TempDir()
	writeDocsIndex(t, dir, "configured docs")
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte("SECRET=value"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := os.Mkdir(filepath.Join(dir, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(dir, ".git", "config"), []byte("[remote]\nurl=secret"), 0o644); err != nil {
		t.Fatal(err)
	}

	handler, _ := docsHandlerWithFS(dir, testLogger(), fstest.MapFS{})
	for _, target := range []string{"/.env", "/.git/config"} {
		t.Run(target, func(t *testing.T) {
			rec := requestDocsPath(t, handler, target)

			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusNotFound, rec.Body.String())
			}

			if strings.Contains(rec.Body.String(), "SECRET") || strings.Contains(rec.Body.String(), "remote") {
				t.Fatalf("dotfile content leaked: %q", rec.Body.String())
			}
		})
	}
}

func TestDocsHandlerConfiguredDirectoryRejectsSymlinkEscape(t *testing.T) {
	dir := t.TempDir()
	writeDocsIndex(t, dir, "configured docs")
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("outside secret"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(dir, "secret.txt")); err != nil {
		t.Skipf("symlink not available: %v", err)
	}

	handler, _ := docsHandlerWithFS(dir, testLogger(), fstest.MapFS{})
	rec := requestDocsPath(t, handler, "/secret.txt")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusNotFound, rec.Body.String())
	}

	if strings.Contains(rec.Body.String(), "outside secret") {
		t.Fatalf("symlink target content leaked: %q", rec.Body.String())
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

func TestDocsHandlerEmbeddedDocsDoesNotListDirectories(t *testing.T) {
	handler, _ := docsHandlerWithFS("", testLogger(), fstest.MapFS{
		"embedded/index.html":     &fstest.MapFile{Data: []byte("embedded docs")},
		"embedded/assets/main.js": &fstest.MapFile{Data: []byte("console.log('asset')")},
	})

	rec := requestDocsPath(t, handler, "/assets/")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusNotFound, rec.Body.String())
	}

	if strings.Contains(rec.Body.String(), "main.js") {
		t.Fatalf("directory listing exposed embedded file name: %q", rec.Body.String())
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
		`href="` + docsPlaceholderStylesheet + `"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("placeholder body missing %q: %q", want, body)
		}
	}

	for _, blocked := range []string{"<style", "<script", " style="} {
		if strings.Contains(strings.ToLower(body), blocked) {
			t.Fatalf("placeholder body contains inline style/script marker %q: %q", blocked, body)
		}
	}
}

func TestDocsServerHandlerServesPlaceholderStylesheet(t *testing.T) {
	handler := docsServerHandler(http.NotFoundHandler())

	rec := requestDocsPath(t, handler, docsPlaceholderStylesheet)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	if got := rec.Header().Get("Content-Type"); got != docsPlaceholderStylesheetCT {
		t.Fatalf("Content-Type = %q, want %q", got, docsPlaceholderStylesheetCT)
	}

	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}

	body := rec.Body.String()
	for _, want := range []string{"font-family: system-ui", "max-width: 720px", "code {"} {
		if !strings.Contains(body, want) {
			t.Fatalf("placeholder stylesheet missing %q: %q", want, body)
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
	assertDocsHeader(t, rec, "Cross-Origin-Opener-Policy", "same-origin")
	assertDocsHeader(t, rec, "Cross-Origin-Resource-Policy", "same-origin")
	assertDocsHeader(t, rec, "Cross-Origin-Embedder-Policy", "require-corp")
	assertDocsHeader(t, rec, "Origin-Agent-Cluster", "?1")
	assertDocsHeader(t, rec, "X-Permitted-Cross-Domain-Policies", "none")
	assertDocsHeader(t, rec, "X-Download-Options", "noopen")

	if got := rec.Header().Get("Content-Security-Policy"); !strings.Contains(got, "default-src 'self'") || !strings.Contains(got, "frame-ancestors 'none'") {
		t.Fatalf("Content-Security-Policy = %q, want docs policy", got)
	}

	if got := rec.Header().Get("Content-Security-Policy"); strings.Contains(got, "'unsafe-inline'") {
		t.Fatalf("Content-Security-Policy = %q, must not allow unsafe-inline", got)
	}

	if got := rec.Header().Get("Strict-Transport-Security"); got != "" {
		t.Fatalf("Strict-Transport-Security over HTTP = %q, want empty", got)
	}
}

func TestDocsServerHandlerAllowsHEAD(t *testing.T) {
	handler := docsServerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodHead, "/", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestDocsServerHandlerRejectsNonReadMethods(t *testing.T) {
	handler := docsServerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called")
	}))

	for _, method := range []string{http.MethodPost, http.MethodTrace, http.MethodConnect, "TRACK"} {
		t.Run(method, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(method, "/", nil)
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusMethodNotAllowed, rec.Body.String())
			}

			if got := rec.Header().Get("Allow"); got != "GET, HEAD" {
				t.Fatalf("Allow = %q, want GET, HEAD", got)
			}

			if got := rec.Header().Get("Cache-Control"); got != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", got)
			}

			assertDocsHeader(t, rec, "X-Content-Type-Options", "nosniff")
			assertDocsHeader(t, rec, "X-Frame-Options", "DENY")
		})
	}
}

func TestDocsServerHandlerRejectsMethodOverrideHeaders(t *testing.T) {
	for _, header := range []string{"X-HTTP-Method", "X-HTTP-Method-Override", "X-Method-Override"} {
		t.Run(header, func(t *testing.T) {
			handler := docsServerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Fatal("handler should not be called")
			}))

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set(header, http.MethodPost)
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}

			if got := rec.Header().Get("Cache-Control"); got != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", got)
			}

			assertDocsHeader(t, rec, "X-Content-Type-Options", "nosniff")
			assertDocsHeader(t, rec, "X-Frame-Options", "DENY")
		})
	}
}

func TestDocsServerHandlerRejectsUnsafeRequestTargets(t *testing.T) {
	handler := docsServerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called")
	}))

	tests := []struct {
		name   string
		target string
	}{
		{name: "absolute form", target: "http://example.test/"},
		{name: "encoded path", target: "/%69ndex.html"},
		{name: "encoded slash", target: "/assets%2Fmain.js"},
		{name: "dot segment", target: "/guide/../index.html"},
		{name: "duplicate slash", target: "/assets//main.js"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tt.target, nil)
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}

			if got := rec.Header().Get("Cache-Control"); got != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", got)
			}

			assertDocsHeader(t, rec, "X-Content-Type-Options", "nosniff")
			assertDocsHeader(t, rec, "X-Frame-Options", "DENY")
		})
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
	req := httptest.NewRequest(http.MethodGet, "/", nil)
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
