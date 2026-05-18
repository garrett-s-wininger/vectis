package main

import (
	"bytes"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"vectis/internal/interfaces"
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

func TestHasDocsIndexFSRejectsMissingIndex(t *testing.T) {
	if hasDocsIndexFS(fstest.MapFS{}) {
		t.Fatal("hasDocsIndexFS returned true for an empty filesystem")
	}

	if hasDocsIndexFS(fstest.MapFS{"index.html": &fstest.MapFile{Mode: fs.ModeDir}}) {
		t.Fatal("hasDocsIndexFS returned true when index.html is a directory")
	}
}
