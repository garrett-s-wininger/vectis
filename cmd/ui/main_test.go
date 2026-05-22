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
	if _, err := apiProxyHandler("localhost:8080"); err == nil {
		t.Fatal("apiProxyHandler accepted URL without scheme and host")
	}
}
