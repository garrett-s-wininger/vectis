package utils

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimeDir_prefersXDG(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", tmpDir)

	got := RuntimeDir()
	if got != filepath.Join(tmpDir, "vectis") {
		t.Fatalf("expected %s, got %s", filepath.Join(tmpDir, "vectis"), got)
	}
}

func TestRuntimeDir_fallsBackToTemp(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "")

	got := RuntimeDir()
	if !strings.Contains(got, "vectis-") {
		t.Fatalf("expected fallback to contain 'vectis-', got %s", got)
	}

	if !strings.HasPrefix(got, os.TempDir()) {
		t.Fatalf("expected prefix %s, got %s", os.TempDir(), got)
	}
}
