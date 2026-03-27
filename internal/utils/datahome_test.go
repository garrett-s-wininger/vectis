package utils

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDataHome_UsesXDGDataHomeWhenSet(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", "/tmp/vectis-xdg")

	if got := DataHome(); got != "/tmp/vectis-xdg" {
		t.Fatalf("expected XDG_DATA_HOME path, got %q", got)
	}
}

func TestDataHome_FallbackUnderHome(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", "")

	want := filepath.Join(mustUserHomeDir(t), ".local", "share")
	if got := DataHome(); got != want {
		t.Fatalf("expected fallback %q, got %q", want, got)
	}
}

func mustUserHomeDir(t *testing.T) string {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("user home dir: %v", err)
	}

	return home
}
