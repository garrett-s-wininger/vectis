package socktest

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// ShortPath returns a socket path under a short temp root when one is available.
func ShortPath(t testing.TB, name string) string {
	t.Helper()
	dir, err := os.MkdirTemp(shortTempRoot(), "vectis-sock-*") //nolint:usetesting // Keep Unix socket paths short on platforms with long test temp roots.
	if err != nil {
		t.Fatalf("mkdtemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, name)
}

func shortTempRoot() string {
	if runtime.GOOS == "windows" {
		return ""
	}

	return "/tmp"
}
