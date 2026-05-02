package socktest

import (
	"os"
	"path/filepath"
	"testing"
)

// ShortPath returns a Unix socket path under /tmp to avoid path-length limits
// on platforms with long temp roots.
func ShortPath(t testing.TB, name string) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "vectis-sock-*")
	if err != nil {
		t.Fatalf("mkdtemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, name)
}
