package utils

import (
	"fmt"
	"os"
	"path/filepath"
)

// RuntimeDir returns a per-user runtime directory for Vectis.
// It prefers $XDG_RUNTIME_DIR/vectis and falls back to
// $TMPDIR/vectis-<uid> for isolation on multi-user systems.
func RuntimeDir() string {
	if runtimeDir := os.Getenv("XDG_RUNTIME_DIR"); runtimeDir != "" {
		return filepath.Join(runtimeDir, "vectis")
	}

	return filepath.Join(os.TempDir(), fmt.Sprintf("vectis-%d", os.Getuid()))
}
