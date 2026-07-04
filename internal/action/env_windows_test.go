//go:build windows

package action

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestSanitizedProcessEnvUsesWindowsPathFallback(t *testing.T) {
	got := SanitizedProcessEnv(`C:\vectis\workspace`, []string{`SystemRoot=C:\Windows`})

	path, ok := lookupEnv(got, "PATH")
	if !ok {
		t.Fatalf("missing PATH in %v", got)
	}

	for _, want := range []string{
		filepath.Join(`C:\Windows`, "System32"),
		`C:\Windows`,
		filepath.Join(`C:\Windows`, "System32", "WindowsPowerShell", "v1.0"),
	} {
		if !strings.Contains(path, want) {
			t.Fatalf("PATH = %q, missing %q", path, want)
		}
	}
}
