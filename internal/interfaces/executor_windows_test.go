//go:build windows

package interfaces

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveExecutablePathUsesPATHEXTOnWindows(t *testing.T) {
	binDir := t.TempDir()
	commandPath := filepath.Join(binDir, "vectis-child-path-command.CMD")
	if err := os.WriteFile(commandPath, []byte("@echo off\r\n"), 0o600); err != nil {
		t.Fatalf("write command: %v", err)
	}

	got, err := resolveExecutablePath("vectis-child-path-command", []string{
		"PATH=" + binDir,
		"PATHEXT=.CMD",
	})

	if err != nil {
		t.Fatalf("resolveExecutablePath: %v", err)
	}

	if got != commandPath {
		t.Fatalf("resolveExecutablePath = %q, want %q", got, commandPath)
	}
}
