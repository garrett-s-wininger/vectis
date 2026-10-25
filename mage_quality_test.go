//go:build mage

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
)

func TestShellSourcesExcludeRuntimeScripts(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, path := range []string{
		".vectis/ci.sh",
		".vectis/authored/check.sh",
		".vectis/scripts/script-123.sh",
		"build/packer/scripts/guest.sh",
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}

		if err := os.WriteFile(path, []byte("echo check"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	got, err := shellSources()
	if err != nil {
		t.Fatal(err)
	}

	want := []string{".vectis/authored/check.sh", ".vectis/ci.sh", "build/packer/scripts/guest.sh"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("shell sources = %v, want %v", got, want)
	}
}

func TestCleanCIWorktreeModuleCache(t *testing.T) {
	goBinary, err := exec.LookPath(goCommand())
	if err != nil {
		t.Skipf("Go executable unavailable: %v", err)
	}

	t.Setenv("GO", goBinary)
	t.Setenv("GOTOOLCHAIN", "local")
	worktree := t.TempDir()
	cache := filepath.Join(worktree, "go", "pkg", "mod")
	moduleDir := filepath.Join(cache, "example.com", "module@v1.0.0")

	if err := os.MkdirAll(moduleDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(moduleDir, "module.go"), []byte("package module\n"), 0o444); err != nil {
		t.Fatal(err)
	}

	if err := os.Chmod(moduleDir, 0o555); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = os.Chmod(moduleDir, 0o755) })

	otherCache := t.TempDir()
	t.Setenv("GOMODCACHE", otherCache)
	marker := filepath.Join(otherCache, "keep")
	if err := os.WriteFile(marker, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := cleanCIWorktreeModuleCache(worktree); err != nil {
		t.Fatalf("clean worktree module cache: %v", err)
	}

	if _, err := os.Stat(moduleDir); !os.IsNotExist(err) {
		t.Fatalf("module cache remains after cleanup: %v", err)
	}

	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("cleanup changed the inherited host cache: %v", err)
	}
}
