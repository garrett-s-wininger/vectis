package source

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestWorkerCheckoutCacheCachesPersistentRemote(t *testing.T) {
	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "cached\n", "cached")

	cache, err := NewWorkerCheckoutCache(filepath.Join(t.TempDir(), "cache"), []string{remote})
	if err != nil {
		t.Fatalf("NewWorkerCheckoutCache: %v", err)
	}

	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	handled, err := cache.Checkout(context.Background(), remote, workspace, nil)
	if err != nil {
		t.Fatalf("Checkout: %v", err)
	}

	if !handled {
		t.Fatal("expected persistent remote to be handled by cache")
	}

	if got := gitOutput(t, workspace, "remote", "get-url", "origin"); got != remote {
		t.Fatalf("origin url = %q, want %q", got, remote)
	}

	if got, err := os.ReadFile(filepath.Join(workspace, "README.md")); err != nil || string(got) != "cached\n" {
		t.Fatalf("workspace README = %q, %v", got, err)
	}
}

func TestWorkerCheckoutCacheIgnoresUnconfiguredRemote(t *testing.T) {
	cache, err := NewWorkerCheckoutCache(filepath.Join(t.TempDir(), "cache"), []string{"https://example.invalid/persistent.git"})
	if err != nil {
		t.Fatalf("NewWorkerCheckoutCache: %v", err)
	}

	workspace := t.TempDir()
	handled, err := cache.Checkout(context.Background(), "https://example.invalid/other.git", workspace, nil)
	if err != nil {
		t.Fatalf("Checkout: %v", err)
	}

	if handled {
		t.Fatal("expected unconfigured remote to bypass cache")
	}
}
