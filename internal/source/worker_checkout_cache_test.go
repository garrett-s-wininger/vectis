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

func TestWorkerCheckoutCacheWarmRemote(t *testing.T) {
	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "warmed\n", "warmed")

	cacheRoot := filepath.Join(t.TempDir(), "cache")
	cache, err := NewWorkerCheckoutCache(cacheRoot, []string{remote})
	if err != nil {
		t.Fatalf("NewWorkerCheckoutCache: %v", err)
	}

	handled, normalizedRemote, err := cache.WarmRemote(context.Background(), remote, nil)
	if err != nil {
		t.Fatalf("WarmRemote: %v", err)
	}

	if !handled {
		t.Fatal("expected persistent remote to be warmed")
	}

	if normalizedRemote != remote {
		t.Fatalf("normalized remote = %q, want %q", normalizedRemote, remote)
	}

	mirrorParent := filepath.Join(cacheRoot, "mirrors")
	entries, err := os.ReadDir(mirrorParent)
	if err != nil {
		t.Fatalf("read mirror parent: %v", err)
	}

	mirrors := 0
	for _, entry := range entries {
		if entry.IsDir() && filepath.Ext(entry.Name()) == ".git" {
			mirrors++
		}
	}

	if mirrors != 1 {
		t.Fatalf("expected one warmed mirror under %s, got %+v", mirrorParent, entries)
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
