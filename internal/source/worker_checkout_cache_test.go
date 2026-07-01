package source

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

	repoPath := cache.repositoryPath(remote)
	currentTarget, err := os.Readlink(filepath.Join(repoPath, "current"))
	if err != nil {
		t.Fatalf("read current generation link: %v", err)
	}

	if !strings.HasPrefix(currentTarget, "generations/") || !strings.HasSuffix(currentTarget, ".git") {
		t.Fatalf("current generation link target = %q, want generations/*.git", currentTarget)
	}

	generationPath := filepath.Join(repoPath, currentTarget)
	if info, err := os.Stat(generationPath); err != nil || !info.IsDir() {
		t.Fatalf("current generation path = %s, info=%v err=%v", generationPath, info, err)
	}
}

func TestWorkerCheckoutCacheWarmRemoteFlipsCurrentGeneration(t *testing.T) {
	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "first\n", "first")

	cache, err := NewWorkerCheckoutCache(filepath.Join(t.TempDir(), "cache"), []string{remote})
	if err != nil {
		t.Fatalf("NewWorkerCheckoutCache: %v", err)
	}

	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("initial WarmRemote: handled=%v err=%v", handled, err)
	}

	currentLink := filepath.Join(cache.repositoryPath(remote), "current")
	firstTarget, err := os.Readlink(currentLink)
	if err != nil {
		t.Fatalf("read first current generation: %v", err)
	}

	writeAndCommit(t, remote, "README.md", "second\n", "second")
	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("second WarmRemote: handled=%v err=%v", handled, err)
	}

	secondTarget, err := os.Readlink(currentLink)
	if err != nil {
		t.Fatalf("read second current generation: %v", err)
	}

	if secondTarget == firstTarget {
		t.Fatalf("current generation did not change: %q", secondTarget)
	}

	if _, err := os.Stat(filepath.Join(cache.repositoryPath(remote), firstTarget)); err != nil {
		t.Fatalf("previous generation was removed before cleanup support exists: %v", err)
	}

	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	handled, err := cache.Checkout(context.Background(), remote, workspace, nil)
	if err != nil || !handled {
		t.Fatalf("Checkout: handled=%v err=%v", handled, err)
	}

	if got, err := os.ReadFile(filepath.Join(workspace, "README.md")); err != nil || string(got) != "second\n" {
		t.Fatalf("workspace README = %q, %v", got, err)
	}
}

func TestWorkerCheckoutCacheCheckoutUsesCurrentGenerationWhileWarmLocked(t *testing.T) {
	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "cached\n", "cached")

	cache, err := NewWorkerCheckoutCache(filepath.Join(t.TempDir(), "cache"), []string{remote})
	if err != nil {
		t.Fatalf("NewWorkerCheckoutCache: %v", err)
	}

	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("WarmRemote: handled=%v err=%v", handled, err)
	}

	lock, err := acquireManagedGitWriterLock(context.Background(), cache.repositoryPath(remote))
	if err != nil {
		t.Fatalf("acquire worker cache writer lock: %v", err)
	}
	defer lock.Close()

	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		handled, err := cache.Checkout(context.Background(), remote, workspace, nil)
		if err == nil && !handled {
			err = ErrNotFound
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Checkout while writer lock held: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("checkout waited behind worker cache writer lock despite current generation")
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
