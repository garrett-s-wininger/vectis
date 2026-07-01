package source

import (
	"bytes"
	"context"
	"errors"
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

func TestWorkerCheckoutCacheRejectsInvalidGenerationRetention(t *testing.T) {
	_, err := NewWorkerCheckoutCache(
		filepath.Join(t.TempDir(), "cache"),
		[]string{"https://example.invalid/repo.git"},
		WithWorkerCheckoutCacheGenerationsToKeep(0),
	)
	if err == nil {
		t.Fatal("expected invalid generation retention to fail")
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

func TestWorkerCheckoutCacheCleanupHonorsConfiguredRetention(t *testing.T) {
	remote := "https://example.invalid/large.git"
	cache, err := NewWorkerCheckoutCache(
		filepath.Join(t.TempDir(), "cache"),
		[]string{remote},
		WithWorkerCheckoutCacheGenerationsToKeep(3),
	)
	if err != nil {
		t.Fatalf("NewWorkerCheckoutCache: %v", err)
	}

	repoPath := cache.repositoryPath(remote)
	generationNames := []string{
		"generation-00000000000000000001-1.git",
		"generation-00000000000000000002-1.git",
		"generation-00000000000000000003-1.git",
		"generation-00000000000000000004-1.git",
	}
	for _, name := range generationNames {
		if err := os.MkdirAll(filepath.Join(repoPath, "generations", name), 0o755); err != nil {
			t.Fatalf("create generation %s: %v", name, err)
		}
	}
	if err := os.Symlink(filepath.Join("generations", generationNames[3]), filepath.Join(repoPath, "current")); err != nil {
		t.Fatalf("create current generation link: %v", err)
	}

	if err := cache.cleanupOldGenerations(context.Background(), repoPath); err != nil {
		t.Fatalf("cleanupOldGenerations: %v", err)
	}

	if _, err := os.Stat(filepath.Join(repoPath, "generations", generationNames[0])); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oldest generation still exists after cleanup: err=%v", err)
	}
	for _, name := range generationNames[1:] {
		if info, err := os.Stat(filepath.Join(repoPath, "generations", name)); err != nil || !info.IsDir() {
			t.Fatalf("kept generation %s: info=%v err=%v", name, info, err)
		}
	}
}

func TestWorkerCheckoutCacheStatsReportsRootFootprint(t *testing.T) {
	remote := "https://example.invalid/large.git"
	cache, err := NewWorkerCheckoutCache(filepath.Join(t.TempDir(), "cache"), []string{remote})
	if err != nil {
		t.Fatalf("NewWorkerCheckoutCache: %v", err)
	}

	repoPath := cache.repositoryPath(remote)
	generationNames := []string{
		"generation-00000000000000000001-1.git",
		"generation-00000000000000000002-1.git",
	}
	for i, name := range generationNames {
		packPath := filepath.Join(repoPath, "generations", name, "objects", "pack")
		if err := os.MkdirAll(packPath, 0o755); err != nil {
			t.Fatalf("create pack dir %s: %v", packPath, err)
		}
		if err := os.WriteFile(filepath.Join(packPath, "pack-test.pack"), bytes.Repeat([]byte{'x'}, 10+i), 0o644); err != nil {
			t.Fatalf("write pack file: %v", err)
		}
		if err := os.WriteFile(filepath.Join(packPath, "pack-test.idx"), []byte("ignored"), 0o644); err != nil {
			t.Fatalf("write index file: %v", err)
		}
	}
	leasePath := filepath.Join(repoPath, "leases", generationNames[0])
	if err := os.MkdirAll(leasePath, 0o755); err != nil {
		t.Fatalf("create lease dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(leasePath, "lease"), []byte("leased"), 0o644); err != nil {
		t.Fatalf("write lease: %v", err)
	}

	stats, err := cache.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}

	if stats.Repositories != 1 || stats.Generations != 2 || stats.PackFiles != 2 || stats.PackBytes != 21 || stats.ActiveLeases != 1 {
		t.Fatalf("stats = %+v, want repositories=1 generations=2 pack_files=2 pack_bytes=21 active_leases=1", stats)
	}
}

func TestWorkerCheckoutCacheCleanupRemovesOldUnleasedGenerations(t *testing.T) {
	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "first\n", "first")

	cache, err := NewWorkerCheckoutCache(filepath.Join(t.TempDir(), "cache"), []string{remote})
	if err != nil {
		t.Fatalf("NewWorkerCheckoutCache: %v", err)
	}

	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("initial WarmRemote: handled=%v err=%v", handled, err)
	}

	firstPath := currentWorkerCheckoutGeneration(t, cache, remote)
	writeAndCommit(t, remote, "README.md", "second\n", "second")
	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("second WarmRemote: handled=%v err=%v", handled, err)
	}

	secondPath := currentWorkerCheckoutGeneration(t, cache, remote)
	writeAndCommit(t, remote, "README.md", "third\n", "third")
	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("third WarmRemote: handled=%v err=%v", handled, err)
	}

	thirdPath := currentWorkerCheckoutGeneration(t, cache, remote)
	if _, err := os.Stat(firstPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("first generation still exists after cleanup: err=%v", err)
	}

	if info, err := os.Stat(secondPath); err != nil || !info.IsDir() {
		t.Fatalf("second generation path = %s, info=%v err=%v", secondPath, info, err)
	}

	if info, err := os.Stat(thirdPath); err != nil || !info.IsDir() {
		t.Fatalf("third generation path = %s, info=%v err=%v", thirdPath, info, err)
	}
}

func TestWorkerCheckoutCacheCleanupKeepsLeasedGeneration(t *testing.T) {
	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "first\n", "first")

	cache, err := NewWorkerCheckoutCache(filepath.Join(t.TempDir(), "cache"), []string{remote})
	if err != nil {
		t.Fatalf("NewWorkerCheckoutCache: %v", err)
	}

	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("initial WarmRemote: handled=%v err=%v", handled, err)
	}

	firstPath := currentWorkerCheckoutGeneration(t, cache, remote)
	mirrorPath, lease, err := cache.acquireCurrentMirrorLease(context.Background(), remote)
	if err != nil {
		t.Fatalf("acquire current mirror lease: %v", err)
	}

	if mirrorPath != firstPath {
		t.Fatalf("leased generation = %q, want %q", mirrorPath, firstPath)
	}

	writeAndCommit(t, remote, "README.md", "second\n", "second")
	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("second WarmRemote: handled=%v err=%v", handled, err)
	}

	writeAndCommit(t, remote, "README.md", "third\n", "third")
	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("third WarmRemote: handled=%v err=%v", handled, err)
	}

	if info, err := os.Stat(firstPath); err != nil || !info.IsDir() {
		t.Fatalf("leased generation path = %s, info=%v err=%v", firstPath, info, err)
	}

	if err := lease.Close(); err != nil {
		t.Fatalf("close generation lease: %v", err)
	}

	writeAndCommit(t, remote, "README.md", "fourth\n", "fourth")
	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("fourth WarmRemote: handled=%v err=%v", handled, err)
	}

	if _, err := os.Stat(firstPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released generation still exists after cleanup: err=%v", err)
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

func currentWorkerCheckoutGeneration(t *testing.T, cache *WorkerCheckoutCache, remote string) string {
	t.Helper()

	path, err := currentGenerationPath(cache.repositoryPath(remote))
	if err != nil {
		t.Fatalf("current generation path: %v", err)
	}

	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		path = resolved
	}

	return path
}
