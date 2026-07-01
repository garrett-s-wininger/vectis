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

	"vectis/internal/observability"
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

	if _, err := os.Stat(filepath.Join(workspace, ".git", "objects", "info", "alternates")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cached checkout should not depend on alternates after clone: err=%v", err)
	}

	if got, want := gitOutput(t, workspace, "remote", "get-url", workerCheckoutCacheRemoteName), filepath.Join(cache.repositoryPath(remote), "current"); got != want {
		t.Fatalf("cache remote url = %q, want %q", got, want)
	}
}

func TestWorkerCheckoutCacheCloneRetriesWithoutHardlinksForLinkFailure(t *testing.T) {
	workspace := t.TempDir()
	mirrorPath := filepath.Join(t.TempDir(), "mirror.git")
	var calls [][]string

	mode, reason, err := cloneWorkerCheckoutCacheWorkspaceWithRunner(context.Background(), workspace, mirrorPath, func(_ context.Context, dir string, args ...string) error {
		if dir != workspace {
			t.Fatalf("clone dir = %q, want %q", dir, workspace)
		}

		calls = append(calls, append([]string(nil), args...))
		if len(calls) == 1 {
			if err := os.MkdirAll(filepath.Join(workspace, ".git", "objects"), 0o755); err != nil {
				t.Fatalf("create partial clone artifact: %v", err)
			}

			return errors.New("fatal: failed to create link: invalid cross-device link")
		}

		if _, err := os.Stat(filepath.Join(workspace, ".git")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("partial clone artifact still exists before retry: err=%v", err)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("cloneWorkerCheckoutCacheWorkspaceWithRunner: %v", err)
	}
	if mode != observability.CheckoutCacheCloneModeCopy || reason != observability.CheckoutCacheCloneReasonRetry {
		t.Fatalf("clone mode=%q reason=%q, want copy/retry", mode, reason)
	}

	assertStringSliceEqual(t, calls[0], []string{"clone", "--local", "--", mirrorPath, "."})
	assertStringSliceEqual(t, calls[1], []string{"clone", "--local", "--no-hardlinks", "--", mirrorPath, "."})
}

func TestWorkerCheckoutCacheCloneSkipsHardlinksWhenProbeFails(t *testing.T) {
	workspace := t.TempDir()
	mirrorPath := filepath.Join(t.TempDir(), "mirror.git")
	var calls [][]string

	mode, reason, err := cloneWorkerCheckoutCacheWorkspaceWithRunnerAndProbe(
		context.Background(),
		workspace,
		mirrorPath,
		func(_ context.Context, dir string, args ...string) error {
			if dir != workspace {
				t.Fatalf("clone dir = %q, want %q", dir, workspace)
			}

			calls = append(calls, append([]string(nil), args...))
			return nil
		},
		func(probeMirrorPath, probeWorkspace string) bool {
			if probeMirrorPath != mirrorPath || probeWorkspace != workspace {
				t.Fatalf("probe got mirror=%q workspace=%q, want mirror=%q workspace=%q", probeMirrorPath, probeWorkspace, mirrorPath, workspace)
			}

			return false
		},
	)

	if err != nil {
		t.Fatalf("cloneWorkerCheckoutCacheWorkspaceWithRunnerAndProbe: %v", err)
	}
	if mode != observability.CheckoutCacheCloneModeCopy || reason != observability.CheckoutCacheCloneReasonProbe {
		t.Fatalf("clone mode=%q reason=%q, want copy/probe", mode, reason)
	}

	if len(calls) != 1 {
		t.Fatalf("clone calls = %d, want 1", len(calls))
	}

	assertStringSliceEqual(t, calls[0], []string{"clone", "--local", "--no-hardlinks", "--", mirrorPath, "."})
}

func TestWorkerCheckoutCacheCloneDoesNotRetryGenericFailure(t *testing.T) {
	workspace := t.TempDir()
	mirrorPath := filepath.Join(t.TempDir(), "mirror.git")
	calls := 0

	_, _, err := cloneWorkerCheckoutCacheWorkspaceWithRunner(context.Background(), workspace, mirrorPath, func(context.Context, string, ...string) error {
		calls++
		return errors.New("fatal: destination path '.' already exists and is not an empty directory")
	})

	if err == nil {
		t.Fatal("expected clone failure")
	}

	if calls != 1 {
		t.Fatalf("clone calls = %d, want 1", calls)
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

func TestWorkerCheckoutCacheRejectsInvalidLeaseTTL(t *testing.T) {
	_, err := NewWorkerCheckoutCache(
		filepath.Join(t.TempDir(), "cache"),
		[]string{"https://example.invalid/repo.git"},
		WithWorkerCheckoutCacheLeaseTTL(0),
	)
	if err == nil {
		t.Fatal("expected invalid lease TTL to fail")
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
	cache, err := NewWorkerCheckoutCache(
		filepath.Join(t.TempDir(), "cache"),
		[]string{remote},
		WithWorkerCheckoutCacheLeaseTTL(time.Hour),
	)
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

	stale := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(filepath.Join(leasePath, "lease"), stale, stale); err != nil {
		t.Fatalf("age lease: %v", err)
	}
	stats, err = cache.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats after stale lease: %v", err)
	}
	if stats.ActiveLeases != 0 {
		t.Fatalf("active leases after stale lease = %d, want 0", stats.ActiveLeases)
	}
}

func TestWorkerCheckoutCacheCleanupDropsStaleLeases(t *testing.T) {
	remote := "https://example.invalid/large.git"
	cache, err := NewWorkerCheckoutCache(
		filepath.Join(t.TempDir(), "cache"),
		[]string{remote},
		WithWorkerCheckoutCacheLeaseTTL(time.Hour),
	)
	if err != nil {
		t.Fatalf("NewWorkerCheckoutCache: %v", err)
	}

	repoPath := cache.repositoryPath(remote)
	generationNames := []string{
		"generation-00000000000000000001-1.git",
		"generation-00000000000000000002-1.git",
		"generation-00000000000000000003-1.git",
	}
	for _, name := range generationNames {
		if err := os.MkdirAll(filepath.Join(repoPath, "generations", name), 0o755); err != nil {
			t.Fatalf("create generation %s: %v", name, err)
		}
	}
	if err := os.Symlink(filepath.Join("generations", generationNames[2]), filepath.Join(repoPath, "current")); err != nil {
		t.Fatalf("create current generation link: %v", err)
	}

	leaseDir := filepath.Join(repoPath, "leases", generationNames[0])
	if err := os.MkdirAll(leaseDir, 0o755); err != nil {
		t.Fatalf("create lease dir: %v", err)
	}
	leasePath := filepath.Join(leaseDir, "stale")
	if err := os.WriteFile(leasePath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("write stale lease: %v", err)
	}
	stale := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(leasePath, stale, stale); err != nil {
		t.Fatalf("age stale lease: %v", err)
	}

	if err := cache.cleanupOldGenerations(context.Background(), repoPath); err != nil {
		t.Fatalf("cleanupOldGenerations: %v", err)
	}

	if _, err := os.Stat(filepath.Join(repoPath, "generations", generationNames[0])); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale-leased generation still exists after cleanup: err=%v", err)
	}
	if _, err := os.Stat(leasePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale lease still exists after cleanup: err=%v", err)
	}
}

func TestWorkerCheckoutCacheCleanupDropsStaleReceivingGenerations(t *testing.T) {
	remote := "https://example.invalid/large.git"
	ttl := time.Hour
	cache, err := NewWorkerCheckoutCache(
		filepath.Join(t.TempDir(), "cache"),
		[]string{remote},
		WithWorkerCheckoutCacheLeaseTTL(ttl),
	)

	if err != nil {
		t.Fatalf("NewWorkerCheckoutCache: %v", err)
	}

	generationsPath := filepath.Join(cache.repositoryPath(remote), "generations")
	names := []string{
		"generation-00000000000000000001-1.git.receiving",
		"generation-00000000000000000002-1.git.receiving",
		"generation-00000000000000000003-1.git",
		"scratch.git.receiving",
	}

	for _, name := range names {
		if err := os.MkdirAll(filepath.Join(generationsPath, name), 0o755); err != nil {
			t.Fatalf("create generation dir %s: %v", name, err)
		}
	}

	now := time.Now()
	stale := now.Add(-2 * ttl)
	if err := os.Chtimes(filepath.Join(generationsPath, names[0]), stale, stale); err != nil {
		t.Fatalf("age stale receiving generation: %v", err)
	}

	fresh := now.Add(-ttl / 2)
	if err := os.Chtimes(filepath.Join(generationsPath, names[1]), fresh, fresh); err != nil {
		t.Fatalf("age fresh receiving generation: %v", err)
	}

	if err := cleanupStaleWorkerCheckoutReceivingGenerations(generationsPath, ttl, now); err != nil {
		t.Fatalf("cleanupStaleWorkerCheckoutReceivingGenerations: %v", err)
	}

	if _, err := os.Stat(filepath.Join(generationsPath, names[0])); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale receiving generation still exists after cleanup: err=%v", err)
	}

	for _, name := range names[1:] {
		if info, err := os.Stat(filepath.Join(generationsPath, name)); err != nil || !info.IsDir() {
			t.Fatalf("kept generation dir %s: info=%v err=%v", name, info, err)
		}
	}
}

func TestWorkerCheckoutCacheLeaseHeartbeatKeepsActiveLeaseFresh(t *testing.T) {
	remote := "https://example.invalid/large.git"
	leaseTTL := 120 * time.Millisecond
	cache, err := NewWorkerCheckoutCache(
		filepath.Join(t.TempDir(), "cache"),
		[]string{remote},
		WithWorkerCheckoutCacheLeaseTTL(leaseTTL),
	)

	if err != nil {
		t.Fatalf("NewWorkerCheckoutCache: %v", err)
	}

	repoPath := cache.repositoryPath(remote)
	generationNames := []string{
		"generation-00000000000000000001-1.git",
		"generation-00000000000000000002-1.git",
		"generation-00000000000000000003-1.git",
	}

	for _, name := range generationNames {
		if err := os.MkdirAll(filepath.Join(repoPath, "generations", name), 0o755); err != nil {
			t.Fatalf("create generation %s: %v", name, err)
		}
	}

	if err := os.Symlink(filepath.Join("generations", generationNames[0]), filepath.Join(repoPath, "current")); err != nil {
		t.Fatalf("create current generation link: %v", err)
	}

	_, lease, err := cache.acquireCurrentMirrorLease(context.Background(), remote)
	if err != nil {
		t.Fatalf("acquire current mirror lease: %v", err)
	}
	defer lease.Close()

	stale := time.Now().Add(-2 * leaseTTL)
	if err := os.Chtimes(lease.path, stale, stale); err != nil {
		t.Fatalf("age active lease: %v", err)
	}

	waitForFreshWorkerCheckoutLease(t, lease.path, leaseTTL)

	if err := cache.flipCurrentGeneration(repoPath, filepath.Join(repoPath, "generations", generationNames[2])); err != nil {
		t.Fatalf("flip current generation: %v", err)
	}

	if err := cache.cleanupOldGenerations(context.Background(), repoPath); err != nil {
		t.Fatalf("cleanupOldGenerations: %v", err)
	}

	if info, err := os.Stat(filepath.Join(repoPath, "generations", generationNames[0])); err != nil || !info.IsDir() {
		t.Fatalf("heartbeat-protected generation path info=%v err=%v", info, err)
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

func TestWorkerCheckoutCacheCheckoutSurvivesGenerationCleanup(t *testing.T) {
	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "first\n", "first")
	firstCommit := gitOutput(t, remote, "rev-parse", "HEAD")

	cache, err := NewWorkerCheckoutCache(filepath.Join(t.TempDir(), "cache"), []string{remote})
	if err != nil {
		t.Fatalf("NewWorkerCheckoutCache: %v", err)
	}

	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if handled, err := cache.Checkout(context.Background(), remote, workspace, nil); err != nil || !handled {
		t.Fatalf("Checkout: handled=%v err=%v", handled, err)
	}

	firstGeneration := currentWorkerCheckoutGeneration(t, cache, remote)
	writeAndCommit(t, remote, "README.md", "second\n", "second")
	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("second WarmRemote: handled=%v err=%v", handled, err)
	}

	writeAndCommit(t, remote, "README.md", "third\n", "third")
	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("third WarmRemote: handled=%v err=%v", handled, err)
	}

	if _, err := os.Stat(firstGeneration); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("first generation still exists after retention cleanup: err=%v", err)
	}

	if got := gitOutput(t, workspace, "rev-parse", "HEAD"); got != firstCommit {
		t.Fatalf("workspace HEAD after generation cleanup = %q, want %q", got, firstCommit)
	}

	git(t, workspace, "cat-file", "-e", firstCommit+"^{commit}")
}

func TestWorkerCheckoutCacheCheckoutAddsLocalCacheRemoteForAuxiliaryRefs(t *testing.T) {
	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "main\n", "main")
	mainCommit := gitOutput(t, remote, "rev-parse", "HEAD")
	defaultBranch := gitOutput(t, remote, "branch", "--show-current")
	git(t, remote, "notes", "--ref=commits", "add", "-m", "cached note", mainCommit)

	git(t, remote, "checkout", "-b", "review/cache")
	writeAndCommit(t, remote, "README.md", "review\n", "review")
	reviewCommit := gitOutput(t, remote, "rev-parse", "HEAD")
	git(t, remote, "update-ref", "refs/pull/123/head", reviewCommit)
	git(t, remote, "checkout", defaultBranch)

	cache, err := NewWorkerCheckoutCache(filepath.Join(t.TempDir(), "cache"), []string{remote})
	if err != nil {
		t.Fatalf("NewWorkerCheckoutCache: %v", err)
	}

	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("WarmRemote: handled=%v err=%v", handled, err)
	}

	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if handled, err := cache.Checkout(context.Background(), remote, workspace, nil); err != nil || !handled {
		t.Fatalf("Checkout: handled=%v err=%v", handled, err)
	}

	git(t, workspace, "fetch", "--no-tags", workerCheckoutCacheRemoteName,
		"refs/notes/commits:refs/notes/commits",
		"refs/pull/123/head:refs/vectis/test/pull-123",
	)

	if got := gitOutput(t, workspace, "notes", "--ref=commits", "show", mainCommit); got != "cached note" {
		t.Fatalf("fetched note = %q, want cached note", got)
	}

	if got := gitOutput(t, workspace, "rev-parse", "refs/vectis/test/pull-123"); got != reviewCommit {
		t.Fatalf("fetched provider ref = %q, want %q", got, reviewCommit)
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

func waitForFreshWorkerCheckoutLease(t *testing.T, path string, leaseTTL time.Duration) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat lease: %v", err)
		}

		if !workerCheckoutLeaseIsStale(info.ModTime(), leaseTTL, time.Now()) {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("lease %s was not refreshed before deadline", path)
}

func assertStringSliceEqual(t *testing.T, got, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("slice length = %d, want %d; got=%v want=%v", len(got), len(want), got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("slice[%d] = %q, want %q; got=%v want=%v", i, got[i], want[i], got, want)
		}
	}
}
