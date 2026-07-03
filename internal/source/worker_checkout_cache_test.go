package source

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"vectis/internal/gitcmd"
	"vectis/internal/observability"
)

func workerCheckoutCacheRoot(t *testing.T) string {
	t.Helper()

	if runtime.GOOS == "windows" {
		skipIfWorkerCheckoutCacheSymlinksUnavailable(t)
	}

	dir, err := os.MkdirTemp("", "vcc-*")
	if err != nil {
		t.Fatalf("create worker checkout cache temp root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	return filepath.Join(dir, "cache")
}

func skipIfWorkerCheckoutCacheSymlinksUnavailable(t *testing.T) {
	t.Helper()

	dir, err := os.MkdirTemp("", "vcc-symlink-*")
	if err != nil {
		t.Fatalf("create checkout cache symlink probe dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	target := filepath.Join(dir, "target")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatalf("create checkout cache symlink probe target: %v", err)
	}

	if err := os.Symlink("target", filepath.Join(dir, "current")); err != nil {
		t.Skipf("worker checkout cache uses directory symlinks; this Windows environment cannot create them: %v", err)
	}
}

func readCheckoutTextFile(t *testing.T, path string) string {
	t.Helper()

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read checkout text file %s: %v", path, err)
	}

	return strings.ReplaceAll(string(got), "\r\n", "\n")
}

func TestWorkerCheckoutCacheCachesPersistentRemote(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "cached\n", "cached")

	cache, err := NewWorkerCheckoutCache(workerCheckoutCacheRoot(t), []string{remote})
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

	if got := readCheckoutTextFile(t, filepath.Join(workspace, "README.md")); got != "cached\n" {
		t.Fatalf("workspace README = %q, want cached", got)
	}

	if _, err := os.Stat(filepath.Join(workspace, ".git", "objects", "info", "alternates")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cached checkout should not depend on alternates after clone: err=%v", err)
	}

	if got, want := gitOutput(t, workspace, "remote", "get-url", workerCheckoutCacheRemoteName), filepath.Join(cache.repositoryPath(remote), "current"); got != want {
		t.Fatalf("cache remote url = %q, want %q", got, want)
	}
	assertNoAutoGitMaintenanceConfig(t, workspace)
}

func TestWorkerCheckoutCacheCloneRetriesWithoutHardlinksForLinkFailure(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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

func TestWorkerCheckoutCacheCloneBorrowsObjectsWhenScopedProbeFails(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()
	mirrorPath := filepath.Join(t.TempDir(), "mirror.git")
	var calls [][]string

	mode, reason, borrowed, err := cloneWorkerCheckoutCacheWorkspaceWithRunnerProbeAndBorrow(
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
		true,
	)

	if err != nil {
		t.Fatalf("cloneWorkerCheckoutCacheWorkspaceWithRunnerProbeAndBorrow: %v", err)
	}
	if mode != observability.CheckoutCacheCloneModeBorrowed || reason != observability.CheckoutCacheCloneReasonProbe || !borrowed {
		t.Fatalf("clone mode=%q reason=%q borrowed=%v, want borrowed/probe/true", mode, reason, borrowed)
	}

	if len(calls) != 1 {
		t.Fatalf("clone calls = %d, want 1", len(calls))
	}

	assertStringSliceEqual(t, calls[0], []string{"clone", "--shared", "--no-hardlinks", "--", mirrorPath, "."})
}

func TestWorkerCheckoutCacheCloneDoesNotRetryGenericFailure(t *testing.T) {
	t.Parallel()

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

func TestWorkerCheckoutCacheGitRunnerAppliesCredentialEnv(t *testing.T) {
	binDir := t.TempDir()
	capturePath := filepath.Join(t.TempDir(), "env.txt")
	gitName := "git"
	script := `#!/bin/sh
{
  printf 'prompt=%s\n' "$GIT_TERMINAL_PROMPT"
  printf 'user=%s\n' "$VECTIS_GIT_USERNAME"
} > "$VECTIS_CAPTURE"
`
	if runtime.GOOS == "windows" {
		gitName = "git.cmd"
		script = `@echo off
(
  echo prompt=%GIT_TERMINAL_PROMPT%
  echo user=%VECTIS_GIT_USERNAME%
) > "%VECTIS_CAPTURE%"
`
	}

	gitPath := filepath.Join(binDir, gitName)
	if err := os.WriteFile(gitPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write git shim: %v", err)
	}

	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("VECTIS_CAPTURE", capturePath)

	err := runWorkerCacheGitNoDirWithEnv(context.Background(), []string{"VECTIS_GIT_USERNAME=alice"}, "status")
	if err != nil {
		t.Fatalf("runWorkerCacheGitNoDirWithEnv: %v", err)
	}

	got, err := os.ReadFile(capturePath)
	if err != nil {
		t.Fatalf("read captured env: %v", err)
	}

	normalized := strings.ReplaceAll(string(got), "\r\n", "\n")
	if !strings.Contains(normalized, "prompt=0\n") || !strings.Contains(normalized, "user=alice\n") {
		t.Fatalf("captured env = %q, want prompt and credential env", got)
	}
}

func TestWorkerCacheMirrorGitArgsDisableImplicitMaintenance(t *testing.T) {
	t.Parallel()

	args := workerCacheMirrorGitArgs("/cache/repo.git", "fetch", "origin")
	settings := gitcmd.NoAutoMaintenanceSettings()
	if len(args) < len(settings)*2+4 {
		t.Fatalf("worker cache mirror git args too short: %+v", args)
	}

	for i, setting := range settings {
		if got, want := args[i*2], "-c"; got != want {
			t.Fatalf("worker cache mirror git args[%d]=%q, want %q; args=%+v", i*2, got, want, args)
		}

		if got, want := args[i*2+1], setting[0]+"="+setting[1]; got != want {
			t.Fatalf("worker cache mirror git args[%d]=%q, want %q; args=%+v", i*2+1, got, want, args)
		}
	}

	assertStringSliceEqual(t, args[len(settings)*2:], []string{"--git-dir", "/cache/repo.git", "fetch", "origin"})
}

func TestWorkerCheckoutCacheWarmRemote(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "warmed\n", "warmed")

	cacheRoot := workerCheckoutCacheRoot(t)
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

	currentTarget = filepath.ToSlash(currentTarget)
	if !strings.HasPrefix(currentTarget, "generations/") || !strings.HasSuffix(currentTarget, ".git") {
		t.Fatalf("current generation link target = %q, want generations/*.git", currentTarget)
	}

	generationPath := filepath.Join(repoPath, currentTarget)
	if info, err := os.Stat(generationPath); err != nil || !info.IsDir() {
		t.Fatalf("current generation path = %s, info=%v err=%v", generationPath, info, err)
	}
}

func TestWorkerCheckoutCacheHydratesCanonicalMirrorFromFallback(t *testing.T) {
	t.Parallel()

	primary := initGitRepo(t)
	writeAndCommit(t, primary, "README.md", "primary\n", "primary")

	upstream := filepath.Join(t.TempDir(), "upstream")
	cloneGitRepo(t, primary, upstream)
	git(t, upstream, "config", "user.name", "Vectis Test")
	git(t, upstream, "config", "user.email", "vectis@example.invalid")
	writeAndCommit(t, upstream, "README.md", "upstream\n", "upstream")
	upstreamCommit := gitOutput(t, upstream, "rev-parse", "HEAD")

	cache, err := NewWorkerCheckoutCacheWithRemotes(workerCheckoutCacheRoot(t), []WorkerCheckoutCacheRemote{
		{
			RemoteURL:          primary,
			FallbackRemoteURLs: []string{upstream},
		},
	})

	if err != nil {
		t.Fatalf("NewWorkerCheckoutCacheWithRemotes: %v", err)
	}

	handled, normalizedRemote, err := cache.WarmRemote(context.Background(), primary, nil)
	if err != nil {
		t.Fatalf("WarmRemote: %v", err)
	}

	if !handled || normalizedRemote != primary {
		t.Fatalf("WarmRemote handled=%v normalized=%q, want primary %q", handled, normalizedRemote, primary)
	}

	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	handled, err = cache.Checkout(context.Background(), primary, workspace, nil)
	if err != nil {
		t.Fatalf("Checkout: %v", err)
	}

	if !handled {
		t.Fatal("expected primary remote to be handled by cache")
	}

	if got := gitOutput(t, workspace, "rev-parse", "HEAD"); got != upstreamCommit {
		t.Fatalf("workspace commit = %q, want fallback commit %q", got, upstreamCommit)
	}

	if got := gitOutput(t, workspace, "remote", "get-url", "origin"); got != primary {
		t.Fatalf("origin url = %q, want primary %q", got, primary)
	}
}

func TestWorkerCheckoutCacheRejectsInvalidGenerationRetention(t *testing.T) {
	t.Parallel()

	_, err := NewWorkerCheckoutCache(
		workerCheckoutCacheRoot(t),
		[]string{"https://example.invalid/repo.git"},
		WithWorkerCheckoutCacheGenerationsToKeep(0),
	)
	if err == nil {
		t.Fatal("expected invalid generation retention to fail")
	}
}

func TestWorkerCheckoutCacheRejectsInvalidLeaseTTL(t *testing.T) {
	t.Parallel()

	_, err := NewWorkerCheckoutCache(
		workerCheckoutCacheRoot(t),
		[]string{"https://example.invalid/repo.git"},
		WithWorkerCheckoutCacheLeaseTTL(0),
	)
	if err == nil {
		t.Fatal("expected invalid lease TTL to fail")
	}
}

func TestWorkerCheckoutCacheWarmRemoteFlipsCurrentGeneration(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "first\n", "first")

	cache, err := NewWorkerCheckoutCache(workerCheckoutCacheRoot(t), []string{remote})
	if err != nil {
		t.Fatalf("NewWorkerCheckoutCache: %v", err)
	}

	if handled, _, changed, err := cache.WarmRemoteStatus(context.Background(), remote, nil); err != nil || !handled || !changed {
		t.Fatalf("initial WarmRemoteStatus: handled=%v changed=%v err=%v", handled, changed, err)
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

	if got := readCheckoutTextFile(t, filepath.Join(workspace, "README.md")); got != "second\n" {
		t.Fatalf("workspace README = %q, want second", got)
	}
}

func TestWorkerCheckoutCacheWarmRemoteSkipsUnchangedGeneration(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "first\n", "first")

	cache, err := NewWorkerCheckoutCache(workerCheckoutCacheRoot(t), []string{remote})
	if err != nil {
		t.Fatalf("NewWorkerCheckoutCache: %v", err)
	}

	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("initial WarmRemote: handled=%v err=%v", handled, err)
	}

	repoPath := cache.repositoryPath(remote)
	currentLink := filepath.Join(repoPath, "current")
	firstTarget, err := os.Readlink(currentLink)
	if err != nil {
		t.Fatalf("read first current generation: %v", err)
	}

	if handled, _, changed, err := cache.WarmRemoteStatus(context.Background(), remote, nil); err != nil || !handled || changed {
		t.Fatalf("unchanged WarmRemoteStatus: handled=%v changed=%v err=%v", handled, changed, err)
	}

	secondTarget, err := os.Readlink(currentLink)
	if err != nil {
		t.Fatalf("read second current generation: %v", err)
	}

	if secondTarget != firstTarget {
		t.Fatalf("unchanged warm flipped current generation: got %q, want %q", secondTarget, firstTarget)
	}

	generations, err := workerCheckoutGenerationPaths(repoPath)
	if err != nil {
		t.Fatalf("workerCheckoutGenerationPaths: %v", err)
	}

	if len(generations) != 1 {
		t.Fatalf("generation count after unchanged warm = %d, want 1: %v", len(generations), generations)
	}

	receiving, err := filepath.Glob(filepath.Join(repoPath, "generations", "*"+workerCheckoutReceivingSuffix))
	if err != nil {
		t.Fatalf("glob receiving generations: %v", err)
	}

	if len(receiving) != 0 {
		t.Fatalf("unchanged warm left receiving generations: %v", receiving)
	}
}

func TestWorkerCheckoutCacheAdvertisedRefPreflightMatchesCurrent(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "first\n", "first")
	mainCommit := gitOutput(t, remote, "rev-parse", "HEAD")
	defaultBranch := gitOutput(t, remote, "branch", "--show-current")
	git(t, remote, "notes", "--ref=commits", "add", "-m", "preflight note", mainCommit)
	git(t, remote, "checkout", "-b", "review/preflight")
	writeAndCommit(t, remote, "README.md", "review\n", "review")
	reviewCommit := gitOutput(t, remote, "rev-parse", "HEAD")
	git(t, remote, "update-ref", "refs/changes/01/1", reviewCommit)
	git(t, remote, "checkout", defaultBranch)

	cache, err := NewWorkerCheckoutCache(workerCheckoutCacheRoot(t), []string{remote})
	if err != nil {
		t.Fatalf("NewWorkerCheckoutCache: %v", err)
	}

	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("WarmRemote: handled=%v err=%v", handled, err)
	}

	currentPath := currentWorkerCheckoutGeneration(t, cache, remote)
	matched, err := workerCheckoutCacheRemoteRefsMatchCurrent(context.Background(), currentPath, nil, WorkerCheckoutCacheRemote{RemoteURL: remote})
	if err != nil {
		t.Fatalf("workerCheckoutCacheRemoteRefsMatchCurrent: %v", err)
	}

	if !matched {
		t.Fatal("advertised refs should match current generation")
	}

	writeAndCommit(t, remote, "README.md", "second\n", "second")
	matched, err = workerCheckoutCacheRemoteRefsMatchCurrent(context.Background(), currentPath, nil, WorkerCheckoutCacheRemote{RemoteURL: remote})
	if err != nil {
		t.Fatalf("workerCheckoutCacheRemoteRefsMatchCurrent after change: %v", err)
	}

	if matched {
		t.Fatal("advertised refs matched after remote changed")
	}
}

func TestWorkerCheckoutCacheAdvertisedRefPreflightOverlaysFallbackRefs(t *testing.T) {
	t.Parallel()

	primary := initGitRepo(t)
	writeAndCommit(t, primary, "README.md", "primary\n", "primary")

	fallback := filepath.Join(t.TempDir(), "fallback")
	cloneGitRepo(t, primary, fallback)
	git(t, fallback, "config", "user.name", "Vectis Test")
	git(t, fallback, "config", "user.email", "vectis@example.invalid")
	git(t, fallback, "checkout", "-b", "review/fallback")
	writeAndCommit(t, fallback, "README.md", "fallback\n", "fallback")
	fallbackCommit := gitOutput(t, fallback, "rev-parse", "HEAD")
	git(t, fallback, "update-ref", "refs/pull/456/head", fallbackCommit)

	cache, err := NewWorkerCheckoutCacheWithRemotes(workerCheckoutCacheRoot(t), []WorkerCheckoutCacheRemote{
		{
			RemoteURL:          primary,
			FallbackRemoteURLs: []string{fallback},
		},
	})

	if err != nil {
		t.Fatalf("NewWorkerCheckoutCacheWithRemotes: %v", err)
	}

	if handled, _, err := cache.WarmRemote(context.Background(), primary, nil); err != nil || !handled {
		t.Fatalf("WarmRemote: handled=%v err=%v", handled, err)
	}

	currentPath := currentWorkerCheckoutGeneration(t, cache, primary)
	matched, err := workerCheckoutCacheRemoteRefsMatchCurrent(context.Background(), currentPath, nil, WorkerCheckoutCacheRemote{
		RemoteURL:          primary,
		FallbackRemoteURLs: []string{fallback},
	})

	if err != nil {
		t.Fatalf("workerCheckoutCacheRemoteRefsMatchCurrent: %v", err)
	}

	if !matched {
		t.Fatal("advertised refs with fallback overlay should match current generation")
	}
}

func TestWorkerCheckoutCacheWarmRefspecsLimitInitialMirror(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "main\n", "main")
	mainCommit := gitOutput(t, remote, "rev-parse", "HEAD")
	defaultBranch := gitOutput(t, remote, "branch", "--show-current")
	git(t, remote, "notes", "--ref=commits", "add", "-m", "not warmed", mainCommit)
	git(t, remote, "checkout", "-b", "review/not-warmed")
	writeAndCommit(t, remote, "README.md", "review\n", "review")
	reviewCommit := gitOutput(t, remote, "rev-parse", "HEAD")
	git(t, remote, "update-ref", "refs/pull/789/head", reviewCommit)
	git(t, remote, "checkout", defaultBranch)

	cache, err := NewWorkerCheckoutCacheWithRemotes(workerCheckoutCacheRoot(t), []WorkerCheckoutCacheRemote{
		{
			RemoteURL:    remote,
			WarmRefspecs: []string{"+refs/heads/*:refs/heads/*"},
		},
	})

	if err != nil {
		t.Fatalf("NewWorkerCheckoutCacheWithRemotes: %v", err)
	}

	if handled, _, changed, err := cache.WarmRemoteStatus(context.Background(), remote, nil); err != nil || !handled || !changed {
		t.Fatalf("WarmRemoteStatus: handled=%v changed=%v err=%v", handled, changed, err)
	}

	currentPath := currentWorkerCheckoutGeneration(t, cache, remote)
	if got := gitOutput(t, currentPath, "show-ref", "--verify", "refs/heads/"+defaultBranch); got == "" {
		t.Fatalf("expected default branch ref in warm mirror")
	}

	if err := gitErr(currentPath, "show-ref", "--verify", "refs/notes/commits"); err == nil {
		t.Fatal("notes ref was warmed despite heads-only policy")
	}

	if err := gitErr(currentPath, "show-ref", "--verify", "refs/pull/789/head"); err == nil {
		t.Fatal("provider ref was warmed despite heads-only policy")
	}

	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if handled, err := cache.Checkout(context.Background(), remote, workspace, nil); err != nil || !handled {
		t.Fatalf("Checkout: handled=%v err=%v", handled, err)
	}

	if got := gitOutput(t, workspace, "rev-parse", "HEAD"); got != mainCommit {
		t.Fatalf("workspace HEAD = %q, want %q", got, mainCommit)
	}
}

func TestWorkerCheckoutCacheFetchRefspecsHydratesOutsideWarmPolicy(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "main\n", "main")
	defaultBranch := gitOutput(t, remote, "branch", "--show-current")
	git(t, remote, "checkout", "-b", "review/demand")
	writeAndCommit(t, remote, "README.md", "review\n", "review")
	reviewCommit := gitOutput(t, remote, "rev-parse", "HEAD")
	git(t, remote, "update-ref", "refs/pull/456/head", reviewCommit)
	git(t, remote, "checkout", defaultBranch)

	var hydrations []string
	cache, err := NewWorkerCheckoutCacheWithRemotes(workerCheckoutCacheRoot(t), []WorkerCheckoutCacheRemote{
		{
			RemoteURL:    remote,
			WarmRefspecs: []string{"+refs/heads/*:refs/heads/*"},
		},
	}, WithWorkerCheckoutCacheDemandHydrationRecorder(func(_ context.Context, outcome string) {
		hydrations = append(hydrations, outcome)
	}))

	if err != nil {
		t.Fatalf("NewWorkerCheckoutCacheWithRemotes: %v", err)
	}

	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("WarmRemote: handled=%v err=%v", handled, err)
	}

	firstGeneration := currentWorkerCheckoutGeneration(t, cache, remote)
	if err := gitErr(firstGeneration, "show-ref", "--verify", "refs/pull/456/head"); err == nil {
		t.Fatal("provider ref was unexpectedly present before demand hydration")
	}

	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if handled, err := cache.Checkout(context.Background(), remote, workspace, nil); err != nil || !handled {
		t.Fatalf("Checkout: handled=%v err=%v", handled, err)
	}

	handled, err := cache.FetchRefspecs(context.Background(), remote, workspace, []string{
		"+refs/pull/456/head:refs/vectis/test/pull-456",
	}, nil)

	if err != nil || !handled {
		t.Fatalf("FetchRefspecs: handled=%v err=%v", handled, err)
	}

	if got := gitOutput(t, workspace, "rev-parse", "refs/vectis/test/pull-456"); got != reviewCommit {
		t.Fatalf("demand hydrated ref = %q, want %q", got, reviewCommit)
	}

	currentPath := currentWorkerCheckoutGeneration(t, cache, remote)
	if currentPath == firstGeneration {
		t.Fatal("demand hydration did not install a new generation")
	}

	if got := gitOutput(t, currentPath, "rev-parse", "refs/pull/456/head"); got != reviewCommit {
		t.Fatalf("cache provider ref = %q, want %q", got, reviewCommit)
	}

	assertStringSliceContains(t, hydrations, observability.CheckoutCacheDemandHydrationOutcomeSuccess)
}

func TestWorkerCheckoutCacheLargeRepoWorkflowKeepsWarmSetNarrowAndHydratesDynamicRefs(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "main\n", "main")
	for i := 0; i < 4; i++ {
		writeAndCommit(t, remote, filepath.Join("packages", "service", "file-"+string(rune('a'+i))+".txt"), string(bytes.Repeat([]byte{'a' + byte(i)}, 4096)), "large shard")
	}

	mainCommit := gitOutput(t, remote, "rev-parse", "HEAD")
	defaultBranch := gitOutput(t, remote, "branch", "--show-current")
	git(t, remote, "notes", "--ref=commits", "add", "-m", "late analysis payload", mainCommit)

	git(t, remote, "checkout", "-b", "review/large-dynamic")
	writeAndCommit(t, remote, "README.md", "review\n", "review")
	reviewCommit := gitOutput(t, remote, "rev-parse", "HEAD")
	git(t, remote, "update-ref", "refs/pull/900/head", reviewCommit)
	git(t, remote, "update-ref", "refs/changes/00/900/1", reviewCommit)
	git(t, remote, "checkout", defaultBranch)

	var hydrations []string
	var evictions []string
	cache, err := NewWorkerCheckoutCacheWithRemotes(workerCheckoutCacheRoot(t), []WorkerCheckoutCacheRemote{
		{
			RemoteURL:    remote,
			WarmRefspecs: []string{"+refs/heads/*:refs/heads/*"},
		},
	},
		WithWorkerCheckoutCacheGenerationsToKeep(2),
		WithWorkerCheckoutCacheDemandHydrationRecorder(func(_ context.Context, outcome string) {
			hydrations = append(hydrations, outcome)
		}),
		WithWorkerCheckoutCacheGenerationEvictionRecorder(func(_ context.Context, reason string) {
			evictions = append(evictions, reason)
		}),
	)

	if err != nil {
		t.Fatalf("NewWorkerCheckoutCacheWithRemotes: %v", err)
	}

	if handled, _, changed, err := cache.WarmRemoteStatus(context.Background(), remote, nil); err != nil || !handled || !changed {
		t.Fatalf("WarmRemoteStatus: handled=%v changed=%v err=%v", handled, changed, err)
	}

	currentPath := currentWorkerCheckoutGeneration(t, cache, remote)
	if got := gitOutput(t, currentPath, "rev-parse", "refs/heads/"+defaultBranch); got != mainCommit {
		t.Fatalf("warm head = %q, want %q", got, mainCommit)
	}

	for _, ref := range []string{"refs/notes/commits", "refs/pull/900/head", "refs/changes/00/900/1"} {
		if err := gitErr(currentPath, "show-ref", "--verify", ref); err == nil {
			t.Fatalf("%s was warmed despite heads-only policy", ref)
		}
	}

	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if handled, err := cache.Checkout(context.Background(), remote, workspace, nil); err != nil || !handled {
		t.Fatalf("Checkout: handled=%v err=%v", handled, err)
	}

	handled, err := cache.FetchRefspecs(context.Background(), remote, workspace, []string{
		"+refs/notes/*:refs/notes/*",
		"+refs/pull/900/head:refs/vectis/test/pull-900",
		"+refs/changes/00/900/1:refs/vectis/test/change-900",
	}, nil)

	if err != nil || !handled {
		t.Fatalf("FetchRefspecs: handled=%v err=%v", handled, err)
	}

	if got := gitOutput(t, workspace, "notes", "--ref=commits", "show", mainCommit); got != "late analysis payload" {
		t.Fatalf("fetched note = %q, want late analysis payload", got)
	}

	if got := gitOutput(t, workspace, "rev-parse", "refs/vectis/test/pull-900"); got != reviewCommit {
		t.Fatalf("fetched pull ref = %q, want %q", got, reviewCommit)
	}

	if got := gitOutput(t, workspace, "rev-parse", "refs/vectis/test/change-900"); got != reviewCommit {
		t.Fatalf("fetched change ref = %q, want %q", got, reviewCommit)
	}

	assertStringSliceContains(t, hydrations, observability.CheckoutCacheDemandHydrationOutcomeSuccess)
	hydratedGeneration := currentWorkerCheckoutGeneration(t, cache, remote)
	writeAndCommit(t, remote, "README.md", "main-2\n", "main 2")
	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("second WarmRemote: handled=%v err=%v", handled, err)
	}

	writeAndCommit(t, remote, "README.md", "main-3\n", "main 3")
	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("third WarmRemote: handled=%v err=%v", handled, err)
	}

	if _, err := os.Stat(hydratedGeneration); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("hydrated generation still exists after churn retention cleanup: err=%v", err)
	}

	assertStringSliceContains(t, evictions, observability.CheckoutCacheEvictionReasonRetention)
}

func TestWorkerCheckoutCacheCheckoutSelfHealsCorruptCurrentGeneration(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "healthy\n", "healthy")
	git(t, remote, "gc", "--aggressive")

	var evictions []string
	var selfHeals []string
	cache, err := NewWorkerCheckoutCache(workerCheckoutCacheRoot(t), []string{remote},
		WithWorkerCheckoutCacheGenerationEvictionRecorder(func(_ context.Context, reason string) {
			evictions = append(evictions, reason)
		}),
		WithWorkerCheckoutCacheSelfHealRecorder(func(_ context.Context, operation, outcome string) {
			selfHeals = append(selfHeals, operation+":"+outcome)
		}),
	)

	if err != nil {
		t.Fatalf("NewWorkerCheckoutCache: %v", err)
	}

	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("WarmRemote: handled=%v err=%v", handled, err)
	}

	corruptGeneration := currentWorkerCheckoutGeneration(t, cache, remote)
	packFiles, err := filepath.Glob(filepath.Join(corruptGeneration, "objects", "pack", "*.pack"))
	if err != nil {
		t.Fatalf("glob pack files: %v", err)
	}

	if len(packFiles) == 0 {
		t.Fatal("expected pack files to corrupt")
	}

	for _, packFile := range packFiles {
		if err := os.Remove(packFile); err != nil {
			t.Fatalf("remove pack file %s: %v", packFile, err)
		}
	}

	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if handled, err := cache.Checkout(context.Background(), remote, workspace, nil); err != nil || !handled {
		t.Fatalf("Checkout self-heal: handled=%v err=%v", handled, err)
	}

	if got := readCheckoutTextFile(t, filepath.Join(workspace, "README.md")); got != "healthy\n" {
		t.Fatalf("workspace README = %q, want healthy", got)
	}

	if healedGeneration := currentWorkerCheckoutGeneration(t, cache, remote); healedGeneration == corruptGeneration {
		t.Fatal("checkout self-heal did not replace corrupt generation")
	}

	if _, err := os.Stat(corruptGeneration); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupt generation still exists after self-heal: err=%v", err)
	}

	assertStringSliceContains(t, evictions, observability.CheckoutCacheEvictionReasonCorrupt)
	assertStringSliceContains(t, selfHeals, observability.CheckoutCacheSelfHealOperationCheckout+":"+observability.CheckoutCacheSelfHealOutcomeSuccess)
}

func TestWorkerCheckoutCacheCleanupHonorsConfiguredRetention(t *testing.T) {
	t.Parallel()

	remote := "https://example.invalid/large.git"
	var evictions []string
	cache, err := NewWorkerCheckoutCache(
		workerCheckoutCacheRoot(t),
		[]string{remote},
		WithWorkerCheckoutCacheGenerationsToKeep(3),
		WithWorkerCheckoutCacheGenerationEvictionRecorder(func(_ context.Context, reason string) {
			evictions = append(evictions, reason)
		}),
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
	assertStringSliceContains(t, evictions, observability.CheckoutCacheEvictionReasonRetention)
}

func TestWorkerCheckoutCacheCleanupHonorsPackByteBudgetAndLeases(t *testing.T) {
	t.Parallel()

	remote := "https://example.invalid/large.git"
	var evictions []string
	cache, err := NewWorkerCheckoutCache(
		workerCheckoutCacheRoot(t),
		[]string{remote},
		WithWorkerCheckoutCacheGenerationsToKeep(10),
		WithWorkerCheckoutCacheMaxBytes(20),
		WithWorkerCheckoutCacheGenerationEvictionRecorder(func(_ context.Context, reason string) {
			evictions = append(evictions, reason)
		}),
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
		packPath := filepath.Join(repoPath, "generations", name, "objects", "pack")
		if err := os.MkdirAll(packPath, 0o755); err != nil {
			t.Fatalf("create pack dir %s: %v", packPath, err)
		}

		if err := os.WriteFile(filepath.Join(packPath, "pack-test.pack"), bytes.Repeat([]byte{'x'}, 10), 0o644); err != nil {
			t.Fatalf("write pack file: %v", err)
		}
	}

	if err := os.Symlink(filepath.Join("generations", generationNames[2]), filepath.Join(repoPath, "current")); err != nil {
		t.Fatalf("create current generation link: %v", err)
	}

	leaseDir := filepath.Join(repoPath, "leases", generationNames[0])
	if err := os.MkdirAll(leaseDir, 0o755); err != nil {
		t.Fatalf("create lease dir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(leaseDir, "active"), []byte("leased"), 0o644); err != nil {
		t.Fatalf("write active lease: %v", err)
	}

	if err := cache.cleanupOldGenerations(context.Background(), repoPath); err != nil {
		t.Fatalf("cleanupOldGenerations: %v", err)
	}

	if info, err := os.Stat(filepath.Join(repoPath, "generations", generationNames[0])); err != nil || !info.IsDir() {
		t.Fatalf("leased generation was removed: info=%v err=%v", info, err)
	}

	if _, err := os.Stat(filepath.Join(repoPath, "generations", generationNames[1])); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unleased generation still exists after budget cleanup: err=%v", err)
	}

	if info, err := os.Stat(filepath.Join(repoPath, "generations", generationNames[2])); err != nil || !info.IsDir() {
		t.Fatalf("current generation was removed: info=%v err=%v", info, err)
	}
	assertStringSliceContains(t, evictions, observability.CheckoutCacheEvictionReasonBudget)
}

func TestWorkerCheckoutCacheStatsReportsRootFootprint(t *testing.T) {
	t.Parallel()

	remote := "https://example.invalid/large.git"
	cache, err := NewWorkerCheckoutCache(
		workerCheckoutCacheRoot(t),
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
	t.Parallel()

	remote := "https://example.invalid/large.git"
	cache, err := NewWorkerCheckoutCache(
		workerCheckoutCacheRoot(t),
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
	t.Parallel()

	remote := "https://example.invalid/large.git"
	ttl := time.Hour
	cache, err := NewWorkerCheckoutCache(
		workerCheckoutCacheRoot(t),
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
	t.Parallel()

	remote := "https://example.invalid/large.git"
	leaseTTL := 120 * time.Millisecond
	cache, err := NewWorkerCheckoutCache(
		workerCheckoutCacheRoot(t),
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
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "first\n", "first")

	cache, err := NewWorkerCheckoutCache(workerCheckoutCacheRoot(t), []string{remote})
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
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "first\n", "first")

	cache, err := NewWorkerCheckoutCache(workerCheckoutCacheRoot(t), []string{remote})
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

func TestWorkerCheckoutCacheScopedCheckoutKeepsBorrowedGeneration(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "first\n", "first")

	cache, err := NewWorkerCheckoutCache(
		workerCheckoutCacheRoot(t),
		[]string{remote},
		WithWorkerCheckoutCacheGenerationsToKeep(1),
	)

	if err != nil {
		t.Fatalf("NewWorkerCheckoutCache: %v", err)
	}

	cache.hardlinkProbe = func(_, _ string) bool { return false }
	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("initial WarmRemote: handled=%v err=%v", handled, err)
	}

	firstGeneration := currentWorkerCheckoutGeneration(t, cache, remote)
	scoped, err := cache.NewCheckoutCacheScope()
	if err != nil {
		t.Fatalf("NewCheckoutCacheScope: %v", err)
	}

	scope, ok := scoped.(*WorkerCheckoutCacheScope)
	if !ok {
		t.Fatalf("checkout cache scope type = %T, want *WorkerCheckoutCacheScope", scoped)
	}

	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if handled, err := scoped.Checkout(context.Background(), remote, workspace, nil); err != nil || !handled {
		t.Fatalf("scoped Checkout: handled=%v err=%v", handled, err)
	}

	if _, err := os.Stat(filepath.Join(workspace, ".git", "objects", "info", "alternates")); err != nil {
		t.Fatalf("borrowed checkout alternates: %v", err)
	}

	writeAndCommit(t, remote, "README.md", "second\n", "second")
	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("second WarmRemote: handled=%v err=%v", handled, err)
	}

	if info, err := os.Stat(firstGeneration); err != nil || !info.IsDir() {
		t.Fatalf("borrowed generation was removed while scope open: info=%v err=%v", info, err)
	}

	if err := scope.Close(); err != nil {
		t.Fatalf("close checkout cache scope: %v", err)
	}

	writeAndCommit(t, remote, "README.md", "third\n", "third")
	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("third WarmRemote: handled=%v err=%v", handled, err)
	}

	if _, err := os.Stat(firstGeneration); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("borrowed generation still exists after scope close: err=%v", err)
	}
}

func TestWorkerCheckoutCacheCheckoutSurvivesGenerationCleanup(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "first\n", "first")
	firstCommit := gitOutput(t, remote, "rev-parse", "HEAD")

	cache, err := NewWorkerCheckoutCache(workerCheckoutCacheRoot(t), []string{remote})
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
	t.Parallel()

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

	cache, err := NewWorkerCheckoutCache(workerCheckoutCacheRoot(t), []string{remote})
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

func TestWorkerCheckoutCacheFetchRefspecsRefreshesStaleMirror(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "main\n", "main")
	mainCommit := gitOutput(t, remote, "rev-parse", "HEAD")

	cache, err := NewWorkerCheckoutCache(workerCheckoutCacheRoot(t), []string{remote})
	if err != nil {
		t.Fatalf("NewWorkerCheckoutCache: %v", err)
	}

	if handled, _, err := cache.WarmRemote(context.Background(), remote, nil); err != nil || !handled {
		t.Fatalf("initial WarmRemote: handled=%v err=%v", handled, err)
	}

	git(t, remote, "notes", "--ref=commits", "add", "-m", "late cached note", mainCommit)

	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if handled, err := cache.Checkout(context.Background(), remote, workspace, nil); err != nil || !handled {
		t.Fatalf("Checkout: handled=%v err=%v", handled, err)
	}

	handled, err := cache.FetchRefspecs(context.Background(), remote, workspace, []string{
		"+refs/notes/*:refs/notes/*",
	}, nil)

	if err != nil || !handled {
		t.Fatalf("FetchRefspecs: handled=%v err=%v", handled, err)
	}

	if got := gitOutput(t, workspace, "notes", "--ref=commits", "show", mainCommit); got != "late cached note" {
		t.Fatalf("fetched note = %q, want late cached note", got)
	}
}

func TestWorkerCheckoutCacheCheckoutUsesCurrentGenerationWhileWarmLocked(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "cached\n", "cached")

	cache, err := NewWorkerCheckoutCache(workerCheckoutCacheRoot(t), []string{remote})
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
	case <-time.After(workerCheckoutCacheFastCheckoutDeadline()):
		t.Fatal("checkout waited behind worker cache writer lock despite current generation")
	}
}

func workerCheckoutCacheFastCheckoutDeadline() time.Duration {
	if runtime.GOOS == "windows" {
		return 10 * time.Second
	}

	return 2 * time.Second
}

func TestWorkerCheckoutCacheIgnoresUnconfiguredRemote(t *testing.T) {
	t.Parallel()

	cache, err := NewWorkerCheckoutCache(workerCheckoutCacheRoot(t), []string{"https://example.invalid/persistent.git"})
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

func assertStringSliceContains(t *testing.T, got []string, want string) {
	t.Helper()

	for _, value := range got {
		if value == want {
			return
		}
	}

	t.Fatalf("slice %v does not contain %q", got, want)
}
