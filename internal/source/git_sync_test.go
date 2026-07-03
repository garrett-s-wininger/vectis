package source

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"vectis/internal/gitcmd"
)

func TestSyncManagedGitCheckoutClonesAndFetches(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "first\n", "first")
	firstCommit := gitOutput(t, remote, "rev-parse", "HEAD")
	branch := gitOutput(t, remote, "branch", "--show-current")

	checkoutPath := filepath.Join(t.TempDir(), "managed")
	status := SyncManagedGitCheckout(context.Background(), ManagedGitCheckoutRequest{
		CheckoutPath: checkoutPath,
		RemoteURL:    remote,
		DefaultRef:   branch,
	})

	if status.ErrorCode != "" {
		t.Fatalf("sync clone failed: %+v", status)
	}
	if !status.PathExists || !status.PathIsDirectory || !status.GitRepository || !status.DefaultRefResolved || status.ResolvedCommit != firstCommit {
		t.Fatalf("clone status mismatch: %+v", status)
	}

	writeAndCommit(t, remote, "README.md", "second\n", "second")
	secondCommit := gitOutput(t, remote, "rev-parse", "HEAD")

	status = SyncManagedGitCheckout(context.Background(), ManagedGitCheckoutRequest{
		CheckoutPath: checkoutPath,
		RemoteURL:    remote,
		DefaultRef:   branch,
	})

	if status.ErrorCode != "" {
		t.Fatalf("sync fetch failed: %+v", status)
	}
	if status.ResolvedCommit != secondCommit {
		t.Fatalf("fetch should advance HEAD: got %q, want %q; status=%+v", status.ResolvedCommit, secondCommit, status)
	}

	file, err := NewGitCheckout(checkoutPath).ReadFile(context.Background(), Revision{Commit: secondCommit}, "README.md")
	if err != nil {
		t.Fatalf("ReadFile after managed sync: %v", err)
	}
	if got, want := string(file.Content), "second\n"; got != want {
		t.Fatalf("managed checkout content: got %q, want %q", got, want)
	}
}

func TestSyncManagedGitCheckoutDisablesAutomaticMaintenanceAndBroadTags(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "first\n", "first")
	branch := gitOutput(t, remote, "branch", "--show-current")
	git(t, remote, "tag", "ignored/v1")

	checkoutPath := filepath.Join(t.TempDir(), "managed")
	status := SyncManagedGitCheckout(context.Background(), ManagedGitCheckoutRequest{
		CheckoutPath: checkoutPath,
		RemoteURL:    remote,
		DefaultRef:   branch,
	})

	if status.ErrorCode != "" {
		t.Fatalf("sync clone failed: %+v", status)
	}

	assertNoAutoGitMaintenanceConfig(t, checkoutPath)
	if got := gitOutput(t, checkoutPath, "config", "--get", "remote.origin.tagOpt"); got != "--no-tags" {
		t.Fatalf("managed checkout config remote.origin.tagOpt: got %q, want --no-tags", got)
	}

	if got := strings.TrimSpace(gitOutput(t, checkoutPath, "tag", "--list")); got != "" {
		t.Fatalf("managed checkout should not fetch broad tags, got %q", got)
	}

	writeAndCommit(t, remote, "README.md", "second\n", "second")
	git(t, remote, "tag", "ignored/v2")

	status = SyncManagedGitCheckout(context.Background(), ManagedGitCheckoutRequest{
		CheckoutPath: checkoutPath,
		RemoteURL:    remote,
		DefaultRef:   branch,
	})

	if status.ErrorCode != "" {
		t.Fatalf("sync fetch failed: %+v", status)
	}

	if got := strings.TrimSpace(gitOutput(t, checkoutPath, "tag", "--list")); got != "" {
		t.Fatalf("managed checkout fetch should not fetch broad tags, got %q", got)
	}
}

func assertNoAutoGitMaintenanceConfig(t *testing.T, checkoutPath string) {
	t.Helper()

	for _, setting := range gitcmd.NoAutoMaintenanceSettings() {
		if got := gitOutput(t, checkoutPath, "config", "--get", setting[0]); got != setting[1] {
			t.Fatalf("git config %s: got %q, want %q", setting[0], got, setting[1])
		}
	}
}

func TestManagedGitCommandArgsDisableImplicitMaintenance(t *testing.T) {
	t.Parallel()

	args := managedGitCommandArgs("fetch", "origin")
	settings := gitcmd.NoAutoMaintenanceSettings()
	if len(args) < len(settings)*2+2 {
		t.Fatalf("managed git args too short: %+v", args)
	}

	for i, setting := range settings {
		if got, want := args[i*2], "-c"; got != want {
			t.Fatalf("managed git args[%d]=%q, want %q; args=%+v", i*2, got, want, args)
		}

		if got, want := args[i*2+1], setting[0]+"="+setting[1]; got != want {
			t.Fatalf("managed git args[%d]=%q, want %q; args=%+v", i*2+1, got, want, args)
		}
	}

	if got := strings.Join(args[len(settings)*2:], " "); got != "fetch origin" {
		t.Fatalf("managed git command tail=%q, want fetch origin; args=%+v", got, args)
	}
}

func TestSyncManagedGitCheckoutFetchesPlainDefaultTagOnDemand(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "release\n", "release")
	releaseCommit := gitOutput(t, remote, "rev-parse", "HEAD")
	git(t, remote, "tag", "release/v1")
	git(t, remote, "tag", "ignored/v1")

	checkoutPath := filepath.Join(t.TempDir(), "managed")
	status := SyncManagedGitCheckout(context.Background(), ManagedGitCheckoutRequest{
		CheckoutPath: checkoutPath,
		RemoteURL:    remote,
		DefaultRef:   "release/v1",
	})

	if status.ErrorCode != "" {
		t.Fatalf("sync clone with plain default tag failed: %+v", status)
	}

	if !status.DefaultRefResolved || status.ResolvedCommit != releaseCommit {
		t.Fatalf("plain default tag status mismatch: %+v", status)
	}

	if got := strings.TrimSpace(gitOutput(t, checkoutPath, "tag", "--list")); got != "release/v1" {
		t.Fatalf("managed checkout should fetch only requested default tag, got %q", got)
	}
}

func TestSyncManagedGitCheckoutFetchesExplicitDefaultTagOnDemand(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "release\n", "release")
	releaseCommit := gitOutput(t, remote, "rev-parse", "HEAD")
	git(t, remote, "tag", "release/v1")
	git(t, remote, "tag", "ignored/v1")

	checkoutPath := filepath.Join(t.TempDir(), "managed")
	status := SyncManagedGitCheckout(context.Background(), ManagedGitCheckoutRequest{
		CheckoutPath: checkoutPath,
		RemoteURL:    remote,
		DefaultRef:   "refs/tags/release/v1",
	})

	if status.ErrorCode != "" {
		t.Fatalf("sync clone with explicit default tag failed: %+v", status)
	}

	if !status.DefaultRefResolved || status.ResolvedCommit != releaseCommit {
		t.Fatalf("explicit default tag status mismatch: %+v", status)
	}

	if got := strings.TrimSpace(gitOutput(t, checkoutPath, "tag", "--list")); got != "release/v1" {
		t.Fatalf("managed checkout should fetch only requested explicit default tag, got %q", got)
	}
}

func TestHydrateManagedGitRefFetchesOneMissingBranch(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "main\n", "main")
	defaultBranch := gitOutput(t, remote, "branch", "--show-current")

	checkoutPath := filepath.Join(t.TempDir(), "managed")
	status := SyncManagedGitCheckout(context.Background(), ManagedGitCheckoutRequest{
		CheckoutPath: checkoutPath,
		RemoteURL:    remote,
		DefaultRef:   defaultBranch,
	})

	if status.ErrorCode != "" {
		t.Fatalf("initial sync failed: %+v", status)
	}

	git(t, remote, "checkout", "-b", "feature/on-demand")
	writeAndCommit(t, remote, "README.md", "feature\n", "feature")
	featureCommit := gitOutput(t, remote, "rev-parse", "HEAD")
	git(t, remote, "checkout", defaultBranch)

	managed := NewManagedGitCheckout(checkoutPath)
	if _, err := managed.ResolveRevision(context.Background(), "feature/on-demand"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected feature branch to be missing before hydration, got %v", err)
	}

	status = HydrateManagedGitRef(context.Background(), ManagedGitRefHydrationRequest{
		CheckoutPath: checkoutPath,
		Ref:          "feature/on-demand",
	})

	if status.ErrorCode != "" {
		t.Fatalf("hydrate missing feature branch failed: %+v", status)
	}

	if !status.DefaultRefResolved || status.ResolvedCommit != featureCommit {
		t.Fatalf("hydrated feature branch status mismatch: %+v", status)
	}

	if got := gitOutput(t, checkoutPath, "for-each-ref", "--format=%(refname)", "refs/vectis/candidates"); got != "" {
		t.Fatalf("hydrate should clean candidate refs, got %q", got)
	}

	rev, err := managed.ResolveRevision(context.Background(), "feature/on-demand")
	if err != nil {
		t.Fatalf("resolve hydrated feature branch: %v", err)
	}

	if rev.Commit != featureCommit {
		t.Fatalf("hydrated feature branch commit: got %q, want %q", rev.Commit, featureCommit)
	}
}

func TestHydrateManagedGitRefDoesNotPublishFailedCandidate(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "main\n", "main")
	defaultBranch := gitOutput(t, remote, "branch", "--show-current")

	checkoutPath := filepath.Join(t.TempDir(), "managed")
	status := SyncManagedGitCheckout(context.Background(), ManagedGitCheckoutRequest{
		CheckoutPath: checkoutPath,
		RemoteURL:    remote,
		DefaultRef:   defaultBranch,
	})

	if status.ErrorCode != "" {
		t.Fatalf("initial sync failed: %+v", status)
	}

	status = HydrateManagedGitRef(context.Background(), ManagedGitRefHydrationRequest{
		CheckoutPath: checkoutPath,
		Ref:          "feature/missing",
	})

	if status.ErrorCode != "source_ref_not_found" {
		t.Fatalf("expected fetch failure hydrating missing branch, got %+v", status)
	}

	if got := gitOutput(t, checkoutPath, "for-each-ref", "--format=%(refname)", "refs/remotes/origin/feature/missing"); got != "" {
		t.Fatalf("failed hydrate should not publish destination ref, got %q", got)
	}

	if got := gitOutput(t, checkoutPath, "for-each-ref", "--format=%(refname)", "refs/vectis/candidates"); got != "" {
		t.Fatalf("failed hydrate should not leave candidate refs, got %q", got)
	}
}

func TestHydrateManagedGitRefFallsBackAcrossReplicaRemotes(t *testing.T) {
	t.Parallel()

	mirror := initGitRepo(t)
	writeAndCommit(t, mirror, "README.md", "main\n", "main")
	defaultBranch := gitOutput(t, mirror, "branch", "--show-current")

	upstream := filepath.Join(t.TempDir(), "upstream")
	cloneGitRepo(t, mirror, upstream)
	git(t, upstream, "config", "user.name", "Vectis Test")
	git(t, upstream, "config", "user.email", "vectis@example.invalid")
	git(t, upstream, "config", "commit.gpgsign", "false")

	checkoutPath := filepath.Join(t.TempDir(), "managed")
	status := SyncManagedGitCheckout(context.Background(), ManagedGitCheckoutRequest{
		CheckoutPath: checkoutPath,
		RemoteURL:    mirror,
		DefaultRef:   defaultBranch,
	})

	if status.ErrorCode != "" {
		t.Fatalf("initial sync failed: %+v", status)
	}

	git(t, upstream, "checkout", "-b", "feature/upstream-only")
	writeAndCommit(t, upstream, "README.md", "feature\n", "feature")
	featureCommit := gitOutput(t, upstream, "rev-parse", "HEAD")
	git(t, upstream, "checkout", defaultBranch)

	status = HydrateManagedGitRef(context.Background(), ManagedGitRefHydrationRequest{
		CheckoutPath:       checkoutPath,
		Ref:                "feature/upstream-only",
		FallbackRemoteURLs: []string{mirror, upstream},
	})

	if status.ErrorCode != "" {
		t.Fatalf("hydrate via fallback remote failed: %+v", status)
	}

	if !status.DefaultRefResolved || status.ResolvedCommit != featureCommit {
		t.Fatalf("fallback hydrated feature status mismatch: %+v", status)
	}

	if status.HydrationRemote != "vectis-fallback-2" || status.HydrationTier != "fallback-2" {
		t.Fatalf("fallback hydration evidence: got remote=%q tier=%q", status.HydrationRemote, status.HydrationTier)
	}

	if got := gitOutput(t, checkoutPath, "remote", "get-url", "vectis-fallback-1"); got != mirror {
		t.Fatalf("configured first fallback remote: got %q, want %q", got, mirror)
	}

	if got := gitOutput(t, checkoutPath, "remote", "get-url", "vectis-fallback-2"); got != upstream {
		t.Fatalf("configured second fallback remote: got %q, want %q", got, upstream)
	}

	status = HydrateManagedGitRef(context.Background(), ManagedGitRefHydrationRequest{
		CheckoutPath:       checkoutPath,
		Ref:                "feature/upstream-only",
		FallbackRemoteURLs: []string{upstream},
	})

	if status.ErrorCode != "" {
		t.Fatalf("hydrate after fallback remote update failed: %+v", status)
	}

	if status.HydrationRemote != "vectis-fallback-1" || status.HydrationTier != "fallback-1" {
		t.Fatalf("updated fallback hydration evidence: got remote=%q tier=%q", status.HydrationRemote, status.HydrationTier)
	}

	if got := gitOutput(t, checkoutPath, "remote", "get-url", "vectis-fallback-1"); got != upstream {
		t.Fatalf("rewritten first fallback remote: got %q, want %q", got, upstream)
	}

	if got := gitOutput(t, checkoutPath, "remote"); strings.Contains(got, "vectis-fallback-2") {
		t.Fatalf("fallback remote update should remove stale tier 2 remote, got remotes:\n%s", got)
	}

	if got := gitOutput(t, checkoutPath, "for-each-ref", "--format=%(refname)", "refs/vectis/candidates"); got != "" {
		t.Fatalf("fallback hydrate should clean candidate refs, got %q", got)
	}

	if got := gitOutput(t, checkoutPath, "rev-parse", "refs/remotes/origin/feature/upstream-only"); got != featureCommit {
		t.Fatalf("fallback hydrate should publish managed origin ref: got %q, want %q", got, featureCommit)
	}
}

func TestHydrateManagedGitRefFetchesProviderRefs(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "main\n", "main")
	defaultBranch := gitOutput(t, remote, "branch", "--show-current")

	checkoutPath := filepath.Join(t.TempDir(), "managed")
	status := SyncManagedGitCheckout(context.Background(), ManagedGitCheckoutRequest{
		CheckoutPath: checkoutPath,
		RemoteURL:    remote,
		DefaultRef:   defaultBranch,
	})

	if status.ErrorCode != "" {
		t.Fatalf("initial sync failed: %+v", status)
	}

	git(t, remote, "checkout", "-b", "review/provider-ref")
	writeAndCommit(t, remote, "README.md", "review\n", "review")
	reviewCommit := gitOutput(t, remote, "rev-parse", "HEAD")
	git(t, remote, "update-ref", "refs/pull/123/head", reviewCommit)
	git(t, remote, "update-ref", "refs/changes/34/1234/5", reviewCommit)
	git(t, remote, "checkout", defaultBranch)

	managed := NewManagedGitCheckout(checkoutPath)
	for _, ref := range []string{"refs/pull/123/head", "refs/changes/34/1234/5"} {
		t.Run(ref, func(t *testing.T) {
			if _, err := managed.ResolveRevision(context.Background(), ref); !errors.Is(err, ErrNotFound) {
				t.Fatalf("provider ref should be missing before hydration, got %v", err)
			}

			status := HydrateManagedGitRef(context.Background(), ManagedGitRefHydrationRequest{
				CheckoutPath: checkoutPath,
				Ref:          ref,
			})

			if status.ErrorCode != "" {
				t.Fatalf("hydrate provider ref failed: %+v", status)
			}

			if !status.DefaultRefResolved || status.ResolvedCommit != reviewCommit {
				t.Fatalf("provider ref status mismatch: %+v", status)
			}

			if got := gitOutput(t, checkoutPath, "rev-parse", managedGitHydratedRef(ref)); got != reviewCommit {
				t.Fatalf("hydrated provider ref target: got %q, want %q", got, reviewCommit)
			}

			if _, err := NewGitCheckout(checkoutPath).ResolveRevision(context.Background(), ref); !errors.Is(err, ErrNotFound) {
				t.Fatalf("plain checkout should not resolve provider hydrated ref, got %v", err)
			}

			rev, err := managed.ResolveRevision(context.Background(), ref)
			if err != nil {
				t.Fatalf("resolve hydrated provider ref: %v", err)
			}

			if rev.Commit != reviewCommit {
				t.Fatalf("hydrated provider ref commit: got %q, want %q", rev.Commit, reviewCommit)
			}
		})
	}
}

func TestHydrateManagedGitContextFetchesAuxiliaryNotesRef(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "main\n", "main")
	defaultBranch := gitOutput(t, remote, "branch", "--show-current")

	checkoutPath := filepath.Join(t.TempDir(), "managed")
	status := SyncManagedGitCheckout(context.Background(), ManagedGitCheckoutRequest{
		CheckoutPath: checkoutPath,
		RemoteURL:    remote,
		DefaultRef:   defaultBranch,
	})

	if status.ErrorCode != "" {
		t.Fatalf("initial sync failed: %+v", status)
	}

	git(t, remote, "checkout", "-b", "review/notes")
	writeAndCommit(t, remote, "README.md", "review\n", "review")
	reviewCommit := gitOutput(t, remote, "rev-parse", "HEAD")
	git(t, remote, "notes", "--ref=commits", "add", "-m", "review note", reviewCommit)
	git(t, remote, "update-ref", "refs/pull/789/head", reviewCommit)
	git(t, remote, "checkout", defaultBranch)

	if got := gitOutput(t, checkoutPath, "for-each-ref", "--format=%(refname)", "refs/notes"); got != "" {
		t.Fatalf("managed checkout should not have notes before context hydration, got %q", got)
	}

	status = HydrateManagedGitContext(context.Background(), ManagedGitRefHydrationRequest{
		CheckoutPath:  checkoutPath,
		Ref:           "refs/pull/789/head",
		AuxiliaryRefs: []string{"refs/notes/commits"},
	})

	if status.ErrorCode != "" {
		t.Fatalf("hydrate provider ref with notes failed: %+v", status)
	}

	if !status.DefaultRefResolved || status.ResolvedCommit != reviewCommit {
		t.Fatalf("provider ref status mismatch: %+v", status)
	}

	if len(status.AuxiliaryRefs) != 1 || status.AuxiliaryRefs[0] != "refs/notes/commits" {
		t.Fatalf("hydrated auxiliary refs mismatch: %+v", status.AuxiliaryRefs)
	}

	managed := NewManagedGitCheckout(checkoutPath)
	commits, err := managed.ListCommits(context.Background(), ListCommitsOptions{
		Ref:   "refs/pull/789/head",
		Limit: 1,
	})

	if err != nil {
		t.Fatalf("ListCommits hydrated provider ref: %v", err)
	}

	if len(commits.Commits) != 1 || commits.Commits[0].Commit != reviewCommit || commits.Commits[0].Subject != "review" {
		t.Fatalf("hydrated provider commit listing mismatch: %+v", commits)
	}

	note, err := managed.ReadNote(context.Background(), ReadNoteOptions{
		Ref:      "refs/pull/789/head",
		NotesRef: "refs/notes/commits",
	})

	if err != nil {
		t.Fatalf("ReadNote hydrated provider ref: %v", err)
	}

	if got, want := string(note.Content), "review note\n"; got != want {
		t.Fatalf("hydrated note: got %q, want %q", got, want)
	}
}

func TestHydrateManagedGitContextRejectsUnsupportedAuxiliaryRefs(t *testing.T) {
	t.Parallel()

	status := HydrateManagedGitContext(context.Background(), ManagedGitRefHydrationRequest{
		CheckoutPath:  t.TempDir(),
		Ref:           "main",
		AuxiliaryRefs: []string{"refs/heads/main"},
	})

	if status.ErrorCode != "git_auxiliary_refs_invalid" {
		t.Fatalf("expected invalid auxiliary refs, got %+v", status)
	}
}

func TestHydrateManagedGitContextRequiresAuxiliaryRefs(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "main\n", "main")
	defaultBranch := gitOutput(t, remote, "branch", "--show-current")

	checkoutPath := filepath.Join(t.TempDir(), "managed")
	status := SyncManagedGitCheckout(context.Background(), ManagedGitCheckoutRequest{
		CheckoutPath: checkoutPath,
		RemoteURL:    remote,
		DefaultRef:   defaultBranch,
	})

	if status.ErrorCode != "" {
		t.Fatalf("initial sync failed: %+v", status)
	}

	status = HydrateManagedGitContext(context.Background(), ManagedGitRefHydrationRequest{
		CheckoutPath:  checkoutPath,
		Ref:           defaultBranch,
		AuxiliaryRefs: []string{"refs/notes/commits"},
	})

	if status.ErrorCode != "source_auxiliary_ref_not_found" {
		t.Fatalf("expected missing auxiliary ref, got %+v", status)
	}
}

func TestSyncManagedGitCheckoutFetchesProviderDefaultRefOnDemand(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "main\n", "main")
	defaultBranch := gitOutput(t, remote, "branch", "--show-current")

	git(t, remote, "checkout", "-b", "review/default-provider-ref")
	writeAndCommit(t, remote, "README.md", "review\n", "review")
	reviewCommit := gitOutput(t, remote, "rev-parse", "HEAD")
	git(t, remote, "update-ref", "refs/pull/456/head", reviewCommit)
	git(t, remote, "checkout", defaultBranch)

	checkoutPath := filepath.Join(t.TempDir(), "managed")
	status := SyncManagedGitCheckout(context.Background(), ManagedGitCheckoutRequest{
		CheckoutPath: checkoutPath,
		RemoteURL:    remote,
		DefaultRef:   "refs/pull/456/head",
	})

	if status.ErrorCode != "" {
		t.Fatalf("sync provider default ref failed: %+v", status)
	}

	if !status.DefaultRefResolved || status.ResolvedCommit != reviewCommit {
		t.Fatalf("provider default ref status mismatch: %+v", status)
	}

	rev, err := NewManagedGitCheckout(checkoutPath).ResolveRevision(context.Background(), "refs/pull/456/head")
	if err != nil {
		t.Fatalf("resolve provider default ref: %v", err)
	}

	if rev.Commit != reviewCommit {
		t.Fatalf("provider default ref commit: got %q, want %q", rev.Commit, reviewCommit)
	}
}

func TestManagedGitFetchRemotesSortsFallbacksByNumericSuffix(t *testing.T) {
	t.Parallel()

	repo := initGitRepo(t)
	git(t, repo, "remote", "add", "vectis-fallback-10", repo)
	git(t, repo, "remote", "add", "vectis-fallback-2", repo)
	git(t, repo, "remote", "add", "vectis-fallback-1", repo)
	git(t, repo, "remote", "add", "vectis-fallback-upstream", repo)

	got := managedGitFetchRemotes(context.Background(), repo, "")
	want := []string{
		"origin",
		"vectis-fallback-1",
		"vectis-fallback-2",
		"vectis-fallback-10",
		"vectis-fallback-upstream",
	}

	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("fallback remotes order:\ngot:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}

	got = managedGitFetchRemotes(context.Background(), repo, "vectis-fallback-10")
	want = []string{
		"vectis-fallback-10",
		"origin",
		"vectis-fallback-1",
		"vectis-fallback-2",
		"vectis-fallback-upstream",
	}

	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("preferred fallback remotes order:\ngot:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestHydrateManagedGitRefWaitsForManagedWriterLock(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "main\n", "main")
	defaultBranch := gitOutput(t, remote, "branch", "--show-current")

	checkoutPath := filepath.Join(t.TempDir(), "managed")
	status := SyncManagedGitCheckout(context.Background(), ManagedGitCheckoutRequest{
		CheckoutPath: checkoutPath,
		RemoteURL:    remote,
		DefaultRef:   defaultBranch,
	})

	if status.ErrorCode != "" {
		t.Fatalf("initial sync failed: %+v", status)
	}

	git(t, remote, "checkout", "-b", "feature/waits-for-lock")
	writeAndCommit(t, remote, "README.md", "feature\n", "feature")
	featureCommit := gitOutput(t, remote, "rev-parse", "HEAD")
	git(t, remote, "checkout", defaultBranch)

	lock, err := acquireManagedGitWriterLock(context.Background(), checkoutPath)
	if err != nil {
		t.Fatalf("acquire managed writer lock: %v", err)
	}

	locked := true
	defer func() {
		if locked {
			_ = lock.Close()
		}
	}()

	done := make(chan GitCheckoutStatus, 1)
	go func() {
		done <- HydrateManagedGitRef(context.Background(), ManagedGitRefHydrationRequest{
			CheckoutPath: checkoutPath,
			Ref:          "feature/waits-for-lock",
		})
	}()

	select {
	case status := <-done:
		t.Fatalf("hydrate completed while managed writer lock was held: %+v", status)
	case <-time.After(100 * time.Millisecond):
	}

	if err := lock.Close(); err != nil {
		t.Fatalf("release managed writer lock: %v", err)
	}

	locked = false

	select {
	case status := <-done:
		if status.ErrorCode != "" {
			t.Fatalf("hydrate after lock release failed: %+v", status)
		}

		if !status.DefaultRefResolved || status.ResolvedCommit != featureCommit {
			t.Fatalf("hydrated feature branch status mismatch: %+v", status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("hydrate did not complete after managed writer lock release")
	}
}

func TestHydrateManagedGitRefContextExpiresWaitingForManagedWriterLock(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "main\n", "main")
	defaultBranch := gitOutput(t, remote, "branch", "--show-current")

	checkoutPath := filepath.Join(t.TempDir(), "managed")
	status := SyncManagedGitCheckout(context.Background(), ManagedGitCheckoutRequest{
		CheckoutPath: checkoutPath,
		RemoteURL:    remote,
		DefaultRef:   defaultBranch,
	})

	if status.ErrorCode != "" {
		t.Fatalf("initial sync failed: %+v", status)
	}

	lock, err := acquireManagedGitWriterLock(context.Background(), checkoutPath)
	if err != nil {
		t.Fatalf("acquire managed writer lock: %v", err)
	}
	defer lock.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	status = HydrateManagedGitRef(ctx, ManagedGitRefHydrationRequest{
		CheckoutPath: checkoutPath,
		Ref:          defaultBranch,
	})

	if status.ErrorCode != "git_lock_failed" {
		t.Fatalf("expected git_lock_failed while lock is held, got %+v", status)
	}
}

func TestManagedGitCheckoutCommitFileWaitsForManagedWriterLock(t *testing.T) {
	t.Parallel()

	repo := initGitRepo(t)
	writeAndCommit(t, repo, "README.md", "main\n", "main")
	branch := gitOutput(t, repo, "branch", "--show-current")
	parent := gitOutput(t, repo, "rev-parse", "HEAD")

	lock, err := acquireManagedGitWriterLock(context.Background(), repo)
	if err != nil {
		t.Fatalf("acquire managed writer lock: %v", err)
	}

	locked := true
	defer func() {
		if locked {
			_ = lock.Close()
		}
	}()

	done := make(chan error, 1)
	go func() {
		_, err := NewManagedGitCheckout(repo).CommitFile(context.Background(), CommitFileOptions{
			Ref:          branch,
			Path:         ".vectis/jobs/build.json",
			Content:      []byte(`{"root":{"id":"root","uses":"builtins/script","with":{"script":"true"}}}`),
			Message:      "add build",
			ExpectedHead: parent,
		})

		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("managed commit completed while writer lock was held: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	if err := lock.Close(); err != nil {
		t.Fatalf("release managed writer lock: %v", err)
	}

	locked = false

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("managed commit after lock release failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("managed commit did not complete after writer lock release")
	}
}

func TestManagedGitCheckoutCommitFileContextExpiresWaitingForManagedWriterLock(t *testing.T) {
	t.Parallel()

	repo := initGitRepo(t)
	writeAndCommit(t, repo, "README.md", "main\n", "main")
	branch := gitOutput(t, repo, "branch", "--show-current")
	parent := gitOutput(t, repo, "rev-parse", "HEAD")

	lock, err := acquireManagedGitWriterLock(context.Background(), repo)
	if err != nil {
		t.Fatalf("acquire managed writer lock: %v", err)
	}
	defer lock.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	_, err = NewManagedGitCheckout(repo).CommitFile(ctx, CommitFileOptions{
		Ref:          branch,
		Path:         ".vectis/jobs/build.json",
		Content:      []byte(`{"root":{"id":"root","uses":"builtins/script","with":{"script":"true"}}}`),
		Message:      "add build",
		ExpectedHead: parent,
	})

	if !errors.Is(err, ErrBusy) {
		t.Fatalf("expected ErrBusy while writer lock is held, got %v", err)
	}
}

func TestManagedGitCheckoutResolvesHydratedRemoteBranchByPlainName(t *testing.T) {
	t.Parallel()

	remote := initGitRepo(t)
	writeAndCommit(t, remote, "README.md", "main\n", "main")
	defaultBranch := gitOutput(t, remote, "branch", "--show-current")

	checkoutPath := filepath.Join(t.TempDir(), "managed")
	status := SyncManagedGitCheckout(context.Background(), ManagedGitCheckoutRequest{
		CheckoutPath: checkoutPath,
		RemoteURL:    remote,
		DefaultRef:   defaultBranch,
	})

	if status.ErrorCode != "" {
		t.Fatalf("initial sync failed: %+v", status)
	}

	git(t, remote, "checkout", "-b", "feature/ref-fallback")
	writeAndCommit(t, remote, "README.md", "feature\n", "feature")
	featureCommit := gitOutput(t, remote, "rev-parse", "HEAD")
	git(t, remote, "checkout", defaultBranch)

	status = SyncManagedGitCheckout(context.Background(), ManagedGitCheckoutRequest{
		CheckoutPath: checkoutPath,
		RemoteURL:    remote,
		DefaultRef:   defaultBranch,
	})

	if status.ErrorCode != "" {
		t.Fatalf("resync default branch failed: %+v", status)
	}

	managed := NewManagedGitCheckout(checkoutPath)
	if _, err := managed.ResolveRevision(context.Background(), "feature/ref-fallback"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("managed resync should not broad-fetch feature branch, got %v", err)
	}

	status = HydrateManagedGitRef(context.Background(), ManagedGitRefHydrationRequest{
		CheckoutPath: checkoutPath,
		Ref:          "feature/ref-fallback",
	})

	if status.ErrorCode != "" {
		t.Fatalf("hydrate feature branch failed: %+v", status)
	}

	if _, err := NewGitCheckout(checkoutPath).ResolveRevision(context.Background(), "feature/ref-fallback"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("plain GitCheckout should not resolve remote-only branch by plain name, got %v", err)
	}

	rev, err := managed.ResolveRevision(context.Background(), "feature/ref-fallback")
	if err != nil {
		t.Fatalf("managed ResolveRevision plain branch: %v", err)
	}

	if rev.Commit != featureCommit {
		t.Fatalf("managed plain branch commit: got %q, want %q", rev.Commit, featureCommit)
	}

	listing, err := managed.ListBranches(context.Background(), ListBranchesOptions{
		Prefix: "feature/",
		Limit:  10,
	})

	if err != nil {
		t.Fatalf("managed ListBranches: %v", err)
	}

	branches := listing.Branches
	if len(branches) != 1 ||
		branches[0].Name != "feature/ref-fallback" ||
		branches[0].Ref != "refs/remotes/origin/feature/ref-fallback" ||
		branches[0].Commit != featureCommit ||
		branches[0].Remote != "origin" {
		t.Fatalf("managed branch list mismatch: %+v", branches)
	}

	if listing.Truncated {
		t.Fatalf("managed branch list should not be truncated: %+v", listing)
	}

	status = managed.Status(context.Background(), "refs/heads/feature/ref-fallback")
	if status.ErrorCode != "" || !status.DefaultRefResolved || status.ResolvedCommit != featureCommit {
		t.Fatalf("managed refs/heads fallback status mismatch: %+v", status)
	}

	file, err := managed.ReadFile(context.Background(), rev, "README.md")
	if err != nil {
		t.Fatalf("managed ReadFile feature branch: %v", err)
	}

	if got, want := string(file.Content), "feature\n"; got != want {
		t.Fatalf("feature branch content: got %q, want %q", got, want)
	}
}

func TestSyncManagedGitCheckoutRequiresRemoteURLToClone(t *testing.T) {
	t.Parallel()

	status := SyncManagedGitCheckout(context.Background(), ManagedGitCheckoutRequest{
		CheckoutPath: filepath.Join(t.TempDir(), "managed"),
		DefaultRef:   "HEAD",
	})

	if status.ErrorCode != "git_clone_failed" || !strings.Contains(status.ErrorMessage, "canonical_url") {
		t.Fatalf("expected canonical_url clone failure, got %+v", status)
	}
}

func TestNormalizeGitRemoteURLRejectsUnsafeValues(t *testing.T) {
	t.Parallel()

	for _, remoteURL := range []string{"", "-config", "https://example.invalid/repo.git\n"} {
		t.Run(remoteURL, func(t *testing.T) {
			_, err := normalizeGitRemoteURL(remoteURL)
			if err == nil || !errors.Is(err, ErrInvalidReference) {
				t.Fatalf("expected ErrInvalidReference, got %v", err)
			}
		})
	}
}
