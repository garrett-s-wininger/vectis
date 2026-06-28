package source

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSyncManagedGitCheckoutClonesAndFetches(t *testing.T) {
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

	for key, want := range map[string]string{
		"gc.auto":                "0",
		"maintenance.auto":       "false",
		"fetch.writeCommitGraph": "false",
		"remote.origin.tagOpt":   "--no-tags",
	} {
		if got := gitOutput(t, checkoutPath, "config", "--get", key); got != want {
			t.Fatalf("managed checkout config %s: got %q, want %q", key, got, want)
		}
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

func TestSyncManagedGitCheckoutFetchesPlainDefaultTagOnDemand(t *testing.T) {
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

	if status.ErrorCode != "git_fetch_failed" {
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

	if got := gitOutput(t, checkoutPath, "remote", "get-url", "vectis-fallback-1"); got != mirror {
		t.Fatalf("configured first fallback remote: got %q, want %q", got, mirror)
	}

	if got := gitOutput(t, checkoutPath, "remote", "get-url", "vectis-fallback-2"); got != upstream {
		t.Fatalf("configured second fallback remote: got %q, want %q", got, upstream)
	}

	if got := gitOutput(t, checkoutPath, "for-each-ref", "--format=%(refname)", "refs/vectis/candidates"); got != "" {
		t.Fatalf("fallback hydrate should clean candidate refs, got %q", got)
	}

	if got := gitOutput(t, checkoutPath, "rev-parse", "refs/remotes/origin/feature/upstream-only"); got != featureCommit {
		t.Fatalf("fallback hydrate should publish managed origin ref: got %q, want %q", got, featureCommit)
	}
}

func TestManagedGitFetchRemotesSortsFallbacksByNumericSuffix(t *testing.T) {
	repo := initGitRepo(t)
	git(t, repo, "remote", "add", "vectis-fallback-10", repo)
	git(t, repo, "remote", "add", "vectis-fallback-2", repo)
	git(t, repo, "remote", "add", "vectis-fallback-1", repo)
	git(t, repo, "remote", "add", "vectis-fallback-upstream", repo)

	got := managedGitFetchRemotes(context.Background(), repo)
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
}

func TestHydrateManagedGitRefWaitsForManagedWriterLock(t *testing.T) {
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
			Content:      []byte(`{"root":{"id":"root","uses":"builtins/shell","with":{"command":"true"}}}`),
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
		Content:      []byte(`{"root":{"id":"root","uses":"builtins/shell","with":{"command":"true"}}}`),
		Message:      "add build",
		ExpectedHead: parent,
	})

	if !errors.Is(err, ErrBusy) {
		t.Fatalf("expected ErrBusy while writer lock is held, got %v", err)
	}
}

func TestManagedGitCheckoutResolvesFetchedRemoteBranchByPlainName(t *testing.T) {
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
		t.Fatalf("fetch feature branch failed: %+v", status)
	}

	if _, err := NewGitCheckout(checkoutPath).ResolveRevision(context.Background(), "feature/ref-fallback"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("plain GitCheckout should not resolve remote-only branch by plain name, got %v", err)
	}

	managed := NewManagedGitCheckout(checkoutPath)
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
	status := SyncManagedGitCheckout(context.Background(), ManagedGitCheckoutRequest{
		CheckoutPath: filepath.Join(t.TempDir(), "managed"),
		DefaultRef:   "HEAD",
	})

	if status.ErrorCode != "git_clone_failed" || !strings.Contains(status.ErrorMessage, "canonical_url") {
		t.Fatalf("expected canonical_url clone failure, got %+v", status)
	}
}

func TestNormalizeGitRemoteURLRejectsUnsafeValues(t *testing.T) {
	for _, remoteURL := range []string{"", "-config", "https://example.invalid/repo.git\n"} {
		t.Run(remoteURL, func(t *testing.T) {
			_, err := normalizeGitRemoteURL(remoteURL)
			if err == nil || !errors.Is(err, ErrInvalidReference) {
				t.Fatalf("expected ErrInvalidReference, got %v", err)
			}
		})
	}
}
