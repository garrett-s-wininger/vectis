package source

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
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
