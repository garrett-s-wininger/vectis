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
