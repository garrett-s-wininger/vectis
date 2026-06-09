package source

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGitCheckoutResolveRevisionAndReadFile(t *testing.T) {
	repo := initGitRepo(t)

	writeAndCommit(t, repo, ".vectis/jobs/build.json", `{"version":1}`+"\n", "first")
	firstCommit := gitOutput(t, repo, "rev-parse", "HEAD")

	writeAndCommit(t, repo, ".vectis/jobs/build.json", `{"version":2}`+"\n", "second")

	checkout := NewGitCheckout(repo)
	head, err := checkout.ResolveRevision(context.Background(), "HEAD")
	if err != nil {
		t.Fatalf("resolve HEAD: %v", err)
	}

	if head.Commit == "" || head.Commit == firstCommit {
		t.Fatalf("expected HEAD to resolve to second commit, got %q first=%q", head.Commit, firstCommit)
	}

	first, err := checkout.ReadFile(context.Background(), Revision{Commit: firstCommit}, ".vectis/jobs/build.json")
	if err != nil {
		t.Fatalf("read first revision: %v", err)
	}

	if got, want := string(first.Content), `{"version":1}`+"\n"; got != want {
		t.Fatalf("first revision content: got %q, want %q", got, want)
	}

	current, err := checkout.ReadFile(context.Background(), head, ".vectis/jobs/build.json")
	if err != nil {
		t.Fatalf("read current revision: %v", err)
	}

	if got, want := string(current.Content), `{"version":2}`+"\n"; got != want {
		t.Fatalf("current revision content: got %q, want %q", got, want)
	}

	if current.BlobSHA == "" {
		t.Fatal("expected blob sha")
	}
}

func TestGitCheckoutReadFileMissingPath(t *testing.T) {
	repo := initGitRepo(t)
	writeAndCommit(t, repo, "README.md", "hello\n", "readme")

	checkout := NewGitCheckout(repo)
	rev, err := checkout.ResolveRevision(context.Background(), "HEAD")
	if err != nil {
		t.Fatalf("resolve HEAD: %v", err)
	}

	_, err = checkout.ReadFile(context.Background(), rev, ".vectis/missing.json")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestGitCheckoutReadFileRejectsUnsafePaths(t *testing.T) {
	checkout := NewGitCheckout(t.TempDir())
	rev := Revision{Commit: "0123456789abcdef0123456789abcdef01234567"}

	for _, filePath := range []string{"", "/absolute", "../secret", "dir/../../secret", "bad\npath"} {
		t.Run(filePath, func(t *testing.T) {
			_, err := checkout.ReadFile(context.Background(), rev, filePath)
			if !errors.Is(err, ErrInvalidReference) {
				t.Fatalf("expected ErrInvalidReference, got %v", err)
			}
		})
	}
}

func TestGitCheckoutReadFileRequiresResolvedCommit(t *testing.T) {
	checkout := NewGitCheckout(t.TempDir())

	_, err := checkout.ReadFile(context.Background(), Revision{Commit: "HEAD"}, ".vectis.yml")
	if !errors.Is(err, ErrInvalidReference) {
		t.Fatalf("expected ErrInvalidReference, got %v", err)
	}
}

func TestGitCheckoutReadFileHonorsMaxFileBytes(t *testing.T) {
	repo := initGitRepo(t)
	writeAndCommit(t, repo, "large.txt", strings.Repeat("x", 16), "large")

	checkout := NewGitCheckout(repo, WithMaxFileBytes(8))
	rev, err := checkout.ResolveRevision(context.Background(), "HEAD")
	if err != nil {
		t.Fatalf("resolve HEAD: %v", err)
	}

	_, err = checkout.ReadFile(context.Background(), rev, "large.txt")
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("expected ErrTooLarge, got %v", err)
	}
}

func TestGitCheckoutStatusReportsHealthyCheckout(t *testing.T) {
	repo := initGitRepo(t)
	writeAndCommit(t, repo, "README.md", "hello\n", "readme")
	commit := gitOutput(t, repo, "rev-parse", "HEAD")

	checkout := NewGitCheckout(repo)
	status := checkout.Status(context.Background(), "HEAD")

	if status.ErrorCode != "" {
		t.Fatalf("expected healthy status, got error %s: %s", status.ErrorCode, status.ErrorMessage)
	}

	if status.CheckoutPath != repo || !status.PathExists || !status.PathIsDirectory || !status.GitRepository {
		t.Fatalf("checkout status mismatch: %+v", status)
	}

	gotWorkTree, err := filepath.EvalSymlinks(status.WorkTreePath)
	if err != nil {
		t.Fatalf("eval work tree path %q: %v", status.WorkTreePath, err)
	}

	wantWorkTree, err := filepath.EvalSymlinks(repo)
	if err != nil {
		t.Fatalf("eval repo path %q: %v", repo, err)
	}

	if gotWorkTree != wantWorkTree {
		t.Fatalf("work tree path: got %q, want %q", status.WorkTreePath, repo)
	}

	if status.DefaultRef != "HEAD" || !status.DefaultRefResolved || status.ResolvedCommit != commit {
		t.Fatalf("default ref status mismatch: %+v", status)
	}
}

func TestGitCheckoutStatusAllowsMissingDefaultRef(t *testing.T) {
	repo := initGitRepo(t)
	writeAndCommit(t, repo, "README.md", "hello\n", "readme")

	checkout := NewGitCheckout(repo)
	status := checkout.Status(context.Background(), "")

	if status.ErrorCode != "" {
		t.Fatalf("expected missing default ref to be usable, got error %s: %s", status.ErrorCode, status.ErrorMessage)
	}

	if !status.GitRepository || status.DefaultRefResolved || status.ResolvedCommit != "" {
		t.Fatalf("default ref status mismatch: %+v", status)
	}
}

func TestGitCheckoutStatusReportsMissingPath(t *testing.T) {
	checkout := NewGitCheckout(filepath.Join(t.TempDir(), "missing"))
	status := checkout.Status(context.Background(), "HEAD")

	if status.ErrorCode != "checkout_path_missing" {
		t.Fatalf("error code: got %q, want checkout_path_missing; status=%+v", status.ErrorCode, status)
	}

	if status.PathExists || status.PathIsDirectory || status.GitRepository {
		t.Fatalf("missing path should not look usable: %+v", status)
	}
}

func TestGitCheckoutStatusReportsNonGitDirectory(t *testing.T) {
	checkout := NewGitCheckout(t.TempDir())
	status := checkout.Status(context.Background(), "HEAD")

	if status.ErrorCode != "not_git_checkout" {
		t.Fatalf("error code: got %q, want not_git_checkout; status=%+v", status.ErrorCode, status)
	}

	if !status.PathExists || !status.PathIsDirectory || status.GitRepository {
		t.Fatalf("non-git directory status mismatch: %+v", status)
	}
}

func TestGitCheckoutStatusReportsMissingDefaultRef(t *testing.T) {
	repo := initGitRepo(t)
	writeAndCommit(t, repo, "README.md", "hello\n", "readme")

	checkout := NewGitCheckout(repo)
	status := checkout.Status(context.Background(), "refs/heads/missing")

	if status.ErrorCode != "default_ref_not_found" {
		t.Fatalf("error code: got %q, want default_ref_not_found; status=%+v", status.ErrorCode, status)
	}

	if !status.GitRepository || status.DefaultRefResolved || status.ResolvedCommit != "" {
		t.Fatalf("missing default ref status mismatch: %+v", status)
	}
}

func initGitRepo(t *testing.T) string {
	t.Helper()

	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}

	repo := t.TempDir()
	git(t, repo, "init")
	git(t, repo, "config", "user.name", "Vectis Test")
	git(t, repo, "config", "user.email", "vectis@example.invalid")
	git(t, repo, "config", "commit.gpgsign", "false")

	return repo
}

func writeAndCommit(t *testing.T, repo, name, content, message string) {
	t.Helper()

	path := filepath.Join(repo, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}

	git(t, repo, "add", name)
	git(t, repo, "commit", "-m", message)
}

func gitOutput(t *testing.T, repo string, args ...string) string {
	t.Helper()

	cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}

	return strings.TrimSpace(string(out))
}

func git(t *testing.T, repo string, args ...string) {
	t.Helper()
	_ = gitOutput(t, repo, args...)
}
