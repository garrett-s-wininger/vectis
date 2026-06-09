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

func initGitRepo(t *testing.T) string {
	t.Helper()

	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}

	repo := t.TempDir()
	git(t, repo, "init")
	git(t, repo, "config", "user.name", "Vectis Test")
	git(t, repo, "config", "user.email", "vectis@example.invalid")
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
