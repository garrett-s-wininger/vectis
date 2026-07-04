package gittest

import (
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"vectis/internal/gitcmd"
)

var commitClock int64 = 1_700_000_000

// InitRepository creates a small SHA-1 Git repository without spawning git init.
func InitRepository(t testing.TB) string {
	t.Helper()

	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}

	repo := t.TempDir()
	if _, err := git.PlainInitWithOptions(repo, &git.PlainInitOptions{
		InitOptions: git.InitOptions{
			DefaultBranch: plumbing.NewBranchReferenceName("main"),
		},
	}); err != nil {
		t.Fatalf("initialize git repo %s: %v", repo, err)
	}

	settings := [][2]string{
		{"user.name", "Vectis Test"},
		{"user.email", "vectis@example.invalid"},
		{"commit.gpgsign", "false"},
	}

	settings = append(settings, gitcmd.NoAutoMaintenanceSettings()...)
	if err := gitcmd.WriteWorkTreeConfigSettings(repo, settings); err != nil {
		t.Fatalf("configure git repo %s: %v", repo, err)
	}

	return repo
}

// CommitAll snapshots the worktree into a Git commit and advances HEAD.
func CommitAll(t testing.TB, repo, message string) string {
	t.Helper()

	gitRepo, err := git.PlainOpen(repo)
	if err != nil {
		t.Fatalf("open git repo %s: %v", repo, err)
	}

	worktree, err := gitRepo.Worktree()
	if err != nil {
		t.Fatalf("open git worktree %s: %v", repo, err)
	}

	if err := worktree.AddGlob("."); err != nil {
		t.Fatalf("stage git fixture files for %s: %v", repo, err)
	}

	when := time.Unix(atomic.AddInt64(&commitClock, 1), 0).UTC()
	sig := &object.Signature{
		Name:  "Vectis Test",
		Email: "vectis@example.invalid",
		When:  when,
	}

	hash, err := worktree.Commit(message, &git.CommitOptions{
		All:       true,
		Author:    sig,
		Committer: sig,
	})
	
	if err != nil {
		t.Fatalf("commit git fixture %s: %v", repo, err)
	}

	return hash.String()
}
