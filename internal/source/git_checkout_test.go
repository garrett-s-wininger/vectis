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

func TestGitCommandEnvDisablesOptionalLocks(t *testing.T) {
	t.Setenv("GIT_OPTIONAL_LOCKS", "1")

	env := gitCommandEnv([]string{"GIT_INDEX_FILE=/tmp/vectis-index"})
	if got := envValue(env, "GIT_OPTIONAL_LOCKS"); got != "0" {
		t.Fatalf("GIT_OPTIONAL_LOCKS=%q, want 0; env=%v", got, env)
	}

	if got := envValue(env, "GIT_INDEX_FILE"); got != "/tmp/vectis-index" {
		t.Fatalf("GIT_INDEX_FILE=%q, want /tmp/vectis-index; env=%v", got, env)
	}

	explicit := gitCommandEnv([]string{"GIT_OPTIONAL_LOCKS=1"})
	if got := envValue(explicit, "GIT_OPTIONAL_LOCKS"); got != "1" {
		t.Fatalf("explicit GIT_OPTIONAL_LOCKS=%q, want 1; env=%v", got, explicit)
	}
}

func TestNormalizeRefAcceptsSafeRefs(t *testing.T) {
	for _, ref := range []string{
		"HEAD",
		"main",
		"feature/build",
		"refs/heads/main",
		"refs/tags/v1.2.3",
		"refs/pull/123/head",
		"refs/changes/34/1234/5",
		"0123456789abcdef0123456789abcdef01234567",
	} {
		t.Run(ref, func(t *testing.T) {
			got, err := normalizeRef(ref)
			if err != nil {
				t.Fatalf("normalizeRef(%q): %v", ref, err)
			}

			if got != ref {
				t.Fatalf("normalizeRef(%q)=%q", ref, got)
			}
		})
	}
}

func TestNormalizeRefRejectsRevisionExpressions(t *testing.T) {
	for _, ref := range []string{
		"-main",
		"/main",
		"main/",
		"main..other",
		"HEAD~1",
		"main^{}",
		"main:path",
		"feature/*",
		"main@{1}",
		"refs/heads/.hidden",
		"refs/heads/main.lock",
	} {
		t.Run(ref, func(t *testing.T) {
			if _, err := normalizeRef(ref); !errors.Is(err, ErrInvalidReference) {
				t.Fatalf("expected ErrInvalidReference for %q, got %v", ref, err)
			}
		})
	}
}

func TestGitCheckoutCommitFileUpdatesBranchWithoutCheckout(t *testing.T) {
	repo := initGitRepo(t)
	writeAndCommit(t, repo, "README.md", "hello\n", "readme")
	branch := gitOutput(t, repo, "branch", "--show-current")
	parent := gitOutput(t, repo, "rev-parse", "HEAD")

	checkout := NewGitCheckout(repo)
	content := `{"root":{"id":"root","uses":"builtins/script","with":{"script":"true"}}}`
	commit, err := checkout.CommitFile(context.Background(), CommitFileOptions{
		Ref:          branch,
		Path:         ".vectis/jobs/build.json",
		Content:      []byte(content),
		Message:      "add build definition",
		ExpectedHead: parent,
	})
	if err != nil {
		t.Fatalf("CommitFile: %v", err)
	}

	if commit.RequestedRef != branch || commit.ParentCommit != parent || commit.Commit == "" || commit.Commit == parent || commit.Path != ".vectis/jobs/build.json" || commit.BlobSHA == "" {
		t.Fatalf("commit response mismatch: %+v parent=%s", commit, parent)
	}

	if got := gitOutput(t, repo, "rev-parse", "HEAD"); got != commit.Commit {
		t.Fatalf("branch head: got %q, want %q", got, commit.Commit)
	}

	if got := gitOutput(t, repo, "show", commit.Commit+":.vectis/jobs/build.json"); got != content {
		t.Fatalf("committed file content: got %q, want %q", got, content)
	}

	unchanged, err := checkout.CommitFile(context.Background(), CommitFileOptions{
		Ref:          branch,
		Path:         ".vectis/jobs/build.json",
		Content:      []byte(content),
		ExpectedHead: commit.Commit,
	})
	if err != nil {
		t.Fatalf("CommitFile unchanged: %v", err)
	}
	if unchanged.Commit != commit.Commit || unchanged.ParentCommit != commit.Commit {
		t.Fatalf("unchanged commit should return current head: %+v want %s", unchanged, commit.Commit)
	}

	_, err = checkout.CommitFile(context.Background(), CommitFileOptions{
		Ref:          branch,
		Path:         ".vectis/jobs/build.json",
		Content:      []byte(`{"root":{"id":"root","uses":"builtins/script","with":{"script":"false"}}}`),
		ExpectedHead: parent,
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected stale expected head conflict, got %v", err)
	}

	_, err = checkout.CommitFile(context.Background(), CommitFileOptions{
		Ref:        branch,
		Path:       ".vectis/jobs/build.json",
		Content:    []byte(`{"root":{"id":"root","uses":"builtins/script","with":{"script":"false"}}}`),
		CreateOnly: true,
	})

	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected create-only existing definition conflict, got %v", err)
	}
}

func TestGitCheckoutDeleteFileUpdatesBranchWithoutCheckout(t *testing.T) {
	repo := initGitRepo(t)
	writeAndCommit(t, repo, ".vectis/jobs/build.json", `{"root":{"id":"root","uses":"builtins/script","with":{"script":"true"}}}`, "add build")
	branch := gitOutput(t, repo, "branch", "--show-current")
	parent := gitOutput(t, repo, "rev-parse", "HEAD")

	checkout := NewGitCheckout(repo)
	deleted, err := checkout.DeleteFile(context.Background(), DeleteFileOptions{
		Ref:          branch,
		Path:         ".vectis/jobs/build.json",
		Message:      "delete build definition",
		ExpectedHead: parent,
	})

	if err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}

	if deleted.RequestedRef != branch || deleted.ParentCommit != parent || deleted.Commit == "" || deleted.Commit == parent || deleted.Path != ".vectis/jobs/build.json" || deleted.BlobSHA != "" {
		t.Fatalf("delete response mismatch: %+v parent=%s", deleted, parent)
	}

	if got := gitOutput(t, repo, "rev-parse", "HEAD"); got != deleted.Commit {
		t.Fatalf("branch head: got %q, want %q", got, deleted.Commit)
	}

	cmd := exec.Command("git", "-C", repo, "cat-file", "-e", deleted.Commit+":.vectis/jobs/build.json")
	if err := cmd.Run(); err == nil {
		t.Fatalf("deleted definition still exists at %s", deleted.Commit)
	}

	_, err = checkout.DeleteFile(context.Background(), DeleteFileOptions{
		Ref:  branch,
		Path: ".vectis/jobs/build.json",
	})

	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected missing file not found, got %v", err)
	}

	_, err = checkout.DeleteFile(context.Background(), DeleteFileOptions{
		Ref:          branch,
		Path:         "README.md",
		ExpectedHead: parent,
	})

	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected stale expected head conflict, got %v", err)
	}
}

func TestGitCheckoutListBranches(t *testing.T) {
	repo := initGitRepo(t)
	writeAndCommit(t, repo, "README.md", "main\n", "main")
	defaultBranch := gitOutput(t, repo, "branch", "--show-current")

	git(t, repo, "checkout", "-b", "feature/list-branches")
	writeAndCommit(t, repo, "README.md", "feature\n", "feature")
	featureCommit := gitOutput(t, repo, "rev-parse", "HEAD")
	git(t, repo, "checkout", defaultBranch)

	git(t, repo, "checkout", "-b", "release/list-branches")
	writeAndCommit(t, repo, "README.md", "release\n", "release")
	git(t, repo, "checkout", defaultBranch)

	checkout := NewGitCheckout(repo)
	listing, err := checkout.ListBranches(context.Background(), ListBranchesOptions{
		Prefix: "feature/",
		Limit:  10,
	})

	if err != nil {
		t.Fatalf("ListBranches: %v", err)
	}

	branches := listing.Branches
	if len(branches) != 1 {
		t.Fatalf("branches with feature prefix: got %+v", branches)
	}
	if listing.Truncated {
		t.Fatalf("feature-prefixed branches should not be truncated: %+v", listing)
	}

	if branches[0].Name != "feature/list-branches" ||
		branches[0].Ref != "refs/heads/feature/list-branches" ||
		branches[0].Commit != featureCommit ||
		branches[0].Remote != "" {
		t.Fatalf("branch response mismatch: %+v", branches[0])
	}

	listing, err = checkout.ListBranches(context.Background(), ListBranchesOptions{Limit: 1})
	if err != nil {
		t.Fatalf("ListBranches limit: %v", err)
	}

	branches = listing.Branches
	if len(branches) != 1 {
		t.Fatalf("limited branches: got %+v", branches)
	}
	if !listing.Truncated {
		t.Fatalf("expected limited branches to be truncated: %+v", listing)
	}

	if _, err := checkout.ListBranches(context.Background(), ListBranchesOptions{Prefix: "feature/*"}); !errors.Is(err, ErrInvalidReference) {
		t.Fatalf("expected invalid prefix, got %v", err)
	}
}

func TestGitCheckoutListTree(t *testing.T) {
	repo := initGitRepo(t)
	writeAndCommit(t, repo, ".vectis/jobs/build.json", `{"name":"build"}`+"\n", "build")
	writeAndCommit(t, repo, ".vectis/jobs/deploy.json", `{"name":"deploy"}`+"\n", "deploy")
	writeAndCommit(t, repo, "README.md", "hello\n", "readme")
	commit := gitOutput(t, repo, "rev-parse", "HEAD")

	checkout := NewGitCheckout(repo)
	listing, err := checkout.ListTree(context.Background(), ListTreeOptions{
		Ref:   "HEAD",
		Path:  ".vectis",
		Limit: 10,
	})

	if err != nil {
		t.Fatalf("ListTree .vectis: %v", err)
	}

	if listing.RequestedRef != "HEAD" ||
		listing.Revision.Commit != commit ||
		listing.Path != ".vectis" ||
		listing.Recursive ||
		listing.Truncated ||
		len(listing.Entries) != 1 {
		t.Fatalf(".vectis listing mismatch: %+v", listing)
	}

	if got := listing.Entries[0]; got.Path != ".vectis/jobs" || got.Name != "jobs" || got.Type != "tree" || got.Mode != "040000" || got.ObjectSHA == "" {
		t.Fatalf(".vectis tree entry mismatch: %+v", got)
	}

	listing, err = checkout.ListTree(context.Background(), ListTreeOptions{
		Ref:   "HEAD",
		Path:  ".vectis/jobs",
		Limit: 10,
	})

	if err != nil {
		t.Fatalf("ListTree .vectis/jobs: %v", err)
	}

	entries := map[string]TreeEntry{}
	for _, entry := range listing.Entries {
		entries[entry.Path] = entry
	}

	if build := entries[".vectis/jobs/build.json"]; build.Name != "build.json" || build.Type != "blob" || build.Mode != "100644" || build.ObjectSHA == "" || build.SizeBytes == 0 {
		t.Fatalf("build entry mismatch: %+v", build)
	}

	if deploy := entries[".vectis/jobs/deploy.json"]; deploy.Name != "deploy.json" || deploy.Type != "blob" || deploy.Mode != "100644" || deploy.ObjectSHA == "" || deploy.SizeBytes == 0 {
		t.Fatalf("deploy entry mismatch: %+v", deploy)
	}

	listing, err = checkout.ListTree(context.Background(), ListTreeOptions{
		Ref:       "HEAD",
		Recursive: true,
		Limit:     2,
	})

	if err != nil {
		t.Fatalf("ListTree recursive limit: %v", err)
	}

	if !listing.Recursive || !listing.Truncated || listing.NextCursor == "" || len(listing.Entries) != 2 {
		t.Fatalf("recursive limited listing mismatch: %+v", listing)
	}

	cursor := listing.NextCursor
	listing, err = checkout.ListTree(context.Background(), ListTreeOptions{
		Ref:       "HEAD",
		Recursive: true,
		Limit:     10,
		Cursor:    cursor,
	})
	if err != nil {
		t.Fatalf("ListTree recursive cursor: %v", err)
	}
	if listing.Truncated || len(listing.Entries) == 0 {
		t.Fatalf("cursor listing mismatch: %+v", listing)
	}
	for _, entry := range listing.Entries {
		if entry.Path <= cursor {
			t.Fatalf("cursor listing returned %q at or before cursor %q: %+v", entry.Path, cursor, listing)
		}
	}

	if _, err := checkout.ListTree(context.Background(), ListTreeOptions{Ref: "HEAD", Path: "../secret"}); !errors.Is(err, ErrInvalidReference) {
		t.Fatalf("expected invalid tree path, got %v", err)
	}

	if _, err := checkout.ListTree(context.Background(), ListTreeOptions{Ref: "HEAD", Path: "missing"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected missing tree path, got %v", err)
	}
}

func TestGitCheckoutListDefinitionFiles(t *testing.T) {
	repo := initGitRepo(t)
	writeAndCommit(t, repo, ".vectis/jobs/00-note.txt", "not a definition\n", "note")
	writeAndCommit(t, repo, ".vectis/jobs/build.json", `{"name":"build"}`+"\n", "build")
	buildBlob := gitOutput(t, repo, "rev-parse", "HEAD:.vectis/jobs/build.json")
	writeAndCommit(t, repo, ".vectis/jobs/nested/deploy.json", `{"name":"deploy"}`+"\n", "deploy")
	writeAndCommit(t, repo, ".vectis/jobs/nested/readme.md", "not json\n", "nested readme")
	commit := gitOutput(t, repo, "rev-parse", "HEAD")

	checkout := NewGitCheckout(repo)
	listing, err := checkout.ListDefinitionFiles(context.Background(), ListDefinitionFilesOptions{
		Ref:   "HEAD",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListDefinitionFiles: %v", err)
	}

	if listing.RequestedRef != "HEAD" ||
		listing.Revision.Commit != commit ||
		listing.Path != DefaultDefinitionPath ||
		listing.Truncated ||
		len(listing.Files) != 2 {
		t.Fatalf("definition file listing mismatch: %+v", listing)
	}

	files := map[string]DefinitionFile{}
	for _, file := range listing.Files {
		files[file.Path] = file
	}

	if build := files[".vectis/jobs/build.json"]; build.Name != "build.json" || build.BlobSHA != buildBlob || build.SizeBytes == 0 {
		t.Fatalf("build definition file mismatch: %+v", build)
	}

	if deploy := files[".vectis/jobs/nested/deploy.json"]; deploy.Name != "deploy.json" || deploy.BlobSHA == "" || deploy.SizeBytes == 0 {
		t.Fatalf("deploy definition file mismatch: %+v", deploy)
	}

	listing, err = checkout.ListDefinitionFiles(context.Background(), ListDefinitionFilesOptions{
		Ref:   "HEAD",
		Path:  ".vectis/jobs",
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("ListDefinitionFiles limit: %v", err)
	}
	if len(listing.Files) != 1 || listing.Files[0].Path != ".vectis/jobs/build.json" {
		t.Fatalf("limited definition files mismatch: %+v", listing)
	}
	if !listing.Truncated || listing.NextCursor != ".vectis/jobs/build.json" {
		t.Fatalf("expected limited definition listing to be truncated: %+v", listing)
	}

	listing, err = checkout.ListDefinitionFiles(context.Background(), ListDefinitionFilesOptions{
		Ref:    "HEAD",
		Path:   ".vectis/jobs",
		Limit:  10,
		Cursor: ".vectis/jobs/build.json",
	})
	if err != nil {
		t.Fatalf("ListDefinitionFiles cursor: %v", err)
	}
	if listing.Truncated || len(listing.Files) != 1 || listing.Files[0].Path != ".vectis/jobs/nested/deploy.json" {
		t.Fatalf("cursor definition files mismatch: %+v", listing)
	}

	if _, err := checkout.ListDefinitionFiles(context.Background(), ListDefinitionFilesOptions{Ref: "HEAD", Path: "../secret"}); !errors.Is(err, ErrInvalidReference) {
		t.Fatalf("expected invalid definition path, got %v", err)
	}

	if _, err := checkout.ListDefinitionFiles(context.Background(), ListDefinitionFilesOptions{Ref: "HEAD", Cursor: "../secret"}); !errors.Is(err, ErrInvalidReference) {
		t.Fatalf("expected invalid definition cursor, got %v", err)
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

func TestGitCheckoutStatusReportsObjectStorePressure(t *testing.T) {
	repo := initGitRepo(t)
	writeAndCommit(t, repo, "README.md", "hello\n", "readme")
	commit := gitOutput(t, repo, "rev-parse", "HEAD")
	git(t, repo, "update-ref", "refs/vectis/hydrated/1111111111111111111111111111111111111111", commit)
	git(t, repo, "update-ref", "refs/vectis/hydrated/2222222222222222222222222222222222222222", commit)

	commonDir := gitOutput(t, repo, "rev-parse", "--git-common-dir")
	if !filepath.IsAbs(commonDir) {
		commonDir = filepath.Join(repo, commonDir)
	}

	packDir := filepath.Join(commonDir, "objects", "pack")
	if err := os.MkdirAll(packDir, 0o755); err != nil {
		t.Fatalf("mkdir pack dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(packDir, "pack-pressure.pack"), []byte("pack-bytes"), 0o644); err != nil {
		t.Fatalf("write pack: %v", err)
	}
	if err := os.WriteFile(filepath.Join(packDir, "pack-pressure.keep"), []byte("keep"), 0o644); err != nil {
		t.Fatalf("write keep: %v", err)
	}
	if err := os.WriteFile(filepath.Join(packDir, "pack-pressure.lock"), []byte("lock"), 0o644); err != nil {
		t.Fatalf("write pack lock: %v", err)
	}
	if err := os.WriteFile(filepath.Join(packDir, "multi-pack-index"), []byte("midx"), 0o644); err != nil {
		t.Fatalf("write multi-pack-index: %v", err)
	}

	infoDir := filepath.Join(commonDir, "objects", "info")
	if err := os.MkdirAll(infoDir, 0o755); err != nil {
		t.Fatalf("mkdir objects info: %v", err)
	}
	if err := os.WriteFile(filepath.Join(infoDir, "commit-graph"), []byte("graph"), 0o644); err != nil {
		t.Fatalf("write commit graph: %v", err)
	}
	if err := os.WriteFile(filepath.Join(commonDir, "gc.pid"), []byte("12345\n"), 0o644); err != nil {
		t.Fatalf("write gc pid: %v", err)
	}

	status := NewGitCheckout(repo).Status(context.Background(), "HEAD")
	if status.ErrorCode != "" {
		t.Fatalf("expected healthy status, got error %s: %s", status.ErrorCode, status.ErrorMessage)
	}

	objectStore := status.ObjectStore
	if objectStore.PackFiles != 1 ||
		objectStore.PackBytes != int64(len("pack-bytes")) ||
		objectStore.PackKeepFiles != 1 ||
		objectStore.LooseObjects == 0 ||
		objectStore.LooseObjectScanLimit != gitObjectLooseScanLimit ||
		objectStore.HydratedRefs != 2 ||
		objectStore.HydratedRefsTruncated ||
		objectStore.HydratedRefScanLimit != gitHydratedRefScanLimit ||
		!objectStore.CommitGraph ||
		!objectStore.MultiPackIndex {
		t.Fatalf("object store pressure mismatch: %+v", objectStore)
	}

	gotIndicators := strings.Join(objectStore.MaintenanceIndicatorFiles, ",")
	if gotIndicators != "gc.pid,objects/pack/pack-pressure.lock" {
		t.Fatalf("maintenance indicators: got %q", gotIndicators)
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

func cloneGitRepo(t *testing.T, source, dest string) {
	t.Helper()

	cmd := exec.Command("git", "clone", source, dest)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git clone %s %s: %v\n%s", source, dest, err, out)
	}
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
