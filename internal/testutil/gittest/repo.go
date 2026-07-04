package gittest

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

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
	gitDir := filepath.Join(repo, ".git")
	for _, dir := range []string{
		filepath.Join(gitDir, "objects", "info"),
		filepath.Join(gitDir, "objects", "pack"),
		filepath.Join(gitDir, "refs", "heads"),
		filepath.Join(gitDir, "refs", "tags"),
		filepath.Join(gitDir, "info"),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("create git dir %s: %v", dir, err)
		}
	}

	writeFile(t, filepath.Join(gitDir, "HEAD"), []byte("ref: refs/heads/main\n"))
	writeFile(t, filepath.Join(gitDir, "info", "exclude"), nil)
	writeFile(t, filepath.Join(gitDir, "config"), []byte(`[core]
	repositoryformatversion = 0
	filemode = false
	bare = false
	logallrefupdates = true
	ignorecase = true
	symlinks = false
[user]
	name = Vectis Test
	email = vectis@example.invalid
[commit]
	gpgsign = false
`))

	if err := gitcmd.WriteWorkTreeConfigSettings(repo, gitcmd.NoAutoMaintenanceSettings()); err != nil {
		t.Fatalf("configure git repo %s: %v", repo, err)
	}

	return repo
}

// CommitAll snapshots the worktree into a Git commit and advances HEAD.
func CommitAll(t testing.TB, repo, message string) string {
	t.Helper()

	gitDir, err := gitcmd.WorkTreeGitDir(repo)
	if err != nil {
		t.Fatalf("find git dir for %s: %v", repo, err)
	}

	tree := writeTree(t, repo, gitDir)
	var parents []string
	if parent, ok, err := gitcmd.ResolveGitDirRef(gitDir, "HEAD"); err != nil {
		t.Fatalf("resolve HEAD for %s: %v", repo, err)
	} else if ok {
		parents = append(parents, parent)
	}

	commit := writeCommit(t, gitDir, tree, parents, message)
	headRef, ok, err := gitcmd.SymbolicGitDirRef(gitDir, "HEAD")
	if err != nil {
		t.Fatalf("resolve symbolic HEAD for %s: %v", repo, err)
	}

	if !ok {
		t.Fatalf("HEAD for %s is not symbolic", repo)
	}

	if err := gitcmd.WriteGitDirRef(gitDir, headRef, commit); err != nil {
		t.Fatalf("advance %s to %s: %v", headRef, commit, err)
	}

	cmd := exec.Command("git", gitcmd.NoAutoMaintenanceArgs("-C", repo, "reset", "--mixed", "-q", "HEAD")...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git reset fixture index for %s: %v\n%s", repo, err, out)
	}

	return commit
}

type treeEntry struct {
	mode string
	name string
	oid  string
}

func writeTree(t testing.TB, dir, gitDir string) string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read tree dir %s: %v", dir, err)
	}

	treeEntries := make([]treeEntry, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if name == ".git" {
			continue
		}

		path := filepath.Join(dir, name)
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}

		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				t.Fatalf("read symlink %s: %v", path, err)
			}

			treeEntries = append(treeEntries, treeEntry{
				mode: "120000",
				name: name,
				oid:  writeObject(t, gitDir, "blob", []byte(filepath.ToSlash(target))),
			})
		case info.IsDir():
			treeEntries = append(treeEntries, treeEntry{
				mode: "40000",
				name: name,
				oid:  writeTree(t, path, gitDir),
			})
		case info.Mode().IsRegular():
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}

			treeEntries = append(treeEntries, treeEntry{
				mode: "100644",
				name: name,
				oid:  writeObject(t, gitDir, "blob", data),
			})
		}
	}

	sort.Slice(treeEntries, func(i, j int) bool {
		return treeEntries[i].name < treeEntries[j].name
	})

	var body bytes.Buffer
	for _, entry := range treeEntries {
		body.WriteString(entry.mode)
		body.WriteByte(' ')
		body.WriteString(entry.name)
		body.WriteByte(0)

		rawOID, err := hex.DecodeString(entry.oid)
		if err != nil {
			t.Fatalf("decode object id %s: %v", entry.oid, err)
		}

		body.Write(rawOID)
	}

	return writeObject(t, gitDir, "tree", body.Bytes())
}

func writeCommit(t testing.TB, gitDir, tree string, parents []string, message string) string {
	t.Helper()

	when := atomic.AddInt64(&commitClock, 1)
	message = strings.TrimRight(message, "\n") + "\n"

	var body bytes.Buffer
	body.WriteString("tree ")
	body.WriteString(tree)
	body.WriteByte('\n')
	for _, parent := range parents {
		body.WriteString("parent ")
		body.WriteString(parent)
		body.WriteByte('\n')
	}

	fmt.Fprintf(&body, "author Vectis Test <vectis@example.invalid> %d +0000\n", when)
	fmt.Fprintf(&body, "committer Vectis Test <vectis@example.invalid> %d +0000\n", when)
	body.WriteByte('\n')
	body.WriteString(message)

	return writeObject(t, gitDir, "commit", body.Bytes())
}

func writeObject(t testing.TB, gitDir, typ string, body []byte) string {
	t.Helper()

	store := append([]byte(fmt.Sprintf("%s %d\x00", typ, len(body))), body...)
	sum := sha1.Sum(store)
	oid := hex.EncodeToString(sum[:])

	objectPath := filepath.Join(gitDir, "objects", oid[:2], oid[2:])
	if _, err := os.Stat(objectPath); err == nil {
		return oid
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat object %s: %v", objectPath, err)
	}

	if err := os.MkdirAll(filepath.Dir(objectPath), 0o755); err != nil {
		t.Fatalf("create object dir %s: %v", filepath.Dir(objectPath), err)
	}

	var compressed bytes.Buffer
	zw := zlib.NewWriter(&compressed)
	if _, err := zw.Write(store); err != nil {
		t.Fatalf("compress object %s: %v", oid, err)
	}

	if err := zw.Close(); err != nil {
		t.Fatalf("close compressed object %s: %v", oid, err)
	}
	
	writeFile(t, objectPath, compressed.Bytes())
	return oid
}

func writeFile(t testing.TB, path string, data []byte) {
	t.Helper()

	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
