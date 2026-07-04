package gitcmd

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveGitDirRefResolvesLooseSymbolicRefs(t *testing.T) {
	gitDir := t.TempDir()
	head := filepath.Join(gitDir, "HEAD")
	mainRef := filepath.Join(gitDir, "refs", "heads", "main")
	if err := os.MkdirAll(filepath.Dir(mainRef), 0o755); err != nil {
		t.Fatalf("create ref dir: %v", err)
	}

	if err := os.WriteFile(head, []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatalf("write HEAD: %v", err)
	}

	if err := os.WriteFile(mainRef, []byte("1111111111111111111111111111111111111111\n"), 0o644); err != nil {
		t.Fatalf("write main ref: %v", err)
	}

	got, ok, err := ResolveGitDirRef(gitDir, "HEAD")
	if err != nil {
		t.Fatalf("ResolveGitDirRef: %v", err)
	}

	if !ok || got != "1111111111111111111111111111111111111111" {
		t.Fatalf("ResolveGitDirRef got %q ok=%v", got, ok)
	}
}

func TestResolveGitDirRefResolvesPackedRefs(t *testing.T) {
	gitDir := t.TempDir()
	packedRefs := "" +
		"# pack-refs with: peeled fully-peeled sorted\n" +
		"2222222222222222222222222222222222222222 refs/remotes/origin/main\n"
	if err := os.WriteFile(filepath.Join(gitDir, "packed-refs"), []byte(packedRefs), 0o644); err != nil {
		t.Fatalf("write packed-refs: %v", err)
	}

	got, ok, err := ResolveGitDirRef(gitDir, "refs/remotes/origin/main")
	if err != nil {
		t.Fatalf("ResolveGitDirRef: %v", err)
	}

	if !ok || got != "2222222222222222222222222222222222222222" {
		t.Fatalf("ResolveGitDirRef got %q ok=%v", got, ok)
	}
}

func TestSymbolicGitDirRefReturnsLooseTarget(t *testing.T) {
	gitDir := t.TempDir()
	refPath := filepath.Join(gitDir, "refs", "remotes", "origin", "HEAD")
	if err := os.MkdirAll(filepath.Dir(refPath), 0o755); err != nil {
		t.Fatalf("create ref dir: %v", err)
	}

	if err := os.WriteFile(refPath, []byte("ref: refs/remotes/origin/main\n"), 0o644); err != nil {
		t.Fatalf("write symbolic ref: %v", err)
	}

	got, ok, err := SymbolicGitDirRef(gitDir, "refs/remotes/origin/HEAD")
	if err != nil {
		t.Fatalf("SymbolicGitDirRef: %v", err)
	}

	if !ok || got != "refs/remotes/origin/main" {
		t.Fatalf("SymbolicGitDirRef got %q ok=%v", got, ok)
	}
}

func TestWriteGitDirRefWritesLooseRef(t *testing.T) {
	gitDir := t.TempDir()
	if err := WriteGitDirRef(gitDir, "refs/pull/123/head", "3333333333333333333333333333333333333333"); err != nil {
		t.Fatalf("WriteGitDirRef: %v", err)
	}

	got, ok, err := ResolveGitDirRef(gitDir, "refs/pull/123/head")
	if err != nil {
		t.Fatalf("ResolveGitDirRef: %v", err)
	}
	
	if !ok || got != "3333333333333333333333333333333333333333" {
		t.Fatalf("ResolveGitDirRef got %q ok=%v", got, ok)
	}
}
