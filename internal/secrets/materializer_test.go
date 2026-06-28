package secrets

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMaterializeFilesWritesUnderWorkspaceSecretsDir(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()
	result, err := MaterializeFiles(workspace, []FileMaterial{{
		ID:   "npm-token",
		Path: "npm/token",
		Data: []byte("secret-value"),
	}})
	if err != nil {
		t.Fatalf("MaterializeFiles: %v", err)
	}

	wantDir := filepath.Join(workspace, ".vectis", "secrets")
	if result.Dir != wantDir {
		t.Fatalf("materialized dir = %q, want %q", result.Dir, wantDir)
	}

	if len(result.Files) != 1 {
		t.Fatalf("materialized files = %v, want one file", result.Files)
	}

	got, err := os.ReadFile(filepath.Join(wantDir, "npm", "token"))
	if err != nil {
		t.Fatalf("read materialized secret: %v", err)
	}

	if string(got) != "secret-value" {
		t.Fatalf("materialized secret = %q, want secret-value", string(got))
	}

	info, err := os.Stat(filepath.Join(wantDir, "npm", "token"))
	if err != nil {
		t.Fatalf("stat materialized secret: %v", err)
	}

	if gotMode := info.Mode().Perm(); gotMode != DefaultFileMode {
		t.Fatalf("secret mode = %v, want %v", gotMode, DefaultFileMode)
	}

	parentInfo, err := os.Stat(filepath.Join(wantDir, "npm"))
	if err != nil {
		t.Fatalf("stat materialized secret parent: %v", err)
	}

	if gotMode := parentInfo.Mode().Perm(); gotMode != 0o700 {
		t.Fatalf("secret parent mode = %v, want %v", gotMode, os.FileMode(0o700))
	}

	if err := CleanupMaterialized(workspace); err != nil {
		t.Fatalf("CleanupMaterialized: %v", err)
	}

	if _, err := os.Stat(wantDir); !os.IsNotExist(err) {
		t.Fatalf("secret dir still exists after cleanup: %v", err)
	}
}

func TestMaterializeFilesRejectsGroupReadableMode(t *testing.T) {
	t.Parallel()

	if _, err := MaterializeFiles(t.TempDir(), []FileMaterial{{
		ID:   "bad-mode",
		Path: "token",
		Data: []byte("x"),
		Mode: 0o640,
	}}); err == nil {
		t.Fatal("MaterializeFiles accepted group-readable mode")
	}
}

func TestMaterializeFilesRejectsSymlinkParent(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()
	root := filepath.Join(workspace, ".vectis", "secrets")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("mkdir secrets root: %v", err)
	}

	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "npm")); err != nil {
		t.Fatalf("symlink secret parent: %v", err)
	}

	if _, err := MaterializeFiles(workspace, []FileMaterial{{
		ID:   "npm-token",
		Path: "npm/token",
		Data: []byte("secret"),
	}}); err == nil {
		t.Fatal("MaterializeFiles accepted symlink parent")
	}

	if _, err := os.Stat(filepath.Join(outside, "token")); !os.IsNotExist(err) {
		t.Fatalf("secret written through symlink parent: %v", err)
	}
}

func TestMaterializeFilesRejectsSymlinkVectisDirectory(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(workspace, ".vectis")); err != nil {
		t.Fatalf("symlink .vectis: %v", err)
	}

	if _, err := MaterializeFiles(workspace, []FileMaterial{{
		ID:   "npm-token",
		Path: "npm/token",
		Data: []byte("secret"),
	}}); err == nil {
		t.Fatal("MaterializeFiles accepted symlink .vectis directory")
	}

	if _, err := os.Stat(filepath.Join(outside, "secrets")); !os.IsNotExist(err) {
		t.Fatalf("materializer created secrets directory through symlink: %v", err)
	}
}

func TestCleanupMaterializedRejectsSymlinkComponent(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "keep.txt"), []byte("keep"), 0o600); err != nil {
		t.Fatalf("write outside marker: %v", err)
	}

	if err := os.Symlink(outside, filepath.Join(workspace, ".vectis")); err != nil {
		t.Fatalf("symlink .vectis: %v", err)
	}

	if err := CleanupMaterialized(workspace); err == nil {
		t.Fatal("CleanupMaterialized accepted symlink component")
	}

	if _, err := os.Stat(filepath.Join(outside, "keep.txt")); err != nil {
		t.Fatalf("outside marker removed: %v", err)
	}
}

func TestMaterializeFilesRejectsUnsafePaths(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"", "/token", "../token", "nested/../token", "nested//token", `nested\token`} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			if _, err := MaterializeFiles(t.TempDir(), []FileMaterial{{ID: "bad", Path: path, Data: []byte("x")}}); err == nil {
				t.Fatalf("MaterializeFiles accepted unsafe path %q", path)
			}
		})
	}
}
