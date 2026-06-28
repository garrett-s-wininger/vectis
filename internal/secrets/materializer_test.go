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

	if err := CleanupMaterialized(workspace); err != nil {
		t.Fatalf("CleanupMaterialized: %v", err)
	}

	if _, err := os.Stat(wantDir); !os.IsNotExist(err) {
		t.Fatalf("secret dir still exists after cleanup: %v", err)
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
