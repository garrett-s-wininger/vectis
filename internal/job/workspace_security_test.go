package job

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"vectis/internal/interfaces/mocks"
)

func TestCleanupWorkspacePathRefusesWorkspaceRoot(t *testing.T) {
	root := t.TempDir()
	marker := filepath.Join(root, "keep.txt")
	if err := os.WriteFile(marker, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	logger := mocks.NewMockLogger()
	cleanupWorkspacePath(root, root, logger)

	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("cleanup removed workspace root marker: %v", err)
	}

	if got := strings.Join(logger.GetErrorCalls(), "\n"); !strings.Contains(got, "Refusing to remove workspace") {
		t.Fatalf("expected cleanup refusal log, got %v", logger.GetErrorCalls())
	}
}

func TestCleanupWorkspacePathRemovesDirectAutoWorkspaceChild(t *testing.T) {
	root := t.TempDir()
	workspace := filepath.Join(root, "vectis-run-123")
	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}

	cleanupWorkspacePath(workspace, root, mocks.NewMockLogger())

	if _, err := os.Stat(workspace); !os.IsNotExist(err) {
		t.Fatalf("workspace still exists after cleanup: %v", err)
	}
}

func TestCleanupWorkspacePathHandlesSymlinkedWorkspaceRoot(t *testing.T) {
	parent := t.TempDir()
	realRoot := filepath.Join(parent, "real-root")
	if err := os.Mkdir(realRoot, 0o700); err != nil {
		t.Fatalf("mkdir real root: %v", err)
	}

	root := filepath.Join(parent, "root-link")
	if err := os.Symlink(realRoot, root); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	workspace := filepath.Join(root, "vectis-run-123")
	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}

	cleanupWorkspacePath(workspace, root, mocks.NewMockLogger())

	if _, err := os.Stat(filepath.Join(realRoot, "vectis-run-123")); !os.IsNotExist(err) {
		t.Fatalf("workspace still exists after cleanup through symlinked root: %v", err)
	}
}

func TestCleanupWorkspacePathRefusesWorkspaceSymlink(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	marker := filepath.Join(outside, "keep.txt")
	if err := os.WriteFile(marker, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write outside marker: %v", err)
	}

	workspace := filepath.Join(root, "vectis-run-123")
	if err := os.Symlink(outside, workspace); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	logger := mocks.NewMockLogger()
	cleanupWorkspacePath(workspace, root, logger)

	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("cleanup followed workspace symlink and removed outside marker: %v", err)
	}

	if got := strings.Join(logger.GetErrorCalls(), "\n"); !strings.Contains(got, "workspace path must not be a symlink") {
		t.Fatalf("expected symlink refusal log, got %v", logger.GetErrorCalls())
	}
}

func TestWarnIfExplicitWorkspaceRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}

	workspace := filepath.Join(root, "workspace-link")
	if err := os.Symlink(target, workspace); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	err := warnIfExplicitWorkspaceBroadPermissions(workspace, mocks.NewMockLogger())
	if err == nil || !strings.Contains(err.Error(), "workspace must not be a symlink") {
		t.Fatalf("warnIfExplicitWorkspaceBroadPermissions error = %v, want symlink rejection", err)
	}
}

func TestEnsureWorkspacePrivateDirCreatesOwnerOnlyDirectory(t *testing.T) {
	workspace := t.TempDir()

	if err := ensureWorkspacePrivateDir(workspace, ".tmp"); err != nil {
		t.Fatalf("ensureWorkspacePrivateDir: %v", err)
	}

	info, err := os.Stat(filepath.Join(workspace, ".tmp"))
	if err != nil {
		t.Fatalf("stat .tmp: %v", err)
	}

	if !info.IsDir() {
		t.Fatal(".tmp is not a directory")
	}

	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o700 {
		t.Fatalf(".tmp mode = %v, want 0700", info.Mode().Perm())
	}
}

func TestEnsureWorkspacePrivateDirRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}

	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}

	if err := os.Symlink(target, filepath.Join(workspace, ".tmp")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	err := ensureWorkspacePrivateDir(workspace, ".tmp")
	if err == nil || !strings.Contains(err.Error(), ".tmp must not be a symlink") {
		t.Fatalf("ensureWorkspacePrivateDir error = %v, want symlink rejection", err)
	}
}

func TestEnsureWorkspacePrivateDirRejectsFile(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, ".tmp"), []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write .tmp file: %v", err)
	}

	err := ensureWorkspacePrivateDir(workspace, ".tmp")
	if err == nil || !strings.Contains(err.Error(), ".tmp is not a directory") {
		t.Fatalf("ensureWorkspacePrivateDir error = %v, want file rejection", err)
	}
}
