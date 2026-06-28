package job

import (
	"os"
	"path/filepath"
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
