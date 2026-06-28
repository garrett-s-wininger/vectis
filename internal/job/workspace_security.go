package job

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"vectis/internal/interfaces"
)

const autoWorkspaceMode os.FileMode = 0o700

func secureAutoWorkspace(workspace, root string) error {
	if err := validateAutoWorkspaceCleanupPath(workspace, root); err != nil {
		return err
	}

	if runtime.GOOS == "windows" {
		return nil
	}

	if err := os.Chmod(workspace, autoWorkspaceMode); err != nil {
		return fmt.Errorf("chmod auto workspace: %w", err)
	}

	return nil
}

func warnIfExplicitWorkspaceBroadPermissions(workspace string, logger interfaces.Logger) error {
	info, err := os.Stat(workspace)
	if err != nil {
		return fmt.Errorf("stat workspace: %w", err)
	}

	if !info.IsDir() {
		return fmt.Errorf("workspace is not a directory: %s", workspace)
	}

	if runtime.GOOS == "windows" {
		return nil
	}

	if info.Mode().Perm()&0o077 != 0 && logger != nil {
		logger.Warn("Workspace %s is group/world accessible (%v); use owner-only permissions for stronger local isolation", workspace, info.Mode().Perm())
	}

	return nil
}

func validateAutoWorkspaceCleanupPath(workspace, root string) error {
	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return fmt.Errorf("workspace path is required")
	}

	root = strings.TrimSpace(root)
	if root == "" {
		return fmt.Errorf("workspace root is required")
	}

	absWorkspace, err := filepath.Abs(workspace)
	if err != nil {
		return fmt.Errorf("resolve workspace: %w", err)
	}

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("resolve workspace root: %w", err)
	}

	absWorkspace = filepath.Clean(absWorkspace)
	absRoot = filepath.Clean(absRoot)
	if absWorkspace == string(filepath.Separator) {
		return fmt.Errorf("workspace path must not be filesystem root")
	}

	if absWorkspace == absRoot {
		return fmt.Errorf("workspace path must not equal workspace root")
	}

	rel, err := filepath.Rel(absRoot, absWorkspace)
	if err != nil {
		return fmt.Errorf("resolve workspace relative to root: %w", err)
	}

	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return fmt.Errorf("workspace must stay under workspace root")
	}

	if strings.ContainsRune(rel, filepath.Separator) {
		return fmt.Errorf("auto workspace must be a direct child of workspace root")
	}

	if !strings.HasPrefix(filepath.Base(absWorkspace), "vectis-") {
		return fmt.Errorf("auto workspace name must start with vectis-")
	}

	return nil
}
