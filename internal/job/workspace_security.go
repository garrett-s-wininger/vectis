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
	info, err := os.Lstat(workspace)
	if err != nil {
		return fmt.Errorf("stat workspace: %w", err)
	}

	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("workspace must not be a symlink: %s", workspace)
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

func ensureWorkspacePrivateDir(workspace, name string) error {
	if strings.TrimSpace(workspace) == "" {
		return fmt.Errorf("workspace is required")
	}

	if strings.TrimSpace(name) == "" || filepath.IsAbs(name) || strings.ContainsRune(name, filepath.Separator) {
		return fmt.Errorf("workspace private dir name is invalid")
	}

	path := filepath.Join(workspace, name)
	info, err := os.Lstat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("stat %s: %w", name, err)
		}

		if err := os.Mkdir(path, 0o700); err != nil {
			if !os.IsExist(err) {
				return fmt.Errorf("create %s: %w", name, err)
			}
		}

		info, err = os.Lstat(path)
		if err != nil {
			return fmt.Errorf("stat %s: %w", name, err)
		}
	}

	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%s must not be a symlink", name)
	}

	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", name)
	}

	if runtime.GOOS == "windows" {
		return nil
	}

	if err := os.Chmod(path, 0o700); err != nil {
		return fmt.Errorf("chmod %s: %w", name, err)
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

	workspaceForValidation := filepath.Clean(absWorkspace)
	rootForValidation := filepath.Clean(absRoot)
	if realRoot, err := filepath.EvalSymlinks(rootForValidation); err != nil {
		return fmt.Errorf("resolve workspace root symlinks: %w", err)
	} else {
		rootForValidation = filepath.Clean(realRoot)
	}

	info, err := os.Lstat(workspaceForValidation)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("stat workspace: %w", err)
		}

		rootForValidation = filepath.Clean(absRoot)
	} else {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("workspace path must not be a symlink")
		}

		realWorkspace, err := filepath.EvalSymlinks(workspaceForValidation)
		if err != nil {
			return fmt.Errorf("resolve workspace symlinks: %w", err)
		}

		workspaceForValidation = filepath.Clean(realWorkspace)
	}

	if workspaceForValidation == string(filepath.Separator) {
		return fmt.Errorf("workspace path must not be filesystem root")
	}

	if workspaceForValidation == rootForValidation {
		return fmt.Errorf("workspace path must not equal workspace root")
	}

	rel, err := filepath.Rel(rootForValidation, workspaceForValidation)
	if err != nil {
		return fmt.Errorf("resolve workspace relative to root: %w", err)
	}

	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return fmt.Errorf("workspace must stay under workspace root")
	}

	if strings.ContainsRune(rel, filepath.Separator) {
		return fmt.Errorf("auto workspace must be a direct child of workspace root")
	}

	if !strings.HasPrefix(filepath.Base(workspaceForValidation), "vectis-") {
		return fmt.Errorf("auto workspace name must start with vectis-")
	}

	return nil
}
