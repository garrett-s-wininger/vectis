package secrets

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const WorkspaceSecretsDir = ".vectis/secrets"

type MaterializationResult struct {
	Dir   string
	Files []string
}

func MaterializeFiles(workspace string, files []FileMaterial) (MaterializationResult, error) {
	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return MaterializationResult{}, fmt.Errorf("secrets: workspace is required")
	}

	if err := secureWorkspaceRelativePath(workspace, filepath.Dir(filepath.FromSlash(WorkspaceSecretsDir))); err != nil {
		return MaterializationResult{}, err
	}

	root := filepath.Join(workspace, filepath.FromSlash(WorkspaceSecretsDir))
	if err := os.MkdirAll(root, 0o700); err != nil {
		return MaterializationResult{}, fmt.Errorf("secrets: create materialization directory: %w", err)
	}

	if err := secureWorkspaceRelativePath(workspace, WorkspaceSecretsDir); err != nil {
		return MaterializationResult{}, err
	}

	if err := chmodOwnerOnlyDir(root); err != nil {
		return MaterializationResult{}, fmt.Errorf("secrets: secure materialization directory: %w", err)
	}

	result := MaterializationResult{
		Dir:   root,
		Files: make([]string, 0, len(files)),
	}

	for _, file := range files {
		target, err := secretTargetPath(root, file.Path)
		if err != nil {
			return result, err
		}

		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			return result, fmt.Errorf("secrets: create parent directory for %q: %w", file.ID, err)
		}
		if err := secureSecretParent(root, filepath.Dir(target)); err != nil {
			return result, fmt.Errorf("secrets: secure parent directory for %q: %w", file.ID, err)
		}

		mode := file.Mode
		if mode == 0 {
			mode = DefaultFileMode
		}
		mode &= 0o777
		if err := validateSecretFileMode(mode); err != nil {
			return result, fmt.Errorf("secrets: invalid file mode for %q: %w", file.ID, err)
		}

		f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
		if err != nil {
			return result, fmt.Errorf("secrets: create file for %q: %w", file.ID, err)
		}

		if _, err := f.Write(file.Data); err != nil {
			_ = f.Close()
			return result, fmt.Errorf("secrets: write file for %q: %w", file.ID, err)
		}

		if err := f.Close(); err != nil {
			return result, fmt.Errorf("secrets: close file for %q: %w", file.ID, err)
		}

		if err := os.Chmod(target, mode); err != nil {
			return result, fmt.Errorf("secrets: chmod file for %q: %w", file.ID, err)
		}

		result.Files = append(result.Files, target)
	}

	return result, nil
}

func CleanupMaterialized(workspace string) error {
	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return fmt.Errorf("secrets: workspace is required")
	}

	if err := secureWorkspaceRelativePath(workspace, WorkspaceSecretsDir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return err
	}

	return os.RemoveAll(filepath.Join(workspace, filepath.FromSlash(WorkspaceSecretsDir)))
}

func secretTargetPath(root, rawPath string) (string, error) {
	rawPath, err := canonicalFileDeliveryPath(rawPath)
	if err != nil {
		return "", fmt.Errorf("secrets: %w", err)
	}

	target := filepath.Join(root, filepath.FromSlash(rawPath))
	rel, err := filepath.Rel(root, target)
	if err != nil {
		return "", fmt.Errorf("secrets: resolve file path %q: %w", rawPath, err)
	}

	if rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return "", fmt.Errorf("secrets: file path %q escapes the secrets directory", rawPath)
	}

	return target, nil
}

func secureSecretParent(root, parent string) error {
	rel, err := filepath.Rel(root, parent)
	if err != nil {
		return fmt.Errorf("resolve parent path: %w", err)
	}

	if rel == "." {
		return chmodOwnerOnlyDir(root)
	}

	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return fmt.Errorf("parent path escapes the secrets directory")
	}

	if err := chmodOwnerOnlyDir(root); err != nil {
		return err
	}

	current := root
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}

		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("path component %q is a symlink", current)
		}

		if !info.IsDir() {
			return fmt.Errorf("path component %q is not a directory", current)
		}

		if err := chmodOwnerOnlyDir(current); err != nil {
			return err
		}
	}

	return nil
}

func chmodOwnerOnlyDir(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}

	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("path component %q is a symlink", path)
	}

	if !info.IsDir() {
		return fmt.Errorf("path component %q is not a directory", path)
	}

	return os.Chmod(path, 0o700)
}

func secureWorkspaceRelativePath(workspace, relPath string) error {
	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return fmt.Errorf("secrets: workspace is required")
	}

	absWorkspace, err := filepath.Abs(workspace)
	if err != nil {
		return fmt.Errorf("secrets: resolve workspace: %w", err)
	}

	current := filepath.Clean(absWorkspace)
	for _, part := range strings.Split(filepath.FromSlash(relPath), string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}

		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}

			return err
		}

		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("secrets: path component %q is a symlink", current)
		}
	}

	return nil
}
