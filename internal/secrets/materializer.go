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

	root := filepath.Join(workspace, filepath.FromSlash(WorkspaceSecretsDir))
	if err := os.MkdirAll(root, 0o700); err != nil {
		return MaterializationResult{}, fmt.Errorf("secrets: create materialization directory: %w", err)
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

		mode := file.Mode
		if mode == 0 {
			mode = DefaultFileMode
		}
		mode &= 0o777

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

	return os.RemoveAll(filepath.Join(workspace, filepath.FromSlash(WorkspaceSecretsDir)))
}

func secretTargetPath(root, rawPath string) (string, error) {
	rawPath = strings.TrimSpace(rawPath)
	if rawPath == "" {
		return "", fmt.Errorf("secrets: file path is required")
	}

	if filepath.IsAbs(rawPath) || strings.HasPrefix(rawPath, "/") || strings.Contains(rawPath, `\`) {
		return "", fmt.Errorf("secrets: file path %q must be relative and slash-separated", rawPath)
	}

	for _, part := range strings.Split(rawPath, "/") {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("secrets: file path %q must not contain empty, current-directory, or parent-directory segments", rawPath)
		}
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
