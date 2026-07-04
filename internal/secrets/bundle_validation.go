package secrets

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
)

var ErrInvalidBundle = errors.New("secrets: invalid bundle")

type expectedFileMaterial struct {
	path string
}

func ValidateResolvedBundle(req ResolveRequest, bundle Bundle) error {
	expected, err := expectedFileMaterials(req.Secrets)
	if err != nil {
		return err
	}

	seen := make(map[string]struct{}, len(bundle.Files))
	for _, file := range bundle.Files {
		id := strings.TrimSpace(file.ID)
		if id == "" {
			return fmt.Errorf("%w: file material id is required", ErrInvalidBundle)
		}

		if id != file.ID {
			return fmt.Errorf("%w: file material id %q is not canonical", ErrInvalidBundle, file.ID)
		}

		expectedFile, ok := expected[id]
		if !ok {
			return fmt.Errorf("%w: file material for undeclared secret %q", ErrInvalidBundle, id)
		}

		if _, ok := seen[id]; ok {
			return fmt.Errorf("%w: duplicate file material for secret %q", ErrInvalidBundle, id)
		}

		seen[id] = struct{}{}
		path, err := canonicalFileDeliveryPath(file.Path)
		if err != nil {
			return fmt.Errorf("%w: file material for secret %q has invalid path: %w", ErrInvalidBundle, id, err)
		}

		if path != file.Path {
			return fmt.Errorf("%w: file material path %q is not canonical", ErrInvalidBundle, file.Path)
		}

		if path != expectedFile.path {
			return fmt.Errorf("%w: file material for secret %q targets %q, want %q", ErrInvalidBundle, id, path, expectedFile.path)
		}

		if err := validateSecretFileMode(file.Mode); err != nil {
			return fmt.Errorf("%w: file material for secret %q has invalid mode: %w", ErrInvalidBundle, id, err)
		}
	}

	for id := range expected {
		if _, ok := seen[id]; !ok {
			return fmt.Errorf("%w: missing file material for secret %q", ErrInvalidBundle, id)
		}
	}

	return nil
}

func expectedFileMaterials(refs []Reference) (map[string]expectedFileMaterial, error) {
	expected := make(map[string]expectedFileMaterial, len(refs))
	paths := make(map[string]string, len(refs))
	for _, ref := range refs {
		id := strings.TrimSpace(ref.ID)
		if id == "" {
			return nil, fmt.Errorf("%w: requested secret id is required", ErrInvalidBundle)
		}

		if id != ref.ID {
			return nil, fmt.Errorf("%w: requested secret id %q is not canonical", ErrInvalidBundle, ref.ID)
		}

		if _, ok := expected[id]; ok {
			return nil, fmt.Errorf("%w: duplicate requested secret id %q", ErrInvalidBundle, id)
		}

		if ref.Delivery.Type != DeliveryTypeFile {
			return nil, fmt.Errorf("%w: requested secret %q has unsupported delivery type %q", ErrInvalidBundle, id, ref.Delivery.Type)
		}

		path, err := canonicalFileDeliveryPath(ref.Delivery.Path)
		if err != nil {
			return nil, fmt.Errorf("%w: requested secret %q has invalid file path: %w", ErrInvalidBundle, id, err)
		}

		if path != ref.Delivery.Path {
			return nil, fmt.Errorf("%w: requested secret %q file path %q is not canonical", ErrInvalidBundle, id, ref.Delivery.Path)
		}

		if firstID, ok := paths[path]; ok {
			return nil, fmt.Errorf("%w: requested secret %q duplicates file path %q first used by secret %q", ErrInvalidBundle, id, path, firstID)
		}

		expected[id] = expectedFileMaterial{path: path}
		paths[path] = id
	}

	return expected, nil
}

func canonicalFileDeliveryPath(raw string) (string, error) {
	path := strings.TrimSpace(raw)
	if path == "" {
		return "", fmt.Errorf("file path is required")
	}

	if filepath.IsAbs(path) || strings.HasPrefix(path, "/") || strings.Contains(path, `\`) {
		return "", fmt.Errorf("file path %q must be relative and slash-separated", path)
	}

	for _, part := range strings.Split(path, "/") {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("file path %q must not contain empty, current-directory, or parent-directory segments", path)
		}
	}

	return path, nil
}

func validateSecretFileMode(mode fs.FileMode) error {
	if mode == 0 {
		return nil
	}

	perm := mode.Perm()
	if perm&0o077 != 0 {
		return fmt.Errorf("mode %04o must not grant group or other permissions", perm)
	}

	if perm&0o111 != 0 {
		return fmt.Errorf("mode %04o must not grant executable permissions", perm)
	}

	return nil
}
