package refspec

import (
	"fmt"
	"path"
	"path/filepath"
	"strings"
)

const DefaultDefinitionPath = ".vectis/jobs"

func NormalizeRef(ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", fmt.Errorf("revision is required")
	}

	if ref == "HEAD" || looksLikeFullObjectID(ref) {
		return ref, nil
	}

	if strings.HasPrefix(ref, "-") ||
		strings.HasPrefix(ref, "/") ||
		strings.HasSuffix(ref, "/") ||
		strings.Contains(ref, "//") ||
		strings.Contains(ref, "..") ||
		strings.Contains(ref, "@{") ||
		strings.ContainsAny(ref, "\x00\n\r~^:?*[\\") ||
		ref == "@" {
		return "", fmt.Errorf("unsafe revision %q", ref)
	}

	for _, part := range strings.Split(ref, "/") {
		if part == "" ||
			strings.HasPrefix(part, ".") ||
			strings.HasSuffix(part, ".") ||
			strings.HasSuffix(part, ".lock") {
			return "", fmt.Errorf("unsafe revision %q", ref)
		}
	}

	return ref, nil
}

func NormalizeTreeListPath(filePath string) (string, error) {
	filePath = strings.TrimSpace(filepath.ToSlash(filePath))
	if filePath == "" || filePath == "." {
		return "", nil
	}

	if strings.ContainsAny(filePath, "\x00\n\r") || path.IsAbs(filePath) {
		return "", fmt.Errorf("unsafe tree path %q", filePath)
	}

	cleanPath := path.Clean(filePath)
	if cleanPath == "." {
		return "", nil
	}

	if cleanPath == ".." || strings.HasPrefix(cleanPath, "../") {
		return "", fmt.Errorf("unsafe tree path %q", filePath)
	}

	return cleanPath, nil
}

func NormalizeTreePath(filePath string) (string, error) {
	filePath = strings.TrimSpace(filepath.ToSlash(filePath))
	if filePath == "" {
		return "", fmt.Errorf("file path is required")
	}

	if strings.ContainsAny(filePath, "\x00\n\r") || path.IsAbs(filePath) {
		return "", fmt.Errorf("unsafe file path %q", filePath)
	}

	cleanPath := path.Clean(filePath)
	if cleanPath == "." || cleanPath == ".." || strings.HasPrefix(cleanPath, "../") {
		return "", fmt.Errorf("unsafe file path %q", filePath)
	}

	return cleanPath, nil
}

func DefinitionPathForJobID(jobID string) (string, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" || strings.Contains(jobID, "/") || strings.Contains(jobID, "\\") {
		return "", fmt.Errorf("job_id cannot derive a source path")
	}

	parts := strings.Split(jobID, ".")
	for _, part := range parts {
		if !validDefinitionPathJobIDPart(part) {
			return "", fmt.Errorf("job_id cannot derive a source path")
		}
	}

	return path.Join(DefaultDefinitionPath, path.Join(parts...)+".json"), nil
}

func validDefinitionPathJobIDPart(part string) bool {
	if part == "" || part == "." || part == ".." {
		return false
	}

	for _, r := range part {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '-' ||
			r == '_' ||
			r == '.' {
			continue
		}

		return false
	}

	return true
}

func looksLikeFullObjectID(ref string) bool {
	if len(ref) != 40 && len(ref) != 64 {
		return false
	}

	for _, r := range ref {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return false
		}
	}

	return true
}
