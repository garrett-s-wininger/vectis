package source

import (
	"errors"
	"path"
	"strings"
)

func DefinitionPathForJobID(jobID string) (string, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" || strings.Contains(jobID, "/") || strings.Contains(jobID, "\\") {
		return "", errors.New("job_id cannot derive a source path")
	}

	parts := strings.Split(jobID, ".")
	for _, part := range parts {
		if !validDefinitionPathJobIDPart(part) {
			return "", errors.New("job_id cannot derive a source path")
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
