package platform

import (
	"path/filepath"
	"strings"
)

// IsCrossPlatformAbsPath reports whether path is absolute under either POSIX or
// Windows spelling, independent of the host running the validation.
func IsCrossPlatformAbsPath(path string) bool {
	path = strings.TrimSpace(path)
	if path == "" {
		return false
	}

	if filepath.IsAbs(path) || strings.HasPrefix(path, "/") || strings.HasPrefix(path, `\`) {
		return true
	}

	if len(path) >= 2 && path[1] == ':' && isASCIIAlpha(path[0]) {
		return true
	}

	return false
}

func isASCIIAlpha(c byte) bool {
	return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')
}
