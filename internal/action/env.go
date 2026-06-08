package action

import (
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

const defaultProcessPath = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

func (s *ExecutionState) CommandEnv() []string {
	if s == nil {
		return SanitizedProcessEnv("", os.Environ())
	}

	if len(s.ProcessEnv) > 0 {
		return append([]string(nil), s.ProcessEnv...)
	}

	return SanitizedProcessEnv(s.Workspace, os.Environ())
}

func SanitizedProcessEnv(workspace string, base []string) []string {
	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		workspace = "."
	}

	tmpDir := filepath.Join(workspace, ".tmp")
	path := envValueOrDefault(base, "PATH", defaultProcessPath)

	values := map[string]string{
		"CI":     "true",
		"HOME":   workspace,
		"PATH":   path,
		"TMPDIR": tmpDir,
		"VECTIS": "true",
	}

	if runtime.GOOS == "windows" {
		values["TEMP"] = tmpDir
		values["TMP"] = tmpDir
		values["USERPROFILE"] = workspace
		copyIfSet(values, base, "COMSPEC")
		copyIfSet(values, base, "PATHEXT")
		copyIfSet(values, base, "SystemRoot")
		copyIfSet(values, base, "WINDIR")
	}

	return sortedEnv(values)
}

func AppendEnv(env []string, key, value string) []string {
	key = strings.TrimSpace(key)
	if key == "" || strings.Contains(key, "=") {
		return append([]string(nil), env...)
	}

	out := make([]string, 0, len(env)+1)
	replaced := false
	for _, entry := range env {
		k, _, ok := strings.Cut(entry, "=")
		if ok && envKeyEqual(k, key) {
			if !replaced {
				out = append(out, key+"="+value)
				replaced = true
			}

			continue
		}

		out = append(out, entry)
	}

	if !replaced {
		out = append(out, key+"="+value)
	}

	return out
}

func copyIfSet(values map[string]string, base []string, key string) {
	if value, ok := lookupEnv(base, key); ok && value != "" {
		values[key] = value
	}
}

func envValueOrDefault(base []string, key, fallback string) string {
	if value, ok := lookupEnv(base, key); ok && value != "" {
		return value
	}

	return fallback
}

func lookupEnv(env []string, key string) (string, bool) {
	for _, entry := range env {
		k, v, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}

		if envKeyEqual(k, key) {
			return v, true
		}
	}

	return "", false
}

func envKeyEqual(a, b string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(a, b)
	}

	return a == b
}

func sortedEnv(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key+"="+values[key])
	}

	return out
}
