package api

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

func TestAPIErrorCodeReferenceMentionsEmittedCodes(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "website", "docs", "using", "api-error-code-reference.md"))
	if err != nil {
		t.Fatalf("read API error code reference: %v", err)
	}

	docText := string(raw)
	codes := collectAPIErrorCodes(t)

	var missing []string
	for code := range codes {
		if !strings.Contains(docText, "`"+code+"`") {
			missing = append(missing, code)
		}
	}

	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("API error code reference is missing codes: %s", strings.Join(missing, ", "))
	}
}

func collectAPIErrorCodes(t *testing.T) map[string]bool {
	t.Helper()

	codes := map[string]bool{}
	constantRe := regexp.MustCompile(`apiErr\w+\s+apiErrorCode\s*=\s*"([a-z0-9_]+)"`)
	literalRe := regexp.MustCompile(`writeAPIError\s*\([^,\n]+,\s*http\.Status\w+,\s*"([a-z0-9_]+)"`)

	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(raw)

		for _, match := range constantRe.FindAllStringSubmatch(text, -1) {
			codes[match[1]] = true
		}
		for _, match := range literalRe.FindAllStringSubmatch(text, -1) {
			codes[match[1]] = true
		}

		return nil
	})
	if err != nil {
		t.Fatalf("scan API source for error codes: %v", err)
	}

	if len(codes) == 0 {
		t.Fatal("no API error codes found")
	}

	return codes
}
