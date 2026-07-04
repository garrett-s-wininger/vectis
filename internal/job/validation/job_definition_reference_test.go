package validation

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"vectis/internal/action/builtins"
)

func TestJobDefinitionReferenceMentionsProtoFields(t *testing.T) {
	doc := readRepoFile(t, "website", "docs", "using", "job-definition-reference.md")
	proto := readRepoFile(t, "api", "proto", "common.proto")

	for _, field := range jobDefinitionProtoFields(t, proto) {
		requireReferenceToken(t, doc, field)
	}
}

func TestJobDefinitionReferenceMentionsBuiltinActions(t *testing.T) {
	doc := readRepoFile(t, "website", "docs", "using", "job-definition-reference.md")

	descriptors, err := builtins.NewRegistry().ListDescriptors()
	if err != nil {
		t.Fatalf("list built-in descriptors: %v", err)
	}

	for _, descriptor := range descriptors {
		requireReferenceToken(t, doc, descriptor.CanonicalName)

		for _, field := range descriptor.InputSchema.Fields {
			requireReferenceToken(t, doc, field.Name)
		}

		for _, port := range descriptor.PortSchema {
			requireReferenceToken(t, doc, port.Name)
		}
	}

	for _, token := range []string{
		"execution",
		"local",
		"distributed",
		"host",
		"vm",
		"file",
		"success",
		"name",
		"path",
		"content_type",
		"metadata_json",
		"max_bytes",
	} {
		requireReferenceToken(t, doc, token)
	}
}

func readRepoFile(t *testing.T, parts ...string) string {
	t.Helper()

	path := filepath.Join(append([]string{"..", "..", ".."}, parts...)...)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	return string(data)
}

func jobDefinitionProtoFields(t *testing.T, proto string) []string {
	t.Helper()

	wantedMessages := map[string]bool{
		"Job":             true,
		"Node":            true,
		"NodePort":        true,
		"NodeInput":       true,
		"NodeOutputRef":   true,
		"SecretDelivery":  true,
		"SecretReference": true,
	}

	messageRe := regexp.MustCompile(`^\s*message\s+([A-Za-z0-9_]+)\s*\{`)
	fieldRe := regexp.MustCompile(`^\s*(?:repeated\s+)?(?:map<[^>]+>|[A-Za-z0-9_.]+)\s+([a-z][a-z0-9_]*)\s*=`)

	seen := map[string]bool{}
	var fields []string
	current := ""

	for _, line := range strings.Split(proto, "\n") {
		if match := messageRe.FindStringSubmatch(line); match != nil {
			if wantedMessages[match[1]] {
				current = match[1]
			} else {
				current = ""
			}
			continue
		}

		if current == "" {
			continue
		}

		if strings.HasPrefix(strings.TrimSpace(line), "}") {
			current = ""
			continue
		}

		match := fieldRe.FindStringSubmatch(line)
		if match == nil || seen[match[1]] {
			continue
		}

		seen[match[1]] = true
		fields = append(fields, match[1])
	}

	if len(fields) == 0 {
		t.Fatal("no job definition proto fields found")
	}

	return fields
}

func requireReferenceToken(t *testing.T, doc, token string) {
	t.Helper()

	for _, needle := range []string{
		"`" + token + "`",
		`"` + token + `"`,
	} {
		if strings.Contains(doc, needle) {
			return
		}
	}

	t.Fatalf("job definition reference does not mention %q", token)
}
