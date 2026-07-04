package api

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestArtifactsReferenceMentionsResponseFieldsAndRoutes(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "website", "docs", "using", "artifacts.md"))
	if err != nil {
		t.Fatalf("read artifacts reference: %v", err)
	}
	docText := string(raw)

	var missing []string
	for _, token := range artifactReferenceTokens() {
		if !containsDocToken(docText, token) {
			missing = append(missing, token)
		}
	}

	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("artifacts reference is missing tokens: %s", strings.Join(missing, ", "))
	}
}

func artifactReferenceTokens() []string {
	tokens := []string{
		"builtins/upload-artifact",
		"name",
		"path",
		"content_type",
		"metadata_json",
		"max_bytes",
		"limit",
		"cursor",
		"task_id",
		"task_attempt_id",
		"execution_id",
		"next_cursor",
		"artifact_not_found",
		"artifacts_not_configured",
		"artifact_service_error",
		"artifact_blob_unavailable",
		"artifact_blob_mismatch",
	}

	responseType := reflect.TypeOf(artifactResponse{})
	for i := 0; i < responseType.NumField(); i++ {
		tag := responseType.Field(i).Tag.Get("json")
		name, _, _ := strings.Cut(tag, ",")
		if name != "" && name != "-" {
			tokens = append(tokens, name)
		}
	}

	server := &APIServer{}
	for _, route := range server.routeSpecs(false) {
		if strings.Contains(route.Pattern, "/artifacts") {
			tokens = append(tokens, route.Pattern)
		}
	}

	return tokens
}

func containsDocToken(docText, token string) bool {
	for _, needle := range []string{
		"`" + token + "`",
		`"` + token + `"`,
	} {
		if strings.Contains(docText, needle) {
			return true
		}
	}

	return false
}
