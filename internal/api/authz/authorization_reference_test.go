package authz

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

func TestAuthorizationReferenceMentionsActionsAndRoles(t *testing.T) {
	docText := readAuthorizationReference(t)
	source := readAuthzSource(t)

	actionValues := extractStringConstants(t, source, `Action\w+\s+Action\s+=\s+"([^"]+)"`)
	roleValues := extractStringConstants(t, source, `Role\w+\s+=\s+"([^"]+)"`)

	var missing []string
	for _, value := range append(actionValues, roleValues...) {
		if !strings.Contains(docText, "`"+value+"`") {
			missing = append(missing, value)
		}
	}

	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("authorization reference is missing actions or roles: %s", strings.Join(missing, ", "))
	}
}

func TestAuthorizationReferenceMentionsRolePermissionRows(t *testing.T) {
	docText := readAuthorizationReference(t)

	roles := []string{RoleViewer, RoleTrigger, RoleOperator, RoleAdmin}
	var missing []string
	for _, role := range roles {
		actions := rolePermissions[role]
		expectedRowPrefix := "| `" + role + "` | " + joinBacktickedActions(actions) + " |"
		if !strings.Contains(docText, expectedRowPrefix) {
			missing = append(missing, role)
		}
	}

	if len(missing) > 0 {
		t.Fatalf("authorization reference is missing role permission rows: %s", strings.Join(missing, ", "))
	}
}

func readAuthorizationReference(t *testing.T) string {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "website", "docs", "operating", "reference", "authorization-reference.md"))
	if err != nil {
		t.Fatalf("read authorization reference: %v", err)
	}

	return string(raw)
}

func readAuthzSource(t *testing.T) string {
	t.Helper()

	raw, err := os.ReadFile("authz.go")
	if err != nil {
		t.Fatalf("read authz source: %v", err)
	}

	return string(raw)
}

func extractStringConstants(t *testing.T, source, pattern string) []string {
	t.Helper()

	re := regexp.MustCompile(pattern)
	matches := re.FindAllStringSubmatch(source, -1)
	if len(matches) == 0 {
		t.Fatalf("no string constants matched %q", pattern)
	}

	values := make([]string, 0, len(matches))
	for _, match := range matches {
		values = append(values, match[1])
	}

	return values
}

func joinBacktickedActions(actions []Action) string {
	parts := make([]string, 0, len(actions))
	for _, action := range actions {
		parts = append(parts, "`"+string(action)+"`")
	}

	return strings.Join(parts, ", ")
}
