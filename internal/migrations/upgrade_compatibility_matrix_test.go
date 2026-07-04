package migrations_test

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestUpgradeCompatibilityMatrixMentionsOperatorContract(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "website", "docs", "operating", "reference", "upgrade-compatibility-matrix.md"))
	if err != nil {
		t.Fatalf("read upgrade compatibility matrix: %v", err)
	}
	doc := string(raw)

	tokens := []string{
		"vectis-cli database migrate",
		"GET /api/v1/schema/status",
		"schema_migrations",
		"has_schema",
		"current_version",
		"vectis-cli health check --strict",
		"old binaries",
		"new binaries",
		"Previous artifacts only",
		"Database restore plus previous artifacts",
		"Roll-forward repair",
		"Safe down migration",
		"SQL database",
		"Queue persistence",
		"Log storage",
		"Artifact CAS",
		"Secret envelopes and keys",
		"SPIFFE authority material",
		"Deploy config/secrets",
		"REST API v1",
		"gRPC/protobuf",
		"OpenAPI",
		"CLI",
		"Metrics and health",
	}

	var missing []string
	for _, token := range tokens {
		if !upgradeMatrixContains(doc, token) {
			missing = append(missing, token)
		}
	}

	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("upgrade compatibility matrix is missing tokens: %s", strings.Join(missing, ", "))
	}
}

func upgradeMatrixContains(doc, token string) bool {
	for _, needle := range []string{
		"`" + token + "`",
		"|" + token + " |",
		token,
	} {
		if strings.Contains(doc, needle) {
			return true
		}
	}

	return false
}
