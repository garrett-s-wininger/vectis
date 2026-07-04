package audit

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestAuditEventCatalogMentionsRegisteredEvents(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "website", "docs", "operating", "reference", "audit-event-catalog.md"))
	if err != nil {
		t.Fatalf("read audit event catalog: %v", err)
	}

	docText := string(raw)
	var missing []string
	for _, def := range EventDefinitions {
		expectedRowPrefix := "| `" + def.Type + "` | `" + string(def.DefaultDurability) + "` |"
		if !strings.Contains(docText, expectedRowPrefix) {
			missing = append(missing, def.Type+"="+string(def.DefaultDurability))
		}
	}

	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("audit event catalog is missing event durability rows: %s", strings.Join(missing, ", "))
	}
}

func TestAuditEventCatalogMentionsDurabilityValues(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "website", "docs", "operating", "reference", "audit-event-catalog.md"))
	if err != nil {
		t.Fatalf("read audit event catalog: %v", err)
	}

	docText := string(raw)
	durabilities := []Durability{
		DurabilityDisabled,
		DurabilityBestEffort,
		DurabilityDurableBestEffort,
		DurabilityFailClosed,
	}

	var missing []string
	for _, durability := range durabilities {
		value := string(durability)
		if !strings.Contains(docText, "`"+value+"`") {
			missing = append(missing, value)
		}
	}

	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("audit event catalog is missing durabilities: %s", strings.Join(missing, ", "))
	}
}
