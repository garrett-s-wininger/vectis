package config

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/pelletier/go-toml/v2"
)

func TestConfigurationReferenceMentionsDefaultKeys(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "website", "docs", "operating", "reference", "configuration-key-reference.md"))
	if err != nil {
		t.Fatalf("read configuration key reference: %v", err)
	}

	var defaults map[string]any
	if err := toml.Unmarshal([]byte(defaultsToml), &defaults); err != nil {
		t.Fatalf("parse defaults TOML: %v", err)
	}

	keys := map[string]bool{}
	flattenDefaultKeys("", defaults, keys)

	docText := string(raw)
	var missing []string
	for key := range keys {
		if !strings.Contains(docText, "`"+key+"`") {
			missing = append(missing, key)
		}
	}

	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("configuration key reference is missing defaults: %s", strings.Join(missing, ", "))
	}
}

func flattenDefaultKeys(prefix string, value any, out map[string]bool) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			next := key
			if prefix != "" {
				next = prefix + "." + key
			}
			flattenDefaultKeys(next, child, out)
		}
	default:
		if prefix != "" {
			out[prefix] = true
		}
	}
}
