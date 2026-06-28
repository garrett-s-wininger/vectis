package cmd_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLifecycleContractDocumentsEveryDaemon(t *testing.T) {
	docPath := filepath.Join("..", "website", "docs", "operating", "reference", "lifecycle-contracts.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read lifecycle contract: %v", err)
	}

	doc := string(raw)
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read cmd directory: %v", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		if _, err := os.Stat(filepath.Join(entry.Name(), "main.go")); err != nil {
			continue
		}

		if entry.Name() == "cli" {
			continue
		}

		binary := "vectis-" + entry.Name()
		rowPrefix := "| `" + binary + "` |"
		if !strings.Contains(doc, rowPrefix) {
			t.Fatalf("%s is missing from %s", binary, docPath)
		}
	}
}
