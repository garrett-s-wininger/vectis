package observability

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

func TestMetricsCatalogMentionsRegisteredMetricNames(t *testing.T) {
	docPath := filepath.Join("..", "..", "website", "docs", "operating", "reference", "metrics-catalog.md")
	doc, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read metrics catalog: %v", err)
	}

	docText := string(doc)
	metrics, err := registeredMetricNames()
	if err != nil {
		t.Fatal(err)
	}

	if len(metrics) == 0 {
		t.Fatal("found no registered metric names")
	}

	var missing []string
	for name := range metrics {
		if !strings.Contains(docText, "`"+name+"`") {
			missing = append(missing, name)
		}
	}

	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("metrics catalog is missing registered metrics: %s", strings.Join(missing, ", "))
	}
}

func registeredMetricNames() (map[string]bool, error) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		return nil, err
	}

	metricConstructor := regexp.MustCompile(`\.(?:Int64Counter|Float64Histogram|Int64ObservableGauge|Float64ObservableGauge)\("([^"]+)"`)
	out := make(map[string]bool)
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}

		raw, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}

		for _, match := range metricConstructor.FindAllSubmatch(raw, -1) {
			out[string(match[1])] = true
		}
	}

	return out, nil
}
