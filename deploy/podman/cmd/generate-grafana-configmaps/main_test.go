package main

import (
	"errors"
	"io"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestRenderGrafanaConfigMapsProducesYAMLConfigMaps(t *testing.T) {
	out, err := renderGrafanaConfigMaps()
	if err != nil {
		t.Fatal(err)
	}

	dec := yaml.NewDecoder(strings.NewReader(out))
	seen := map[string]map[string]any{}
	for {
		var doc map[string]any
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode generated YAML: %v", err)
		}
		if len(doc) == 0 {
			continue
		}
		if doc["kind"] != "ConfigMap" {
			t.Fatalf("kind = %v, want ConfigMap", doc["kind"])
		}
		metadata, ok := doc["metadata"].(map[string]any)
		if !ok {
			t.Fatalf("metadata has type %T", doc["metadata"])
		}
		name, ok := metadata["name"].(string)
		if !ok || name == "" {
			t.Fatalf("metadata.name = %#v", metadata["name"])
		}
		seen[name] = doc
	}

	if len(seen) != 2 {
		t.Fatalf("generated %d ConfigMaps, want 2", len(seen))
	}
	requireConfigMapDataKey(t, seen, "vectis-grafana-dashboard-provider", "dashboards.yaml")
	requireConfigMapDataKey(t, seen, "vectis-grafana-dashboards", "vectis-overview.json")
}

func requireConfigMapDataKey(t *testing.T, docs map[string]map[string]any, name, key string) {
	t.Helper()

	doc, ok := docs[name]
	if !ok {
		t.Fatalf("ConfigMap %s not found", name)
	}
	data, ok := doc["data"].(map[string]any)
	if !ok {
		t.Fatalf("%s data has type %T", name, doc["data"])
	}
	if value, ok := data[key].(string); !ok || value == "" {
		t.Fatalf("%s data[%s] = %#v", name, key, data[key])
	}
}
