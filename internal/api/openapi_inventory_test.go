package api

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenAPISpecRouteInventory(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "website", "static", "openapi", "v1.json"))
	if err != nil {
		t.Fatalf("read OpenAPI spec: %v", err)
	}

	var spec struct {
		Paths map[string]map[string]json.RawMessage `json:"paths"`
	}

	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("decode OpenAPI spec: %v", err)
	}

	if len(spec.Paths) == 0 {
		t.Fatal("OpenAPI spec has no paths")
	}

	server := &APIServer{}
	for _, route := range server.routeSpecs(false) {
		method, path, ok := strings.Cut(route.Pattern, " ")
		if !ok {
			t.Fatalf("route pattern %q is not METHOD path", route.Pattern)
		}

		methods, ok := spec.Paths[path]
		if !ok {
			t.Fatalf("OpenAPI spec missing path %q", path)
		}

		if _, ok := methods[strings.ToLower(method)]; !ok {
			t.Fatalf("OpenAPI spec missing %s operation for %q", method, path)
		}
	}
}

func TestOpenAPISpecOperationIDsAreUnique(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "website", "static", "openapi", "v1.json"))
	if err != nil {
		t.Fatalf("read OpenAPI spec: %v", err)
	}

	var spec struct {
		Paths map[string]map[string]struct {
			OperationID string `json:"operationId"`
		} `json:"paths"`
	}

	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("decode OpenAPI spec: %v", err)
	}

	seen := make(map[string]string)
	for path, methods := range spec.Paths {
		for method, operation := range methods {
			if operation.OperationID == "" {
				t.Fatalf("OpenAPI %s %s is missing operationId", strings.ToUpper(method), path)
			}

			where := strings.ToUpper(method) + " " + path
			if prev, ok := seen[operation.OperationID]; ok {
				t.Fatalf("OpenAPI operationId %q is used by both %s and %s", operation.OperationID, prev, where)
			}

			seen[operation.OperationID] = where
		}
	}
}
