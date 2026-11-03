package main

import (
	"strings"
	"testing"
)

func TestLintDocumentAcceptsMinimalValidContract(t *testing.T) {
	diagnostics := lintDocument(validDocument())
	if len(diagnostics) != 0 {
		t.Fatalf("expected no diagnostics, got %#v", diagnostics)
	}
}

func TestLintDocumentRejectsDuplicateOperationIDs(t *testing.T) {
	doc := validDocument()
	paths := doc["paths"].(map[string]any)
	paths["/api/v1/other"] = map[string]any{
		"get": map[string]any{
			"operationId":          "getVersion",
			"summary":              "Read another thing",
			"tags":                 []any{"Operations"},
			"security":             []any{map[string]any{"bearerAuth": []any{}}},
			"x-vectis-auth-action": "admin:*",
			"responses": map[string]any{
				"200":     map[string]any{"description": "OK"},
				"default": map[string]any{"$ref": "#/components/responses/Error"},
			},
		},
	}

	assertDiagnosticContains(t, lintDocument(doc), "duplicate operationId")
}

func TestLintDocumentRequiresAuthActionForBearerOperations(t *testing.T) {
	doc := validDocument()
	op := doc["paths"].(map[string]any)["/api/v1/version"].(map[string]any)["get"].(map[string]any)
	delete(op, "x-vectis-auth-action")

	assertDiagnosticContains(t, lintDocument(doc), "bearer-protected operation must declare x-vectis-auth-action")
}

func TestLintDocumentRejectsUnresolvedRefs(t *testing.T) {
	doc := validDocument()
	op := doc["paths"].(map[string]any)["/api/v1/version"].(map[string]any)["get"].(map[string]any)
	op["responses"].(map[string]any)["default"] = map[string]any{"$ref": "#/components/responses/Missing"}

	assertDiagnosticContains(t, lintDocument(doc), "unresolvable reference")
}

func validDocument() map[string]any {
	return map[string]any{
		"openapi": "3.0.3",
		"info": map[string]any{
			"title":       "Vectis HTTP API",
			"version":     "v1",
			"description": "Test contract",
		},
		"tags": []any{
			map[string]any{"name": "Operations"},
		},
		"paths": map[string]any{
			"/api/v1/version": map[string]any{
				"get": map[string]any{
					"operationId":          "getVersion",
					"summary":              "Read version",
					"tags":                 []any{"Operations"},
					"security":             []any{map[string]any{"bearerAuth": []any{}}},
					"x-vectis-auth-action": "admin:*",
					"responses": map[string]any{
						"200":     map[string]any{"description": "Version"},
						"default": map[string]any{"$ref": "#/components/responses/Error"},
					},
				},
			},
		},
		"components": map[string]any{
			"securitySchemes": map[string]any{
				"bearerAuth": map[string]any{
					"type":   "http",
					"scheme": "bearer",
				},
			},
			"responses": map[string]any{
				"Error": map[string]any{
					"description": "Error",
				},
			},
		},
	}
}

func assertDiagnosticContains(t *testing.T, diagnostics []string, want string) {
	t.Helper()

	for _, diagnostic := range diagnostics {
		if strings.Contains(diagnostic, want) {
			return
		}
	}

	t.Fatalf("expected diagnostic containing %q, got %#v", want, diagnostics)
}

func TestLintDocumentPathParameters(t *testing.T) {
	parameter := func(name string, required bool) map[string]any {
		return map[string]any{"name": name, "in": "path", "required": required, "schema": map[string]any{"type": "string"}}
	}

	for _, tc := range []struct {
		name      string
		configure func(map[string]any, map[string]any, map[string]any)
		want      string
	}{
		{name: "missing", want: `missing path parameter "id"`},
		{name: "inline", configure: func(_ map[string]any, _ map[string]any, op map[string]any) {
			op["parameters"] = []any{parameter("id", true)}
		}},
		{name: "inherited reference", configure: func(doc, item, _ map[string]any) {
			doc["components"].(map[string]any)["parameters"] = map[string]any{"ID": parameter("id", true)}
			item["parameters"] = []any{map[string]any{"$ref": "#/components/parameters/ID"}}
		}},
		{name: "operation reference", configure: func(doc, _ map[string]any, op map[string]any) {
			doc["components"].(map[string]any)["parameters"] = map[string]any{"ID": parameter("id", true)}
			op["parameters"] = []any{map[string]any{"$ref": "#/components/parameters/ID"}}
		}},
		{name: "operation overrides inherited", configure: func(_, item, op map[string]any) {
			item["parameters"] = []any{parameter("id", true)}
			op["parameters"] = []any{parameter("id", false)}
		}, want: "must set required to true"},
		{name: "query does not satisfy path", configure: func(_, _, op map[string]any) {
			p := parameter("id", true)
			p["in"] = "query"
			op["parameters"] = []any{p}
		}, want: `missing path parameter "id"`},
		{name: "case sensitive", configure: func(_, _, op map[string]any) {
			op["parameters"] = []any{parameter("ID", true)}
		}, want: `missing path parameter "id"`},
		{name: "extra placeholder", configure: func(_, _, op map[string]any) {
			op["parameters"] = []any{parameter("id", true), parameter("other", true)}
		}, want: `path parameter "other" has no matching placeholder`},
		{name: "duplicate", configure: func(_, _, op map[string]any) {
			op["parameters"] = []any{parameter("id", true), parameter("id", true)}
		}, want: `duplicate path parameter "id"`},
		{name: "schema missing", configure: func(_, _, op map[string]any) {
			p := parameter("id", true)
			delete(p, "schema")
			op["parameters"] = []any{p}
		}, want: "must declare one non-empty schema or content object"},
		{name: "content parameter", configure: func(_, _, op map[string]any) {
			p := parameter("id", true)
			delete(p, "schema")
			p["content"] = map[string]any{"application/json": map[string]any{"schema": map[string]any{"type": "string"}}}
			op["parameters"] = []any{p}
		}},
		{name: "unresolved parameter reference", configure: func(_, _, op map[string]any) {
			op["parameters"] = []any{map[string]any{"$ref": "#/components/parameters/Missing"}}
		}, want: "unresolvable reference"},
		{name: "cyclic parameter reference", configure: func(doc, _ map[string]any, op map[string]any) {
			ref := map[string]any{"$ref": "#/components/parameters/ID"}
			doc["components"].(map[string]any)["parameters"] = map[string]any{"ID": ref}
			op["parameters"] = []any{ref}
		}, want: "cyclic parameter reference"},
		{name: "malformed parameter array", configure: func(_, _, op map[string]any) {
			op["parameters"] = "id"
		}, want: "parameters must be an array"},
		{name: "malformed parameter reference", configure: func(doc, _, op map[string]any) {
			doc["components"].(map[string]any)["parameters"] = map[string]any{"ID": "id"}
			op["parameters"] = []any{map[string]any{"$ref": "#/components/parameters/ID"}}
		}, want: "must resolve to a parameter object"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := validDocument()
			paths := doc["paths"].(map[string]any)
			item := paths["/api/v1/version"].(map[string]any)
			delete(paths, "/api/v1/version")

			paths["/api/v1/items/{id}"] = item
			op := item["get"].(map[string]any)
			if tc.configure != nil {
				tc.configure(doc, item, op)
			}

			diagnostics := lintDocument(doc)
			if tc.want != "" {
				assertDiagnosticContains(t, diagnostics, tc.want)
			} else if len(diagnostics) != 0 {
				t.Fatalf("expected valid parameter declarations, got %v", diagnostics)
			}
		})
	}
}

func TestRepositoryOpenAPIContract(t *testing.T) {
	diagnostics, err := lintFile("../../website/static/openapi/v1.json")
	if err != nil {
		t.Fatal(err)
	}

	if len(diagnostics) != 0 {
		t.Fatalf("invalid repository contract:\n%s", strings.Join(diagnostics, "\n"))
	}
}
