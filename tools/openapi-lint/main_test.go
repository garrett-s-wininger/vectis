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
