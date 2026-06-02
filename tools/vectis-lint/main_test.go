package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLintDirReportsPublicRouteWithoutReasonComment(t *testing.T) {
	dir := t.TempDir()
	writeLintTestFile(t, dir, "routes.go", `package api

var _ = routeAuthPolicy{mode: routeAuthPublic}
`)

	diagnostics, err := lintDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	if len(diagnostics) != 1 {
		t.Fatalf("expected 1 diagnostic, got %d: %#v", len(diagnostics), diagnostics)
	}

	if diagnostics[0].line != 3 {
		t.Fatalf("expected diagnostic on line 3, got %d", diagnostics[0].line)
	}

	if !strings.Contains(diagnostics[0].message, publicRouteReasonMarker) {
		t.Fatalf("diagnostic does not mention marker: %q", diagnostics[0].message)
	}
}

func TestLintDirAllowsNearbyPublicRouteReasonComments(t *testing.T) {
	tests := map[string]string{
		"previous_line": `package api

// public route: orchestrator liveness probe
var _ = routeAuthPolicy{mode: routeAuthPublic}
`,
		"same_line": `package api

var _ = routeSpec{Auth: routeAuthPolicy{mode: routeAuthPublic}} // public route: password login
`,
		"next_line": `package api

var _ = routeAuthPolicy{mode: routeAuthPublic}
// public route: readiness probe
`,
	}

	for name, source := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeLintTestFile(t, dir, "routes.go", source)

			diagnostics, err := lintDir(dir)
			if err != nil {
				t.Fatal(err)
			}

			if len(diagnostics) != 0 {
				t.Fatalf("expected no diagnostics, got %#v", diagnostics)
			}
		})
	}
}

func TestLintDirRejectsPublicRouteMarkerWithoutReason(t *testing.T) {
	dir := t.TempDir()
	writeLintTestFile(t, dir, "routes.go", `package api

// public route:
var _ = routeAuthPolicy{mode: routeAuthPublic}
`)

	diagnostics, err := lintDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	if len(diagnostics) != 1 {
		t.Fatalf("expected 1 diagnostic, got %d: %#v", len(diagnostics), diagnostics)
	}
}

func TestLintDirRejectsPublicRouteMarkerOutsideComments(t *testing.T) {
	dir := t.TempDir()
	writeLintTestFile(t, dir, "routes.go", `package api

var _ = "public route: not a comment"
var _ = routeAuthPolicy{mode: routeAuthPublic}
`)

	diagnostics, err := lintDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	if len(diagnostics) != 1 {
		t.Fatalf("expected 1 diagnostic, got %d: %#v", len(diagnostics), diagnostics)
	}
}

func TestLintDirIgnoresTestFiles(t *testing.T) {
	dir := t.TempDir()
	writeLintTestFile(t, dir, "routes_test.go", `package api

var _ = routeAuthPolicy{mode: routeAuthPublic}
`)

	diagnostics, err := lintDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	if len(diagnostics) != 0 {
		t.Fatalf("expected no diagnostics, got %#v", diagnostics)
	}
}

func writeLintTestFile(t *testing.T, dir, name, source string) {
	t.Helper()

	if err := os.WriteFile(filepath.Join(dir, name), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
}
