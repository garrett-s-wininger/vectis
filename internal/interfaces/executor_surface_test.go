package interfaces

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestWorkerExecutionPackagesDoNotBypassHardenedExecutor(t *testing.T) {
	root := findRepoRoot(t)
	checkDirs := []string{
		"cmd/worker",
		"cmd/worker-core",
		"internal/action",
		"internal/job",
		"internal/workercore",
	}

	var violations []string
	for _, dir := range checkDirs {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if entry.IsDir() {
				if entry.Name() == "mocks" {
					return filepath.SkipDir
				}

				return nil
			}

			if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}

			violations = append(violations, rawExecCalls(t, root, path)...)
			return nil
		})

		if err != nil {
			t.Fatalf("scan %s: %v", dir, err)
		}
	}

	if len(violations) > 0 {
		t.Fatalf("process-launching worker code must use interfaces.ExecExecutor, found raw os/exec usage:\n%s", strings.Join(violations, "\n"))
	}
}

func TestProviderProcessEscapeHatchesAreExplicitlyAllowed(t *testing.T) {
	root := findRepoRoot(t)
	checkDirs := []string{
		"cmd",
		"internal",
	}
	allowed := map[string]map[string]struct{}{
		"internal/platform/lima_virtual_machine.go": {
			"StartProviderProcess": {},
		},
	}

	var violations []string
	for _, dir := range checkDirs {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if entry.IsDir() {
				if entry.Name() == "mocks" {
					return filepath.SkipDir
				}

				return nil
			}

			if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}

			violations = append(violations, providerProcessEscapeCalls(t, root, path, allowed)...)
			return nil
		})

		if err != nil {
			t.Fatalf("scan %s: %v", dir, err)
		}
	}

	if len(violations) > 0 {
		t.Fatalf("provider process launch escape hatches must be explicitly allowlisted:\n%s", strings.Join(violations, "\n"))
	}
}

var allowedWorkerExecSelectors = map[string]map[string]struct{}{
	"internal/action/builtins/test.go": {
		"ExitError": {},
	},
}

func rawExecCalls(t *testing.T, root, path string) []string {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	rel, err := filepath.Rel(root, path)
	if err != nil {
		rel = path
	}

	execNames := map[string]struct{}{}
	var violations []string
	for _, imp := range file.Imports {
		importPath, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			t.Fatalf("unquote import path in %s: %v", path, err)
		}

		if importPath != "os/exec" {
			continue
		}

		name := "exec"
		if imp.Name != nil {
			name = imp.Name.Name
		}

		switch name {
		case "_", ".":
			pos := fset.Position(imp.Pos())
			violations = append(violations, rel+":"+strconv.Itoa(pos.Line)+": unsupported os/exec import form")
		default:
			execNames[name] = struct{}{}
		}
	}

	if len(execNames) == 0 {
		return violations
	}

	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}

		ident, ok := selector.X.(*ast.Ident)
		if !ok {
			return true
		}

		if _, ok := execNames[ident.Name]; !ok {
			return true
		}

		if isAllowedWorkerExecSelector(rel, selector.Sel.Name) {
			return true
		}

		pos := fset.Position(selector.Pos())
		violations = append(violations, rel+":"+strconv.Itoa(pos.Line)+": os/exec."+selector.Sel.Name)
		return true
	})

	return violations
}

func providerProcessEscapeCalls(t *testing.T, root, path string, allowed map[string]map[string]struct{}) []string {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	rel, err := filepath.Rel(root, path)
	if err != nil {
		rel = path
	}

	var violations []string
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}

		name := calledFunctionName(call.Fun)
		if name != "StartProcess" && name != "StartProviderProcess" {
			return true
		}

		if isAllowedProcessEscape(rel, name, allowed) {
			return true
		}

		pos := fset.Position(call.Pos())
		violations = append(violations, rel+":"+strconv.Itoa(pos.Line)+": "+name)
		return true
	})

	return violations
}

func calledFunctionName(expr ast.Expr) string {
	switch expr := expr.(type) {
	case *ast.Ident:
		return expr.Name
	case *ast.SelectorExpr:
		return expr.Sel.Name
	default:
		return ""
	}
}

func isAllowedWorkerExecSelector(rel, selector string) bool {
	allowed, ok := allowedWorkerExecSelectors[rel]
	if !ok {
		return false
	}

	_, ok = allowed[selector]
	return ok
}

func isAllowedProcessEscape(rel, name string, allowed map[string]map[string]struct{}) bool {
	allowedNames, ok := allowed[rel]
	if !ok {
		return false
	}

	_, ok = allowedNames[name]
	return ok
}

func findRepoRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get cwd: %v", err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find repo root containing go.mod")
		}

		dir = parent
	}
}
