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
		t.Fatalf("process-launching worker code must use interfaces.ExecExecutor/StartProcess, found raw os/exec calls:\n%s", strings.Join(violations, "\n"))
	}
}

func rawExecCalls(t *testing.T, root, path string) []string {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	execNames := map[string]struct{}{}
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

		if name != "_" && name != "." {
			execNames[name] = struct{}{}
		}
	}

	if len(execNames) == 0 {
		return nil
	}

	var violations []string
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}

		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}

		if selector.Sel.Name != "Command" && selector.Sel.Name != "CommandContext" {
			return true
		}

		ident, ok := selector.X.(*ast.Ident)
		if !ok {
			return true
		}

		if _, ok := execNames[ident.Name]; !ok {
			return true
		}

		pos := fset.Position(call.Pos())
		rel, err := filepath.Rel(root, pos.Filename)
		if err != nil {
			rel = pos.Filename
		}

		violations = append(violations, rel+":"+strconv.Itoa(pos.Line))
		return true
	})

	return violations
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
