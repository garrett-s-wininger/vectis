package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const publicRouteReasonMarker = "public route:"

type diagnostic struct {
	path    string
	line    int
	column  int
	message string
}

func (d diagnostic) String() string {
	return fmt.Sprintf("%s:%d:%d: %s", d.path, d.line, d.column, d.message)
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [file-or-dir ...]\n", os.Args[0])
		fmt.Fprintln(flag.CommandLine.Output(), "Checks API route auth opt-outs for source-level reason comments.")
	}
	flag.Parse()

	targets := flag.Args()
	if len(targets) == 0 {
		targets = []string{"./internal/api"}
	}

	diagnostics, err := lintTargets(targets)
	if err != nil {
		fmt.Fprintf(os.Stderr, "vectis-lint: %v\n", err)
		os.Exit(2)
	}

	for _, diag := range diagnostics {
		fmt.Fprintln(os.Stderr, diag.String())
	}

	if len(diagnostics) > 0 {
		os.Exit(1)
	}
}

func lintTargets(targets []string) ([]diagnostic, error) {
	var diagnostics []diagnostic
	for _, target := range targets {
		info, err := os.Stat(target)
		if err != nil {
			return nil, err
		}

		var targetDiagnostics []diagnostic
		if info.IsDir() {
			targetDiagnostics, err = lintDir(target)
		} else {
			targetDiagnostics, err = lintFile(target)
		}

		if err != nil {
			return nil, err
		}

		diagnostics = append(diagnostics, targetDiagnostics...)
	}

	sort.Slice(diagnostics, func(i, j int) bool {
		if diagnostics[i].path != diagnostics[j].path {
			return diagnostics[i].path < diagnostics[j].path
		}

		if diagnostics[i].line != diagnostics[j].line {
			return diagnostics[i].line < diagnostics[j].line
		}

		return diagnostics[i].column < diagnostics[j].column
	})

	return diagnostics, nil
}

func lintDir(dir string) ([]diagnostic, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var diagnostics []diagnostic
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		if !shouldLintGoFile(path) {
			continue
		}

		fileDiagnostics, err := lintFile(path)
		if err != nil {
			return nil, err
		}

		diagnostics = append(diagnostics, fileDiagnostics...)
	}

	return diagnostics, nil
}

func shouldLintGoFile(path string) bool {
	return strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go")
}

func lintFile(path string) ([]diagnostic, error) {
	if !shouldLintGoFile(path) {
		return nil, nil
	}

	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	var diagnostics []diagnostic
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok || !isRouteAuthPolicyLiteral(lit) || !routeAuthPolicyLiteralIsPublic(lit) {
			return true
		}

		start := fset.Position(lit.Pos())
		end := fset.Position(lit.End())
		if !hasPublicRouteReasonComment(fset, file.Comments, start.Line, end.Line) {
			diagnostics = append(diagnostics, diagnostic{
				path:   path,
				line:   start.Line,
				column: start.Column,
				message: fmt.Sprintf(
					"public route auth opt-out must have a nearby %q comment with a reason",
					publicRouteReasonMarker,
				),
			})
		}

		return true
	})

	return diagnostics, nil
}

func isRouteAuthPolicyLiteral(lit *ast.CompositeLit) bool {
	ident, ok := lit.Type.(*ast.Ident)
	return ok && ident.Name == "routeAuthPolicy"
}

func routeAuthPolicyLiteralIsPublic(lit *ast.CompositeLit) bool {
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}

		key, ok := kv.Key.(*ast.Ident)
		if !ok || key.Name != "mode" {
			continue
		}

		value, ok := kv.Value.(*ast.Ident)
		return ok && value.Name == "routeAuthPublic"
	}

	return false
}

func hasPublicRouteReasonComment(fset *token.FileSet, comments []*ast.CommentGroup, startLine, endLine int) bool {
	startLine = max(1, startLine-1)
	endLine++

	for _, group := range comments {
		for _, comment := range group.List {
			commentStartLine := fset.Position(comment.Pos()).Line
			for offset, text := range strings.Split(comment.Text, "\n") {
				line := commentStartLine + offset
				if line < startLine || line > endLine {
					continue
				}

				_, after, ok := strings.Cut(text, publicRouteReasonMarker)
				if !ok {
					continue
				}

				if strings.TrimSpace(after) != "" {
					return true
				}
			}
		}
	}

	return false
}
