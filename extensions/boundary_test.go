package extensions_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

var allowedVectisExtensionImports = []string{
	"vectis/api/gen/go",
	"vectis/extensions/",
	"vectis/sdk/",
}

func TestExtensionsDoNotImportVectisInternals(t *testing.T) {
	root := "."
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "vendor":
				return filepath.SkipDir
			default:
				return nil
			}
		}

		if !strings.HasSuffix(path, ".go") {
			return nil
		}

		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			t.Errorf("parse imports for %s: %v", path, err)
			return nil
		}

		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Errorf("decode import in %s: %v", path, err)
				continue
			}

			if strings.HasPrefix(importPath, "vectis/") && !allowedVectisExtensionImport(importPath) {
				t.Errorf("%s imports %q; extensions may only depend on api/gen/go, sdk, or extensions packages", path, importPath)
			}
		}

		return nil
	}); err != nil {
		t.Fatalf("walk extensions: %v", err)
	}
}

func TestCoreImportsSecretProvidersOnlyAtCompositionPoints(t *testing.T) {
	root := ".."
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "api", "vendor":
				return filepath.SkipDir
			default:
				return nil
			}
		}

		if !strings.HasSuffix(path, ".go") {
			return nil
		}

		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			t.Errorf("parse imports for %s: %v", path, err)
			return nil
		}

		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Errorf("decode import in %s: %v", path, err)
				continue
			}

			if strings.HasPrefix(importPath, "vectis/extensions/secrets/") && !allowedSecretProviderImportFile(path) {
				t.Errorf("%s imports %q; standard secret providers may only be imported at composition points", path, importPath)
			}
		}

		return nil
	}); err != nil {
		t.Fatalf("walk repository: %v", err)
	}
}

func allowedVectisExtensionImport(importPath string) bool {
	for _, allowed := range allowedVectisExtensionImports {
		if strings.HasSuffix(allowed, "/") {
			if strings.HasPrefix(importPath, allowed) {
				return true
			}

			continue
		}

		if importPath == allowed {
			return true
		}
	}

	return false
}

func allowedSecretProviderImportFile(path string) bool {
	path = filepath.ToSlash(path)
	if strings.HasPrefix(path, "../extensions/secrets/") {
		return true
	}

	switch path {
	case "../cmd/cli/secrets.go",
		"../cmd/cli/secrets_test.go",
		"../cmd/local/main.go",
		"../cmd/local/main_test.go",
		"../cmd/secrets/main.go",
		"../internal/localspiffe/authority_test.go":
		return true
	default:
		return false
	}
}
