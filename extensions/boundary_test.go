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
	assertExtensionImportsOnlyAtCompositionPoints(t, "vectis/extensions/secrets/", allowedSecretProviderImportFile, "standard secret providers")
}

func TestCoreImportsArtifactProvidersOnlyAtCompositionPoints(t *testing.T) {
	assertExtensionImportsOnlyAtCompositionPoints(t, "vectis/extensions/artifacts/", allowedArtifactProviderImportFile, "artifact storage providers")
}

func TestCoreImportsAuthProvidersOnlyAtCompositionPoints(t *testing.T) {
	assertExtensionImportsOnlyAtCompositionPoints(t, "vectis/extensions/auth/", allowedAuthProviderImportFile, "auth providers")
}

func TestCoreImportsActionExtensionsOnlyAtCompositionPoints(t *testing.T) {
	assertExtensionImportsOnlyAtCompositionPoints(t, "vectis/extensions/actions/", allowedActionExtensionImportFile, "action extensions")
}

func TestCoreImportsSCMProvidersOnlyAtCompositionPoints(t *testing.T) {
	assertExtensionImportsOnlyAtCompositionPoints(t, "vectis/extensions/scm/", allowedSCMProviderImportFile, "SCM providers")
}

func assertExtensionImportsOnlyAtCompositionPoints(t *testing.T, importPrefix string, allowFile func(string) bool, label string) {
	t.Helper()

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

			if strings.HasPrefix(importPath, importPrefix) && !allowFile(path) {
				t.Errorf("%s imports %q; %s may only be imported at composition points", path, importPath, label)
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
		"../internal/localspiffe/authority_test.go",
		"../tests/integration/local/secrets_spiffe_test.go":
		return true
	default:
		return false
	}
}

func allowedArtifactProviderImportFile(path string) bool {
	path = filepath.ToSlash(path)
	if strings.HasPrefix(path, "../extensions/artifacts/") {
		return true
	}

	switch path {
	case "../cmd/artifact/main.go":
		return true
	default:
		return false
	}
}

func allowedAuthProviderImportFile(path string) bool {
	path = filepath.ToSlash(path)
	if strings.HasPrefix(path, "../extensions/auth/") {
		return true
	}
	if strings.HasPrefix(path, "../deploy/ldap/") {
		return true
	}

	switch path {
	case "../cmd/api/main.go":
		return true
	default:
		return false
	}
}

func allowedActionExtensionImportFile(path string) bool {
	path = filepath.ToSlash(path)
	if strings.HasPrefix(path, "../extensions/actions/") {
		return true
	}

	if strings.HasPrefix(path, "../deploy/gerrit/") {
		return true
	}

	return false
}

func allowedSCMProviderImportFile(path string) bool {
	path = filepath.ToSlash(path)
	if strings.HasPrefix(path, "../extensions/scm/") {
		return true
	}

	if strings.HasPrefix(path, "../deploy/gerrit/") {
		return true
	}

	switch path {
	case "../cmd/scm-gerrit-stream/main.go",
		"../cmd/scm-gerrit-stream/main_test.go",
		"../cmd/scm-poller/main.go":
		return true
	default:
		return false
	}
}
