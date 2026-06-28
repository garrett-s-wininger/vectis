//go:build mage

package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// WebsiteA11y runs the website accessibility test suite.
func WebsiteA11y() error {
	browsersPath, err := filepath.Abs(filepath.Join("website", "node_modules", ".cache", "ms-playwright"))
	if err != nil {
		return err
	}

	env := map[string]string{"PLAYWRIGHT_BROWSERS_PATH": browsersPath}
	if err := run("website", env, "npm", "ci"); err != nil {
		return err
	}

	if err := run("website", env, "npx", "playwright", "install", "chromium"); err != nil {
		return err
	}

	return run("website", env, "npm", "run", "test:a11y")
}

// Format applies standard Go fixes, formatting, and module tidying.
func Format() error {
	if err := FormatShell(); err != nil {
		return err
	}

	if err := run("", nil, goCommand(), "fix", "./..."); err != nil {
		return err
	}

	if err := run("", nil, goCommand(), "fmt", "./..."); err != nil {
		return err
	}

	return run("", nil, goCommand(), "mod", "tidy")
}

// FuzzAPIAuth runs the API auth fuzz targets.
func FuzzAPIAuth() error {
	fuzztime := envDefault("FUZZTIME", "30s")
	if err := run("", nil, goCommand(), "test", "-fuzz=FuzzBearerToken", "-fuzztime="+fuzztime, "./internal/api"); err != nil {
		return err
	}

	if err := run("", nil, goCommand(), "test", "-fuzz=FuzzHashAPIToken", "-fuzztime="+fuzztime, "./internal/api"); err != nil {
		return err
	}

	return run("", nil, goCommand(), "test", "-fuzz=FuzzActionForRequest", "-fuzztime="+fuzztime, "./internal/api/authz")
}

// Lint runs route and Go static analysis checks.
func Lint() error {
	if err := LintAPIRoutes(); err != nil {
		return err
	}

	if err := LintOpenAPI(); err != nil {
		return err
	}

	if err := LintWorkflows(); err != nil {
		return err
	}

	if err := LintShellFormat(); err != nil {
		return err
	}

	version := envDefault("GOLANGCI_LINT_VERSION", "v2.6.1")
	return run("", nil, goCommand(), "run", "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@"+version, "run", "./...")
}

// LintAPIRoutes runs the API route security lint.
func LintAPIRoutes() error {
	return run("", nil, goCommand(), "run", "./tools/vectis-lint", "./internal/api")
}

// LintOpenAPI validates the generated OpenAPI document.
func LintOpenAPI() error {
	return run("", nil, goCommand(), "run", "./tools/openapi-lint", filepath.Join("website", "static", "openapi", "v1.json"))
}

// LintWorkflows runs GitHub Actions workflow linting.
func LintWorkflows() error {
	version := envDefault("ACTIONLINT_VERSION", "v1.7.7")
	return run("", nil, goCommand(), "run", "github.com/rhysd/actionlint/cmd/actionlint@"+version)
}

// LintShellFormat checks shell formatting.
func LintShellFormat() error {
	sources, err := shellSources()
	if err != nil {
		return err
	}

	version := envDefault("SHFMT_VERSION", "v3.12.0")
	args := append([]string{"run", "mvdan.cc/sh/v3/cmd/shfmt@" + version, "-d"}, sources...)
	return run("", nil, goCommand(), args...)
}

// FormatShell formats shell scripts.
func FormatShell() error {
	sources, err := shellSources()
	if err != nil {
		return err
	}

	version := envDefault("SHFMT_VERSION", "v3.12.0")
	args := append([]string{"run", "mvdan.cc/sh/v3/cmd/shfmt@" + version, "-w"}, sources...)
	return run("", nil, goCommand(), args...)
}

// LintShell checks shell scripts with shellcheck after format verification.
func LintShell() error {
	if err := LintShellFormat(); err != nil {
		return err
	}

	sources, err := shellSources()
	if err != nil {
		return err
	}

	return run("", nil, envDefault("SHELLCHECK", "shellcheck"), sources...)
}

// LintContainer checks the main container build file.
func LintContainer() error {
	return run("", nil, envDefault("HADOLINT", "hadolint"), filepath.Join("build", "Containerfile"))
}

// FormatGoStrict applies gofumpt to repository Go sources.
func FormatGoStrict() error {
	sources, err := goFormatSources()
	if err != nil {
		return err
	}

	version := envDefault("GOFUMPT_VERSION", "v0.9.1")
	args := append([]string{"run", "mvdan.cc/gofumpt@" + version, "-w"}, sources...)
	return run("", nil, goCommand(), args...)
}

// LintGoStrict runs extra opt-in golangci-lint linters.
func LintGoStrict() error {
	if err := LintAPIRoutes(); err != nil {
		return err
	}

	linters := strings.TrimSpace(os.Getenv("GOLANGCI_STRICT_LINTERS"))
	if linters == "" {
		fmt.Println("No extra strict golangci-lint linters configured.")
		return nil
	}

	version := envDefault("GOLANGCI_LINT_VERSION", "v2.6.1")
	return run("", nil, goCommand(), "run", "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@"+version, "run", "--enable="+linters, "./...")
}

// Vulncheck runs govulncheck with the module's declared Go toolchain.
func Vulncheck() error {
	env := map[string]string{
		"GOTOOLCHAIN": "go" + goModGoVersion(),
	}

	version := envDefault("GOVULNCHECK_VERSION", "v1.1.4")
	return run("", env, goCommand(), "run", "golang.org/x/vuln/cmd/govulncheck@"+version, "./...")
}

func shellSources() ([]string, error) {
	return filesWithSuffix([]string{".vectis", filepath.Join("build", "packer", "scripts")}, ".sh", nil)
}

func goFormatSources() ([]string, error) {
	excludedDirs := map[string]struct{}{
		".git":                                   {},
		filepath.Join("api", "gen", "go"):        {},
		filepath.Join("cmd", "docs", "embedded"): {},
		filepath.Join("website", "node_modules"): {},
		filepath.Join("website", "build"):        {},
	}

	return filesWithSuffix([]string{"."}, ".go", excludedDirs)
}

func filesWithSuffix(roots []string, suffix string, excludedDirs map[string]struct{}) ([]string, error) {
	var files []string
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			clean := filepath.Clean(path)
			if d.IsDir() {
				if _, excluded := excludedDirs[clean]; excluded {
					return filepath.SkipDir
				}

				return nil
			}

			if strings.HasSuffix(d.Name(), suffix) {
				files = append(files, filepath.ToSlash(clean))
			}

			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	sort.Strings(files)
	return files, nil
}
