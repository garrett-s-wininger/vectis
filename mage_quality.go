//go:build mage

package main

import (
	"path/filepath"
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

	version := envDefault("GOLANGCI_LINT_VERSION", "v2.6.1")
	return run("", nil, goCommand(), "run", "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@"+version, "run", "./...")
}

// LintAPIRoutes runs the API route security lint.
func LintAPIRoutes() error {
	return run("", nil, goCommand(), "run", "./tools/vectis-lint", "./internal/api")
}

// Vulncheck runs govulncheck with the module's declared Go toolchain.
func Vulncheck() error {
	env := map[string]string{
		"GOTOOLCHAIN": "go" + goModGoVersion(),
	}

	version := envDefault("GOVULNCHECK_VERSION", "v1.1.4")
	return run("", env, goCommand(), "run", "golang.org/x/vuln/cmd/govulncheck@"+version, "./...")
}
