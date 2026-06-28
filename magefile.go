//go:build mage

package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

var appNames = []string{
	"api",
	"artifact",
	"catalog",
	"cell-ingress",
	"cli",
	"cron",
	"local",
	"log",
	"log-forwarder",
	"orchestrator",
	"queue",
	"reconciler",
	"registry",
	"secrets",
	"spiffe",
	"worker",
	"worker-core",
}

type buildConfig struct {
	args      []string
	cgo       string
	strip     bool
	outputExt string
}

// Doctor runs the native development environment preflight.
func Doctor() error {
	args := strings.Fields(os.Getenv("VECTIS_DOCTOR_ARGS"))
	if runtime.GOOS == "windows" {
		shell, err := firstOnPath("pwsh", "powershell.exe", "powershell")
		if err != nil {
			return err

		}
		psArgs := []string{"-NoProfile", "-ExecutionPolicy", "Bypass", "-File", filepath.Join("scripts", "dev-doctor.ps1")}
		psArgs = append(psArgs, args...)
		return run("", nil, shell, psArgs...)
	}

	shArgs := append([]string{filepath.Join("scripts", "dev-doctor.sh")}, args...)
	return run("", nil, "sh", shArgs...)
}

// Proto regenerates Go protobuf stubs.
func Proto() error {
	if err := os.RemoveAll(filepath.Join("api", "gen")); err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Join("api", "gen", "go"), 0o755); err != nil {
		return err
	}

	protoFiles, err := filepath.Glob(filepath.Join("api", "proto", "*.proto"))
	if err != nil {
		return err
	}

	if len(protoFiles) == 0 {
		return fmt.Errorf("no protobuf files found in %s", filepath.Join("api", "proto"))
	}

	sort.Strings(protoFiles)
	args := []string{
		"-I", filepath.ToSlash(filepath.Join("api", "proto")),
		"--plugin=protoc-gen-go=" + protocPlugin("PROTOC_GEN_GO", "protoc-gen-go"),
		"--plugin=protoc-gen-go-grpc=" + protocPlugin("PROTOC_GEN_GO_GRPC", "protoc-gen-go-grpc"),
		"--go_out=" + filepath.ToSlash(filepath.Join("api", "gen", "go")),
		"--go_opt=paths=source_relative",
		"--go-grpc_out=" + filepath.ToSlash(filepath.Join("api", "gen", "go")),
		"--go-grpc_opt=paths=source_relative",
	}

	for _, file := range protoFiles {
		args = append(args, filepath.ToSlash(file))
	}

	return run("", nil, envDefault("PROTOC", "protoc"), args...)
}

// Build builds the default local binaries.
func Build() error {
	return buildBinaries(buildConfig{
		args:      strings.Fields(os.Getenv("BUILD_OPTS")),
		cgo:       envDefault("CGO_ENABLED", "1"),
		outputExt: hostExecutableExt(),
	})
}

// BuildContainer builds the container-profile binaries with nosqlite tags.
func BuildContainer() error {
	return buildBinaries(buildConfig{
		args:  []string{"-tags=nosqlite"},
		cgo:   "0",
		strip: true,
	})
}

func buildBinaries(cfg buildConfig) error {
	apps := append([]string(nil), appNames...)
	if !truthy(os.Getenv("SKIP_WEB_BUILD")) {
		if !truthy(os.Getenv("SKIP_DOCS_ASSETS")) {
			if err := buildDocsAssets(); err != nil {
				return err
			}
		}

		apps = append(apps, "docs")
	}

	outDir := envDefault("OUT_DIR", "bin")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	ldflags := buildLDFlags(cfg.strip)
	for _, app := range apps {
		out := filepath.Join(outDir, "vectis-"+app+cfg.outputExt)
		pkg := "./" + filepath.ToSlash(filepath.Join("cmd", app))
		args := append([]string{"build"}, cfg.args...)
		args = append(args, "-ldflags", ldflags, "-o", out, pkg)
		if err := run("", map[string]string{"CGO_ENABLED": cfg.cgo}, goCommand(), args...); err != nil {
			return err
		}
	}

	return nil
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

// Test runs the full Go test suite.
func Test() error {
	return run("", nil, goCommand(), "test", "./...")
}

// TestQuick runs the fast feedback test set.
func TestQuick() error {
	args := []string{
		"test",
		"-count=1",
		"-timeout=60s",
		"./internal/...",
		"./cmd/...",
		"./api/...",
		"./sdk/...",
		"./examples/...",
		"./tools/...",
	}

	return run("", nil, goCommand(), args...)
}

func buildDocsAssets() error {
	if err := run("website", nil, "npm", "ci"); err != nil {
		return err
	}

	if err := run("website", nil, "npm", "run", "build"); err != nil {
		return err
	}

	dest := filepath.Join("cmd", "docs", "embedded")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return err
	}

	entries, err := os.ReadDir(dest)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.Name() == ".gitkeep" {
			continue
		}

		if err := os.RemoveAll(filepath.Join(dest, entry.Name())); err != nil {
			return err
		}
	}

	if err := copyDir(filepath.Join("website", "build"), dest); err != nil {
		return err
	}

	stamp := []byte(time.Now().UTC().Format(time.RFC3339) + "\n")
	return os.WriteFile(filepath.Join(dest, ".stamp"), stamp, 0o644)
}

func copyDir(src, dest string) error {
	return filepath.WalkDir(src, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		if rel == "." {
			return nil
		}

		target := filepath.Join(dest, rel)
		info, err := entry.Info()
		if err != nil {
			return err
		}

		if entry.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}

		return copyFile(path, target, info.Mode())
	})
}

func copyFile(src, dest string, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}

	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}

	return out.Close()
}

func buildLDFlags(strip bool) string {
	flags := []string{
		"-X vectis/internal/version.Version=" + gitOutput("dev", "describe", "--tags", "--always", "--dirty"),
		"-X vectis/internal/version.Commit=" + gitOutput("unknown", "rev-parse", "--short=12", "HEAD"),
		"-X vectis/internal/version.BuildDate=" + time.Now().UTC().Format("2006-01-02T15:04:05Z"),
	}

	if strip {
		flags = append(flags, "-s", "-w")
	}

	return strings.Join(flags, " ")
}

func gitOutput(fallback string, args ...string) string {
	cmd := exec.Command("git", args...)
	out, err := cmd.Output()
	if err != nil {
		return fallback
	}

	value := strings.TrimSpace(string(out))
	if value == "" {
		return fallback
	}

	return value
}

func protocPlugin(envName, tool string) string {
	if value := os.Getenv(envName); value != "" {
		return value
	}

	gopath := goEnv("GOPATH")
	if gopath == "" {
		return exeName(tool)
	}

	return filepath.Join(gopath, "bin", exeName(tool))
}

func goEnv(name string) string {
	cmd := exec.Command(goCommand(), "env", name)
	out, err := cmd.Output()
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(out))
}

func goCommand() string {
	return envDefault("GO", "go")
}

func firstOnPath(names ...string) (string, error) {
	for _, name := range names {
		path, err := exec.LookPath(name)
		if err == nil {
			return path, nil
		}
	}

	return "", fmt.Errorf("none of these commands are on PATH: %s", strings.Join(names, ", "))
}

func exeName(name string) string {
	if runtime.GOOS == "windows" && !strings.HasSuffix(strings.ToLower(name), ".exe") {
		return name + ".exe"
	}

	return name
}

func hostExecutableExt() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}

	return ""
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}

func truthy(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func run(dir string, extraEnv map[string]string, name string, args ...string) error {
	fmt.Println("$", commandLine(name, args))
	cmd := exec.Command(name, args...)
	if dir != "" {
		cmd.Dir = dir
	}

	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), envPairs(extraEnv)...)
	}

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	return cmd.Run()
}

func envPairs(values map[string]string) []string {
	pairs := make([]string, 0, len(values))
	for key, value := range values {
		pairs = append(pairs, key+"="+value)
	}

	sort.Strings(pairs)
	return pairs
}

func commandLine(name string, args []string) string {
	parts := append([]string{name}, args...)
	return strings.Join(parts, " ")
}
