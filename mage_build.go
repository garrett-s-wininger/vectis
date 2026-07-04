//go:build mage

package main

import (
	"fmt"
	"io"
	"os"
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
	"scm-gerrit-stream",
	"scm-poller",
	"secrets",
	"spiffe",
	"worker",
	"worker-core",
}

var docsAppNames = []string{"docs"}
var frontendAppNames = []string{"docs", "ui"}

type buildConfig struct {
	apps      []string
	args      []string
	cgo       string
	env       map[string]string
	outDir    string
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

// Build builds the default local Go service and CLI binaries.
func Build() error {
	return buildBinaries(localBuildConfig(appNames))
}

// BuildFull builds the default local binaries plus docs and UI assets/binaries.
func BuildFull() error {
	if err := FrontendAssets(); err != nil {
		return err
	}

	return buildBinaries(localBuildConfig(combineAppNames(appNames, frontendAppNames)))
}

// BuildDocs rebuilds the docs assets and vectis-docs binary.
func BuildDocs() error {
	if err := DocsAssets(); err != nil {
		return err
	}

	return buildBinaries(localBuildConfig(docsAppNames))
}

// BuildUI rebuilds the UI assets and vectis-ui binary.
func BuildUI() error {
	if err := UIAssets(); err != nil {
		return err
	}

	return buildBinaries(localBuildConfig([]string{"ui"}))
}

// BuildFrontend rebuilds the docs/UI assets and their serving binaries.
func BuildFrontend() error {
	if err := FrontendAssets(); err != nil {
		return err
	}

	return buildBinaries(localBuildConfig(frontendAppNames))
}

// BuildContainer builds the container-profile binary set with nosqlite tags.
func BuildContainer() error {
	return buildBinaries(buildConfig{
		apps:  combineAppNames(appNames, docsAppNames),
		args:  []string{"-tags=nosqlite"},
		cgo:   "0",
		strip: true,
	})
}

// BuildWindows cross-builds Windows binaries with the nosqlite build tag.
func BuildWindows() error {
	arch := windowsTargetArch()
	outDir := os.Getenv("WINDOWS_OUT_DIR")
	if outDir == "" {
		outDir = os.Getenv("OUT_DIR")
	}

	if outDir == "" {
		outDir = filepath.Join("bin", "windows-"+arch)
	}

	return buildBinaries(buildConfig{
		apps:      appNames,
		args:      strings.Fields(envDefault("WINDOWS_BUILD_OPTS", "-tags=nosqlite")),
		cgo:       "0",
		env:       windowsTargetEnv("0"),
		outDir:    outDir,
		outputExt: ".exe",
	})
}

// Clean removes generated local build, test, and docs artifacts.
func Clean() error {
	paths := []string{
		filepath.Join("artifacts", "perf"),
		filepath.Join("artifacts", "deploy"),
		filepath.Join("artifacts", "release-readiness"),
		envDefault("OUT_DIR", "bin"),
		"states",
		filepath.Join("website", ".docusaurus"),
		filepath.Join("website", "build"),
		filepath.Join("ui", "dist"),
	}

	for _, path := range paths {
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}

	traces, err := filepath.Glob(filepath.Join("formal", "tla", "*_TTrace_*"))
	if err != nil {
		return err
	}

	for _, trace := range traces {
		if err := os.RemoveAll(trace); err != nil {
			return err
		}
	}

	if err := cleanDirExcept(filepath.Join("cmd", "docs", "embedded"), ".gitkeep"); err != nil {
		return err
	}

	return cleanDirExcept(filepath.Join("cmd", "ui", "embedded"), ".gitkeep")
}

// PodmanGrafanaConfigmaps regenerates Podman Grafana configmaps.
func PodmanGrafanaConfigmaps() error {
	return run("", nil, goCommand(), "run", "./deploy/podman/cmd/generate-grafana-configmaps", "-o", filepath.Join("deploy", "podman", "grafana-configmaps.gen.yaml"))
}

// DocsAssets rebuilds and embeds the documentation site assets.
func DocsAssets() error {
	return buildDocsAssets()
}

// UIAssets rebuilds and embeds the browser UI assets.
func UIAssets() error {
	return buildUIAssets()
}

// FrontendAssets rebuilds and embeds both docs and browser UI assets.
func FrontendAssets() error {
	if err := buildDocsAssets(); err != nil {
		return err
	}

	return buildUIAssets()
}

// DeployArtifactsRender renders Linux deployment artifacts.
func DeployArtifactsRender() error {
	out := envDefault("DEPLOY_LINUX_OUT", filepath.Join("artifacts", "deploy", "linux"))
	return run("", nil, goCommand(), "run", "./cmd/cli", "deploy", "linux", "render", "--output", out)
}

// DeployArtifactsTest tests Linux deployment artifact rendering.
func DeployArtifactsTest() error {
	return run("", nil, goCommand(), "test", "./deploy/linux/...")
}

func buildBinaries(cfg buildConfig) error {
	apps := append([]string(nil), cfg.apps...)
	if len(apps) == 0 {
		apps = append(apps, appNames...)
	}

	outDir := cfg.outDir
	if outDir == "" {
		outDir = envDefault("OUT_DIR", "bin")
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	return buildBinariesBatch(cfg, apps, outDir)
}

func buildBinariesBatch(cfg buildConfig, apps []string, outDir string) error {
	tmpDir, err := os.MkdirTemp(outDir, ".vectis-build-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	ldflags := buildLDFlags(cfg.strip)
	args := append([]string{"build"}, cfg.args...)
	args = append(args, "-ldflags", ldflags, "-o", tmpDir)
	for _, app := range apps {
		pkg := "./" + filepath.ToSlash(filepath.Join("cmd", app))
		args = append(args, pkg)
	}

	if err := run("", buildTargetEnv(cfg), goCommand(), args...); err != nil {
		return err
	}

	for _, app := range apps {
		src, err := builtBinaryPath(tmpDir, app, cfg.outputExt)
		if err != nil {
			return err
		}

		out := filepath.Join(outDir, "vectis-"+app+cfg.outputExt)
		if err := replaceBuiltBinary(src, out); err != nil {
			return fmt.Errorf("install built binary %s: %w", app, err)
		}
	}

	return nil
}

func builtBinaryPath(dir, app, outputExt string) (string, error) {
	candidates := []string{app + outputExt, app + ".exe", app}
	seen := make(map[string]struct{}, len(candidates))
	for _, name := range candidates {
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}

		path := filepath.Join(dir, name)
		info, err := os.Stat(path)
		if err == nil && !info.IsDir() {
			return path, nil
		}
	}

	return "", fmt.Errorf("go build did not produce an executable for %s in %s", app, dir)
}

func replaceBuiltBinary(src, dest string) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	if err := os.Remove(dest); err != nil && !os.IsNotExist(err) {
		return err
	}

	return os.Rename(src, dest)
}

func localBuildConfig(apps []string) buildConfig {
	return buildConfig{
		apps:      apps,
		args:      strings.Fields(os.Getenv("BUILD_OPTS")),
		cgo:       envDefault("CGO_ENABLED", "1"),
		outputExt: targetExecutableExt(),
	}
}

func combineAppNames(groups ...[]string) []string {
	var apps []string
	for _, group := range groups {
		apps = append(apps, group...)
	}

	return apps
}

func buildTargetEnv(cfg buildConfig) map[string]string {
	env := map[string]string{"CGO_ENABLED": cfg.cgo}
	for key, value := range cfg.env {
		env[key] = value
	}

	for _, key := range []string{
		"GOOS",
		"GOARCH",
		"GOAMD64",
		"GOARM",
		"GOARM64",
		"GOMIPS",
		"GOMIPS64",
		"GOPPC64",
		"GORISCV64",
		"GOWASM",
	} {
		if _, exists := env[key]; exists {
			continue
		}

		if value := os.Getenv("TARGET_" + key); value != "" {
			env[key] = value
		}
	}

	return env
}

func windowsTargetEnv(cgo string) map[string]string {
	env := map[string]string{
		"CGO_ENABLED": cgo,
		"GOOS":        "windows",
		"GOARCH":      windowsTargetArch(),
	}

	for _, key := range []string{"GOAMD64", "GOARM", "GOARM64"} {
		if value := os.Getenv("WINDOWS_" + key); value != "" {
			env[key] = value
			continue
		}

		if value := os.Getenv("TARGET_" + key); value != "" {
			env[key] = value
		}
	}

	return env
}

func windowsTargetArch() string {
	if value := os.Getenv("WINDOWS_GOARCH"); value != "" {
		return value
	}

	if value := os.Getenv("TARGET_GOARCH"); value != "" {
		return value
	}

	return "amd64"
}

func buildDocsAssets() error {
	if err := run("website", nil, "npm", "ci"); err != nil {
		return err
	}

	if err := run("website", map[string]string{"NO_UPDATE_NOTIFIER": "1"}, "npm", "run", "build"); err != nil {
		return err
	}

	dest := filepath.Join("cmd", "docs", "embedded")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return err
	}

	if err := cleanDirExcept(dest, ".gitkeep"); err != nil {
		return err
	}

	if err := copyDir(filepath.Join("website", "build"), dest); err != nil {
		return err
	}

	stamp := []byte(time.Now().UTC().Format(time.RFC3339) + "\n")
	return os.WriteFile(filepath.Join(dest, ".stamp"), stamp, 0o644)
}

func buildUIAssets() error {
	if err := run("ui", nil, "npm", "ci"); err != nil {
		return err
	}

	if err := run("ui", nil, "npm", "run", "build"); err != nil {
		return err
	}

	dest := filepath.Join("cmd", "ui", "embedded")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return err
	}

	if err := cleanDirExcept(dest, ".gitkeep"); err != nil {
		return err
	}

	if err := copyDir(filepath.Join("ui", "dist"), dest); err != nil {
		return err
	}

	stamp := []byte(time.Now().UTC().Format(time.RFC3339) + "\n")
	return os.WriteFile(filepath.Join(dest, ".stamp"), stamp, 0o644)
}

func cleanDirExcept(dir string, keep ...string) error {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}

	if err != nil {
		return err
	}

	keepSet := make(map[string]struct{}, len(keep))
	for _, name := range keep {
		keepSet[name] = struct{}{}
	}

	for _, entry := range entries {
		if _, ok := keepSet[entry.Name()]; ok {
			continue
		}

		if err := os.RemoveAll(filepath.Join(dir, entry.Name())); err != nil {
			return err
		}
	}

	return nil
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
