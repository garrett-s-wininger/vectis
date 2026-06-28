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
		outputExt: targetExecutableExt(),
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

	return cleanDirExcept(filepath.Join("cmd", "docs", "embedded"), ".gitkeep")
}

// PodmanGrafanaConfigmaps regenerates Podman Grafana configmaps.
func PodmanGrafanaConfigmaps() error {
	return run("", nil, goCommand(), "run", "./deploy/podman/cmd/generate-grafana-configmaps", "-o", filepath.Join("deploy", "podman", "grafana-configmaps.gen.yaml"))
}

// DocsAssets rebuilds and embeds the documentation site assets.
func DocsAssets() error {
	return buildDocsAssets()
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

		if err := run("", buildTargetEnv(cfg.cgo), goCommand(), args...); err != nil {
			return err
		}
	}

	return nil
}

func buildTargetEnv(cgo string) map[string]string {
	env := map[string]string{"CGO_ENABLED": cgo}
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
		if value := os.Getenv("TARGET_" + key); value != "" {
			env[key] = value
		}
	}

	return env
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

	if err := cleanDirExcept(dest, ".gitkeep"); err != nil {
		return err
	}

	if err := copyDir(filepath.Join("website", "build"), dest); err != nil {
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
