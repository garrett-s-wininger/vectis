//go:build e2e

package linux_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"vectis/internal/platform"
)

const (
	defaultPackageProvider = "auto"
	defaultPackageInstance = "vectis-package-smoke"
	defaultRPMInstance     = "vectis-package-rpm-smoke"
	defaultPackageTimeout  = 10 * time.Minute
)

type packageSmokeCase struct {
	envPackagePath  string
	envPackagePaths []string
	profile         string
	instance        string
	remoteDir       string
	parseCommand    []string
	installCommand  []string
	removeCommand   []string
	verifyInstalled packageSmokeVerifier
	verifyRemoved   packageSmokeVerifier
}

type packageSmokeVerifier func(context.Context, *testing.T, platform.VirtualMachineManager, string, *bytes.Buffer, *bytes.Buffer)

func runPackageSmoke(t *testing.T, smoke packageSmokeCase) {
	t.Helper()

	packagePaths := packagePathsFromEnv(t, smoke)

	ctx, cancel := context.WithTimeout(context.Background(), packageTimeout(t))
	defer cancel()

	var stdout, stderr bytes.Buffer
	provider := platform.ResolveVirtualMachineProvider(envOrDefault("VECTIS_E2E_PACKAGE_LINUX_PROVIDER", defaultPackageProvider))
	manager, err := platform.NewVirtualMachineManager(platform.VirtualMachineManagerConfig{
		Provider:     provider,
		ProviderPath: strings.TrimSpace(os.Getenv("VECTIS_E2E_PACKAGE_LINUX_PROVIDER_PATH")),
		Stdout:       &stdout,
		Stderr:       &stderr,
	})

	if err != nil {
		skipOrFatal(t, "create VM manager: %v", err)
	}

	if err := manager.CheckAvailable(); err != nil {
		skipOrFatal(t, "%s is not usable: %v", provider, err)
	}

	exists, err := manager.InstanceExists(ctx, smoke.instance)
	if err != nil {
		t.Fatalf("check package smoke VM: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	if !exists {
		skipOrFatal(t, "package smoke VM %q does not exist; run make vm-package-smoke-prepare", smoke.instance)
	}

	if err := manager.Start(ctx, smoke.instance); err != nil {
		t.Fatalf("start package smoke VM: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	if err := manager.Shell(ctx, smoke.instance, nil, "sh", "-c", `test "$(cat /etc/vectis/package-smoke-profile)" = "$1"`, "vectis-package-smoke-profile", smoke.profile); err != nil {
		skipOrFatal(t, "package smoke VM %q is not prepared for %s packages; run make vm-package-smoke-prepare", smoke.instance, smoke.profile)
	}

	if !truthyEnv("VECTIS_E2E_KEEP_PACKAGE_LINUX") {
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cleanupCancel()
			if err := manager.Stop(cleanupCtx, smoke.instance); err != nil {
				t.Errorf("stop package smoke VM: %v", err)
			}
		})
	}

	_ = manager.Shell(ctx, smoke.instance, nil, smoke.removeCommand...)

	stageDir := stagePackages(t, packagePaths)
	remotePackages := make([]string, 0, len(packagePaths))
	for _, packagePath := range packagePaths {
		remotePackages = append(remotePackages, path.Join(smoke.remoteDir, filepath.Base(packagePath)))
	}

	installed := false
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()

		if installed {
			_ = manager.Shell(cleanupCtx, smoke.instance, nil, smoke.removeCommand...)
		}

		_ = manager.Shell(cleanupCtx, smoke.instance, nil, "sudo", "rm", "-rf", smoke.remoteDir)
	})

	if err := manager.Shell(ctx, smoke.instance, nil, "sudo", "rm", "-rf", smoke.remoteDir); err != nil {
		t.Fatalf("remove previous package stage: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	if err := manager.CopyDir(ctx, stageDir, smoke.instance, smoke.remoteDir); err != nil {
		t.Fatalf("copy package to guest: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	if len(smoke.parseCommand) > 0 {
		for _, remotePackage := range remotePackages {
			if err := manager.Shell(ctx, smoke.instance, nil, appendCommand(smoke.parseCommand, remotePackage)...); err != nil {
				t.Fatalf("package metadata did not parse: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
			}
		}
	}

	if err := manager.Shell(ctx, smoke.instance, nil, appendCommand(smoke.installCommand, remotePackages...)...); err != nil {
		t.Fatalf("install package set: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	installed = true

	verifier := smoke.verifyInstalled
	if verifier == nil {
		verifier = verifyCLIInstalled
	}

	verifier(ctx, t, manager, smoke.instance, &stdout, &stderr)
	if err := manager.Shell(ctx, smoke.instance, nil, smoke.removeCommand...); err != nil {
		t.Fatalf("remove package set: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	installed = false

	verifier = smoke.verifyRemoved
	if verifier == nil {
		verifier = verifyCLIRemoved
	}

	verifier(ctx, t, manager, smoke.instance, &stdout, &stderr)
}

func packagePathsFromEnv(t *testing.T, smoke packageSmokeCase) []string {
	t.Helper()

	envKeys := append([]string{}, smoke.envPackagePaths...)
	if smoke.envPackagePath != "" {
		envKeys = append([]string{smoke.envPackagePath}, envKeys...)
	}

	packagePaths := make([]string, 0, len(envKeys))
	for _, envKey := range envKeys {
		rawPackagePaths := strings.TrimSpace(os.Getenv(envKey))
		if rawPackagePaths == "" {
			skipOrFatal(t, "%s is not set", envKey)
		}

		for _, packagePath := range splitPackagePathEnv(rawPackagePaths) {
			if _, err := os.Stat(packagePath); err != nil {
				skipOrFatal(t, "%s %s is not readable: %v", envKey, packagePath, err)
			}

			packagePaths = append(packagePaths, packagePath)
		}
	}

	if len(packagePaths) == 0 {
		t.Fatal("package smoke case did not declare package env keys")
	}

	return packagePaths
}

func splitPackagePathEnv(raw string) []string {
	var out []string
	for _, field := range strings.Fields(raw) {
		for _, value := range filepath.SplitList(field) {
			value = strings.TrimSpace(value)
			if value != "" {
				out = append(out, value)
			}
		}
	}

	return out
}

func verifyCLIInstalled(ctx context.Context, t *testing.T, manager platform.VirtualMachineManager, instance string, stdout, stderr *bytes.Buffer) {
	t.Helper()

	if err := manager.Shell(ctx, instance, nil, "test", "-x", "/usr/bin/vectis-cli"); err != nil {
		t.Fatalf("packaged CLI was not installed executable: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	if err := manager.Shell(ctx, instance, nil, "vectis-cli", "--version"); err != nil {
		t.Fatalf("packaged CLI did not run: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	if err := manager.Shell(ctx, instance, nil, "test", "!", "-e", "/etc/systemd/system/vectis.target"); err != nil {
		t.Fatalf("vectis-cli package should not install service units: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
}

func verifyCLIRemoved(ctx context.Context, t *testing.T, manager platform.VirtualMachineManager, instance string, stdout, stderr *bytes.Buffer) {
	t.Helper()

	if err := manager.Shell(ctx, instance, nil, "test", "!", "-e", "/usr/bin/vectis-cli"); err != nil {
		t.Fatalf("packaged CLI remained after removal: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
}

func stagePackages(t *testing.T, packagePaths []string) string {
	t.Helper()

	dir := t.TempDir()
	for _, packagePath := range packagePaths {
		destination := filepath.Join(dir, filepath.Base(packagePath))
		if err := copyFile(destination, packagePath); err != nil {
			t.Fatal(err)
		}
	}

	return dir
}

func appendCommand(base []string, args ...string) []string {
	out := append([]string{}, base...)
	return append(out, args...)
}

func copyFile(destination, source string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(destination, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}

	return out.Close()
}

func packageTimeout(t *testing.T) time.Duration {
	t.Helper()

	raw := strings.TrimSpace(os.Getenv("VECTIS_E2E_PACKAGE_LINUX_TIMEOUT"))
	if raw == "" {
		return defaultPackageTimeout
	}

	timeout, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("invalid VECTIS_E2E_PACKAGE_LINUX_TIMEOUT %q: %v", raw, err)
	}

	return timeout
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}

	return fallback
}

func skipOrFatal(t *testing.T, format string, args ...any) {
	t.Helper()

	msg := fmt.Sprintf(format, args...)
	if truthyEnv("VECTIS_E2E_REQUIRE") {
		t.Fatal(msg)
	}

	t.Skip(msg)
}

func truthyEnv(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
