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
	defaultPackageTemplate = "ubuntu-lts"
	defaultRPMInstance     = "vectis-package-rpm-smoke"
	defaultRPMTemplate     = "fedora"
	defaultPackageTimeout  = 10 * time.Minute
)

type packageSmokeCase struct {
	envPackagePath string
	instance       string
	template       string
	remoteDir      string
	parseCommand   []string
	installCommand []string
	removeCommand  []string
}

func runPackageSmoke(t *testing.T, smoke packageSmokeCase) {
	t.Helper()

	packagePath := strings.TrimSpace(os.Getenv(smoke.envPackagePath))
	if packagePath == "" {
		skipOrFatal(t, "%s is not set", smoke.envPackagePath)
	}

	if _, err := os.Stat(packagePath); err != nil {
		skipOrFatal(t, "%s %s is not readable: %v", smoke.envPackagePath, packagePath, err)
	}

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
		if err := manager.Create(ctx, smoke.instance, smoke.template); err != nil {
			t.Fatalf("create package smoke VM: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
		}
	}

	if err := manager.Start(ctx, smoke.instance); err != nil {
		t.Fatalf("start package smoke VM: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
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

	stageDir := stagePackage(t, packagePath)
	remotePackage := path.Join(smoke.remoteDir, filepath.Base(packagePath))
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
		if err := manager.Shell(ctx, smoke.instance, nil, append(smoke.parseCommand, remotePackage)...); err != nil {
			t.Fatalf("package metadata did not parse: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
		}
	}

	if err := manager.Shell(ctx, smoke.instance, nil, append(smoke.installCommand, remotePackage)...); err != nil {
		t.Fatalf("install vectis-cli package: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	installed = true

	if err := manager.Shell(ctx, smoke.instance, nil, "test", "-x", "/usr/bin/vectis-cli"); err != nil {
		t.Fatalf("packaged CLI was not installed executable: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	if err := manager.Shell(ctx, smoke.instance, nil, "vectis-cli", "--version"); err != nil {
		t.Fatalf("packaged CLI did not run: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	if err := manager.Shell(ctx, smoke.instance, nil, "test", "!", "-e", "/etc/systemd/system/vectis.target"); err != nil {
		t.Fatalf("vectis-cli package should not install service units: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	if err := manager.Shell(ctx, smoke.instance, nil, smoke.removeCommand...); err != nil {
		t.Fatalf("remove vectis-cli package: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	installed = false
	if err := manager.Shell(ctx, smoke.instance, nil, "test", "!", "-e", "/usr/bin/vectis-cli"); err != nil {
		t.Fatalf("packaged CLI remained after removal: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
}

func stagePackage(t *testing.T, packagePath string) string {
	t.Helper()

	dir := t.TempDir()
	destination := filepath.Join(dir, filepath.Base(packagePath))
	if err := copyFile(destination, packagePath); err != nil {
		t.Fatal(err)
	}

	return dir
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
