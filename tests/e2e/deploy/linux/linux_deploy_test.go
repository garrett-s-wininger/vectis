//go:build e2e

package linux_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	linuxdeploy "vectis/deploy/linux"
)

const (
	defaultDeployLinuxProvider = "auto"
	defaultDeployLinuxProfile  = linuxdeploy.VMSmokeProfileUnits
	defaultDeployLinuxTimeout  = 10 * time.Minute
)

var deployLinuxLocalRequiredBinaries = []string{
	"vectis-api",
	"vectis-artifact",
	"vectis-catalog",
	"vectis-cell-ingress",
	"vectis-cron",
	"vectis-local",
	"vectis-log",
	"vectis-orchestrator",
	"vectis-queue",
	"vectis-reconciler",
	"vectis-registry",
	"vectis-worker",
	"vectis-worker-core",
}

type commandResult struct {
	stdout string
	stderr string
}

func TestE2ELinuxDeploySmoke(t *testing.T) {
	ctx := context.Background()
	provider := envOrDefault("VECTIS_E2E_DEPLOY_LINUX_PROVIDER", defaultDeployLinuxProvider)
	providerPath := strings.TrimSpace(os.Getenv("VECTIS_E2E_DEPLOY_LINUX_PROVIDER_PATH"))
	instance := strings.TrimSpace(os.Getenv("VECTIS_E2E_DEPLOY_LINUX_INSTANCE"))
	template := strings.TrimSpace(os.Getenv("VECTIS_E2E_DEPLOY_LINUX_TEMPLATE"))
	profile := strings.ToLower(envOrDefault("VECTIS_E2E_DEPLOY_LINUX_PROFILE", defaultDeployLinuxProfile))
	binaryDir := strings.TrimSpace(os.Getenv("VECTIS_E2E_DEPLOY_LINUX_BINARY_DIR"))
	timeout := deployLinuxTimeout(t)

	requireDeployLinuxProvider(t, ctx, provider, providerPath)
	if profile == linuxdeploy.VMSmokeProfileLocal {
		requireDeployLinuxBinaryDir(t, binaryDir)
	}

	var stdout, stderr bytes.Buffer
	opts := linuxdeploy.VMSmokeOptions{
		Provider:      provider,
		ProviderPath:  providerPath,
		Instance:      instance,
		Template:      template,
		Profile:       profile,
		BinaryDir:     binaryDir,
		KeepArtifacts: truthyEnv("VECTIS_E2E_KEEP_DEPLOY_LINUX"),
		Stdout:        &stdout,
		Stderr:        &stderr,
	}

	if !opts.KeepArtifacts {
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if _, err := linuxdeploy.RunVMSmokeClean(cleanupCtx, opts); err != nil {
				t.Errorf("clean Linux deploy smoke artifacts: %v", err)
			}
			if _, err := linuxdeploy.RunVMSmokeDown(cleanupCtx, opts); err != nil {
				t.Errorf("stop Linux deploy smoke VM: %v", err)
			}
		})
	}

	verifyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	result, err := linuxdeploy.RunVMSmokeVerify(verifyCtx, opts)
	if err != nil {
		t.Fatalf("run Linux deploy smoke: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	if result.Status != "verified" {
		t.Fatalf("deploy linux status = %q, want verified\nstdout:\n%s\nstderr:\n%s", result.Status, stdout.String(), stderr.String())
	}
	if result.Profile != profile {
		t.Fatalf("deploy linux profile = %q, want %q", result.Profile, profile)
	}
	if strings.TrimSpace(result.Provider) == "" {
		t.Fatalf("deploy linux output missing provider: %+v", result)
	}
	if strings.TrimSpace(result.Instance) == "" {
		t.Fatalf("deploy linux output missing instance: %+v", result)
	}
	if !opts.KeepArtifacts && !result.GuestCleaned {
		t.Fatalf("Linux deploy smoke did not report guest cleanup: %+v", result)
	}
}

func requireDeployLinuxProvider(t *testing.T, ctx context.Context, provider, providerPath string) {
	t.Helper()

	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "", "auto", "lima":
		limactl := providerPath
		if limactl == "" {
			limactl = "limactl"
		}

		requireCommand(t, limactl)
		checkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if _, err := runCommand(checkCtx, limactl, "--version"); err != nil {
			skipOrFatal(t, "limactl is not usable: %v", err)
		}
	default:
		skipOrFatal(t, "unsupported Linux deploy e2e provider %q", provider)
	}
}

func requireDeployLinuxBinaryDir(t *testing.T, binaryDir string) {
	t.Helper()

	if binaryDir == "" {
		skipOrFatal(t, "VECTIS_E2E_DEPLOY_LINUX_PROFILE=local requires VECTIS_E2E_DEPLOY_LINUX_BINARY_DIR")
	}

	for _, binary := range deployLinuxLocalRequiredBinaries {
		requireExecutable(t, filepath.Join(binaryDir, binary), binary)
	}
}

func deployLinuxTimeout(t *testing.T) time.Duration {
	t.Helper()

	raw := strings.TrimSpace(os.Getenv("VECTIS_E2E_DEPLOY_LINUX_TIMEOUT"))
	if raw == "" {
		return defaultDeployLinuxTimeout
	}

	timeout, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("invalid VECTIS_E2E_DEPLOY_LINUX_TIMEOUT %q: %v", raw, err)
	}

	return timeout
}

func requireExecutable(t *testing.T, path, label string) {
	t.Helper()

	info, err := os.Stat(path)
	if err != nil {
		skipOrFatal(t, "%s executable %s is not available", label, path)
	}

	if info.IsDir() {
		skipOrFatal(t, "%s path %s is a directory", label, path)
	}

	if runtime.GOOS != "windows" && info.Mode()&0o111 == 0 {
		skipOrFatal(t, "%s binary %s is not executable", label, path)
	}
}

func requireCommand(t *testing.T, name string) {
	t.Helper()

	if _, err := exec.LookPath(name); err != nil {
		skipOrFatal(t, "%s is not available on PATH", name)
	}
}

func runCommand(ctx context.Context, name string, args ...string) (commandResult, error) {
	cmd := exec.CommandContext(ctx, name, args...) // #nosec G204 -- e2e harness controls command names/args.
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	result := commandResult{stdout: stdout.String(), stderr: stderr.String()}
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return result, fmt.Errorf("%s %s timed out: stdout=%q stderr=%q", name, strings.Join(args, " "), result.stdout, result.stderr)
		}

		return result, fmt.Errorf("%s %s failed: %w\nstdout:\n%s\nstderr:\n%s", name, strings.Join(args, " "), err, result.stdout, result.stderr)
	}

	return result, nil
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
