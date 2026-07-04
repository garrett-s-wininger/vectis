//go:build e2e

package linux_test

import (
	"bytes"
	"context"
	"testing"

	"vectis/internal/platform"
)

func verifyLocalInstalled(ctx context.Context, t *testing.T, manager platform.VirtualMachineManager, instance string, stdout, stderr *bytes.Buffer) {
	t.Helper()

	for _, path := range []string{
		"/usr/bin/vectis-cli",
		"/usr/bin/vectis-local",
		"/usr/lib/vectis-local/bin/vectis-local",
		"/usr/lib/vectis-local/bin/vectis-api",
		"/usr/lib/vectis-local/bin/vectis-artifact",
		"/usr/lib/vectis-local/bin/vectis-orchestrator",
		"/usr/lib/vectis-local/bin/vectis-secrets",
		"/usr/lib/vectis-local/bin/vectis-worker-core",
	} {
		if err := manager.Shell(ctx, instance, nil, "test", "-x", path); err != nil {
			t.Fatalf("expected executable package path %s: %v\nstdout:\n%s\nstderr:\n%s", path, err, stdout.String(), stderr.String())
		}
	}

	for _, path := range []string{
		"/usr/lib/systemd/system/vectis-local.service",
		"/usr/lib/systemd/system/vectis.target",
		"/etc/vectis/vectis.env",
	} {
		if err := manager.Shell(ctx, instance, nil, "test", "!", "-e", path); err != nil {
			t.Fatalf("vectis-local package should not install %s: %v\nstdout:\n%s\nstderr:\n%s", path, err, stdout.String(), stderr.String())
		}
	}

	if err := manager.Shell(ctx, instance, nil, "vectis-local", "--help"); err != nil {
		t.Fatalf("packaged vectis-local wrapper did not run: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
}

func verifyLocalRemoved(ctx context.Context, t *testing.T, manager platform.VirtualMachineManager, instance string, stdout, stderr *bytes.Buffer) {
	t.Helper()

	for _, path := range []string{
		"/usr/bin/vectis-local",
		"/usr/lib/vectis-local/bin/vectis-local",
		"/usr/lib/vectis-local/bin/vectis-api",
	} {
		if err := manager.Shell(ctx, instance, nil, "test", "!", "-e", path); err != nil {
			t.Fatalf("package path remained after removal %s: %v\nstdout:\n%s\nstderr:\n%s", path, err, stdout.String(), stderr.String())
		}
	}

	if err := manager.Shell(ctx, instance, nil, "test", "!", "-e", "/usr/bin/vectis-cli"); err != nil {
		t.Fatalf("packaged CLI remained after local package removal: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
}
