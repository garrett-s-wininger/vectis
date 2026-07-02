//go:build e2e

package linux_test

import (
	"bytes"
	"context"
	"testing"

	"vectis/internal/platform"
)

func verifyServicesInstalled(ctx context.Context, t *testing.T, manager platform.VirtualMachineManager, instance string, stdout, stderr *bytes.Buffer) {
	t.Helper()

	for _, path := range []string{
		"/usr/bin/vectis-cli",
		"/usr/bin/vectis-api",
		"/usr/bin/vectis-artifact",
		"/usr/bin/vectis-orchestrator",
		"/usr/bin/vectis-secrets",
		"/usr/bin/vectis-spiffe",
		"/usr/bin/vectis-worker",
		"/usr/bin/vectis-worker-core",
		"/usr/lib/systemd/system/vectis.target",
		"/usr/lib/systemd/system/vectis-api.service",
		"/usr/lib/systemd/system/vectis-artifact.service",
		"/usr/lib/systemd/system/vectis-db-migrate.service",
		"/usr/lib/systemd/system/vectis-orchestrator.service",
		"/usr/lib/systemd/system/vectis-retention-scheduled-cleanup.service",
		"/usr/lib/systemd/system/vectis-retention-scheduled-cleanup.timer",
		"/usr/lib/systemd/system/vectis-secrets.service",
		"/usr/lib/systemd/system/vectis-spiffe.service",
		"/usr/lib/systemd/system/vectis-worker-core.service",
		"/usr/lib/sysusers.d/vectis.conf",
		"/usr/lib/tmpfiles.d/vectis.conf",
		"/usr/share/doc/vectis-common/examples/vectis.env.example",
		"/usr/share/doc/vectis-common/examples/vectis-db-migrate.env.example",
		"/usr/share/doc/vectis-common/examples/vectis-retention-scheduled-cleanup.env.example",
		"/usr/share/doc/vectis-api/examples/vectis-api.env.example",
	} {
		if err := manager.Shell(ctx, instance, nil, "test", "-e", path); err != nil {
			t.Fatalf("expected package path %s: %v\nstdout:\n%s\nstderr:\n%s", path, err, stdout.String(), stderr.String())
		}
	}

	for _, path := range []string{
		"/usr/bin/vectis-cli",
		"/usr/bin/vectis-api",
		"/usr/bin/vectis-artifact",
		"/usr/bin/vectis-orchestrator",
		"/usr/bin/vectis-secrets",
		"/usr/bin/vectis-spiffe",
		"/usr/bin/vectis-worker",
		"/usr/bin/vectis-worker-core",
	} {
		if err := manager.Shell(ctx, instance, nil, "test", "-x", path); err != nil {
			t.Fatalf("expected executable package path %s: %v\nstdout:\n%s\nstderr:\n%s", path, err, stdout.String(), stderr.String())
		}
	}

	if err := manager.Shell(ctx, instance, nil, "test", "!", "-e", "/etc/vectis/vectis.env"); err != nil {
		t.Fatalf("services package should not install live /etc/vectis config: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	if err := manager.Shell(ctx, instance, nil, "/bin/sh", "-lc", "sudo systemd-analyze verify /usr/lib/systemd/system/vectis.target /usr/lib/systemd/system/vectis*.service /usr/lib/systemd/system/vectis*.timer"); err != nil {
		t.Fatalf("packaged systemd units did not verify: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
}

func verifyServicesRemoved(ctx context.Context, t *testing.T, manager platform.VirtualMachineManager, instance string, stdout, stderr *bytes.Buffer) {
	t.Helper()

	for _, path := range []string{
		"/usr/bin/vectis-cli",
		"/usr/bin/vectis-api",
		"/usr/bin/vectis-artifact",
		"/usr/bin/vectis-orchestrator",
		"/usr/bin/vectis-secrets",
		"/usr/bin/vectis-spiffe",
		"/usr/bin/vectis-worker-core",
		"/usr/lib/systemd/system/vectis.target",
		"/usr/lib/systemd/system/vectis-api.service",
		"/usr/lib/systemd/system/vectis-artifact.service",
		"/usr/lib/systemd/system/vectis-orchestrator.service",
		"/usr/lib/systemd/system/vectis-retention-scheduled-cleanup.service",
		"/usr/lib/systemd/system/vectis-retention-scheduled-cleanup.timer",
		"/usr/lib/systemd/system/vectis-secrets.service",
		"/usr/lib/systemd/system/vectis-spiffe.service",
		"/usr/lib/systemd/system/vectis-worker-core.service",
		"/usr/lib/sysusers.d/vectis.conf",
		"/usr/lib/tmpfiles.d/vectis.conf",
		"/usr/share/doc/vectis-common/examples/vectis.env.example",
		"/usr/share/doc/vectis-common/examples/vectis-retention-scheduled-cleanup.env.example",
		"/usr/share/doc/vectis-api/examples/vectis-api.env.example",
	} {
		if err := manager.Shell(ctx, instance, nil, "test", "!", "-e", path); err != nil {
			t.Fatalf("package path remained after removal %s: %v\nstdout:\n%s\nstderr:\n%s", path, err, stdout.String(), stderr.String())
		}
	}
}

func servicePackageRemovalNames() []string {
	return []string{
		"vectis-services",
		"vectis-api",
		"vectis-artifact",
		"vectis-catalog",
		"vectis-cell-ingress",
		"vectis-cron",
		"vectis-docs",
		"vectis-log",
		"vectis-log-forwarder",
		"vectis-orchestrator",
		"vectis-queue",
		"vectis-reconciler",
		"vectis-registry",
		"vectis-secrets",
		"vectis-spiffe",
		"vectis-worker",
		"vectis-worker-core",
		"vectis-common",
		"vectis-cli",
	}
}
