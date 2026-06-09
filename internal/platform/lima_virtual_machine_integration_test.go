package platform

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestVirtualMachineIntegration_LimaProvider(t *testing.T) {
	instance := strings.TrimSpace(os.Getenv("VECTIS_TEST_LIMA_INSTANCE"))
	if instance == "" {
		t.Skip("set VECTIS_TEST_LIMA_INSTANCE to run Lima VM provider integration test")
	}

	workspaceRoot := strings.TrimSpace(os.Getenv("VECTIS_TEST_LIMA_WORKSPACE_ROOT"))
	if workspaceRoot == "" {
		workspaceRoot = "."
	}

	workspace, err := os.MkdirTemp(workspaceRoot, ".vectis-lima-work-*")
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(workspace) })

	absWorkspace, err := filepath.Abs(workspace)
	if err != nil {
		t.Fatalf("resolve workspace: %v", err)
	}

	executor, err := NewVirtualMachineCommandExecutor(VirtualMachineConfig{
		Provider:           VirtualMachineProviderLima,
		Instance:           instance,
		ProviderPath:       os.Getenv("VECTIS_TEST_LIMA_PATH"),
		GuestWorkspaceRoot: os.Getenv("VECTIS_TEST_LIMA_GUEST_WORKSPACE_ROOT"),
		Start:              os.Getenv("VECTIS_TEST_LIMA_START") == "1",
	})

	if err != nil {
		t.Fatalf("new VM executor: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	guestEnv := []string{"CI=true", "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"}
	process, err := executor.Start(ctx, "sh", []string{
		"-c",
		"set -eu; test \"$(uname -s)\" = Linux; test \"$CI\" = true; pwd; touch vectis-lima-marker",
	}, absWorkspace, guestEnv)

	if err != nil {
		t.Fatalf("start lima command: %v", err)
	}

	stdout, stdoutErr := io.ReadAll(process.Stdout())
	stderr, stderrErr := io.ReadAll(process.Stderr())
	if stdoutErr != nil {
		t.Fatalf("read stdout: %v", stdoutErr)
	}

	if stderrErr != nil {
		t.Fatalf("read stderr: %v", stderrErr)
	}

	if err := process.Wait(); err != nil {
		t.Fatalf("lima command failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
	}

	if strings.TrimSpace(os.Getenv("VECTIS_TEST_LIMA_GUEST_WORKSPACE_ROOT")) == "" {
		if _, err := os.Stat(filepath.Join(absWorkspace, "vectis-lima-marker")); err != nil {
			t.Fatalf("expected Lima command to write marker through writable mounted workspace: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
		}

		return
	}

	process, err = executor.Start(ctx, "test", []string{"-f", "vectis-lima-marker"}, absWorkspace, guestEnv)
	if err != nil {
		t.Fatalf("start lima marker check: %v", err)
	}

	_, _ = io.Copy(io.Discard, process.Stdout())
	markerStderr, _ := io.ReadAll(process.Stderr())
	if err := process.Wait(); err != nil {
		t.Fatalf("expected marker to persist in guest workspace: %v\nstderr:\n%s", err, markerStderr)
	}
}
