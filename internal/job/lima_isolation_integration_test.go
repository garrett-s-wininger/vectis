package job_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
	"vectis/internal/platform"
)

func TestVirtualMachineIntegration_ActionIsolation(t *testing.T) {
	instance := strings.TrimSpace(os.Getenv("VECTIS_TEST_LIMA_INSTANCE"))
	if instance == "" {
		t.Skip("set VECTIS_TEST_LIMA_INSTANCE to run VM isolation integration test")
	}

	workspaceRoot := strings.TrimSpace(os.Getenv("VECTIS_TEST_LIMA_WORKSPACE_ROOT"))
	if workspaceRoot == "" {
		workspaceRoot = "."
	}

	workspace, err := os.MkdirTemp(workspaceRoot, ".vectis-lima-isolation-*") //nolint:usetesting // Honor caller-selected Lima workspace root.
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(workspace) })

	absWorkspace, err := filepath.Abs(workspace)
	if err != nil {
		t.Fatalf("resolve workspace: %v", err)
	}

	vmExecutor, err := platform.NewVirtualMachineCommandExecutor(platform.VirtualMachineConfig{
		Provider:           platform.VirtualMachineProviderLima,
		Instance:           instance,
		ProviderPath:       os.Getenv("VECTIS_TEST_LIMA_PATH"),
		GuestWorkspaceRoot: os.Getenv("VECTIS_TEST_LIMA_GUEST_WORKSPACE_ROOT"),
		Start:              os.Getenv("VECTIS_TEST_LIMA_START") == "1",
	})

	if err != nil {
		t.Fatalf("new VM executor: %v", err)
	}

	executor := job.NewExecutor(
		job.WithVMProcessExecutor(vmExecutor),
		job.WithDefaultIsolation(action.IsolationVM),
	)

	jobID := "test-lima-action-isolation"
	runID := "test-lima-action-isolation-run"
	rootID := "root"
	rootUses := "builtins/sequence"
	vmStepID := "vm-step"
	hostStepID := "host-step"
	shellUses := "builtins/script"
	hostIsolation := action.IsolationHost
	testJob := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &rootUses,
			Steps: []*api.Node{
				{
					Id:   &vmStepID,
					Uses: &shellUses,
					With: map[string]string{
						"command": `set -eu; test "$(uname -s)" = Linux; printf 'vm-action\n'`,
					},
				},
				{
					Id:        &hostStepID,
					Uses:      &shellUses,
					Isolation: &hostIsolation,
					With: map[string]string{
						"command": `printf 'host-action\n'`,
					},
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	logClient := mocks.NewMockLogClient()
	logger := mocks.NewMockLogger()
	if err := executor.ExecuteJobInWorkspace(ctx, testJob, logClient, logger, absWorkspace); err != nil {
		t.Fatalf("execute action-isolated job: %v", err)
	}

	var logs strings.Builder
	for _, chunk := range logClient.GetChunks() {
		logs.Write(chunk.GetData())
		logs.WriteByte('\n')
	}

	gotLogs := logs.String()
	for _, want := range []string{"vm-action", "host-action"} {
		if !strings.Contains(gotLogs, want) {
			t.Fatalf("expected logs to contain %q, got:\n%s", want, gotLogs)
		}
	}
}
