//go:build unix

package job_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
	"vectis/internal/taskgraph"
)

func TestExecutorTimeoutTerminatesScriptProcessGroup(t *testing.T) {
	workspace := t.TempDir()
	childPIDPath := filepath.Join(workspace, "child.pid")
	childTermPath := filepath.Join(workspace, "child.term")

	jobID := "test-timeout-process-group"
	runID := "test-timeout-process-group-run"
	timeoutUses := "builtins/timeout"
	scriptUses := "builtins/script"
	testJob := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   executorStrp("timed-script"),
			Uses: &timeoutUses,
			With: map[string]string{"duration": "500ms"},
			Ports: map[string]*api.NodePort{taskgraph.BodyPort: executorNodePort(&api.Node{
				Id:   executorStrp("script"),
				Uses: &scriptUses,
				With: map[string]string{
					"script": `(trap 'echo term > child.term; exit 0' TERM; sleep 30) & echo $! > child.pid; wait`,
				},
			})},
		},
	}

	err := executeJobInWorkspaceWithOptionsAndWait(
		t,
		job.NewExecutor(),
		testJob,
		mocks.NewMockLogClient(),
		mocks.NewMockLogger(),
		workspace,
		job.ExecuteOptions{},
	)

	if err == nil || !strings.Contains(err.Error(), "timeout exceeded after 500ms") {
		t.Fatalf("ExecuteJob error = %v, want timeout", err)
	}

	pidBytes, err := os.ReadFile(childPIDPath)
	if err != nil {
		t.Fatalf("read child pid: %v", err)
	}

	childPID, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
	if err != nil {
		t.Fatalf("parse child pid %q: %v", string(pidBytes), err)
	}

	t.Cleanup(func() { _ = syscall.Kill(childPID, syscall.SIGKILL) })

	if err := waitForFile(childTermPath, 2*time.Second); err != nil {
		t.Fatal(err)
	}

	if err := waitForNoProcessInJobTest(childPID, 2*time.Second); err != nil {
		t.Fatal(err)
	}
}

func waitForFile(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}

		time.Sleep(10 * time.Millisecond)
	}

	return os.ErrNotExist
}

func waitForNoProcessInJobTest(pid int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); err == syscall.ESRCH {
			return nil
		}

		time.Sleep(10 * time.Millisecond)
	}

	return context.DeadlineExceeded
}
