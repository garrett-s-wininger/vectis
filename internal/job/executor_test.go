package job_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
	"vectis/internal/action/builtins"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
	"vectis/internal/secrets"
	"vectis/internal/taskgraph"
)

func executorStrp(s string) *string { return &s }

func executeAndWait(t *testing.T, executor *job.Executor, testJob *api.Job, mockLogClient *mocks.MockLogClient, mockLogger *mocks.MockLogger) error {
	t.Helper()
	return executeAndWaitWithOptions(t, executor, testJob, mockLogClient, mockLogger, job.ExecuteOptions{})
}

func executeAndWaitWithOptions(t *testing.T, executor *job.Executor, testJob *api.Job, mockLogClient *mocks.MockLogClient, mockLogger *mocks.MockLogger, opts job.ExecuteOptions) error {
	t.Helper()
	streamCh := make(chan job.LogStreamWaiter, 1)
	executor.TestLogStreamHook = streamCh
	defer func() { executor.TestLogStreamHook = nil }()

	err := executor.ExecuteJobWithOptions(context.Background(), testJob, mockLogClient, mockLogger, opts)

	select {
	case stream := <-streamCh:
		if waitErr := stream.WaitForDone(5 * time.Second); waitErr != nil {
			t.Errorf("wait for log stream done: %v", waitErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for log stream hook")
	}

	return err
}

func executeTaskAndWait(t *testing.T, executor *job.Executor, testJob *api.Job, taskKey string, mockLogClient *mocks.MockLogClient, mockLogger *mocks.MockLogger) error {
	t.Helper()
	return executeTaskAndWaitWithOptions(t, executor, testJob, taskKey, mockLogClient, mockLogger, job.ExecuteOptions{})
}

func executeTaskAndWaitWithOptions(t *testing.T, executor *job.Executor, testJob *api.Job, taskKey string, mockLogClient *mocks.MockLogClient, mockLogger *mocks.MockLogger, opts job.ExecuteOptions) error {
	t.Helper()
	streamCh := make(chan job.LogStreamWaiter, 1)
	executor.TestLogStreamHook = streamCh
	defer func() { executor.TestLogStreamHook = nil }()

	err := executor.ExecuteTaskWithOptions(context.Background(), testJob, taskKey, mockLogClient, mockLogger, opts)

	select {
	case stream := <-streamCh:
		if waitErr := stream.WaitForDone(5 * time.Second); waitErr != nil {
			t.Errorf("wait for log stream done: %v", waitErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for log stream hook")
	}

	return err
}

func executeJobInWorkspaceWithOptionsAndWait(t *testing.T, executor *job.Executor, testJob *api.Job, mockLogClient *mocks.MockLogClient, mockLogger *mocks.MockLogger, workspace string, opts job.ExecuteOptions) error {
	t.Helper()
	streamCh := make(chan job.LogStreamWaiter, 1)
	executor.TestLogStreamHook = streamCh
	defer func() { executor.TestLogStreamHook = nil }()

	err := executor.ExecuteJobInWorkspaceWithOptions(context.Background(), testJob, mockLogClient, mockLogger, workspace, opts)

	select {
	case stream := <-streamCh:
		if waitErr := stream.WaitForDone(5 * time.Second); waitErr != nil {
			t.Errorf("wait for log stream done: %v", waitErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for log stream hook")
	}

	return err
}

func logChunkStrings(chunks []*api.LogChunk) []string {
	out := make([]string, len(chunks))
	for i, chunk := range chunks {
		out[i] = string(chunk.GetData())
	}
	return out
}

func executorNodePort(nodes ...*api.Node) *api.NodePort {
	return &api.NodePort{Nodes: nodes}
}

func assertLogContains(t *testing.T, chunks []string, expected string) {
	t.Helper()
	for _, chunk := range chunks {
		if strings.Contains(chunk, expected) {
			return
		}
	}
	t.Fatalf("expected log chunk containing %q not found. Got chunks: %v", expected, chunks)
}

func assertLogNotContains(t *testing.T, chunks []string, unexpected string) {
	t.Helper()
	for _, chunk := range chunks {
		if strings.Contains(chunk, unexpected) {
			t.Fatalf("unexpected log chunk containing %q found. Got chunks: %v", unexpected, chunks)
		}
	}
}

func repoPath(t *testing.T, elem ...string) string {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}

	for {
		candidate := filepath.Join(append([]string{wd}, elem...)...)
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		} else if !os.IsNotExist(err) {
			t.Fatalf("Stat %s: %v", candidate, err)
		}

		parent := filepath.Dir(wd)
		if parent == wd {
			t.Fatalf("could not find repo path %s", filepath.Join(elem...))
		}

		wd = parent
	}
}

type executorDescriptorResolver map[string]actionregistry.Descriptor

func (r executorDescriptorResolver) ResolveDescriptor(uses string) (actionregistry.Descriptor, error) {
	descriptor, ok := r[uses]
	if !ok {
		return actionregistry.Descriptor{}, fmt.Errorf("unknown action: %s", uses)
	}

	return descriptor, nil
}

func TestExecutor_ExecuteJob_Success(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	jobID := "test-job-1"
	nodeID := "node-1"
	uses := "builtins/shell"
	runID := "test-run-1"
	testJob := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &nodeID,
			Uses: &uses,
			With: map[string]string{
				"command": "echo hello",
			},
		},
	}

	err := executeAndWait(t, executor, testJob, mockLogClient, mockLogger)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if mockLogClient.GetStreamCount() < 1 {
		t.Errorf("expected at least 1 log stream, got %d", mockLogClient.GetStreamCount())
	}

	chunks := mockLogClient.GetChunks()
	if len(chunks) == 0 {
		t.Fatal("expected log chunks to be sent")
	}

	for i, chunk := range chunks {
		if chunk.GetRunId() == "" {
			t.Errorf("chunk %d: expected run ID to be set", i)
		}
	}

	for i, chunk := range chunks {
		expectedSeq := int64(i + 1)
		if chunk.GetSequence() != expectedSeq {
			t.Errorf("chunk %d: expected sequence %d, got %d", i, expectedSeq, chunk.GetSequence())
		}
	}

	chunkStrings := make([]string, len(chunks))
	for i, chunk := range chunks {
		chunkStrings[i] = string(chunk.GetData())
	}

	expectedMessages := []string{
		"Starting job execution: test-job-1",
		"Executing node: builtins/shell",
		"echo hello",
		"Command completed successfully",
	}

	for _, expected := range expectedMessages {
		found := false
		for _, actual := range chunkStrings {
			if strings.Contains(actual, expected) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected log chunk containing %q not found. Got chunks: %v", expected, chunkStrings)
		}
	}

	infoCalls := mockLogger.GetInfoCalls()
	if len(infoCalls) == 0 {
		t.Error("expected info log calls")
	}

	hasStartMsg := false
	for _, msg := range infoCalls {
		if strings.Contains(msg, "Starting job execution: test-job-1") {
			hasStartMsg = true
			break
		}
	}
	if !hasStartMsg {
		t.Error("expected logger to contain 'Starting job execution: test-job-1'")
	}
	// "Job completed successfully" is logged by the worker, not the executor.

	errorCalls := mockLogger.GetErrorCalls()
	if len(errorCalls) > 0 {
		t.Errorf("expected no error logs, got: %v", errorCalls)
	}
}

func TestExecutor_ExecuteJob_MaterializesSecretFiles(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()
	workspace := t.TempDir()

	jobID := "test-job-secrets"
	nodeID := "node-1"
	uses := "builtins/shell"
	runID := "test-run-secrets"
	testJob := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &nodeID,
			Uses: &uses,
			With: map[string]string{
				"command": `test "$(cat "$VECTIS_SECRETS_DIR/npm/token")" = "secret-value"`,
			},
		},
	}

	err := executeJobInWorkspaceWithOptionsAndWait(t, executor, testJob, mockLogClient, mockLogger, workspace, job.ExecuteOptions{
		SecretFiles: []secrets.FileMaterial{{
			ID:   "npm-token",
			Path: "npm/token",
			Data: []byte("secret-value"),
			Mode: secrets.DefaultFileMode,
		}},
	})

	if err != nil {
		t.Fatalf("ExecuteJobInWorkspaceWithOptions: %v", err)
	}

	if runtime.GOOS != "windows" {
		info, err := os.Stat(filepath.Join(workspace, ".vectis"))
		if err != nil {
			t.Fatalf("stat .vectis: %v", err)
		}

		if gotMode := info.Mode().Perm(); gotMode != 0o700 {
			t.Fatalf(".vectis mode = %v, want 0700", gotMode)
		}
	}

	materializedPath := filepath.Join(workspace, ".vectis", "secrets", "npm", "token")
	if _, err := os.Stat(materializedPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("materialized secret file still exists after cleanup, stat err=%v", err)
	}
}

func TestExecutor_ExecuteJob_DoesNotInheritWorkerEnvironment(t *testing.T) {
	t.Setenv("VECTIS_DATABASE_DSN", "postgres://secret")
	t.Setenv("SPIFFE_ENDPOINT_SOCKET", "unix:///tmp/spire-agent.sock")

	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	jobID := "test-job-env"
	nodeID := "node-1"
	uses := "builtins/shell"
	runID := "test-run-env"
	testJob := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &nodeID,
			Uses: &uses,
			With: map[string]string{
				"command": `if env | grep -q 'VECTIS_DATABASE_DSN\|SPIFFE_ENDPOINT_SOCKET'; then echo leaked; exit 1; fi; echo clean`,
			},
		},
	}

	err := executeAndWait(t, executor, testJob, mockLogClient, mockLogger)
	if err != nil {
		t.Fatalf("expected sanitized child environment, got %v", err)
	}

	for _, chunk := range mockLogClient.GetChunks() {
		data := string(chunk.GetData())
		if strings.Contains(data, "postgres://secret") || strings.Contains(data, "unix:///tmp/spire-agent.sock") {
			t.Fatalf("child environment leaked worker secret in log chunk %q", data)
		}
	}
}

func TestExecutor_ExecuteTask_ExecutesSelectedNodeOnly(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	testJob := &api.Job{
		Id:    executorStrp("test-job-task-selected"),
		RunId: executorStrp("test-run-task-selected"),
		Root: &api.Node{
			Id:   executorStrp("root-node"),
			Uses: executorStrp("builtins/sequence"),
			Steps: []*api.Node{
				{
					Id:   executorStrp("first"),
					Uses: executorStrp("builtins/shell"),
					With: map[string]string{"command": "echo task-first-marker"},
				},
				{
					Id:   executorStrp("second"),
					Uses: executorStrp("builtins/shell"),
					With: map[string]string{"command": "echo task-second-marker"},
				},
			},
		},
	}

	err := executeTaskAndWait(t, executor, testJob, "second", mockLogClient, mockLogger)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	chunks := logChunkStrings(mockLogClient.GetChunks())
	assertLogContains(t, chunks, "Starting task execution: test-job-task-selected task second")
	assertLogContains(t, chunks, "task-second-marker")
	assertLogNotContains(t, chunks, "task-first-marker")
}

func TestExecutor_ExecuteTask_VerifiesActionLockWithOriginalNodePath(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	testJob := &api.Job{
		Id:    executorStrp("test-job-task-lock-path"),
		RunId: executorStrp("test-run-task-lock-path"),
		Root: &api.Node{
			Id:   executorStrp("root-node"),
			Uses: executorStrp("builtins/sequence"),
			Steps: []*api.Node{
				{
					Id:   executorStrp("first"),
					Uses: executorStrp("builtins/shell"),
					With: map[string]string{"command": "echo task-first-marker"},
				},
				{
					Id:   executorStrp("second"),
					Uses: executorStrp("builtins/shell"),
					With: map[string]string{"command": "echo task-second-marker"},
				},
			},
		},
	}

	descriptor, err := builtins.NewRegistry().ResolveDescriptor("builtins/shell")
	if err != nil {
		t.Fatalf("ResolveDescriptor: %v", err)
	}

	err = executeTaskAndWaitWithOptions(t, executor, testJob, "second", mockLogClient, mockLogger, job.ExecuteOptions{
		ActionLocks: []actionregistry.ActionLock{{
			NodeID:     "second",
			NodePath:   "root.steps[1]",
			Uses:       "builtins/shell",
			Descriptor: descriptor,
		}},
	})

	if err != nil {
		t.Fatalf("expected task lock to verify with original node path, got %v", err)
	}

	chunks := logChunkStrings(mockLogClient.GetChunks())
	assertLogContains(t, chunks, "task-second-marker")
	assertLogNotContains(t, chunks, "task-first-marker")
}

func TestExecutor_ExecuteTask_ExecutesLocalChildTasks(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	testJob := &api.Job{
		Id:    executorStrp("test-job-task-parent"),
		RunId: executorStrp("test-run-task-parent"),
		Root: &api.Node{
			Id:   executorStrp("root-node"),
			Uses: executorStrp("builtins/sequence"),
			Steps: []*api.Node{
				{
					Id:   executorStrp("branch"),
					Uses: executorStrp("builtins/sequence"),
					Steps: []*api.Node{
						{
							Id:   executorStrp("nested"),
							Uses: executorStrp("builtins/shell"),
							With: map[string]string{"command": "echo nested-child-marker"},
						},
					},
				},
			},
		},
	}

	err := executeTaskAndWait(t, executor, testJob, "branch", mockLogClient, mockLogger)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	chunks := logChunkStrings(mockLogClient.GetChunks())
	assertLogContains(t, chunks, "Starting task execution: test-job-task-parent task branch")
	assertLogContains(t, chunks, "Executing node: builtins/sequence")
	assertLogContains(t, chunks, "nested-child-marker")
}

func TestExecutor_ExecuteTask_RootTaskExecutesLocalChildren(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	testJob := &api.Job{
		Id:    executorStrp("test-job-task-root"),
		RunId: executorStrp("test-run-task-root"),
		Root: &api.Node{
			Id:   executorStrp("root-node"),
			Uses: executorStrp("builtins/sequence"),
			Steps: []*api.Node{
				{
					Id:   executorStrp("child"),
					Uses: executorStrp("builtins/shell"),
					With: map[string]string{"command": "echo root-child-marker"},
				},
			},
		},
	}

	err := executeTaskAndWait(t, executor, testJob, dal.RootTaskKey, mockLogClient, mockLogger)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	chunks := logChunkStrings(mockLogClient.GetChunks())
	assertLogContains(t, chunks, "Starting task execution: test-job-task-root task root")
	assertLogContains(t, chunks, "Executing node: builtins/sequence")
	assertLogContains(t, chunks, "root-child-marker")
}

func TestExecutor_ExecuteTask_RootTaskExecutesExplicitPorts(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	testJob := &api.Job{
		Id:    executorStrp("test-job-task-root-ports"),
		RunId: executorStrp("test-run-task-root-ports"),
		Root: &api.Node{
			Id:   executorStrp("root-node"),
			Uses: executorStrp("builtins/sequence"),
			Ports: map[string]*api.NodePort{
				taskgraph.StepsPort: executorNodePort(&api.Node{
					Id:   executorStrp("child"),
					Uses: executorStrp("builtins/shell"),
					With: map[string]string{"command": "echo explicit-port-marker"},
				}),
			},
		},
	}

	err := executeTaskAndWait(t, executor, testJob, dal.RootTaskKey, mockLogClient, mockLogger)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	chunks := logChunkStrings(mockLogClient.GetChunks())
	assertLogContains(t, chunks, "Starting task execution: test-job-task-root-ports task root")
	assertLogContains(t, chunks, "explicit-port-marker")
}

func TestExecutor_ExecuteTask_StopsAtDistributedBoundary(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	testJob := &api.Job{
		Id:    executorStrp("test-job-task-boundary"),
		RunId: executorStrp("test-run-task-boundary"),
		Root: &api.Node{
			Id:   executorStrp("root-node"),
			Uses: executorStrp("builtins/sequence"),
			Steps: []*api.Node{
				{
					Id:   executorStrp("setup"),
					Uses: executorStrp("builtins/shell"),
					With: map[string]string{"command": "echo local-setup-marker"},
				},
				{
					Id:   executorStrp("fanout"),
					Uses: executorStrp("builtins/parallel"),
					Steps: []*api.Node{
						{
							Id:   executorStrp("distributed-child"),
							Uses: executorStrp("builtins/shell"),
							With: map[string]string{"command": "echo distributed-child-marker"},
						},
					},
				},
				{
					Id:   executorStrp("after"),
					Uses: executorStrp("builtins/shell"),
					With: map[string]string{"command": "echo after-boundary-marker"},
				},
			},
		},
	}

	err := executeTaskAndWait(t, executor, testJob, dal.RootTaskKey, mockLogClient, mockLogger)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	chunks := logChunkStrings(mockLogClient.GetChunks())
	assertLogContains(t, chunks, "local-setup-marker")
	assertLogNotContains(t, chunks, "distributed-child-marker")
	assertLogNotContains(t, chunks, "after-boundary-marker")
}

func TestExecutor_ExecuteTask_MissingTask(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	testJob := &api.Job{
		Id:    executorStrp("test-job-task-missing"),
		RunId: executorStrp("test-run-task-missing"),
		Root: &api.Node{
			Id:   executorStrp("root-node"),
			Uses: executorStrp("builtins/sequence"),
		},
	}

	err := executor.ExecuteTask(context.Background(), testJob, "missing", mockLogClient, mockLogger)
	if err == nil || !strings.Contains(err.Error(), `task node "missing" not found`) {
		t.Fatalf("expected missing task error, got %v", err)
	}
}

func TestExecutor_ExecuteJob_NilRoot(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	jobID := "test-job-1"
	testJob := &api.Job{
		Id:   &jobID,
		Root: nil,
	}

	err := executor.ExecuteJob(context.Background(), testJob, mockLogClient, mockLogger)
	if err == nil {
		t.Error("expected error for nil root")
	}

	if !errors.Is(err, errors.New("job has no root node")) {
		if err.Error() != "job has no root node" {
			t.Errorf("expected 'job has no root node' error, got: %v", err)
		}
	}
}

func TestExecutor_ExecuteJob_EmptyID(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	emptyID := ""
	nodeID := "node-1"
	uses := "builtins/shell"
	testJob := &api.Job{
		Id: &emptyID,
		Root: &api.Node{
			Id:   &nodeID,
			Uses: &uses,
			With: map[string]string{
				"command": "echo hello",
			},
		},
	}

	err := executor.ExecuteJob(context.Background(), testJob, mockLogClient, mockLogger)
	if err == nil {
		t.Error("expected error for empty job id")
	}

	if err != nil && err.Error() != "job has no id" {
		t.Errorf("expected 'job has no id' error, got: %v", err)
	}
}

func TestExecutor_ExecuteJob_StreamLogsError(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	expectedErr := errors.New("stream creation failed")
	mockLogClient.SetStreamError(expectedErr)

	jobID := "test-job-1"
	nodeID := "node-1"
	uses := "builtins/shell"
	runID := "test-run-1"
	testJob := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &nodeID,
			Uses: &uses,
			With: map[string]string{
				"command": "echo hello",
			},
		},
	}

	origFlushTimeout := job.LogFlushTimeoutForTest()
	origRetryBase := job.LogRetryBaseForTest()
	origRetryMax := job.LogRetryMaxForTest()
	job.SetLogFlushTimeoutForTest(250 * time.Millisecond)
	job.SetLogRetryBaseForTest(10 * time.Millisecond)
	job.SetLogRetryMaxForTest(25 * time.Millisecond)
	t.Cleanup(func() {
		job.SetLogFlushTimeoutForTest(origFlushTimeout)
		job.SetLogRetryBaseForTest(origRetryBase)
		job.SetLogRetryMaxForTest(origRetryMax)
	})

	err := executor.ExecuteJob(context.Background(), testJob, mockLogClient, mockLogger)
	if err != nil {
		t.Errorf("expected no error when stream stays unavailable; run outcome must not depend on log delivery: %v", err)
	}

	warnCalls := mockLogger.GetWarnCalls()
	foundFlushWarn := false
	for _, msg := range warnCalls {
		if strings.Contains(msg, "flush will continue in background") || strings.Contains(msg, "Log stream flush incomplete") {
			foundFlushWarn = true
			break
		}
	}

	if !foundFlushWarn {
		t.Errorf("expected warning about incomplete log flush, got warns: %v", warnCalls)
	}
}

func TestExecutor_ExecuteJob_StreamUnavailableAtStart_ThenRecovers(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()
	mockLogClient.SetStreamError(errors.New("temporarily unavailable"))

	origFlushTimeout := job.LogFlushTimeoutForTest()
	origRetryBase := job.LogRetryBaseForTest()
	origRetryMax := job.LogRetryMaxForTest()
	job.SetLogFlushTimeoutForTest(2 * time.Second)
	job.SetLogRetryBaseForTest(10 * time.Millisecond)
	job.SetLogRetryMaxForTest(30 * time.Millisecond)
	t.Cleanup(func() {
		job.SetLogFlushTimeoutForTest(origFlushTimeout)
		job.SetLogRetryBaseForTest(origRetryBase)
		job.SetLogRetryMaxForTest(origRetryMax)
	})

	go func() {
		time.Sleep(60 * time.Millisecond)
		mockLogClient.SetStreamError(nil)
	}()

	jobID := "test-job-recover"
	nodeID := "node-1"
	uses := "builtins/shell"
	runID := "test-run-recover"
	testJob := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &nodeID,
			Uses: &uses,
			With: map[string]string{
				"command": "echo hello",
			},
		},
	}

	err := executeAndWait(t, executor, testJob, mockLogClient, mockLogger)
	if err != nil {
		t.Fatalf("expected success after stream recovery, got %v", err)
	}

	if got := len(mockLogClient.GetChunks()); got == 0 {
		t.Fatal("expected queued logs to flush after stream recovery")
	}
}

func TestExecutor_ExecuteJob_UnknownAction(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	jobID := "test-job-1"
	nodeID := "node-1"
	uses := "builtins/unknown-action"
	testJob := &api.Job{
		Id: &jobID,
		Root: &api.Node{
			Id:   &nodeID,
			Uses: &uses,
			With: map[string]string{},
		},
	}

	err := executor.ExecuteJob(context.Background(), testJob, mockLogClient, mockLogger)
	if err == nil {
		t.Error("expected error for unknown action")
	}
}

func TestExecutor_ExecuteJob_CustomProcessAction(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()
	processExecutor := mocks.NewMockExecExecutor()
	process := mocks.NewMockProcess()
	process.SetStdout("custom-staging\n")
	processExecutor.SetProcess(process)

	lockedDescriptor := actionregistry.Descriptor{
		CanonicalName: "acme/deploy",
		Version:       "v1",
		Digest:        "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		Source:        actionregistry.SourceLocalFilesystem,
		Runtime:       actionregistry.RuntimeProcess,
		RuntimeConfig: map[string]string{"command": "./deploy.sh"},
		InputSchema: actionregistry.InputSchema{
			Fields: []actionregistry.InputField{{
				Name:     "environment",
				Type:     action.FieldString,
				Required: true,
			}},
		},
	}

	liveDescriptor := lockedDescriptor
	liveDescriptor.SourcePath = "/opt/vectis/actions/acme/deploy"

	testJob := &api.Job{
		Id:    executorStrp("test-job-custom-process"),
		RunId: executorStrp("test-run-custom-process"),
		Root: &api.Node{
			Id:   executorStrp("root"),
			Uses: executorStrp("acme/deploy@v1"),
			With: map[string]string{"environment": "staging"},
		},
	}

	locks := []actionregistry.ActionLock{{
		NodeID:     "root",
		NodePath:   "root",
		Uses:       "acme/deploy@v1",
		Descriptor: lockedDescriptor,
	}}

	err := executeAndWaitWithOptions(t, executor, testJob, mockLogClient, mockLogger, job.ExecuteOptions{
		ActionLocks:     locks,
		ActionResolver:  executorDescriptorResolver{"acme/deploy@v1": liveDescriptor},
		ProcessExecutor: processExecutor,
	})
	if err != nil {
		t.Fatalf("expected custom process action to execute, got %v", err)
	}

	chunks := logChunkStrings(mockLogClient.GetChunks())
	assertLogContains(t, chunks, "Executing node: acme/deploy")
	assertLogContains(t, chunks, "custom-staging")

	if got := processExecutor.GetPaths(); len(got) != 1 || got[0] != "sh" {
		t.Fatalf("process paths = %v, want [sh]", got)
	}

	if got := processExecutor.GetWorkDirs(); len(got) != 1 || got[0] != "/opt/vectis/actions/acme/deploy" {
		t.Fatalf("process workdirs = %v, want live descriptor source path", got)
	}

	env := strings.Join(processExecutor.GetEnvs()[0], "\n")
	if !strings.Contains(env, "VECTIS_INPUT_ENVIRONMENT=staging") {
		t.Fatalf("custom process env missing input: %s", env)
	}
}

func TestExecutor_ExecuteJob_FrozenCustomActionSurvivesRemovedLiveDescriptor(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()
	processExecutor := mocks.NewMockExecExecutor()
	process := mocks.NewMockProcess()
	process.SetStdout("historical run\n")
	processExecutor.SetProcess(process)

	lockedDescriptor := actionregistry.Descriptor{
		CanonicalName: "acme/deploy",
		Version:       "v1",
		Digest:        "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		Source:        actionregistry.SourceLocalFilesystem,
		Runtime:       actionregistry.RuntimeProcess,
		RuntimeConfig: map[string]string{"command": "echo historical run"},
		InputSchema: actionregistry.InputSchema{
			Fields: []actionregistry.InputField{{
				Name:     "environment",
				Type:     action.FieldString,
				Required: true,
			}},
		},
	}

	testJob := &api.Job{
		Id:    executorStrp("test-job-removed-action"),
		RunId: executorStrp("test-run-removed-action"),
		Root: &api.Node{
			Id:   executorStrp("root"),
			Uses: executorStrp("acme/deploy@v1"),
			With: map[string]string{"environment": "staging"},
		},
	}

	locks := []actionregistry.ActionLock{{
		NodeID:     "root",
		NodePath:   "root",
		Uses:       "acme/deploy@v1",
		Descriptor: lockedDescriptor,
	}}

	err := executeAndWaitWithOptions(t, executor, testJob, mockLogClient, mockLogger, job.ExecuteOptions{
		ActionLocks:     locks,
		ActionResolver:  executorDescriptorResolver{},
		ProcessExecutor: processExecutor,
	})

	if err != nil {
		t.Fatalf("expected frozen descriptor to execute after live removal, got %v", err)
	}

	chunks := logChunkStrings(mockLogClient.GetChunks())
	assertLogContains(t, chunks, "Executing node: acme/deploy")
	assertLogContains(t, chunks, "historical run")
}

func TestExecutor_ExecuteJob_RevokedFrozenCustomActionIsBlocked(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()
	processExecutor := mocks.NewMockExecExecutor()
	processExecutor.SetProcess(mocks.NewMockProcess())

	lockedDescriptor := actionregistry.Descriptor{
		CanonicalName: "acme/deploy",
		Version:       "v1",
		Digest:        "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		Source:        actionregistry.SourceLocalFilesystem,
		Runtime:       actionregistry.RuntimeProcess,
		RuntimeConfig: map[string]string{"command": "echo should-not-run"},
		InputSchema: actionregistry.InputSchema{
			Fields: []actionregistry.InputField{{
				Name:     "environment",
				Type:     action.FieldString,
				Required: true,
			}},
		},
	}

	revokedDescriptor := lockedDescriptor
	revokedDescriptor.Status = actionregistry.DescriptorStatusRevoked
	revokedDescriptor.StatusReason = "CVE-2026-0001"

	testJob := &api.Job{
		Id:    executorStrp("test-job-revoked-action"),
		RunId: executorStrp("test-run-revoked-action"),
		Root: &api.Node{
			Id:   executorStrp("root"),
			Uses: executorStrp("acme/deploy@v1"),
			With: map[string]string{"environment": "staging"},
		},
	}

	locks := []actionregistry.ActionLock{{
		NodeID:     "root",
		NodePath:   "root",
		Uses:       "acme/deploy@v1",
		Descriptor: lockedDescriptor,
	}}

	err := executeAndWaitWithOptions(t, executor, testJob, mockLogClient, mockLogger, job.ExecuteOptions{
		ActionLocks:     locks,
		ActionResolver:  executorDescriptorResolver{"acme/deploy@v1": revokedDescriptor},
		ProcessExecutor: processExecutor,
	})

	if err == nil || !strings.Contains(err.Error(), "revoked: CVE-2026-0001") {
		t.Fatalf("expected revoked action error, got %v", err)
	}

	if got := processExecutor.GetPaths(); len(got) != 0 {
		t.Fatalf("revoked action started process paths = %v, want none", got)
	}
}

func TestExecutor_ExecuteJob_CustomGreetExample(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	var testJob api.Job
	payload, err := os.ReadFile(repoPath(t, "examples", "custom-greet.json"))
	if err != nil {
		t.Fatalf("ReadFile custom-greet example: %v", err)
	}

	if err := json.Unmarshal(payload, &testJob); err != nil {
		t.Fatalf("Unmarshal custom-greet example: %v", err)
	}

	testJob.RunId = executorStrp("test-run-custom-greet-example")
	localSource, err := actionregistry.NewLocalManifestSource(repoPath(t, "examples", "actions"))
	if err != nil {
		t.Fatalf("NewLocalManifestSource: %v", err)
	}

	descriptorResolver := actionregistry.NewCompositeResolver(builtins.NewRegistry(), localSource)
	locks, err := actionregistry.ResolveJobActions(&testJob, descriptorResolver)
	if err != nil {
		t.Fatalf("ResolveJobActions: %v", err)
	}

	if err := executeAndWaitWithOptions(t, executor, &testJob, mockLogClient, mockLogger, job.ExecuteOptions{
		ActionLocks:    locks,
		ActionResolver: descriptorResolver,
	}); err != nil {
		t.Fatalf("ExecuteJobWithOptions: %v", err)
	}

	chunks := logChunkStrings(mockLogClient.GetChunks())
	assertLogContains(t, chunks, "Executing node: examples/greet")
	assertLogContains(t, chunks, "$ echo \"Hello, ${VECTIS_INPUT_NAME}\"")
	assertLogContains(t, chunks, "Hello, Vectis")
	assertLogContains(t, chunks, "Custom action completed successfully")
}

func TestExecutor_ExecuteJob_MissingCommand(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	jobID := "test-job-1"
	nodeID := "node-1"
	uses := "builtins/shell"
	testJob := &api.Job{
		Id: &jobID,
		Root: &api.Node{
			Id:   &nodeID,
			Uses: &uses,
			With: map[string]string{},
		},
	}

	err := executor.ExecuteJob(context.Background(), testJob, mockLogClient, mockLogger)
	if err == nil {
		t.Error("expected error for missing command")
	}
}

func TestExecutor_ExecuteJob_CommandFailure(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	jobID := "test-job-fail"
	runID := "test-job-fail-run"
	nodeID := "node-1"
	uses := "builtins/shell"

	testJob := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &nodeID,
			Uses: &uses,
			With: map[string]string{
				"command": "false", // Always returns exit code 1
			},
		},
	}

	err := executeAndWait(t, executor, testJob, mockLogClient, mockLogger)
	if err == nil {
		t.Error("expected error when command fails")
	}

	if !strings.Contains(err.Error(), "command failed") && !strings.Contains(err.Error(), "exit status 1") {
		t.Errorf("expected error to indicate command failure, got: %v", err)
	}

	chunks := mockLogClient.GetChunks()
	if len(chunks) == 0 {
		t.Fatal("expected log chunks to be sent even on failure")
	}

	for i, chunk := range chunks {
		if chunk.GetRunId() == "" {
			t.Errorf("chunk %d: expected run ID to be set", i)
		}
	}

	chunkStrings := make([]string, len(chunks))
	for i, chunk := range chunks {
		chunkStrings[i] = string(chunk.GetData())
	}

	hasErrorChunk := false
	for _, str := range chunkStrings {
		if strings.Contains(str, "Command failed") || strings.Contains(str, "exit status") {
			hasErrorChunk = true
			break
		}
	}

	if !hasErrorChunk {
		t.Errorf("expected error message in log chunks, got: %v", chunkStrings)
	}

	errorCalls := mockLogger.GetErrorCalls()
	if len(errorCalls) == 0 {
		t.Error("expected error log calls when command fails")
	}

	hasJobFailMsg := false
	for _, msg := range errorCalls {
		if strings.Contains(msg, "Job failed") {
			hasJobFailMsg = true
			break
		}
	}

	if !hasJobFailMsg {
		t.Errorf("expected logger to contain 'Job failed', got: %v", errorCalls)
	}

	infoCalls := mockLogger.GetInfoCalls()
	hasStartMsg := false
	for _, msg := range infoCalls {
		if strings.Contains(msg, "Starting job execution") {
			hasStartMsg = true
			break
		}
	}

	if !hasStartMsg {
		t.Error("expected logger to contain 'Starting job execution' even on failure")
	}
}

func TestExecutor_ExecuteJob_WorkspaceCreationAndCleanup(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	jobID := "test-job-workspace"
	runID := "test-job-workspace-run"
	nodeID := "node-1"
	uses := "builtins/shell"
	testJob := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &nodeID,
			Uses: &uses,
			With: map[string]string{
				"command": "pwd",
			},
		},
	}

	expectedPrefix := filepath.Join(os.TempDir(), "vectis-"+runID+"-")

	err := executor.ExecuteJob(context.Background(), testJob, mockLogClient, mockLogger)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	infoCalls := mockLogger.GetInfoCalls()
	const createdPrefix = "Created workspace: "
	var workspacePath string
	for _, msg := range infoCalls {
		if after, ok := strings.CutPrefix(msg, createdPrefix); ok {
			workspacePath = after
			break
		}
	}

	if workspacePath == "" {
		t.Errorf("expected workspace creation to be logged. Got logs: %v", infoCalls)
	}

	if !strings.HasPrefix(workspacePath, expectedPrefix) {
		t.Errorf("expected workspace path to have prefix %q, got %q", expectedPrefix, workspacePath)
	}

	if _, err := os.Stat(workspacePath); !os.IsNotExist(err) {
		t.Errorf("expected workspace %q to be cleaned up after job completion", workspacePath)
	}
}

func TestExecutor_ExecuteJob_UsesConfiguredWorkspaceRoot(t *testing.T) {
	mockProcessExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("")
	mockProcess.SetStderr("")
	mockProcess.SetWaitError(nil)
	mockProcessExecutor.SetProcess(mockProcess)

	workspaceRoot := t.TempDir()
	executor := job.NewExecutor(
		job.WithProcessExecutor(mockProcessExecutor),
		job.WithWorkspaceRoot(workspaceRoot),
	)

	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	jobID := "test-job-workspace-root"
	runID := "test-job-workspace-root-run"
	nodeID := "node-1"
	uses := "builtins/shell"
	testJob := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &nodeID,
			Uses: &uses,
			With: map[string]string{
				"command": "pwd",
			},
		},
	}

	if err := executor.ExecuteJob(context.Background(), testJob, mockLogClient, mockLogger); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	workDirs := mockProcessExecutor.GetWorkDirs()
	if len(workDirs) != 1 {
		t.Fatalf("expected one command workdir, got %v", workDirs)
	}

	if !strings.HasPrefix(workDirs[0], workspaceRoot+string(os.PathSeparator)) {
		t.Fatalf("expected workspace under %q, got %q", workspaceRoot, workDirs[0])
	}

	if _, err := os.Stat(workDirs[0]); !os.IsNotExist(err) {
		t.Fatalf("expected workspace %q to be cleaned up, stat err=%v", workDirs[0], err)
	}
}

func TestExecutor_ExecuteJob_AutoWorkspaceIsOwnerOnly(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("workspace mode checks use Unix permissions")
	}

	process := mocks.NewMockProcess()
	process.SetStdout("")
	process.SetStderr("")

	modeExecutor := &workspaceModeExecutor{process: process}
	workspaceRoot := t.TempDir()
	executor := job.NewExecutor(
		job.WithProcessExecutor(modeExecutor),
		job.WithWorkspaceRoot(workspaceRoot),
	)

	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	jobID := "test-job-workspace-mode"
	runID := "test-job-workspace-mode-run"
	nodeID := "node-1"
	uses := "builtins/shell"
	testJob := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &nodeID,
			Uses: &uses,
			With: map[string]string{
				"command": "pwd",
			},
		},
	}

	if err := executor.ExecuteJob(context.Background(), testJob, mockLogClient, mockLogger); err != nil {
		t.Fatalf("ExecuteJob: %v", err)
	}

	if modeExecutor.mode != 0o700 {
		t.Fatalf("auto workspace mode = %v, want %v", modeExecutor.mode, os.FileMode(0o700))
	}
}

func TestExecutor_ExecuteJobInWorkspace_WarnsForBroadWorkspacePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("workspace mode checks use Unix permissions")
	}

	mockProcessExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("")
	mockProcess.SetStderr("")
	mockProcessExecutor.SetProcess(mockProcess)

	executor := job.NewExecutor(job.WithProcessExecutor(mockProcessExecutor))
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	workspace := t.TempDir()
	if err := os.Chmod(workspace, 0o755); err != nil {
		t.Fatalf("chmod workspace: %v", err)
	}

	jobID := "test-job-explicit-workspace-mode"
	runID := "test-job-explicit-workspace-mode-run"
	nodeID := "node-1"
	uses := "builtins/shell"
	testJob := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &nodeID,
			Uses: &uses,
			With: map[string]string{
				"command": "pwd",
			},
		},
	}

	if err := executor.ExecuteJobInWorkspace(context.Background(), testJob, mockLogClient, mockLogger, workspace); err != nil {
		t.Fatalf("ExecuteJobInWorkspace: %v", err)
	}

	warns := strings.Join(mockLogger.GetWarnCalls(), "\n")
	if !strings.Contains(warns, "group/world accessible") {
		t.Fatalf("expected broad workspace permission warning, got %v", mockLogger.GetWarnCalls())
	}
}

func TestExecutor_ExecuteJob_WithAsyncWorkspaceCleanupRemovesWorkspaceEventually(t *testing.T) {
	mockProcessExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("")
	mockProcess.SetStderr("")
	mockProcess.SetWaitError(nil)
	mockProcessExecutor.SetProcess(mockProcess)

	workspaceRoot := t.TempDir()
	executor := job.NewExecutor(
		job.WithProcessExecutor(mockProcessExecutor),
		job.WithWorkspaceRoot(workspaceRoot),
		job.WithAsyncWorkspaceCleanup(true),
	)

	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	jobID := "test-job-async-workspace-cleanup"
	runID := "test-job-async-workspace-cleanup-run"
	nodeID := "node-1"
	uses := "builtins/shell"
	testJob := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &nodeID,
			Uses: &uses,
			With: map[string]string{
				"command": "pwd",
			},
		},
	}

	if err := executor.ExecuteJob(context.Background(), testJob, mockLogClient, mockLogger); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	workDirs := mockProcessExecutor.GetWorkDirs()
	if len(workDirs) != 1 {
		t.Fatalf("expected one command workdir, got %v", workDirs)
	}

	if err := job.WaitForAsyncWorkspaceCleanupForTest(2 * time.Second); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(workDirs[0]); !os.IsNotExist(err) {
		t.Fatalf("expected workspace %q to be cleaned up, stat err=%v", workDirs[0], err)
	}
}

func TestExecutor_ExecuteJobInWorkspace_DoesNotRemoveWorkspace(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	workspace := t.TempDir()
	marker := filepath.Join(workspace, "marker.txt")
	if err := os.WriteFile(marker, []byte("keep me"), 0o644); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	jobID := "test-job-explicit-workspace"
	runID := "test-job-explicit-workspace-run"
	nodeID := "node-1"
	uses := "builtins/shell"
	testJob := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &nodeID,
			Uses: &uses,
			With: map[string]string{
				"command": "pwd",
			},
		},
	}

	if err := executor.ExecuteJobInWorkspace(context.Background(), testJob, mockLogClient, mockLogger, workspace); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if _, err := os.Stat(workspace); err != nil {
		t.Fatalf("expected workspace to remain: %v", err)
	}

	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("expected marker to remain: %v", err)
	}
}

type workspaceModeExecutor struct {
	process interfaces.Process
	mode    os.FileMode
}

func (e *workspaceModeExecutor) Start(_ context.Context, _ string, _ []string, workDir string, _ []string) (interfaces.Process, error) {
	info, err := os.Stat(workDir)
	if err != nil {
		return nil, err
	}

	e.mode = info.Mode().Perm()
	if e.process == nil {
		return nil, io.ErrClosedPipe
	}

	return e.process, nil
}

func TestExecutor_ExecuteJobInWorkspace_UsesConfiguredProcessExecutor(t *testing.T) {
	mockProcessExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("")
	mockProcess.SetStderr("")
	mockProcess.SetWaitError(nil)
	mockProcessExecutor.SetProcess(mockProcess)

	executor := job.NewExecutor(job.WithProcessExecutor(mockProcessExecutor))
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	workspace := t.TempDir()
	jobID := "test-job-custom-process-executor"
	runID := "test-job-custom-process-executor-run"
	nodeID := "node-1"
	uses := "builtins/shell"
	testJob := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &nodeID,
			Uses: &uses,
			With: map[string]string{
				"command": "echo custom",
			},
		},
	}

	if err := executor.ExecuteJobInWorkspace(context.Background(), testJob, mockLogClient, mockLogger, workspace); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	paths := mockProcessExecutor.GetPaths()
	args := mockProcessExecutor.GetArgs()
	workDirs := mockProcessExecutor.GetWorkDirs()
	if len(paths) != 1 || paths[0] != "sh" {
		t.Fatalf("expected one shell execution through configured process executor, got paths=%v", paths)
	}
	if len(args) != 1 || len(args[0]) != 2 || args[0][0] != "-c" || args[0][1] != "echo custom" {
		t.Fatalf("expected args [-c echo custom], got %v", args)
	}
	if len(workDirs) != 1 || workDirs[0] != workspace {
		t.Fatalf("expected configured workspace %q, got workDirs=%v", workspace, workDirs)
	}
}

func TestExecutor_ExecuteJobInWorkspace_SelectsIsolationProcessExecutor(t *testing.T) {
	hostProcessExecutor := mocks.NewMockExecExecutor()
	hostProcess := mocks.NewMockProcess()
	hostProcess.SetWaitError(nil)
	hostProcessExecutor.SetProcess(hostProcess)

	vmProcessExecutor := mocks.NewMockExecExecutor()
	vmProcess := mocks.NewMockProcess()
	vmProcess.SetWaitError(nil)
	vmProcessExecutor.SetProcess(vmProcess)

	executor := job.NewExecutor(
		job.WithProcessExecutor(hostProcessExecutor),
		job.WithVMProcessExecutor(vmProcessExecutor),
		job.WithDefaultIsolation(action.IsolationVM),
	)

	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	workspace := t.TempDir()
	jobID := "test-job-action-isolation"
	runID := "test-job-action-isolation-run"
	rootID := "root"
	rootUses := "builtins/sequence"
	vmStepID := "vm-step"
	hostStepID := "host-step"
	inheritStepID := "inherit-step"
	shellUses := "builtins/shell"
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
					With: map[string]string{"command": "echo vm"},
				},
				{
					Id:        &hostStepID,
					Uses:      &shellUses,
					Isolation: &hostIsolation,
					With:      map[string]string{"command": "echo host"},
				},
				{
					Id:   &inheritStepID,
					Uses: &shellUses,
					With: map[string]string{"command": "echo inherit"},
				},
			},
		},
	}

	if err := executor.ExecuteJobInWorkspace(context.Background(), testJob, mockLogClient, mockLogger, workspace); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	hostArgs := hostProcessExecutor.GetArgs()
	if len(hostArgs) != 1 || hostArgs[0][1] != "echo host" {
		t.Fatalf("expected only explicit host action on host executor, got %v", hostArgs)
	}

	vmArgs := vmProcessExecutor.GetArgs()
	if len(vmArgs) != 2 || vmArgs[0][1] != "echo vm" || vmArgs[1][1] != "echo inherit" {
		t.Fatalf("expected inherited VM actions on VM executor, got %v", vmArgs)
	}
}

func TestExecutor_ExecuteJobInWorkspace_JobDefaultIsolationOverridesWorkerDefault(t *testing.T) {
	hostProcessExecutor := mocks.NewMockExecExecutor()
	hostProcess := mocks.NewMockProcess()
	hostProcess.SetWaitError(nil)
	hostProcessExecutor.SetProcess(hostProcess)

	vmProcessExecutor := mocks.NewMockExecExecutor()
	vmProcess := mocks.NewMockProcess()
	vmProcess.SetWaitError(nil)
	vmProcessExecutor.SetProcess(vmProcess)

	executor := job.NewExecutor(
		job.WithProcessExecutor(hostProcessExecutor),
		job.WithVMProcessExecutor(vmProcessExecutor),
	)

	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	workspace := t.TempDir()
	jobID := "test-job-default-isolation"
	runID := "test-job-default-isolation-run"
	rootID := "root"
	rootUses := "builtins/sequence"
	vmStepID := "vm-step"
	hostStepID := "host-step"
	shellUses := "builtins/shell"
	vmIsolation := action.IsolationVM
	hostIsolation := action.IsolationHost
	testJob := &api.Job{
		Id:               &jobID,
		RunId:            &runID,
		DefaultIsolation: &vmIsolation,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &rootUses,
			Steps: []*api.Node{
				{
					Id:   &vmStepID,
					Uses: &shellUses,
					With: map[string]string{"command": "echo vm"},
				},
				{
					Id:        &hostStepID,
					Uses:      &shellUses,
					Isolation: &hostIsolation,
					With:      map[string]string{"command": "echo host"},
				},
			},
		},
	}

	if err := executor.ExecuteJobInWorkspace(context.Background(), testJob, mockLogClient, mockLogger, workspace); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	hostArgs := hostProcessExecutor.GetArgs()
	if len(hostArgs) != 1 || hostArgs[0][1] != "echo host" {
		t.Fatalf("expected explicit host action on host executor, got %v", hostArgs)
	}

	vmArgs := vmProcessExecutor.GetArgs()
	if len(vmArgs) != 1 || vmArgs[0][1] != "echo vm" {
		t.Fatalf("expected inherited job-default VM action on VM executor, got %v", vmArgs)
	}
}

func TestExecutor_ExecuteTaskInWorkspace_UsesJobDefaultIsolation(t *testing.T) {
	hostProcessExecutor := mocks.NewMockExecExecutor()
	hostProcess := mocks.NewMockProcess()
	hostProcess.SetWaitError(nil)
	hostProcessExecutor.SetProcess(hostProcess)

	vmProcessExecutor := mocks.NewMockExecExecutor()
	vmProcess := mocks.NewMockProcess()
	vmProcess.SetWaitError(nil)
	vmProcessExecutor.SetProcess(vmProcess)

	executor := job.NewExecutor(
		job.WithProcessExecutor(hostProcessExecutor),
		job.WithVMProcessExecutor(vmProcessExecutor),
	)

	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	workspace := t.TempDir()
	jobID := "test-task-default-isolation"
	runID := "test-task-default-isolation-run"
	rootID := "root"
	rootUses := "builtins/sequence"
	vmStepID := "vm-step"
	hostStepID := "host-step"
	shellUses := "builtins/shell"
	vmIsolation := action.IsolationVM
	hostIsolation := action.IsolationHost
	testJob := &api.Job{
		Id:               &jobID,
		RunId:            &runID,
		DefaultIsolation: &vmIsolation,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &rootUses,
			Steps: []*api.Node{
				{
					Id:   &vmStepID,
					Uses: &shellUses,
					With: map[string]string{"command": "echo vm task"},
				},
				{
					Id:        &hostStepID,
					Uses:      &shellUses,
					Isolation: &hostIsolation,
					With:      map[string]string{"command": "echo host task"},
				},
			},
		},
	}

	if err := executor.ExecuteTaskInWorkspace(context.Background(), testJob, vmStepID, mockLogClient, mockLogger, workspace); err != nil {
		t.Fatalf("expected VM-default task to run, got %v", err)
	}

	if err := executor.ExecuteTaskInWorkspace(context.Background(), testJob, hostStepID, mockLogClient, mockLogger, workspace); err != nil {
		t.Fatalf("expected host override task to run, got %v", err)
	}

	hostArgs := hostProcessExecutor.GetArgs()
	if len(hostArgs) != 1 || hostArgs[0][1] != "echo host task" {
		t.Fatalf("expected explicit host task on host executor, got %v", hostArgs)
	}

	vmArgs := vmProcessExecutor.GetArgs()
	if len(vmArgs) != 1 || vmArgs[0][1] != "echo vm task" {
		t.Fatalf("expected job-default VM task on VM executor, got %v", vmArgs)
	}
}

func TestExecutor_ExecuteJobInWorkspace_VMIsolationRequiresConfiguredExecutor(t *testing.T) {
	executor := job.NewExecutor()
	mockLogClient := mocks.NewMockLogClient()
	mockLogger := mocks.NewMockLogger()

	workspace := t.TempDir()
	jobID := "test-job-missing-vm-isolation"
	runID := "test-job-missing-vm-isolation-run"
	nodeID := "node-1"
	uses := "builtins/shell"
	isolation := action.IsolationVM
	testJob := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:        &nodeID,
			Uses:      &uses,
			Isolation: &isolation,
			With:      map[string]string{"command": "echo vm"},
		},
	}

	err := executor.ExecuteJobInWorkspace(context.Background(), testJob, mockLogClient, mockLogger, workspace)
	if err == nil || !strings.Contains(err.Error(), "vm isolation requested but no VM process executor is configured") {
		t.Fatalf("expected missing VM executor error, got %v", err)
	}
}

func TestExecutor_ExecuteJobInWorkspace_RequiresWorkspace(t *testing.T) {
	executor := job.NewExecutor()
	err := executor.ExecuteJobInWorkspace(context.Background(), &api.Job{}, mocks.NewMockLogClient(), mocks.NewMockLogger(), "")
	if err == nil || !strings.Contains(err.Error(), "workspace is required") {
		t.Fatalf("expected workspace required error, got %v", err)
	}
}
