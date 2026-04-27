package job_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
)

func executeAndWait(t *testing.T, executor *job.Executor, testJob *api.Job, mockLogClient *mocks.MockLogClient, mockLogger *mocks.MockLogger) error {
	t.Helper()
	streamCh := make(chan job.LogStreamWaiter, 1)
	executor.TestLogStreamHook = streamCh
	defer func() { executor.TestLogStreamHook = nil }()

	err := executor.ExecuteJob(context.Background(), testJob, mockLogClient, mockLogger)

	select {
	case stream := <-streamCh:
		if waitErr := stream.WaitForDone(5 * time.Second); waitErr != nil {
			t.Logf("wait for log stream done: %v", waitErr)
		}
	case <-time.After(5 * time.Second):
		t.Logf("timed out waiting for log stream hook")
	}

	return err
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
