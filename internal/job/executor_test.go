package job_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
)

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

	err := executor.ExecuteJob(context.Background(), testJob, mockLogClient, mockLogger)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if mockLogClient.GetStreamCount() != 1 {
		t.Errorf("expected 1 log stream, got %d", mockLogClient.GetStreamCount())
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

	err := executor.ExecuteJob(context.Background(), testJob, mockLogClient, mockLogger)
	if err == nil {
		t.Error("expected error when stream creation fails")
	}

	if !errors.Is(err, expectedErr) {
		found := false
		for {
			if err == nil {
				break
			}
			if err == expectedErr {
				found = true
				break
			}
			err = errors.Unwrap(err)
		}
		if !found {
			t.Errorf("expected error to wrap %v, got: %v", expectedErr, err)
		}
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

	err := executor.ExecuteJob(context.Background(), testJob, mockLogClient, mockLogger)
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

	expectedPrefix := filepath.Join(os.TempDir(), "vectis-"+jobID+"-")

	err := executor.ExecuteJob(context.Background(), testJob, mockLogClient, mockLogger)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	infoCalls := mockLogger.GetInfoCalls()
	const createdPrefix = "Created workspace: "
	var workspacePath string
	for _, msg := range infoCalls {
		if strings.HasPrefix(msg, createdPrefix) {
			workspacePath = strings.TrimPrefix(msg, createdPrefix)
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
