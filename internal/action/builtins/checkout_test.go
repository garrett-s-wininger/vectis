package builtins

import (
	"context"
	"errors"
	"strings"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
)

func TestCheckoutAction_Type(t *testing.T) {
	checkoutAction := NewCheckoutAction(nil)
	if checkoutAction.Type() != "builtins/checkout" {
		t.Errorf("expected 'builtins/checkout', got '%s'", checkoutAction.Type())
	}
}

func TestNewCheckoutAction_DefaultExecutor(t *testing.T) {
	checkoutAction := NewCheckoutAction(nil)
	if checkoutAction.executor == nil {
		t.Error("expected default executor to be set")
	}
}

func TestCheckoutAction_Execute_MissingUrl(t *testing.T) {
	checkoutAction := NewCheckoutAction(nil)
	state := createTestState(nil)

	result := checkoutAction.Execute(context.Background(), state, map[string]any{}, nil)
	if result.Status != action.StatusFailure {
		t.Errorf("expected failure, got %v", result.Status)
	}

	if result.Error == nil {
		t.Error("expected error for missing url")
	}

	if !strings.Contains(result.Error.Error(), "requires 'url' input") {
		t.Errorf("expected 'requires url input' error, got: %v", result.Error)
	}

	result = checkoutAction.Execute(context.Background(), state, map[string]any{
		"url": "",
	}, nil)

	if result.Status != action.StatusFailure {
		t.Errorf("expected failure for empty url, got %v", result.Status)
	}

	result = checkoutAction.Execute(context.Background(), state, map[string]any{
		"url": 123,
	}, nil)

	if result.Status != action.StatusFailure {
		t.Errorf("expected failure for non-string url, got %v", result.Status)
	}
}

func TestCheckoutAction_Execute_Success(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("Cloning into '.'...\n")
	mockProcess.SetStderr("")
	mockProcess.SetWaitError(nil)
	mockExecutor.SetProcess(mockProcess)

	checkoutAction := NewCheckoutAction(mockExecutor)
	mockStream := &mockLogStream{}
	state := createTestState(mockStream)

	url := "https://github.com/example/repo.git"
	inputs := map[string]any{
		"url": url,
	}

	result := checkoutAction.Execute(context.Background(), state, inputs, nil)

	if result.Status != action.StatusSuccess {
		t.Errorf("expected success, got %v with error: %v", result.Status, result.Error)
	}

	paths := mockExecutor.GetPaths()
	args := mockExecutor.GetArgs()
	if len(paths) != 1 || len(args) != 1 {
		t.Errorf("expected 1 Start call, got paths=%d args=%d", len(paths), len(args))
	}
	if paths[0] != "git" {
		t.Errorf("expected path 'git', got '%s'", paths[0])
	}
	if len(args[0]) != 3 || args[0][0] != "clone" || args[0][1] != url || args[0][2] != "." {
		t.Errorf("expected args [clone %s .], got %v", url, args[0])
	}

	if !mockProcess.WaitCalled() {
		t.Error("expected Wait to be called")
	}

	chunks := mockStream.GetChunks()
	foundSuccess := false
	for _, chunk := range chunks {
		if strings.Contains(string(chunk.GetData()), "Checkout completed successfully") {
			foundSuccess = true
			break
		}
	}
	if !foundSuccess {
		t.Error("expected success message to be logged")
	}
}

func TestCheckoutAction_Execute_CloneFailure(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("")
	mockProcess.SetStderr("fatal: repository not found\n")
	mockProcess.SetWaitError(errors.New("exit status 128"))
	mockExecutor.SetProcess(mockProcess)

	checkoutAction := NewCheckoutAction(mockExecutor)
	mockStream := &mockLogStream{}
	state := createTestState(mockStream)

	inputs := map[string]any{
		"url": "https://github.com/nonexistent/repo.git",
	}
	result := checkoutAction.Execute(context.Background(), state, inputs, nil)

	if result.Status != action.StatusFailure {
		t.Errorf("expected failure, got %v", result.Status)
	}

	if result.Error == nil {
		t.Error("expected error")
	}

	if !strings.Contains(result.Error.Error(), "git clone failed") {
		t.Errorf("expected 'git clone failed' in error, got: %v", result.Error)
	}

	chunks := mockStream.GetChunks()
	foundStderr := false
	for _, chunk := range chunks {
		if chunk.GetStream() == api.Stream_STREAM_STDERR {
			foundStderr = true
			break
		}
	}
	if !foundStderr {
		t.Error("expected error to be logged to stderr")
	}
}

func TestCheckoutAction_Execute_StartError(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockExecutor.SetError(errors.New("exec: \"git\": executable file not found"))

	checkoutAction := NewCheckoutAction(mockExecutor)
	state := createTestState(nil)

	inputs := map[string]any{
		"url": "https://github.com/example/repo.git",
	}
	result := checkoutAction.Execute(context.Background(), state, inputs, nil)

	if result.Status != action.StatusFailure {
		t.Errorf("expected failure, got %v", result.Status)
	}

	if result.Error == nil {
		t.Error("expected error")
	}

	if !strings.Contains(result.Error.Error(), "failed to start git clone") {
		t.Errorf("expected 'failed to start git clone' in error, got: %v", result.Error)
	}
}

func TestCheckoutAction_Execute_WorkspacePassed(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("")
	mockProcess.SetStderr("")
	mockProcess.SetWaitError(nil)
	mockExecutor.SetProcess(mockProcess)

	checkoutAction := NewCheckoutAction(mockExecutor)
	mockStream := &mockLogStream{}
	state := &action.ExecutionState{
		JobID:     "test-job",
		Workspace: "/tmp/vectis-checkout-job",
		Logger:    interfaces.NewLogger("test"),
		LogStream: mockStream,
	}

	inputs := map[string]any{
		"url": "https://github.com/example/repo.git",
	}

	result := checkoutAction.Execute(context.Background(), state, inputs, nil)

	if result.Status != action.StatusSuccess {
		t.Errorf("expected success, got %v with error: %v", result.Status, result.Error)
	}

	workDirs := mockExecutor.GetWorkDirs()
	if len(workDirs) != 1 {
		t.Errorf("expected 1 workDir, got %d", len(workDirs))
	}

	if workDirs[0] != "/tmp/vectis-checkout-job" {
		t.Errorf("expected workspace '/tmp/vectis-checkout-job', got '%s'", workDirs[0])
	}
}
