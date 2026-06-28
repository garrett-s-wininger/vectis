package builtins

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"

	"google.golang.org/grpc/metadata"
)

type mockLogStream struct {
	mu      sync.Mutex
	chunks  []*api.LogChunk
	sendErr error
}

func (m *mockLogStream) Send(chunk *api.LogChunk) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.chunks = append(m.chunks, chunk)
	return nil
}

func (m *mockLogStream) CloseAndRecv() (*api.Empty, error) {
	return &api.Empty{}, nil
}

func (m *mockLogStream) GetChunks() []*api.LogChunk {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*api.LogChunk, len(m.chunks))
	copy(result, m.chunks)
	return result
}

func (m *mockLogStream) Context() context.Context {
	return context.Background()
}

func (m *mockLogStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (m *mockLogStream) Trailer() metadata.MD {
	return nil
}

func (m *mockLogStream) CloseSend() error {
	return nil
}

func (m *mockLogStream) RecvMsg(msg any) error {
	return errors.New("not implemented")
}

func (m *mockLogStream) SendMsg(msg any) error {
	return errors.New("not implemented")
}

var _ api.LogService_StreamLogsClient = (*mockLogStream)(nil)

func createTestState(logStream api.LogService_StreamLogsClient) *action.ExecutionState {
	return &action.ExecutionState{
		JobID:      "test-job",
		Workspace:  "/tmp/vectis-test-workspace",
		ProcessEnv: action.SanitizedProcessEnv("/tmp/vectis-test-workspace", []string{"PATH=/usr/bin", "VECTIS_DATABASE_DSN=secret"}),
		Logger:     interfaces.NewLogger("test"),
		LogStream:  logStream,
	}
}

func TestShellAction_Execute_Success(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("hello world\n")
	mockProcess.SetStderr("")
	mockProcess.SetWaitError(nil)
	mockExecutor.SetProcess(mockProcess)

	shellAction := NewShellAction(mockExecutor)
	mockStream := &mockLogStream{}
	state := createTestState(mockStream)

	inputs := map[string]any{
		"command": "echo hello",
	}

	result := shellAction.Execute(context.Background(), state, inputs, nil)

	if result.Status != action.StatusSuccess {
		t.Errorf("expected success, got %v with error: %v", result.Status, result.Error)
	}

	paths := mockExecutor.GetPaths()
	args := mockExecutor.GetArgs()
	if len(paths) != 1 || len(args) != 1 {
		t.Errorf("expected 1 Start call, got paths=%d args=%d", len(paths), len(args))
	}

	if paths[0] != "sh" {
		t.Errorf("expected path 'sh', got '%s'", paths[0])
	}

	if len(args[0]) != 2 || args[0][0] != "-c" || args[0][1] != "echo hello" {
		t.Errorf("expected args [-c echo hello], got %v", args[0])
	}

	envs := mockExecutor.GetEnvs()
	if len(envs) != 1 {
		t.Fatalf("expected 1 env, got %d", len(envs))
	}

	if _, ok := testEnvLookup(envs[0], "VECTIS_DATABASE_DSN"); ok {
		t.Fatalf("shell action leaked worker database DSN env: %v", envs[0])
	}

	if got, ok := testEnvLookup(envs[0], "PATH"); !ok || got != "/usr/bin" {
		t.Fatalf("PATH env = %q, %v; want /usr/bin", got, ok)
	}

	if !mockProcess.WaitCalled() {
		t.Error("expected Wait to be called")
	}

	chunks := mockStream.GetChunks()
	if len(chunks) == 0 {
		t.Error("expected log chunks to be sent")
	}

	foundCommand := false
	for _, chunk := range chunks {
		if strings.Contains(string(chunk.GetData()), "$ echo hello") {
			foundCommand = true
			break
		}
	}

	if !foundCommand {
		t.Error("expected command to be logged")
	}

	foundOutput := false
	for _, chunk := range chunks {
		if strings.Contains(string(chunk.GetData()), "hello world") {
			foundOutput = true
			break
		}
	}

	if !foundOutput {
		t.Error("expected stdout content to be logged")
	}

	foundSuccess := false
	for _, chunk := range chunks {
		if strings.Contains(string(chunk.GetData()), "Command completed successfully") {
			foundSuccess = true
			break
		}
	}

	if !foundSuccess {
		t.Error("expected success message to be logged")
	}
}

func TestShellAction_Execute_CommandFailure(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("")
	mockProcess.SetStderr("error message\n")
	mockProcess.SetWaitError(errors.New("exit status 1"))
	mockExecutor.SetProcess(mockProcess)

	shellAction := NewShellAction(mockExecutor)
	mockStream := &mockLogStream{}
	state := createTestState(mockStream)

	inputs := map[string]any{
		"command": "false",
	}
	result := shellAction.Execute(context.Background(), state, inputs, nil)

	if result.Status != action.StatusFailure {
		t.Errorf("expected failure, got %v", result.Status)
	}

	if result.Error == nil {
		t.Error("expected error, got nil")
	}

	if !strings.Contains(result.Error.Error(), "command failed") {
		t.Errorf("expected 'command failed' in error, got: %v", result.Error)
	}

	chunks := mockStream.GetChunks()
	foundError := false
	for _, chunk := range chunks {
		if chunk.GetStream() == api.Stream_STREAM_STDERR {
			foundError = true
			break
		}
	}

	if !foundError {
		t.Error("expected error to be logged to stderr")
	}
}

func TestShellAction_Execute_MissingCommand(t *testing.T) {
	shellAction := NewShellAction(nil)
	state := createTestState(nil)

	result := shellAction.Execute(context.Background(), state, map[string]any{}, nil)
	if result.Status != action.StatusFailure {
		t.Errorf("expected failure, got %v", result.Status)
	}

	if result.Error == nil {
		t.Error("expected error for missing command")
	}

	if !strings.Contains(result.Error.Error(), "requires 'command' input") {
		t.Errorf("expected 'requires command input' error, got: %v", result.Error)
	}

	result = shellAction.Execute(context.Background(), state, map[string]any{
		"command": "",
	}, nil)

	if result.Status != action.StatusFailure {
		t.Errorf("expected failure, got %v", result.Status)
	}

	result = shellAction.Execute(context.Background(), state, map[string]any{
		"command": 123,
	}, nil)

	if result.Status != action.StatusFailure {
		t.Errorf("expected failure, got %v", result.Status)
	}
}

func TestShellAction_Execute_StartError(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockExecutor.SetError(errors.New("failed to start: permission denied"))

	shellAction := NewShellAction(mockExecutor)
	state := createTestState(nil)

	inputs := map[string]any{
		"command": "/bin/false",
	}
	result := shellAction.Execute(context.Background(), state, inputs, nil)

	if result.Status != action.StatusFailure {
		t.Errorf("expected failure, got %v", result.Status)
	}

	if result.Error == nil {
		t.Error("expected error")
	}

	if !strings.Contains(result.Error.Error(), "failed to start") {
		t.Errorf("expected 'failed to start' error, got: %v", result.Error)
	}
}

func TestShellAction_Execute_ReadsOutputsFile(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("")
	mockProcess.SetStderr("")
	mockProcess.SetWaitError(nil)
	mockExecutor.SetProcess(mockProcess)

	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "outputs.json"), []byte(`{"image":"vectis","attempts":2,"ok":true}`), 0o600); err != nil {
		t.Fatalf("write outputs: %v", err)
	}

	shellAction := NewShellAction(mockExecutor)
	state := createTestState(&mockLogStream{})
	state.Workspace = workspace

	result := shellAction.Execute(context.Background(), state, map[string]any{
		"command": "mage build",
		"outputs": "outputs.json",
	}, nil)

	if result.Status != action.StatusSuccess {
		t.Fatalf("expected success, got %v with error: %v", result.Status, result.Error)
	}

	if got := result.Outputs["image"]; got != "vectis" {
		t.Fatalf("image output: got %v, want vectis", got)
	}

	if got := result.Outputs["attempts"]; got != float64(2) {
		t.Fatalf("attempts output: got %v, want 2", got)
	}

	if got := result.Outputs["ok"]; got != true {
		t.Fatalf("ok output: got %v, want true", got)
	}
}

func TestShellAction_Execute_RejectsOutputsOutsideWorkspace(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("")
	mockProcess.SetStderr("")
	mockProcess.SetWaitError(nil)
	mockExecutor.SetProcess(mockProcess)

	shellAction := NewShellAction(mockExecutor)
	state := createTestState(&mockLogStream{})
	state.Workspace = t.TempDir()

	result := shellAction.Execute(context.Background(), state, map[string]any{
		"command": "mage build",
		"outputs": "../outputs.json",
	}, nil)

	if result.Status != action.StatusFailure {
		t.Fatalf("expected failure, got %v", result.Status)
	}

	if result.Error == nil || !strings.Contains(result.Error.Error(), "must stay inside the workspace") {
		t.Fatalf("expected workspace path error, got %v", result.Error)
	}
}

func TestShellAction_Execute_StdoutStderrStreaming(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("stdout line 1\nstdout line 2\n")
	mockProcess.SetStderr("stderr line 1\n")
	mockProcess.SetWaitError(nil)
	mockExecutor.SetProcess(mockProcess)

	shellAction := NewShellAction(mockExecutor)
	mockStream := &mockLogStream{}
	state := createTestState(mockStream)

	inputs := map[string]any{
		"command": "echo test",
	}

	result := shellAction.Execute(context.Background(), state, inputs, nil)

	if result.Status != action.StatusSuccess {
		t.Errorf("expected success, got %v with error: %v", result.Status, result.Error)
	}

	chunks := mockStream.GetChunks()
	stdoutCount := 0
	stderrCount := 0
	for _, chunk := range chunks {
		switch chunk.GetStream() {
		case api.Stream_STREAM_STDOUT:
			stdoutCount++
		case api.Stream_STREAM_STDERR:
			stderrCount++
		}
	}

	if stdoutCount == 0 {
		t.Error("expected stdout chunks")
	}

	if stderrCount == 0 {
		t.Error("expected stderr chunks")
	}
}

func TestShellAction_Type(t *testing.T) {
	shellAction := NewShellAction(nil)
	if shellAction.Type() != "builtins/shell" {
		t.Errorf("expected 'builtins/shell', got '%s'", shellAction.Type())
	}
}

func TestShellAction_Execute_UsesStateProcessExecutor(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("")
	mockProcess.SetStderr("")
	mockProcess.SetWaitError(nil)
	mockExecutor.SetProcess(mockProcess)

	shellAction := NewShellAction(nil)
	mockStream := &mockLogStream{}
	state := createTestState(mockStream)
	state.Workspace = "/tmp/vectis-state-executor"
	state.ProcessExecutor = mockExecutor

	inputs := map[string]any{
		"command": "echo state",
	}

	result := shellAction.Execute(context.Background(), state, inputs, nil)
	if result.Status != action.StatusSuccess {
		t.Errorf("expected success, got %v with error: %v", result.Status, result.Error)
	}

	paths := mockExecutor.GetPaths()
	args := mockExecutor.GetArgs()
	workDirs := mockExecutor.GetWorkDirs()
	if len(paths) != 1 || paths[0] != "sh" {
		t.Fatalf("expected one sh execution, got paths=%v", paths)
	}

	if len(args) != 1 || len(args[0]) != 2 || args[0][0] != "-c" || args[0][1] != "echo state" {
		t.Fatalf("expected shell args [-c echo state], got %v", args)
	}

	if len(workDirs) != 1 || workDirs[0] != "/tmp/vectis-state-executor" {
		t.Fatalf("expected workspace from state, got workDirs=%v", workDirs)
	}
}

func TestShellAction_Execute_WorkspacePassed(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("")
	mockProcess.SetStderr("")
	mockProcess.SetWaitError(nil)
	mockExecutor.SetProcess(mockProcess)

	shellAction := NewShellAction(mockExecutor)
	mockStream := &mockLogStream{}
	state := &action.ExecutionState{
		JobID:     "test-job",
		Workspace: "/tmp/vectis-test-job",
		Logger:    interfaces.NewLogger("test"),
		LogStream: mockStream,
	}

	inputs := map[string]any{
		"command": "pwd",
	}

	result := shellAction.Execute(context.Background(), state, inputs, nil)

	if result.Status != action.StatusSuccess {
		t.Errorf("expected success, got %v with error: %v", result.Status, result.Error)
	}

	workDirs := mockExecutor.GetWorkDirs()
	if len(workDirs) != 1 {
		t.Errorf("expected 1 workDir, got %d", len(workDirs))
	}

	if workDirs[0] != "/tmp/vectis-test-job" {
		t.Errorf("expected workspace '/tmp/vectis-test-job', got '%s'", workDirs[0])
	}
}

func testEnvLookup(env []string, key string) (string, bool) {
	for _, entry := range env {
		k, v, ok := strings.Cut(entry, "=")
		if ok && k == key {
			return v, true
		}
	}

	return "", false
}
