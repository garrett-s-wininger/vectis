package builtins

import (
	"context"
	"errors"
	"strings"
	"testing"

	"vectis/internal/action"
	"vectis/internal/action/scriptrunner"
	"vectis/internal/interfaces/mocks"
)

func TestTestActionExecuteTrue(t *testing.T) {
	testAction, process, executor := newMockTestAction(nil)
	state := createTestState(nil)

	result := testAction.Execute(context.Background(), state, map[string]any{"command": "test -f file"}, nil)
	if result.Status != action.StatusSuccess {
		t.Fatalf("status: got %s err=%v, want success", result.Status, result.Error)
	}

	if got, ok := result.Outputs[TestResultOutput].(bool); !ok || !got {
		t.Fatalf("result output: got %+v, want true", result.Outputs)
	}

	if !process.WaitCalled() {
		t.Fatal("expected Wait to be called")
	}

	defaultRunner, err := scriptrunner.Resolve(scriptrunner.Auto, scriptrunner.Auto)
	if err != nil {
		t.Fatalf("Resolve default runner: %v", err)
	}

	if paths := executor.GetPaths(); len(paths) != 1 || paths[0] != defaultRunner.Path {
		t.Fatalf("executor paths = %v, want [%s]", paths, defaultRunner.Path)
	}

	args := executor.GetArgs()
	wantArgs := defaultRunner.InlineArgs("test -f file")
	if len(args) != 1 || strings.Join(args[0], "\x00") != strings.Join(wantArgs, "\x00") {
		t.Fatalf("executor args = %v, want %v", args, wantArgs)
	}
}

func TestTestActionExecuteFalse(t *testing.T) {
	testAction, _, _ := newMockTestAction(errors.New("exit status 1"))
	state := createTestState(nil)

	result := testAction.Execute(context.Background(), state, map[string]any{"command": "test -f file"}, nil)
	if result.Status != action.StatusSuccess {
		t.Fatalf("status: got %s err=%v, want success", result.Status, result.Error)
	}

	if got, ok := result.Outputs[TestResultOutput].(bool); !ok || got {
		t.Fatalf("result output: got %+v, want false", result.Outputs)
	}
}

func TestTestActionExecuteFailure(t *testing.T) {
	testAction, _, _ := newMockTestAction(errors.New("exit status 2"))
	state := createTestState(nil)

	result := testAction.Execute(context.Background(), state, map[string]any{"command": "test -f file"}, nil)
	if result.Status != action.StatusFailure {
		t.Fatalf("status: got %s, want failure", result.Status)
	}

	if result.Error == nil {
		t.Fatal("expected error")
	}
}

func TestTestActionExecuteConfiguredRunner(t *testing.T) {
	testAction, _, executor := newMockTestAction(nil)
	state := createTestState(nil)

	result := testAction.Execute(context.Background(), state, map[string]any{
		"command": "import sys; sys.exit(0)",
		"runner":  "python",
	}, nil)

	if result.Status != action.StatusSuccess {
		t.Fatalf("status: got %s err=%v, want success", result.Status, result.Error)
	}

	if paths := executor.GetPaths(); len(paths) != 1 || paths[0] != "python" {
		t.Fatalf("executor paths = %v, want [python]", paths)
	}

	args := executor.GetArgs()
	wantArgs := []string{"-c", "import sys; sys.exit(0)"}
	if len(args) != 1 || strings.Join(args[0], "\x00") != strings.Join(wantArgs, "\x00") {
		t.Fatalf("executor args = %v, want %v", args, wantArgs)
	}
}

func TestTestActionExecuteUsesStateProcessExecutor(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetWaitError(nil)
	mockExecutor.SetProcess(mockProcess)

	testAction := NewTestAction(nil)
	state := createTestState(nil)
	state.ProcessExecutor = mockExecutor

	result := testAction.Execute(context.Background(), state, map[string]any{
		"command": "exit 0",
		"runner":  "sh",
	}, nil)

	if result.Status != action.StatusSuccess {
		t.Fatalf("status: got %s err=%v, want success", result.Status, result.Error)
	}

	if paths := mockExecutor.GetPaths(); len(paths) != 1 || paths[0] != "sh" {
		t.Fatalf("executor paths = %v, want [sh]", paths)
	}
}

func TestTestActionValidateWith(t *testing.T) {
	testAction := NewTestAction(nil)
	if errs := testAction.ValidateWith(map[string]string{"command": "test -f file", "runner": "pwsh"}); len(errs) != 0 {
		t.Fatalf("expected valid with, got %+v", errs)
	}

	if errs := testAction.ValidateWith(nil); len(errs) == 0 {
		t.Fatal("expected missing command error")
	}

	errs := testAction.ValidateWith(map[string]string{"command": "test -f file", "runner": "fish"})
	if len(errs) == 0 {
		t.Fatal("expected invalid runner error")
	}
}

func TestTestActionType(t *testing.T) {
	if got := NewTestAction(nil).Type(); got != "builtins/test" {
		t.Fatalf("type: got %q, want builtins/test", got)
	}
}

func newMockTestAction(waitErr error) (*TestAction, *mocks.MockProcess, *mocks.MockExecExecutor) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("")
	mockProcess.SetStderr("")
	mockProcess.SetWaitError(waitErr)
	mockExecutor.SetProcess(mockProcess)

	return NewTestAction(mockExecutor), mockProcess, mockExecutor
}
