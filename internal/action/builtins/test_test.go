package builtins

import (
	"context"
	"errors"
	"testing"

	"vectis/internal/action"
	"vectis/internal/interfaces/mocks"
)

func TestTestActionExecuteTrue(t *testing.T) {
	testAction, process := newMockTestAction(nil)
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
}

func TestTestActionExecuteFalse(t *testing.T) {
	testAction, _ := newMockTestAction(errors.New("exit status 1"))
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
	testAction, _ := newMockTestAction(errors.New("exit status 2"))
	state := createTestState(nil)

	result := testAction.Execute(context.Background(), state, map[string]any{"command": "test -f file"}, nil)
	if result.Status != action.StatusFailure {
		t.Fatalf("status: got %s, want failure", result.Status)
	}

	if result.Error == nil {
		t.Fatal("expected error")
	}
}

func TestTestActionValidateWith(t *testing.T) {
	testAction := NewTestAction(nil)
	if errs := testAction.ValidateWith(map[string]string{"command": "test -f file"}); len(errs) != 0 {
		t.Fatalf("expected valid with, got %+v", errs)
	}

	if errs := testAction.ValidateWith(nil); len(errs) == 0 {
		t.Fatal("expected missing command error")
	}
}

func TestTestActionType(t *testing.T) {
	if got := NewTestAction(nil).Type(); got != "builtins/test" {
		t.Fatalf("type: got %q, want builtins/test", got)
	}
}

func newMockTestAction(waitErr error) (*TestAction, *mocks.MockProcess) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("")
	mockProcess.SetStderr("")
	mockProcess.SetWaitError(waitErr)
	mockExecutor.SetProcess(mockProcess)

	return NewTestAction(mockExecutor), mockProcess
}
