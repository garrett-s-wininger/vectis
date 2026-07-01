package builtins

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"vectis/internal/action"
	"vectis/internal/action/scriptrunner"
	"vectis/internal/interfaces/mocks"
)

func TestScriptActionExecuteSuccess(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("hello from script\n")
	mockProcess.SetStderr("")
	mockProcess.SetWaitError(nil)
	mockExecutor.SetProcess(mockProcess)

	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "outputs.json"), []byte(`{"ok":true,"runner":"script"}`), 0o600); err != nil {
		t.Fatalf("write outputs: %v", err)
	}

	scriptAction := NewScriptAction(mockExecutor)
	state := createTestState(&mockLogStream{})
	state.Workspace = workspace
	state.ProcessEnv = action.SanitizedProcessEnv(workspace, []string{"PATH=/usr/bin"})

	result := scriptAction.Execute(context.Background(), state, map[string]any{
		"script":  "echo hello",
		"outputs": "outputs.json",
	}, nil)

	if result.Status != action.StatusSuccess {
		t.Fatalf("expected success, got %s err=%v", result.Status, result.Error)
	}

	if got := result.Outputs["ok"]; got != true {
		t.Fatalf("ok output = %v, want true", got)
	}

	if got := result.Outputs["runner"]; got != "script" {
		t.Fatalf("runner output = %v, want script", got)
	}

	defaultRunner, err := scriptrunner.Resolve(scriptrunner.Auto, scriptrunner.Auto)
	if err != nil {
		t.Fatalf("Resolve default runner: %v", err)
	}

	paths := mockExecutor.GetPaths()
	args := mockExecutor.GetArgs()
	workDirs := mockExecutor.GetWorkDirs()
	if len(paths) != 1 || paths[0] != defaultRunner.Path {
		t.Fatalf("executor paths = %v, want [%s]", paths, defaultRunner.Path)
	}

	if len(args) != 1 {
		t.Fatalf("executor args = %v, want one call", args)
	}

	scriptPath := scriptPathArg(defaultRunner, args[0])
	realWorkspace, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		t.Fatalf("resolve workspace: %v", err)
	}

	rel, err := filepath.Rel(realWorkspace, scriptPath)
	if err != nil {
		t.Fatalf("relative script path: %v", err)
	}

	if strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		t.Fatalf("script path %q escaped workspace %q", scriptPath, realWorkspace)
	}

	if filepath.Dir(rel) != filepath.Join(".vectis", "scripts") {
		t.Fatalf("script dir = %q, want .vectis/scripts", filepath.Dir(rel))
	}

	if filepath.Ext(scriptPath) != defaultRunner.Extension {
		t.Fatalf("script extension = %q, want %q", filepath.Ext(scriptPath), defaultRunner.Extension)
	}

	if _, err := os.Stat(scriptPath); !os.IsNotExist(err) {
		t.Fatalf("script file should be removed after execution, stat err=%v", err)
	}

	if len(workDirs) != 1 || workDirs[0] != workspace {
		t.Fatalf("workdirs = %v, want [%s]", workDirs, workspace)
	}

	if !mockProcess.WaitCalled() {
		t.Fatal("expected Wait to be called")
	}
}

func TestScriptActionExecuteConfiguredRunner(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetWaitError(nil)
	mockExecutor.SetProcess(mockProcess)

	workspace := t.TempDir()
	scriptAction := NewScriptAction(mockExecutor)
	state := createTestState(nil)
	state.Workspace = workspace

	result := scriptAction.Execute(context.Background(), state, map[string]any{
		"script": "print('hello')",
		"runner": "python",
	}, nil)

	if result.Status != action.StatusSuccess {
		t.Fatalf("expected success, got %s err=%v", result.Status, result.Error)
	}

	paths := mockExecutor.GetPaths()
	if len(paths) != 1 || paths[0] != "python" {
		t.Fatalf("executor paths = %v, want [python]", paths)
	}

	args := mockExecutor.GetArgs()
	if len(args) != 1 || len(args[0]) != 1 || filepath.Ext(args[0][0]) != ".py" {
		t.Fatalf("executor args = %v, want one .py file arg", args)
	}
}

func TestScriptActionExecuteRejectsInvalidInputs(t *testing.T) {
	scriptAction := NewScriptAction(nil)
	state := createTestState(nil)
	state.Workspace = t.TempDir()

	for _, inputs := range []map[string]any{
		{},
		{"script": ""},
		{"script": 123},
		{"script": "echo hi", "runner": "fish"},
	} {
		result := scriptAction.Execute(context.Background(), state, inputs, nil)
		if result.Status != action.StatusFailure {
			t.Fatalf("Execute(%v) status = %s, want failure", inputs, result.Status)
		}
	}
}

func TestScriptActionValidateWith(t *testing.T) {
	scriptAction := NewScriptAction(nil)
	if errs := scriptAction.ValidateWith(map[string]string{
		"script":  "echo hello",
		"runner":  "pwsh",
		"outputs": "outputs.json",
	}); len(errs) != 0 {
		t.Fatalf("expected valid with, got %+v", errs)
	}

	errs := scriptAction.ValidateWith(map[string]string{"runner": "fish", "outputs": ""})
	got := make(map[string]string, len(errs))
	for _, err := range errs {
		got[err.Field] = err.Message
	}

	for _, field := range []string{ScriptBodyField, ScriptRunnerField, OutputsField} {
		if got[field] == "" {
			t.Fatalf("expected validation error for %s, got %+v", field, errs)
		}
	}
}

func TestScriptActionExecuteRejectsOutputsOutsideWorkspace(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetWaitError(nil)
	mockExecutor.SetProcess(mockProcess)

	scriptAction := NewScriptAction(mockExecutor)
	state := createTestState(&mockLogStream{})
	state.Workspace = t.TempDir()

	result := scriptAction.Execute(context.Background(), state, map[string]any{
		"script":  "echo hi",
		"outputs": "../outputs.json",
	}, nil)

	if result.Status != action.StatusFailure {
		t.Fatalf("expected failure, got %v", result.Status)
	}

	if result.Error == nil || !strings.Contains(result.Error.Error(), "must stay inside the workspace") {
		t.Fatalf("expected workspace path error, got %v", result.Error)
	}
}

func TestScriptActionExecuteUsesStateProcessExecutor(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetWaitError(nil)
	mockExecutor.SetProcess(mockProcess)

	scriptAction := NewScriptAction(nil)
	state := createTestState(&mockLogStream{})
	state.Workspace = t.TempDir()
	state.ProcessExecutor = mockExecutor

	result := scriptAction.Execute(context.Background(), state, map[string]any{
		"script": "echo state",
		"runner": "sh",
	}, nil)

	if result.Status != action.StatusSuccess {
		t.Fatalf("expected success, got %v with error: %v", result.Status, result.Error)
	}

	paths := mockExecutor.GetPaths()
	workDirs := mockExecutor.GetWorkDirs()
	if len(paths) != 1 || paths[0] != "sh" {
		t.Fatalf("expected one sh execution, got paths=%v", paths)
	}

	if len(workDirs) != 1 || workDirs[0] != state.Workspace {
		t.Fatalf("expected workspace from state, got workDirs=%v", workDirs)
	}
}

func TestScriptActionType(t *testing.T) {
	if got := NewScriptAction(nil).Type(); got != "builtins/script" {
		t.Fatalf("type = %q, want builtins/script", got)
	}
}

func scriptPathArg(runner scriptrunner.Runner, args []string) string {
	for i := len(args) - 1; i >= 0; i-- {
		arg := strings.Trim(args[i], `"`)
		if filepath.Ext(arg) == runner.Extension {
			return arg
		}
	}

	return ""
}
