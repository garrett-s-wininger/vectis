package builtins

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/scriptrunner"
	"vectis/internal/interfaces"
)

const ScriptBodyField = "script"
const ScriptRunnerField = "runner"

type ScriptAction struct {
	executor interfaces.ExecExecutor
}

func NewScriptAction(executor interfaces.ExecExecutor) *ScriptAction {
	return &ScriptAction{executor: executor}
}

func (s *ScriptAction) ValidateWith(with map[string]string) []action.FieldError {
	errs := action.ValidateWithSpec(with, s.InputSchema())

	if runner := strings.TrimSpace(with[ScriptRunnerField]); runner != "" {
		if err := scriptrunner.Validate(runner); err != nil {
			errs = append(errs, action.FieldError{Field: ScriptRunnerField, Message: err.Error()})
		}
	}

	if _, ok := with[OutputsField]; ok && strings.TrimSpace(with[OutputsField]) == "" {
		errs = append(errs, action.FieldError{Field: OutputsField, Message: "must not be empty"})
	}

	return errs
}

func (s *ScriptAction) InputSchema() []action.FieldSpec {
	return []action.FieldSpec{
		{Name: ScriptBodyField, Type: action.FieldString, Required: true},
		{Name: ScriptRunnerField, Type: action.FieldString},
		{Name: OutputsField, Type: action.FieldString},
	}
}

func (s *ScriptAction) Type() string {
	return "builtins/script"
}

func (s *ScriptAction) Execute(ctx context.Context, state *action.ExecutionState, inputs map[string]any, _ action.Ports) action.Result {
	body, ok := inputs[ScriptBodyField].(string)
	if !ok || strings.TrimSpace(body) == "" {
		return action.NewFailureResult(fmt.Errorf("script action requires 'script' input"))
	}

	runnerName := scriptrunner.Auto
	if raw, ok := inputs[ScriptRunnerField].(string); ok && strings.TrimSpace(raw) != "" {
		runnerName = raw
	}

	runner, err := scriptrunner.Resolve(runnerName, scriptrunner.Auto)
	if err != nil {
		return action.NewFailureResult(err)
	}

	scriptPath, err := scriptrunner.WriteWorkspaceScriptFile(state.Workspace, runner, body)
	if err != nil {
		return action.NewFailureResult(fmt.Errorf("prepare script: %w", err))
	}
	defer func() { _ = os.Remove(scriptPath) }()

	state.Logger.Info("Executing script with runner %s: %s", runner.Name, scriptPath)
	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("$ %s %s", runner.Name, scriptPath))

	process, err := processExecutor(s.executor, state).Start(ctx, runner.Path, runner.FileArgs(scriptPath), state.Workspace, state.CommandEnv())
	if err != nil {
		return action.NewFailureResult(fmt.Errorf("failed to start script: %w", err))
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		streamOutput(process.Stdout(), state, api.Stream_STREAM_STDOUT)
	}()

	go func() {
		defer wg.Done()
		streamOutput(process.Stderr(), state, api.Stream_STREAM_STDERR)
	}()

	wg.Wait()
	cmdErr := process.Wait()
	if cmdErr != nil {
		if ctx.Err() != nil {
			return action.NewFailureResult(fmt.Errorf("script cancelled: %w", cmdErr))
		}

		state.Logger.Error("Script failed: %v", cmdErr)
		sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("Script failed: %v", cmdErr))
		return action.NewFailureResult(fmt.Errorf("script failed: %w", cmdErr))
	}

	state.Logger.Info("Script completed successfully")
	sendLog(state, api.Stream_STREAM_STDOUT, "Script completed successfully")

	outputs, err := readOutputsFile("script", state.Workspace, inputs)
	if err != nil {
		state.Logger.Error("Failed to read script outputs: %v", err)
		sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("Failed to read script outputs: %v", err))
		return action.NewFailureResult(err)
	}

	return action.NewSuccessResult(outputs)
}
