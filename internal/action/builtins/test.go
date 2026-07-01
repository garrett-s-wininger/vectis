package builtins

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/scriptrunner"
	"vectis/internal/interfaces"
)

const TestResultOutput = "result"

var exitStatusRe = regexp.MustCompile(`exit status ([0-9]+)`)

type TestAction struct {
	executor interfaces.ExecExecutor
}

func NewTestAction(executor interfaces.ExecExecutor) *TestAction {
	if executor == nil {
		executor = interfaces.NewDirectExecutor()
	}

	return &TestAction{executor: executor}
}

func (t *TestAction) ValidateWith(with map[string]string) []action.FieldError {
	return action.ValidateWithSpec(with, t.InputSchema())
}

func (t *TestAction) InputSchema() []action.FieldSpec {
	return []action.FieldSpec{
		{Name: "command", Type: action.FieldString, Required: true},
	}
}

func (t *TestAction) Type() string {
	return "builtins/test"
}

func (t *TestAction) Execute(ctx context.Context, state *action.ExecutionState, inputs map[string]any, _ action.Ports) action.Result {
	commandStr, ok := inputs["command"].(string)
	if !ok || commandStr == "" {
		return action.NewFailureResult(fmt.Errorf("test action requires 'command' input"))
	}

	state.Logger.Info("Executing test command: %s", commandStr)
	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("$ %s", commandStr))

	runner, err := scriptrunner.Resolve("sh", "sh")
	if err != nil {
		return action.NewFailureResult(err)
	}

	process, err := t.executor.Start(ctx, runner.Path, runner.InlineArgs(commandStr), state.Workspace, state.CommandEnv())
	if err != nil {
		return action.NewFailureResult(fmt.Errorf("failed to start test command: %w", err))
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
	if cmdErr == nil {
		sendLog(state, api.Stream_STREAM_STDOUT, "Test result: true")
		return action.NewSuccessResult(map[string]any{TestResultOutput: true})
	}

	if ctx.Err() != nil {
		return action.NewFailureResult(fmt.Errorf("test command cancelled: %w", cmdErr))
	}

	if exitCode, ok := exitCode(cmdErr); ok && exitCode == 1 {
		sendLog(state, api.Stream_STREAM_STDOUT, "Test result: false")
		return action.NewSuccessResult(map[string]any{TestResultOutput: false})
	}

	state.Logger.Error("Test command failed: %v", cmdErr)
	sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("Test command failed: %v", cmdErr))
	return action.NewFailureResult(fmt.Errorf("test command failed: %w", cmdErr))
}

func exitCode(err error) (int, bool) {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), true
	}

	match := exitStatusRe.FindStringSubmatch(err.Error())
	if len(match) != 2 {
		return 0, false
	}

	code, parseErr := strconv.Atoi(match[1])
	if parseErr != nil {
		return 0, false
	}

	return code, true
}
