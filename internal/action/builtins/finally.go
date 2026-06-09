package builtins

import (
	"context"
	"fmt"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/taskgraph"
)

type FinallyNode struct{}

func (f *FinallyNode) ValidateWith(with map[string]string) []action.FieldError {
	return validateExecutionMode(with)
}

func (f *FinallyNode) Type() string {
	return "builtins/finally"
}

func (f *FinallyNode) LocalOnly() bool {
	return true
}

func (f *FinallyNode) PortSchema() []action.PortSpec {
	return []action.PortSpec{
		{Name: taskgraph.BodyPort, Min: 1, Max: action.PortUnlimited, Required: true, Ordered: true},
		{Name: taskgraph.AlwaysPort, Min: 1, Max: action.PortUnlimited, Required: true, Ordered: true},
	}
}

func (f *FinallyNode) Execute(ctx context.Context, state *action.ExecutionState, _ map[string]any, ports action.Ports) action.Result {
	body := ports.Children(taskgraph.BodyPort)
	always := ports.Children(taskgraph.AlwaysPort)
	if len(body) == 0 {
		return action.NewFailureResult(fmt.Errorf("finally action requires at least one body node"))
	}

	if len(always) == 0 {
		return action.NewFailureResult(fmt.Errorf("finally action requires at least one always node"))
	}

	state.Logger.Info("Executing finally body with %d nodes", len(body))
	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Executing finally body with %d nodes", len(body)))
	bodyResult := executeOrderedChildren(ctx, state, taskgraph.BodyPort, body)

	state.Logger.Info("Executing finally always port with %d nodes", len(always))
	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Executing finally always port with %d nodes", len(always)))
	alwaysResult := executeOrderedChildren(ctx, state, taskgraph.AlwaysPort, always)

	if bodyResult.Status == action.StatusFailure && alwaysResult.Status == action.StatusFailure {
		return action.NewFailureResult(fmt.Errorf("finally body failed: %v; always failed: %w", bodyResult.Error, alwaysResult.Error))
	}

	if bodyResult.Status == action.StatusFailure {
		return bodyResult
	}

	if alwaysResult.Status == action.StatusFailure {
		return alwaysResult
	}

	return bodyResult
}
