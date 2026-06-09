package builtins

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/taskgraph"
)

type IfNode struct{}

func (i *IfNode) ValidateWith(with map[string]string) []action.FieldError {
	return validateExecutionMode(with)
}

func (i *IfNode) Type() string {
	return "builtins/if"
}

func (i *IfNode) LocalOnly() bool {
	return true
}

func (i *IfNode) PortSchema() []action.PortSpec {
	return []action.PortSpec{
		{Name: taskgraph.ConditionPort, Min: 1, Max: 1, Required: true, Ordered: true},
		{Name: taskgraph.ThenPort, Max: action.PortUnlimited, Ordered: true},
		{Name: taskgraph.ElsePort, Max: action.PortUnlimited, Ordered: true},
	}
}

func (i *IfNode) Execute(ctx context.Context, state *action.ExecutionState, _ map[string]any, ports action.Ports) action.Result {
	conditions := ports.Children(taskgraph.ConditionPort)
	if len(conditions) != 1 {
		return action.NewFailureResult(fmt.Errorf("if action requires exactly one condition node"))
	}

	state.Logger.Info("Executing if condition")
	sendLog(state, api.Stream_STREAM_STDOUT, "Executing if condition")
	conditionResult := executeChildNode(ctx, conditions[0], state, taskgraph.ActionInputs(conditions[0].GetWith()))
	if conditionResult.Status == action.StatusFailure {
		state.Logger.Error("If condition failed: %v", conditionResult.Error)
		sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("If condition failed: %v", conditionResult.Error))
		return conditionResult
	}

	matched, err := boolOutput(conditionResult.Outputs, TestResultOutput)
	if err != nil {
		return action.NewFailureResult(fmt.Errorf("if condition output: %w", err))
	}

	branchName := taskgraph.ElsePort
	branch := ports.Children(taskgraph.ElsePort)
	if matched {
		branchName = taskgraph.ThenPort
		branch = ports.Children(taskgraph.ThenPort)
	}

	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("If condition result: %t", matched))
	if len(branch) == 0 {
		state.Logger.Info("If %s branch is empty", branchName)
		sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("If %s branch is empty", branchName))
		return action.NewSuccessResult(nil)
	}

	state.Logger.Info("Executing if %s branch with %d nodes", branchName, len(branch))
	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Executing if %s branch with %d nodes", branchName, len(branch)))
	return executeOrderedChildren(ctx, state, branchName, branch)
}

func executeOrderedChildren(ctx context.Context, state *action.ExecutionState, branchName string, children []*api.Node) action.Result {
	for idx, child := range children {
		sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Executing %s node %d/%d", branchName, idx+1, len(children)))
		result := executeChildNode(ctx, child, state, taskgraph.ActionInputs(child.GetWith()))
		if result.Status == action.StatusFailure {
			state.Logger.Error("If %s branch failed at node %d: %v", branchName, idx+1, result.Error)
			sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("If %s branch node %d failed: %v", branchName, idx+1, result.Error))
			return result
		}
	}

	return action.NewSuccessResult(nil)
}

func boolOutput(outputs map[string]any, name string) (bool, error) {
	if len(outputs) == 0 {
		return false, fmt.Errorf("%q is required", name)
	}

	raw, ok := outputs[name]
	if !ok {
		return false, fmt.Errorf("%q is required", name)
	}

	switch value := raw.(type) {
	case bool:
		return value, nil
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(value))
		if err != nil {
			return false, fmt.Errorf("%q must be a boolean", name)
		}

		return parsed, nil
	default:
		return false, fmt.Errorf("%q must be a boolean", name)
	}
}
