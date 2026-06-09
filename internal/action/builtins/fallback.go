package builtins

import (
	"context"
	"fmt"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/taskgraph"
)

type FallbackNode struct{}

func (f *FallbackNode) ValidateWith(with map[string]string) []action.FieldError {
	return validateExecutionMode(with)
}

func (f *FallbackNode) Type() string {
	return "builtins/fallback"
}

func (f *FallbackNode) LocalOnly() bool {
	return true
}

func (f *FallbackNode) PortSchema() []action.PortSpec {
	return []action.PortSpec{{
		Name:     taskgraph.ChoicesPort,
		Min:      1,
		Max:      action.PortUnlimited,
		Required: true,
		Ordered:  true,
	}}
}

func (f *FallbackNode) Execute(ctx context.Context, state *action.ExecutionState, _ map[string]any, ports action.Ports) action.Result {
	choices := ports.Children(taskgraph.ChoicesPort)
	if len(choices) == 0 {
		return action.NewFailureResult(fmt.Errorf("fallback action requires at least one choice node"))
	}

	var last action.Result
	for idx, child := range choices {
		state.Logger.Info("Executing fallback choice %d/%d", idx+1, len(choices))
		sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Executing fallback choice %d/%d", idx+1, len(choices)))

		last = executeChildNode(ctx, child, state, taskgraph.ActionInputs(child.GetWith()))
		if last.Status == action.StatusSuccess {
			if idx > 0 {
				sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Fallback choice %d/%d succeeded", idx+1, len(choices)))
			}

			return last
		}

		if ctx.Err() != nil {
			return last
		}

		state.Logger.Warn("Fallback choice %d/%d failed: %v", idx+1, len(choices), last.Error)
		sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("Fallback choice %d/%d failed: %v", idx+1, len(choices), last.Error))
	}

	state.Logger.Error("Fallback failed after %d choice(s): %v", len(choices), last.Error)
	sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("Fallback failed after %d choice(s): %v", len(choices), last.Error))
	if last.Error == nil {
		return action.NewFailureResult(fmt.Errorf("fallback failed after %d choice(s)", len(choices)))
	}

	return action.NewFailureResult(fmt.Errorf("fallback failed after %d choice(s): %w", len(choices), last.Error))
}
