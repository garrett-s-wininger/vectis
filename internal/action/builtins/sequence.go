package builtins

import (
	"context"
	"fmt"

	api "vectis/api/gen/go"
	"vectis/internal/action"
)

type SequenceNode struct{}

func (s *SequenceNode) Type() string {
	return "builtins/sequence"
}

func (s *SequenceNode) Execute(ctx context.Context, state *action.ExecutionState, _ map[string]any, children []*api.Node) action.Result {
	if len(children) == 0 {
		return action.NewSuccessResult(nil)
	}

	state.Logger.Info("Executing sequence with %d children", len(children))
	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Executing sequence with %d children", len(children)))

	for i, child := range children {
		sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Executing step %d/%d", i+1, len(children)))

		childInputs := make(map[string]any)
		for k, v := range child.GetWith() {
			childInputs[k] = v
		}

		result := executeChildNode(ctx, child, state, childInputs)
		if result.Status == action.StatusFailure {
			state.Logger.Error("Sequence failed at child %d: %v", i+1, result.Error)
			sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("Step %d failed: %v", i+1, result.Error))
			return result
		}
	}

	state.Logger.Info("Sequence completed successfully")
	sendLog(state, api.Stream_STREAM_STDOUT, "Sequence completed successfully")
	return action.NewSuccessResult(nil)
}

func executeChildNode(ctx context.Context, node *api.Node, state *action.ExecutionState, inputs map[string]any) action.Result {
	act, err := resolveAction(node.GetUses())
	if err != nil {
		return action.NewFailureResult(
			&action.ExecutionError{
				NodeID:  node.GetId(),
				Action:  node.GetUses(),
				Message: "failed to resolve action",
				Cause:   err,
			},
		)
	}

	return act.Execute(ctx, state, inputs, node.GetSteps())
}

func resolveAction(uses string) (action.Node, error) {
	switch uses {
	case "builtins/shell", "shell":
		return NewShellAction(nil), nil
	case "builtins/sequence", "sequence":
		return &SequenceNode{}, nil
	default:
		return nil, fmt.Errorf("unknown action: %s", uses)
	}
}
