package builtins

import (
	"context"
	"fmt"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/observability"

	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	childCtx, span := observability.Tracer("vectis/job").Start(ctx, "run.action.execute", trace.WithSpanKind(trace.SpanKindInternal))
	span.SetAttributes(observability.JobRunAttrs(state.JobID, state.RunID)...)
	span.SetAttributes(
		attribute.String("action.type", node.GetUses()),
		attribute.String("action.node.id", node.GetId()),
		attribute.Bool("action.has_children", len(node.GetSteps()) > 0),
	)
	defer span.End()

	if state.Resolver == nil {
		err := &action.ExecutionError{
			NodeID:  node.GetId(),
			Action:  node.GetUses(),
			Message: "no resolver in execution state (required for sequence steps)",
			Cause:   nil,
		}

		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "missing resolver")
		return action.NewFailureResult(
			err,
		)
	}

	act, err := state.Resolver.Resolve(node.GetUses())
	if err != nil {
		wrapped := &action.ExecutionError{
			NodeID:  node.GetId(),
			Action:  node.GetUses(),
			Message: "failed to resolve action",
			Cause:   err,
		}

		span.RecordError(wrapped)
		span.SetStatus(otelcodes.Error, "resolve action")
		return action.NewFailureResult(
			wrapped,
		)
	}

	result := act.Execute(childCtx, state, inputs, node.GetSteps())
	if result.Status == action.StatusFailure && result.Error != nil {
		span.RecordError(result.Error)
		span.SetStatus(otelcodes.Error, "action failed")
	}

	return result
}
