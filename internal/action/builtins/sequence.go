package builtins

import (
	"context"
	"fmt"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/observability"
	"vectis/internal/taskgraph"

	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type SequenceNode struct{}

func (s *SequenceNode) ValidateWith(with map[string]string) []action.FieldError {
	return validateExecutionMode(with)
}

func (s *SequenceNode) Type() string {
	return "builtins/sequence"
}

func (s *SequenceNode) PortSchema() []action.PortSpec {
	return []action.PortSpec{{
		Name:    taskgraph.StepsPort,
		Max:     action.PortUnlimited,
		Primary: true,
		Ordered: true,
	}}
}

func (s *SequenceNode) Execute(ctx context.Context, state *action.ExecutionState, _ map[string]any, ports action.Ports) action.Result {
	children := ports.Children(taskgraph.StepsPort)
	if len(children) == 0 {
		return action.NewSuccessResult(nil)
	}

	state.Logger.Info("Executing sequence with %d children", len(children))
	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Executing sequence with %d children", len(children)))

	var outputs map[string]any
	for i, child := range children {
		sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Executing step %d/%d", i+1, len(children)))

		result := executeChildNode(ctx, child, state)
		if result.Status == action.StatusFailure {
			state.Logger.Error("Sequence failed at child %d: %v", i+1, result.Error)
			sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("Step %d failed: %v", i+1, result.Error))
			return result
		}

		outputs = result.Outputs
	}

	state.Logger.Info("Sequence completed successfully")
	sendLog(state, api.Stream_STREAM_STDOUT, "Sequence completed successfully")
	return action.NewSuccessResult(outputs)
}

func executeChildNode(ctx context.Context, node *api.Node, state *action.ExecutionState) action.Result {
	childCtx, span := observability.Tracer("vectis/job").Start(ctx, "run.action.execute", trace.WithSpanKind(trace.SpanKindInternal))
	span.SetAttributes(observability.JobRunAttrs(state.JobID, state.RunID)...)
	span.SetAttributes(
		attribute.String("action.type", node.GetUses()),
		attribute.String("action.node.id", node.GetId()),
		attribute.Bool("action.has_children", taskgraph.HasChildren(node)),
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

	inputs, err := action.ResolveNodeInputs(state, node, action.InputSchema(act))
	if err != nil {
		wrapped := &action.ExecutionError{
			NodeID:  node.GetId(),
			Action:  node.GetUses(),
			Message: "failed to resolve node inputs",
			Cause:   err,
		}

		span.RecordError(wrapped)
		span.SetStatus(otelcodes.Error, "resolve inputs")
		return action.NewFailureResult(wrapped)
	}

	result := act.Execute(childCtx, state, inputs, action.Ports(taskgraph.ChildPorts(node)))
	if result.Status == action.StatusFailure && result.Error != nil {
		span.RecordError(result.Error)
		span.SetStatus(otelcodes.Error, "action failed")
		return result
	}

	if result.Status == action.StatusSuccess {
		state.RecordOutputs(node.GetId(), result.Outputs)
	}

	return result
}
