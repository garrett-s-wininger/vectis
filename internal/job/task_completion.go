package job

import (
	"context"

	"vectis/internal/dal"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type TaskCompletionResult struct {
	ExecutionID string
	Status      string
	Children    []dal.TaskExecutionRecord
	Activated   int
}

func RecordTaskCompletion(ctx context.Context, result TaskCompletionResult) {
	span := trace.SpanFromContext(ctx)
	attrs := TaskCompletionAttributes(result)
	span.SetAttributes(attrs...)
	span.AddEvent("task.complete", trace.WithAttributes(attrs...))
}

func TaskCompletionAttributes(result TaskCompletionResult) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("vectis.execution.id", result.ExecutionID),
		attribute.String("vectis.execution.status", result.Status),
		attribute.Int("vectis.task.children.activated", result.Activated),
		attribute.Int("vectis.task.children.dispatchable", len(result.Children)),
		attribute.Bool("vectis.task.fanout.activation_point", len(result.Children) > 0),
	}
}
