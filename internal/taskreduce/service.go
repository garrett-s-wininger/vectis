package taskreduce

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Runner interface {
	Reduce(ctx context.Context, runID string) (Decision, error)
}

type Service struct {
	runner Runner
}

func NewService(runner Runner) *Service {
	return &Service{runner: runner}
}

func (s *Service) Process(ctx context.Context, runID string) (Decision, error) {
	if s == nil {
		return Decision{}, errors.New("task reduce service is required")
	}

	if s.runner == nil {
		return Decision{}, errors.New("task reducer is required")
	}

	decision, err := s.runner.Reduce(ctx, runID)
	if err != nil {
		return Decision{}, err
	}

	RecordDecision(ctx, decision)
	return decision, nil
}

func RecordDecision(ctx context.Context, decision Decision) {
	span := trace.SpanFromContext(ctx)
	attrs := DecisionAttributes(decision)
	span.SetAttributes(attrs...)
	span.AddEvent("task.reduce", trace.WithAttributes(attrs...))
}

func DecisionAttributes(decision Decision) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("vectis.task.reduce.outcome", string(decision.Outcome)),
		attribute.Int("vectis.task.total", decision.Summary.Total),
		attribute.Int("vectis.task.succeeded", decision.Summary.Succeeded),
		attribute.Int("vectis.task.terminal_failed", decision.Summary.TerminalFailed),
		attribute.Int("vectis.task.incomplete", decision.Summary.Incomplete),
	}
}
