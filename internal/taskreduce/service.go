package taskreduce

import (
	"context"
	"errors"
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

	return s.runner.Reduce(ctx, runID)
}
