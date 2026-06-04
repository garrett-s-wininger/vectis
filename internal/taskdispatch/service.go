package taskdispatch

import (
	"context"
	"errors"
	"time"

	"vectis/internal/interfaces"
)

const DefaultServiceInterval = time.Second

type Drainer interface {
	Drain(ctx context.Context, opts DrainOptions) (DrainResult, error)
}

type Metrics interface {
	RecordDrain(ctx context.Context, opts DrainOptions, result DrainResult, err error)
}

type PendingChecker interface {
	HasPending(ctx context.Context, opts DrainOptions) (bool, error)
}

type Service struct {
	logger  interfaces.Logger
	drainer Drainer
	metrics Metrics
	wake    chan struct{}
}

func NewService(logger interfaces.Logger, drainer Drainer) *Service {
	return &Service{
		logger:  logger,
		drainer: drainer,
		wake:    make(chan struct{}, 1),
	}
}

func (s *Service) SetMetrics(metrics Metrics) {
	if s == nil {
		return
	}

	s.metrics = metrics
}

func (s *Service) Notify() {
	if s == nil {
		return
	}

	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Service) Process(ctx context.Context, opts DrainOptions) (DrainResult, error) {
	if s == nil {
		return DrainResult{}, errors.New("task dispatch service is required")
	}

	if s.drainer == nil {
		return DrainResult{}, errors.New("task dispatch drainer is required")
	}

	result, err := s.drainer.Drain(ctx, opts)
	if s.metrics != nil {
		s.metrics.RecordDrain(ctx, opts, result, err)
	}

	return result, err
}

func (s *Service) HasPending(ctx context.Context, opts DrainOptions) (bool, error) {
	if s == nil {
		return false, errors.New("task dispatch service is required")
	}

	if s.drainer == nil {
		return false, errors.New("task dispatch drainer is required")
	}

	checker, ok := s.drainer.(PendingChecker)
	if !ok {
		return false, errors.New("task dispatch pending checker is required")
	}

	return checker.HasPending(ctx, opts)
}

func (s *Service) Run(ctx context.Context, interval time.Duration, opts DrainOptions) error {
	if s == nil {
		return errors.New("task dispatch service is required")
	}

	if s.drainer == nil {
		return errors.New("task dispatch drainer is required")
	}

	if s.wake == nil {
		s.wake = make(chan struct{}, 1)
	}

	interval = normalizeServiceInterval(interval)
	if s.logger != nil {
		s.logger.Info("task dispatch: polling every %v for cell %q with batch size %d", interval, opts.CellID, effectiveDrainLimit(opts.Limit))
	}

	process := func(reason string) {
		result, err := s.Process(ctx, opts)
		if err != nil {
			if s.logger != nil {
				s.logger.Error("task dispatch: %s drain failed: %v", reason, err)
			}
			return
		}

		if s.logger != nil && (result.Listed > 0 || result.Enqueued > 0 || result.Failed > 0) {
			s.logger.Info("task dispatch: %s drained %d intent(s), enqueued=%d failed=%d", reason, result.Listed, result.Enqueued, result.Failed)
		}
	}

	select {
	case <-ctx.Done():
		return nil
	default:
		process("startup")
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if s.logger != nil {
				s.logger.Info("task dispatch: shutting down")
			}
			return nil
		case <-s.wake:
			process("notification")
		case <-ticker.C:
			process("poll")
		}
	}
}

func normalizeServiceInterval(interval time.Duration) time.Duration {
	if interval > 0 {
		return interval
	}

	return DefaultServiceInterval
}

func effectiveDrainLimit(limit int) int {
	if limit > 0 {
		return limit
	}

	return defaultDrainLimit
}
