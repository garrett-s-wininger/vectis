package catalog

import (
	"context"
	"errors"
	"fmt"
	"time"

	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
)

const (
	DefaultInterval  = time.Second
	DefaultBatchSize = 100
)

type InboxProcessor interface {
	ProcessPending(ctx context.Context, limit int) (cell.CatalogInboxProcessResult, error)
}

type Metrics interface {
	RecordProcessResult(ctx context.Context, result cell.CatalogInboxProcessResult)
	RecordProcessError(ctx context.Context)
}

type Service struct {
	logger    interfaces.Logger
	processor InboxProcessor
	metrics   Metrics
}

func NewService(logger interfaces.Logger, events dal.CatalogEventsRepository, updater dal.RunCatalogUpdater) *Service {
	return NewServiceWithProcessor(logger, cell.NewCatalogInboxProcessor(events, updater))
}

func NewServiceWithProcessor(logger interfaces.Logger, processor InboxProcessor) *Service {
	return &Service{
		logger:    logger,
		processor: processor,
	}
}

func (s *Service) SetMetrics(metrics Metrics) {
	s.metrics = metrics
}

func (s *Service) Process(ctx context.Context, batchSize int) (cell.CatalogInboxProcessResult, error) {
	if s.processor == nil {
		return cell.CatalogInboxProcessResult{}, errors.New("catalog inbox processor is not set")
	}

	result, err := s.processor.ProcessPending(ctx, normalizeBatchSize(batchSize))
	if err != nil {
		if s.metrics != nil {
			s.metrics.RecordProcessError(ctx)
		}

		return result, fmt.Errorf("process catalog inbox: %w", err)
	}

	if s.metrics != nil {
		s.metrics.RecordProcessResult(ctx, result)
	}

	if result.Read > 0 && s.logger != nil {
		s.logger.Info("catalog: processed %d events (%d applied, %d failed)", result.Read, result.Applied, result.Failed)
	}

	return result, nil
}

func (s *Service) Run(ctx context.Context, interval time.Duration, batchSize int) error {
	if s.processor == nil {
		return errors.New("catalog inbox processor is not set")
	}

	interval = normalizeInterval(interval)
	batchSize = normalizeBatchSize(batchSize)

	if s.logger != nil {
		s.logger.Info("catalog: polling every %v with batch size %d", interval, batchSize)
	}

	process := func() {
		if _, err := s.Process(ctx, batchSize); err != nil && s.logger != nil {
			s.logger.Error("catalog: process failed: %v", err)
		}
	}

	select {
	case <-ctx.Done():
		return nil
	default:
		process()
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if s.logger != nil {
				s.logger.Info("catalog: shutting down")
			}
			return nil
		case <-ticker.C:
			process()
		}
	}
}

func normalizeInterval(interval time.Duration) time.Duration {
	if interval > 0 {
		return interval
	}

	return DefaultInterval
}

func normalizeBatchSize(batchSize int) int {
	if batchSize > 0 {
		return batchSize
	}

	return DefaultBatchSize
}
