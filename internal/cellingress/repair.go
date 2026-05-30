package cellingress

import (
	"context"
	"fmt"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/queueclient"

	"google.golang.org/protobuf/encoding/protojson"
)

const (
	DefaultExecutionRepairInterval  = 30 * time.Second
	DefaultExecutionRepairBatchSize = 100
)

type ExecutionRepairService struct {
	acceptances   dal.CellExecutionAcceptancesRepository
	queue         interfaces.QueueService
	logger        interfaces.Logger
	clock         interfaces.Clock
	minAttemptGap time.Duration
	batchSize     int
}

func NewExecutionRepairService(
	acceptances dal.CellExecutionAcceptancesRepository,
	queue interfaces.QueueService,
	logger interfaces.Logger,
	clock interfaces.Clock,
) *ExecutionRepairService {
	if clock == nil {
		clock = interfaces.SystemClock{}
	}

	return &ExecutionRepairService{
		acceptances:   acceptances,
		queue:         queue,
		logger:        logger,
		clock:         clock,
		minAttemptGap: DefaultExecutionRepairInterval,
		batchSize:     DefaultExecutionRepairBatchSize,
	}
}

func (s *ExecutionRepairService) SetMinAttemptGap(d time.Duration) {
	if d >= 0 {
		s.minAttemptGap = d
	}
}

func (s *ExecutionRepairService) SetBatchSize(batchSize int) {
	if batchSize > 0 {
		s.batchSize = batchSize
	}
}

func (s *ExecutionRepairService) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultExecutionRepairInterval
	}

	if err := s.Process(ctx); err != nil && s.logger != nil {
		s.logger.Warn("cell execution repair initial pass failed: %v", err)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if s.logger != nil {
				s.logger.Info("Cell execution repair shutting down")
			}
			return
		case <-ticker.C:
			if err := s.Process(ctx); err != nil && s.logger != nil {
				s.logger.Warn("cell execution repair pass failed: %v", err)
			}
		}
	}
}

func (s *ExecutionRepairService) Process(ctx context.Context) error {
	if s.acceptances == nil {
		return fmt.Errorf("cell execution acceptance store is not set")
	}

	if s.queue == nil {
		return fmt.Errorf("queue service is not set")
	}

	cutoff := s.clock.Now().Add(-s.minAttemptGap).UnixNano()
	batch, err := s.acceptances.ListPendingQueueHandoffs(ctx, cutoff, s.batchSize)
	if err != nil {
		return fmt.Errorf("list pending queue handoffs: %w", err)
	}

	for _, handoff := range batch {
		if err := s.repairOne(ctx, handoff); err != nil && s.logger != nil {
			s.logger.Warn("cell execution repair failed for execution %s: %v", handoff.ExecutionID, err)
		}
	}

	return nil
}

func (s *ExecutionRepairService) repairOne(ctx context.Context, handoff dal.CellExecutionQueueHandoff) error {
	var req api.JobRequest
	if err := protojson.Unmarshal([]byte(handoff.RequestJSON), &req); err != nil {
		s.markFailed(ctx, handoff.ExecutionID, err)
		return fmt.Errorf("decode accepted job request: %w", err)
	}

	if err := queueclient.EnqueueWithRetry(ctx, s.queue, &req, s.logger); err != nil {
		s.markFailed(ctx, handoff.ExecutionID, err)
		return fmt.Errorf("enqueue accepted execution: %w", err)
	}

	if err := s.acceptances.MarkEnqueued(ctx, handoff.ExecutionID, s.clock.Now().UnixNano()); err != nil {
		return fmt.Errorf("mark accepted execution enqueued: %w", err)
	}

	if s.logger != nil {
		s.logger.Info("cell execution repair enqueued execution %s for run %s", handoff.ExecutionID, handoff.RunID)
	}

	return nil
}

func (s *ExecutionRepairService) markFailed(ctx context.Context, executionID string, cause error) {
	if err := s.acceptances.MarkEnqueueFailed(ctx, executionID, s.clock.Now().UnixNano(), cause.Error()); err != nil && s.logger != nil {
		s.logger.Warn("cell execution repair failed to record enqueue failure for execution %s: %v", executionID, err)
	}
}
