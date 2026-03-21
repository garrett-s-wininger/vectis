package reconciler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
)

const MinDispatchGap = 30 * time.Second

type Service struct {
	jobs        dal.JobsRepository
	runs        dal.RunsRepository
	logger      interfaces.Logger
	queueClient interfaces.QueueService
	clock       interfaces.Clock
	minGap      time.Duration
}

func NewService(logger interfaces.Logger, db *sql.DB, queue interfaces.QueueService, clock interfaces.Clock) *Service {
	repos := dal.NewSQLRepositories(db)
	return NewServiceWithRepositories(logger, repos.Jobs(), repos.Runs(), queue, clock)
}

func NewServiceWithRepositories(
	logger interfaces.Logger,
	jobs dal.JobsRepository,
	runs dal.RunsRepository,
	queue interfaces.QueueService,
	clock interfaces.Clock,
) *Service {
	if clock == nil {
		clock = interfaces.SystemClock{}
	}

	return &Service{
		jobs:        jobs,
		runs:        runs,
		logger:      logger,
		queueClient: queue,
		clock:       clock,
		minGap:      MinDispatchGap,
	}
}

func (s *Service) SetMinDispatchGap(d time.Duration) {
	if d > 0 {
		s.minGap = d
	}
}

func (s *Service) Process(ctx context.Context) error {
	if s.queueClient == nil {
		return fmt.Errorf("queue client is not set")
	}

	cutoff := s.clock.Now().Add(-s.minGap).Unix()
	batch, err := s.runs.ListQueuedBeforeDispatchCutoff(ctx, cutoff)
	if err != nil {
		return fmt.Errorf("query queued runs: %w", err)
	}

	for _, r := range batch {
		if err := s.dispatchOne(ctx, r.RunID, r.JobID); err != nil {
			s.logger.Error("reconciler: run %s: %v", r.RunID, err)
		}
	}

	return nil
}

func (s *Service) dispatchOne(ctx context.Context, runID, jobID string) error {
	defJSON, err := s.jobs.GetDefinition(ctx, jobID)
	if err != nil {
		if dal.IsNotFound(err) {
			s.logger.Debug("reconciler: skip run %s (job %q has no stored definition; ephemeral runs are not re-enqueued)", runID, jobID)
			return nil
		}

		return fmt.Errorf("load stored job: %w", err)
	}

	var job api.Job
	if err := json.Unmarshal([]byte(defJSON), &job); err != nil {
		return fmt.Errorf("parse job json: %w", err)
	}

	job.Id = &jobID
	job.RunId = &runID

	if _, err := s.queueClient.Enqueue(ctx, &job); err != nil {
		return fmt.Errorf("enqueue: %w", err)
	}

	if err := s.runs.TouchDispatched(ctx, runID); err != nil {
		return fmt.Errorf("touch dispatched: %w", err)
	}

	s.logger.Info("reconciler: re-enqueued run %s (job %s)", runID, jobID)
	return nil
}
