package reconciler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/queueclient"
	"vectis/internal/runpolicy"
)

const MinDispatchGap = 30 * time.Second

type Service struct {
	jobs        dal.JobsRepository
	runs        dal.RunsRepository
	logger      interfaces.Logger
	queueClient interfaces.QueueService
	clock       interfaces.Clock
	minGap      time.Duration
	dbDown      bool
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

	now := s.clock.Now().UTC()
	orphaned, err := s.runs.MarkExpiredRunningAsOrphaned(ctx, now.Unix())
	if err != nil {
		if database.IsUnavailableError(err) {
			s.noteDBUnavailable(err)
			return nil
		}

		return fmt.Errorf("mark expired running leases orphaned: %w", err)
	}
	s.noteDBRecovered()

	for _, runID := range orphaned {
		decision := runpolicy.Decide(runpolicy.Input{Trigger: runpolicy.TriggerLeaseExpired})
		s.logger.Warn("reconciler: run %s moved to orphaned (%s)", runID, decision.ReasonCode)
	}

	cutoff := now.Add(-s.minGap).Unix()
	batch, err := s.runs.ListQueuedBeforeDispatchCutoff(ctx, cutoff)
	if err != nil {
		if database.IsUnavailableError(err) {
			s.noteDBUnavailable(err)
			return nil
		}

		return fmt.Errorf("query queued runs: %w", err)
	}
	s.noteDBRecovered()

	for _, r := range batch {
		if err := s.dispatchOne(ctx, r); err != nil {
			if database.IsUnavailableError(err) {
				s.noteDBUnavailable(err)
				return nil
			}

			s.logger.Error("reconciler: run %s: %v", r.RunID, err)
		}
	}

	return nil
}

func (s *Service) noteDBUnavailable(err error) {
	if !database.IsUnavailableError(err) {
		return
	}

	if !s.dbDown {
		s.dbDown = true
		s.logger.Warn("reconciler: database unavailable; skipping processing until recovery: %v", err)
	}
}

func (s *Service) noteDBRecovered() {
	if s.dbDown {
		s.dbDown = false
		s.logger.Info("reconciler: database connectivity recovered; resuming processing")
	}
}

func (s *Service) dispatchOne(ctx context.Context, qr dal.QueuedRun) error {
	runID, jobID := qr.RunID, qr.JobID
	decision := runpolicy.Decide(runpolicy.Input{Trigger: runpolicy.TriggerDispatchRecover})
	if decision.Outcome != runpolicy.OutcomeRequeue {
		s.logger.Debug("reconciler: skip run %s due to disposition policy (%s)", runID, decision.ReasonCode)
		return nil
	}

	defJSON, err := s.jobs.GetDefinition(ctx, jobID)
	if err != nil {
		if !dal.IsNotFound(err) {
			return fmt.Errorf("load stored job: %w", err)
		}

		defJSON, err = s.jobs.GetDefinitionVersion(ctx, jobID, qr.DefinitionVersion)
		if err != nil {
			if dal.IsNotFound(err) {
				s.logger.Debug("reconciler: skip run %s (no stored job and no job_definitions row for job %q version %d)",
					runID, jobID, qr.DefinitionVersion)
				return nil
			}

			return fmt.Errorf("load job definition version: %w", err)
		}
	}

	var job api.Job
	if err := json.Unmarshal([]byte(defJSON), &job); err != nil {
		return fmt.Errorf("parse job json: %w", err)
	}

	job.Id = &jobID
	job.RunId = &runID

	if err := queueclient.EnqueueWithRetry(ctx, s.queueClient, &job, s.logger); err != nil {
		return fmt.Errorf("enqueue: %w", err)
	}

	if err := s.runs.TouchDispatched(ctx, runID); err != nil {
		return fmt.Errorf("touch dispatched: %w", err)
	}

	s.logger.Info("reconciler: re-enqueued run %s (job %s, %s)", runID, jobID, decision.ReasonCode)
	return nil
}
