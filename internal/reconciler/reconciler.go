package reconciler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
	"vectis/internal/queueclient"
	"vectis/internal/runpolicy"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	dbMu        sync.Mutex
	metrics     *observability.ReconcilerMetrics
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

func (s *Service) SetMetrics(metrics *observability.ReconcilerMetrics) {
	s.metrics = metrics
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

	if s.metrics != nil {
		s.metrics.RecordRunsScanned(ctx, len(batch))
	}

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

	s.dbMu.Lock()
	defer s.dbMu.Unlock()

	if !s.dbDown {
		s.dbDown = true
		s.logger.Warn("reconciler: database unavailable; skipping processing until recovery: %v", err)
	}
}

func (s *Service) noteDBRecovered() {
	s.dbMu.Lock()
	defer s.dbMu.Unlock()

	if s.dbDown {
		s.dbDown = false
		s.logger.Info("reconciler: database connectivity recovered; resuming processing")
	}
}

func (s *Service) dispatchOne(ctx context.Context, qr dal.QueuedRun) error {
	runID, jobID := qr.RunID, qr.JobID
	ctx, span := observability.Tracer("vectis/reconciler").Start(ctx, "reconciler.run.reenqueue", trace.WithSpanKind(trace.SpanKindInternal))
	span.SetAttributes(observability.JobRunAttrs(jobID, runID)...)
	span.SetAttributes(attribute.Int("vectis.run.definition.version", qr.DefinitionVersion))
	defer span.End()

	decision := runpolicy.Decide(runpolicy.Input{Trigger: runpolicy.TriggerDispatchRecover})
	if decision.Outcome != runpolicy.OutcomeRequeue {
		s.logger.Debug("reconciler: skip run %s due to disposition policy (%s)", runID, decision.ReasonCode)
		span.SetAttributes(attribute.String("vectis.reconciler.outcome", observability.ReconcilerOutcomeSkippedPolicy))
		if s.metrics != nil {
			s.metrics.RecordReenqueueOutcome(ctx, observability.ReconcilerOutcomeSkippedPolicy)
		}

		return nil
	}

	defJSON, _, err := s.jobs.GetDefinition(ctx, jobID)
	if err != nil {
		if !dal.IsNotFound(err) {
			span.RecordError(err)
			span.SetStatus(codes.Error, "load stored job")
			if s.metrics != nil {
				s.metrics.RecordReenqueueOutcome(ctx, observability.ReconcilerOutcomeFailedLoadJobDef)
			}

			return fmt.Errorf("load stored job: %w", err)
		}

		defJSON, err = s.jobs.GetDefinitionVersion(ctx, jobID, qr.DefinitionVersion)
		if err != nil {
			if dal.IsNotFound(err) {
				s.logger.Debug("reconciler: skip run %s (no stored job and no job_definitions row for job %q version %d)",
					runID, jobID, qr.DefinitionVersion)
				span.SetAttributes(attribute.String("vectis.reconciler.outcome", observability.ReconcilerOutcomeSkippedMissingJobDef))

				if s.metrics != nil {
					s.metrics.RecordReenqueueOutcome(ctx, observability.ReconcilerOutcomeSkippedMissingJobDef)
				}

				return nil
			}

			span.RecordError(err)
			span.SetStatus(codes.Error, "load job definition version")
			if s.metrics != nil {
				s.metrics.RecordReenqueueOutcome(ctx, observability.ReconcilerOutcomeFailedLoadJobDef)
			}

			return fmt.Errorf("load job definition version: %w", err)
		}
	}

	var job api.Job
	if err := json.Unmarshal([]byte(defJSON), &job); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "parse job json")
		if s.metrics != nil {
			s.metrics.RecordReenqueueOutcome(ctx, observability.ReconcilerOutcomeFailedParseJobDef)
		}

		return fmt.Errorf("parse job json: %w", err)
	}

	job.Id = &jobID
	job.RunId = &runID

	req := &api.JobRequest{Job: &job}
	if err := queueclient.EnqueueWithRetry(ctx, s.queueClient, req, s.logger); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "enqueue")
		if s.metrics != nil {
			s.metrics.RecordReenqueueOutcome(ctx, observability.ReconcilerOutcomeFailedEnqueue)
		}

		return fmt.Errorf("enqueue: %w", err)
	}

	if err := s.runs.TouchDispatched(ctx, runID); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "touch dispatched")
		if s.metrics != nil {
			s.metrics.RecordReenqueueOutcome(ctx, observability.ReconcilerOutcomeFailedTouchRun)
		}

		return fmt.Errorf("touch dispatched: %w", err)
	}

	span.SetAttributes(attribute.String("vectis.reconciler.outcome", observability.ReconcilerOutcomeSuccess))
	if s.metrics != nil {
		s.metrics.RecordReenqueueOutcome(ctx, observability.ReconcilerOutcomeSuccess)
	}

	s.logger.Info("reconciler: re-enqueued run %s (job %s, %s)", runID, jobID, decision.ReasonCode)
	return nil
}
