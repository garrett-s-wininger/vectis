package reconciler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
	"vectis/internal/runpolicy"
	"vectis/internal/taskdispatch"
	"vectis/internal/taskfinalize"
	"vectis/internal/taskreduce"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	MinDispatchGap          = 30 * time.Second
	DefaultServiceLeaseTTL  = 2 * time.Minute
	ServiceLeaseName        = "reconciler"
	TaskFinalizeRepairLimit = 100
)

type Service struct {
	jobs            dal.JobsRepository
	runs            dal.RunsRepository
	dispatch        dal.DispatchEventsRepository
	taskDispatch    dal.TaskDispatchIntentsRepository
	leases          dal.ServiceLeasesRepository
	logger          interfaces.Logger
	queueClient     interfaces.QueueService
	ingress         cell.ExecutionIngress
	clock           interfaces.Clock
	minGap          time.Duration
	dbDown          bool
	dbMu            sync.Mutex
	metrics         *observability.ReconcilerMetrics
	dispatchMetrics dispatchMetrics
	taskMetrics     taskdispatch.Metrics
	leaseName       string
	leaseOwner      string
	leaseTTL        time.Duration
	leaseHeld       bool
	leaseMu         sync.Mutex
}

type dispatchMetrics interface {
	RecordDispatchEvent(ctx context.Context, source, eventType, targetCell string)
}

func NewService(logger interfaces.Logger, db *sql.DB, queue interfaces.QueueService, clock interfaces.Clock) *Service {
	repos := dal.NewSQLRepositories(db)
	s := NewServiceWithRepositories(logger, repos.Jobs(), repos.Runs(), queue, clock)
	s.dispatch = repos.DispatchEvents()
	s.taskDispatch = repos.TaskDispatchIntents()
	s.leases = repos.ServiceLeases()
	return s
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
		leaseName:   ServiceLeaseName,
		leaseOwner:  uuid.NewString(),
		leaseTTL:    DefaultServiceLeaseTTL,
	}
}

func (s *Service) SetMinDispatchGap(d time.Duration) {
	if d > 0 {
		s.minGap = d
	}
}

func (s *Service) SetExecutionIngress(ingress cell.ExecutionIngress) {
	s.ingress = ingress
}

func (s *Service) SetServiceLeases(repo dal.ServiceLeasesRepository) {
	s.leases = repo
}

func (s *Service) SetTaskDispatchIntents(repo dal.TaskDispatchIntentsRepository) {
	s.taskDispatch = repo
}

func (s *Service) SetLeaseOwner(owner string) {
	if owner != "" {
		s.leaseOwner = owner
	}
}

func (s *Service) SetLeaseTTL(d time.Duration) {
	if d > 0 {
		s.leaseTTL = d
	}
}

func (s *Service) recordDispatchEvent(ctx context.Context, runID, targetCellID, eventType string, message *string) {
	if s.dispatchMetrics != nil {
		s.dispatchMetrics.RecordDispatchEvent(ctx, dal.DispatchSourceReconciler, eventType, targetCellID)
	}

	if s.dispatch != nil {
		if err := s.dispatch.Record(ctx, runID, dal.DispatchSourceReconciler, eventType, message); err != nil {
			s.logger.Error("reconciler: failed to record dispatch event for run %s: %v", runID, err)
		}
	}
}

func (s *Service) SetMetrics(metrics *observability.ReconcilerMetrics) {
	s.metrics = metrics
}

func (s *Service) SetDispatchMetrics(metrics dispatchMetrics) {
	s.dispatchMetrics = metrics
}

func (s *Service) SetTaskDispatchMetrics(metrics taskdispatch.Metrics) {
	s.taskMetrics = metrics
}

func (s *Service) Process(ctx context.Context) error {
	if s.queueClient == nil {
		return fmt.Errorf("queue client is not set")
	}

	now := s.clock.Now().UTC()
	acquired, err := s.acquireServiceLease(ctx, now)
	if err != nil {
		return err
	}

	if !acquired {
		return nil
	}

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

	if err := s.repairTaskFinalization(ctx); err != nil {
		if database.IsUnavailableError(err) {
			s.noteDBUnavailable(err)
			return nil
		}

		return fmt.Errorf("repair task finalization: %w", err)
	}

	if err := s.repairTaskDispatch(ctx); err != nil {
		if database.IsUnavailableError(err) {
			s.noteDBUnavailable(err)
			return nil
		}

		return fmt.Errorf("repair task dispatch: %w", err)
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

	leaseRefreshInterval := s.leaseRefreshInterval()
	nextLeaseRefresh := now.Add(leaseRefreshInterval)
	for _, r := range batch {
		if leaseRefreshInterval > 0 {
			refreshNow := s.clock.Now().UTC()
			if !refreshNow.Before(nextLeaseRefresh) {
				acquired, err := s.acquireServiceLease(ctx, refreshNow)
				if err != nil {
					return err
				}

				if !acquired {
					return nil
				}

				nextLeaseRefresh = refreshNow.Add(leaseRefreshInterval)
			}
		}

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

func (s *Service) repairTaskFinalization(ctx context.Context) error {
	candidates, err := s.runs.ListOrphanedTaskFinalizationCandidates(ctx, TaskFinalizeRepairLimit)
	if err != nil {
		s.recordTaskFinalizationRepair(ctx, observability.ReconcilerTaskFinalizationOutcomeError, observability.ReconcilerTaskFinalizationReduceOutcomeUnknown)
		return err
	}

	if len(candidates) == 0 {
		return nil
	}

	for _, summary := range candidates {
		decision := taskreduce.Decide(summary)
		switch decision.Outcome {
		case taskreduce.OutcomeSucceeded:
			if err := s.runs.RepairMarkRunSucceeded(ctx, summary.RunID, ""); err != nil {
				if dal.IsConflict(err) || dal.IsNotFound(err) {
					s.recordTaskFinalizationRepair(ctx, observability.ReconcilerTaskFinalizationOutcomeSkippedConflict, string(decision.Outcome))
					s.logger.Warn("reconciler: task finalization repair skipped run %s: %v", summary.RunID, err)
					continue
				}

				s.recordTaskFinalizationRepair(ctx, observability.ReconcilerTaskFinalizationOutcomeError, string(decision.Outcome))
				return err
			}

			s.recordTaskFinalizationRepair(ctx, observability.ReconcilerTaskFinalizationOutcomeSuccess, string(decision.Outcome))
			s.logger.Info("reconciler: repaired orphaned task run %s to succeeded", summary.RunID)
		case taskreduce.OutcomeFailed:
			reason := taskfinalize.FailureReason(taskfinalize.Decision{Reduce: decision})
			if err := s.runs.RepairMarkRunFailedWithCode(ctx, summary.RunID, dal.FailureCodeExecution, reason); err != nil {
				if dal.IsConflict(err) || dal.IsNotFound(err) {
					s.recordTaskFinalizationRepair(ctx, observability.ReconcilerTaskFinalizationOutcomeSkippedConflict, string(decision.Outcome))
					s.logger.Warn("reconciler: task finalization repair skipped run %s: %v", summary.RunID, err)
					continue
				}

				s.recordTaskFinalizationRepair(ctx, observability.ReconcilerTaskFinalizationOutcomeError, string(decision.Outcome))
				return err
			}

			s.recordTaskFinalizationRepair(ctx, observability.ReconcilerTaskFinalizationOutcomeSuccess, string(decision.Outcome))
			s.logger.Info("reconciler: repaired orphaned task run %s to failed", summary.RunID)
		}
	}

	return nil
}

func (s *Service) recordTaskFinalizationRepair(ctx context.Context, outcome, reduceOutcome string) {
	if s.metrics == nil {
		return
	}

	s.metrics.RecordTaskFinalizationRepair(ctx, outcome, reduceOutcome)
}

func (s *Service) repairTaskDispatch(ctx context.Context) error {
	if s.taskDispatch == nil {
		return nil
	}

	pending, err := s.taskDispatch.ListPending(ctx, config.CellID(), s.clock.Now().UnixNano(), 1)
	if err != nil {
		return err
	}

	if len(pending) == 0 {
		return nil
	}

	ingress, err := s.executionIngress()
	if err != nil {
		return err
	}

	dispatcher := taskdispatch.New(s.runs, s.taskDispatch, s.dispatch, ingress, s.clock)
	dispatcher.SetDispatchMetrics(s.dispatchMetrics)

	service := taskdispatch.NewService(s.logger, dispatcher)
	service.SetMetrics(s.taskMetrics)

	result, err := service.Process(ctx, taskdispatch.DrainOptions{CellID: config.CellID()})
	if err != nil {
		return err
	}

	if result.Listed > 0 || result.Enqueued > 0 || result.Failed > 0 {
		s.logger.Info("reconciler: repaired task dispatch intents listed=%d enqueued=%d failed=%d", result.Listed, result.Enqueued, result.Failed)
	}

	return nil
}

func (s *Service) effectiveLeaseTTL() time.Duration {
	if s.leaseTTL > 0 {
		return s.leaseTTL
	}

	return DefaultServiceLeaseTTL
}

func (s *Service) leaseRefreshInterval() time.Duration {
	if s.leases == nil {
		return 0
	}

	interval := s.effectiveLeaseTTL() / 2
	if interval > 30*time.Second {
		return 30 * time.Second
	}

	return interval
}

func (s *Service) acquireServiceLease(ctx context.Context, now time.Time) (bool, error) {
	if s.leases == nil {
		return true, nil
	}

	leaseName := s.leaseName
	if leaseName == "" {
		leaseName = ServiceLeaseName
	}

	owner := s.leaseOwner
	if owner == "" {
		owner = uuid.NewString()
		s.leaseOwner = owner
	}

	ttl := s.effectiveLeaseTTL()

	acquired, err := s.leases.TryAcquire(ctx, leaseName, owner, now, now.Add(ttl))
	if err != nil {
		if database.IsUnavailableError(err) {
			s.noteDBUnavailable(err)
			return false, nil
		}

		return false, fmt.Errorf("acquire service lease %q: %w", leaseName, err)
	}
	s.noteDBRecovered()

	if !acquired {
		s.noteLeaseSkipped(leaseName)
		return false, nil
	}

	s.noteLeaseAcquired(leaseName, owner, ttl)
	return true, nil
}

func (s *Service) noteLeaseAcquired(name, owner string, ttl time.Duration) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()

	if s.leaseHeld {
		return
	}

	s.leaseHeld = true
	s.logger.Info("reconciler: acquired service lease %q as %s (ttl %v)", name, owner, ttl)
}

func (s *Service) noteLeaseSkipped(name string) {
	s.leaseMu.Lock()
	wasHeld := s.leaseHeld
	s.leaseHeld = false
	s.leaseMu.Unlock()

	if wasHeld {
		s.logger.Warn("reconciler: lost service lease %q; another reconciler is active", name)
		return
	}

	s.logger.Debug("reconciler: service lease %q is held elsewhere; skipping this interval", name)
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

	defJSON, err := s.jobs.GetDefinitionVersion(ctx, jobID, qr.DefinitionVersion)
	if err != nil {
		if dal.IsNotFound(err) {
			s.logger.Debug("reconciler: skip run %s (no job_definitions row for job %q version %d)",
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
	if _, err := cell.AttachPendingExecutionEnvelope(ctx, s.runs, req, runID, s.clock.Now().UnixNano()); err != nil {
		span.RecordError(err)
		s.logger.Error("reconciler: failed to attach execution envelope for run %s: %v", runID, err)
	}

	s.recordDispatchEvent(ctx, runID, qr.OwningCell, dal.DispatchEventAttempt, nil)
	dispatchReq, err := s.recordExecutionPayload(ctx, runID, req, qr.DefinitionHash)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "record execution payload")
		msg := err.Error()
		s.recordDispatchEvent(ctx, runID, qr.OwningCell, dal.DispatchEventFailure, &msg)
		if s.metrics != nil {
			s.metrics.RecordReenqueueOutcome(ctx, observability.ReconcilerOutcomeFailedEnqueue)
		}

		return err
	}
	req = dispatchReq

	ingress, err := s.executionIngress()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "cell ingress endpoints")
		msg := err.Error()
		s.recordDispatchEvent(ctx, runID, qr.OwningCell, dal.DispatchEventFailure, &msg)

		if s.metrics != nil {
			s.metrics.RecordReenqueueOutcome(ctx, observability.ReconcilerOutcomeFailedEnqueue)
		}

		return fmt.Errorf("cell ingress endpoints: %w", err)
	}

	submission, err := cell.NewExecutionSubmission(req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "execution submission")
		msg := err.Error()

		s.recordDispatchEvent(ctx, runID, qr.OwningCell, dal.DispatchEventFailure, &msg)
		if s.metrics != nil {
			s.metrics.RecordReenqueueOutcome(ctx, observability.ReconcilerOutcomeFailedEnqueue)
		}

		return fmt.Errorf("execution submission: %w", err)
	}

	if err := ingress.SubmitExecution(ctx, submission); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "enqueue")
		msg := err.Error()

		s.recordDispatchEvent(ctx, runID, qr.OwningCell, dal.DispatchEventFailure, &msg)
		if s.metrics != nil {
			s.metrics.RecordReenqueueOutcome(ctx, observability.ReconcilerOutcomeFailedEnqueue)
		}

		return fmt.Errorf("enqueue: %w", err)
	}

	if err := s.runs.TouchDispatched(ctx, runID); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "touch dispatched")
		msg := "touch dispatched: " + err.Error()
		s.recordDispatchEvent(ctx, runID, qr.OwningCell, dal.DispatchEventFailure, &msg)
		if s.metrics != nil {
			s.metrics.RecordReenqueueOutcome(ctx, observability.ReconcilerOutcomeFailedTouchRun)
		}

		return fmt.Errorf("touch dispatched: %w", err)
	}
	s.recordDispatchEvent(ctx, runID, qr.OwningCell, dal.DispatchEventSuccess, nil)

	span.SetAttributes(attribute.String("vectis.reconciler.outcome", observability.ReconcilerOutcomeSuccess))
	if s.metrics != nil {
		s.metrics.RecordReenqueueOutcome(ctx, observability.ReconcilerOutcomeSuccess)
	}

	s.logger.Info("reconciler: re-enqueued run %s (job %s, %s)", runID, jobID, decision.ReasonCode)
	return nil
}

func (s *Service) executionIngress() (cell.ExecutionIngress, error) {
	if s.ingress != nil {
		return s.ingress, nil
	}

	endpoints, err := config.CellIngressEndpoints()
	if err != nil {
		return nil, err
	}

	queueClient := s.queueClient
	if database.GlobalAndCellDatabasesAreSplit() {
		queueClient = nil
	}

	return cell.NewExecutionRouterWithOptions(config.CellID(), queueClient, endpoints, s.logger, cell.ExecutionRouterOptions{
		TLSConfigForEndpoint: config.CellIngressHTTPClientTLSConfig,
	}), nil
}

func (s *Service) recordExecutionPayload(ctx context.Context, runID string, req *api.JobRequest, definitionHash string) (*api.JobRequest, error) {
	payloadJSON, err := protojson.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal execution payload: %w", err)
	}

	_, recordedPayloadJSON, err := s.runs.RecordExecutionPayload(ctx, runID, string(payloadJSON), definitionHash)
	if err != nil {
		return nil, fmt.Errorf("record execution payload: %w", err)
	}

	if recordedPayloadJSON == string(payloadJSON) {
		return req, nil
	}

	var recordedReq api.JobRequest
	if err := protojson.Unmarshal([]byte(recordedPayloadJSON), &recordedReq); err != nil {
		return nil, fmt.Errorf("parse recorded execution payload: %w", err)
	}

	return &recordedReq, nil
}
