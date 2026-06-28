package cron

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robfig/cron/v3"

	api "vectis/api/gen/go"
	"vectis/internal/action/actionregistry"
	"vectis/internal/backoff"
	"vectis/internal/cell"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/dispatchmeta"
	"vectis/internal/interfaces"
	"vectis/internal/queueclient"
	sourcepkg "vectis/internal/source"
)

type CronSchedule struct {
	ID                          int64
	TriggerID                   int64
	ScheduleID                  string
	JobID                       string
	CronSpec                    string
	NextRunAt                   time.Time
	SourceRepositoryID          string
	SourceRef                   string
	SourcePath                  string
	SourceOverrideRef           string
	SourceOverridePath          string
	SourceOverrideReason        string
	SourceOverrideCreatedAtUnix int64
}

func (s CronSchedule) effectiveSourceRef() string {
	if ref := strings.TrimSpace(s.SourceOverrideRef); ref != "" {
		return ref
	}

	return strings.TrimSpace(s.SourceRef)
}

func (s CronSchedule) effectiveSourcePath() string {
	if path := strings.TrimSpace(s.SourceOverridePath); path != "" {
		return path
	}

	return strings.TrimSpace(s.SourcePath)
}

type CronService struct {
	jobs                  dal.JobsRepository
	runs                  dal.RunsRepository
	schedules             dal.SchedulesRepository
	sources               dal.SourcesRepository
	dispatch              dal.DispatchEventsRepository
	triggers              dal.TriggerInvocationsRepository
	logger                interfaces.Logger
	queueClient           interfaces.QueueService
	queueClose            func()
	ingress               cell.ExecutionIngress
	parser                cron.Parser
	clock                 interfaces.Clock
	retryMetrics          backoff.RetryMetrics
	actionResolver        actionregistry.Resolver
	newDefinitionResolver sourcepkg.DefinitionResolverFactory
	instanceID            string
	claimTTL              time.Duration
	claimSeq              atomic.Uint64
	mu                    sync.Mutex
}

func NewCronService(logger interfaces.Logger, db *sql.DB) *CronService {
	repos := dal.NewSQLRepositoriesWithCellID(db, config.CellID())
	s := NewCronServiceWithRepositories(logger, repos.Jobs(), repos.Runs(), repos.Schedules())
	s.sources = repos.Sources()
	s.dispatch = repos.DispatchEvents()
	s.triggers = repos.TriggerInvocations()
	return s
}

func NewCronServiceWithRepositories(
	logger interfaces.Logger,
	jobs dal.JobsRepository,
	runs dal.RunsRepository,
	schedules dal.SchedulesRepository,
) *CronService {
	return &CronService{
		jobs:       jobs,
		runs:       runs,
		schedules:  schedules,
		logger:     logger,
		parser:     cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow),
		clock:      interfaces.SystemClock{},
		instanceID: defaultInstanceID(),
		claimTTL:   config.CronClaimTTL(),
	}
}

func (s *CronService) SetQueueClient(client interfaces.QueueService) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queueClose != nil {
		s.queueClose()
		s.queueClose = nil
	}

	s.queueClient = client
}

func (s *CronService) SetExecutionIngress(ingress cell.ExecutionIngress) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ingress = ingress
}

func (s *CronService) SetClock(clock interfaces.Clock) {
	s.clock = clock
}

func (s *CronService) SetRetryMetrics(m backoff.RetryMetrics) {
	s.retryMetrics = m
}

func (s *CronService) SetActionDescriptorResolver(resolver actionregistry.Resolver) {
	s.actionResolver = resolver
}

func (s *CronService) SetTriggerInvocations(triggers dal.TriggerInvocationsRepository) {
	s.triggers = triggers
}

func (s *CronService) SetSources(sources dal.SourcesRepository) {
	s.sources = sources
}

func (s *CronService) SetDefinitionResolverFactory(factory sourcepkg.DefinitionResolverFactory) {
	s.newDefinitionResolver = factory
}

func (s *CronService) SetInstanceID(id string) {
	if id = strings.TrimSpace(id); id != "" {
		s.instanceID = id
	}
}

func (s *CronService) InstanceID() string {
	return s.instanceID
}

func (s *CronService) SetClaimTTL(ttl time.Duration) {
	if ttl > 0 {
		s.claimTTL = ttl
	}
}

func (s *CronService) effectiveClaimTTL() time.Duration {
	if s.claimTTL > 0 {
		return s.claimTTL
	}

	return config.CronClaimTTL()
}

func (s *CronService) nextClaimToken(sched CronSchedule) string {
	owner := strings.TrimSpace(s.instanceID)
	if owner == "" {
		owner = defaultInstanceID()
		s.instanceID = owner
	}

	seq := s.claimSeq.Add(1)
	return fmt.Sprintf("%s:%d:%d:%d", owner, sched.ID, sched.NextRunAt.UTC().Unix(), seq)
}

func defaultInstanceID() string {
	hostname, err := os.Hostname()
	if err == nil {
		hostname = strings.TrimSpace(hostname)
	}

	if hostname != "" {
		return hostname
	}

	return "cron"
}

func (s *CronService) recordDispatchEvent(ctx context.Context, runID, eventType string, message *string) {
	if s.dispatch == nil {
		return
	}

	if err := s.dispatch.Record(ctx, runID, dal.DispatchSourceCron, eventType, message); err != nil {
		s.logger.Error("Failed to record cron dispatch event for run %s: %v", runID, err)
	}
}

func (s *CronService) recordDispatchAttemptOutcome(ctx context.Context, runID, outcomeEventType string, message *string) error {
	if s.dispatch != nil {
		return s.dispatch.RecordDispatchAttemptOutcome(ctx, runID, dal.DispatchSourceCron, outcomeEventType, message)
	}

	if outcomeEventType == dal.DispatchEventSuccess && s.runs != nil {
		return s.runs.TouchDispatched(ctx, runID)
	}

	return nil
}

func (s *CronService) CloseQueueDial() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queueClose != nil {
		s.queueClose()
		s.queueClose = nil
	}
}

func (s *CronService) ConnectToQueue(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queueClose != nil {
		s.queueClose()
		s.queueClose = nil
	}
	s.queueClient = nil

	pin := config.CronQueueAddress()
	mq, err := queueclient.NewManagingQueuePoolService(ctx, s.logger, queueclient.QueuePoolOptions{
		PinnedAddress:   pin,
		RegistryAddress: config.CronRegistryDialAddress(),
		RetryMetrics:    s.retryMetrics,
	})

	if err != nil {
		return fmt.Errorf("failed to connect to queue: %w", err)
	}

	s.queueClient = mq
	s.queueClose = func() { _ = mq.Close() }
	if pin == "" {
		s.logger.Info("Connected to queue via registry resolution")
	}

	return nil
}

func (s *CronService) GetReadySchedules(ctx context.Context) ([]CronSchedule, error) {
	ready, err := s.schedules.GetReady(ctx, s.clock.Now())
	if err != nil {
		return nil, fmt.Errorf("failed to query schedules: %w", err)
	}

	schedules := make([]CronSchedule, 0, len(ready))
	for _, sched := range ready {
		schedules = append(schedules, CronSchedule{
			ID:                          sched.ID,
			TriggerID:                   sched.TriggerID,
			ScheduleID:                  sched.ScheduleID,
			JobID:                       sched.JobID,
			CronSpec:                    sched.CronSpec,
			NextRunAt:                   sched.NextRunAt,
			SourceRepositoryID:          sched.SourceRepositoryID,
			SourceRef:                   sched.SourceRef,
			SourcePath:                  sched.SourcePath,
			SourceOverrideRef:           sched.SourceOverrideRef,
			SourceOverridePath:          sched.SourceOverridePath,
			SourceOverrideReason:        sched.SourceOverrideReason,
			SourceOverrideCreatedAtUnix: sched.SourceOverrideCreatedAtUnix,
		})
	}

	return schedules, nil
}

func (s *CronService) ValidateCronSpec(spec string, t time.Time) (bool, error) {
	schedule, err := s.parser.Parse(spec)
	if err != nil {
		return false, fmt.Errorf("invalid cron spec %q: %w", spec, err)
	}

	// NOTE(garrett): After truncatation, we remove a nanosecond to ensure the next
	// schedule actually matches the current time, otherwise, it'll check against the
	// next minute, always returning false.
	truncated := t.Truncate(time.Minute)
	next := schedule.Next(truncated.Add(-time.Nanosecond))
	return next.Equal(truncated), nil
}

func (s *CronService) CalculateNextRun(spec string, from time.Time) (time.Time, error) {
	schedule, err := s.parser.Parse(spec)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid cron spec %q: %w", spec, err)
	}

	return schedule.Next(from), nil
}

func (s *CronService) TriggerJob(ctx context.Context, jobID string) error {
	return s.triggerJob(ctx, jobID, nil)
}

func (s *CronService) triggerJob(ctx context.Context, jobID string, schedule *CronSchedule) error {
	if schedule == nil || strings.TrimSpace(schedule.SourceRepositoryID) == "" {
		return fmt.Errorf("%w: cron job %s requires a source repository schedule", sourcepkg.ErrInvalidReference, jobID)
	}

	return s.triggerSourceJob(ctx, jobID, schedule)
}

func (s *CronService) triggerSourceJob(ctx context.Context, jobID string, schedule *CronSchedule) error {
	loaded, sourceRec, err := s.loadSourceScheduleDefinition(ctx, jobID, schedule)
	if err != nil {
		return err
	}

	invocationID, err := s.recordTriggerInvocation(ctx, jobID, schedule)
	if err != nil {
		return err
	}

	audit := dal.RunAuditMetadata{
		TriggerInvocationID:   invocationID,
		StartDeadlineUnixNano: dispatchmeta.DeadlineUnixNano(s.clock.Now(), config.DispatchStartTTL()),
	}
	runID, _, definitionVersion, created, err := s.runs.CreateScheduledSourceDefinitionRun(ctx, schedule.ID, schedule.NextRunAt, jobID, loaded.DefinitionJSON, sourceRec, audit)
	if err != nil {
		return err
	}

	definitionJSON := loaded.DefinitionJSON
	job := loaded.Job
	if !created {
		s.logger.Info("Reusing existing source cron run %s for schedule %d at %v", runID, schedule.ID, schedule.NextRunAt)
		definitionJSON, job, err = s.getJobDefinitionVersion(ctx, jobID, definitionVersion)
		if err != nil {
			return err
		}
	}

	job.Id = &jobID
	job.RunId = &runID

	return s.dispatchRun(ctx, job, runID, dal.DefinitionHash(definitionJSON))
}

func (s *CronService) getJobDefinitionVersion(ctx context.Context, jobID string, version int) (string, *api.Job, error) {
	definitionJSON, err := s.jobs.GetDefinitionVersion(ctx, jobID, version)
	if err != nil {
		return "", nil, err
	}

	var job api.Job
	if err := json.Unmarshal([]byte(definitionJSON), &job); err != nil {
		return "", nil, fmt.Errorf("failed to parse job definition version %d: %w", version, err)
	}

	return definitionJSON, &job, nil
}

func (s *CronService) loadSourceScheduleDefinition(ctx context.Context, jobID string, schedule *CronSchedule) (sourcepkg.Definition, dal.JobDefinitionSourceRecord, error) {
	if s.sources == nil {
		return sourcepkg.Definition{}, dal.JobDefinitionSourceRecord{}, fmt.Errorf("source repository schedules are not configured")
	}

	repositoryID := strings.TrimSpace(schedule.SourceRepositoryID)
	repoRec, err := s.sources.GetRepository(ctx, repositoryID)
	if err != nil {
		return sourcepkg.Definition{}, dal.JobDefinitionSourceRecord{}, err
	}

	if !repoRec.Enabled {
		return sourcepkg.Definition{}, dal.JobDefinitionSourceRecord{}, fmt.Errorf("%w: source repository %s is disabled", sourcepkg.ErrInvalidReference, repoRec.RepositoryID)
	}

	ref, definitionPath, err := resolveSourceScheduleReference(jobID, repoRec, schedule)
	if err != nil {
		return sourcepkg.Definition{}, dal.JobDefinitionSourceRecord{}, err
	}

	resolver, err := s.definitionResolver(repoRec)
	if err != nil {
		return sourcepkg.Definition{}, dal.JobDefinitionSourceRecord{}, err
	}

	loaded, err := resolver.ResolveDefinition(ctx, sourcepkg.DefinitionRequest{
		Ref:  ref,
		Path: definitionPath,
	})
	if err != nil {
		return sourcepkg.Definition{}, dal.JobDefinitionSourceRecord{}, err
	}

	sourceRec := sourcepkg.NewJobDefinitionSourceRecord(jobID, repoRec.RepositoryID, loaded)

	return loaded, sourceRec, nil
}

func (s *CronService) definitionResolver(rec dal.SourceRepositoryRecord) (sourcepkg.DefinitionResolver, error) {
	if s.newDefinitionResolver != nil {
		return s.newDefinitionResolver(rec)
	}

	return sourcepkg.NewDefinitionResolverFromRecord(rec)
}

func resolveSourceScheduleReference(jobID string, repoRec dal.SourceRepositoryRecord, schedule *CronSchedule) (string, string, error) {
	target, err := sourcepkg.ResolveDefinitionTarget(sourcepkg.DefinitionTargetRequest{
		JobID:      jobID,
		Ref:        schedule.effectiveSourceRef(),
		DefaultRef: repoRec.DefaultRef,
		Path:       schedule.effectiveSourcePath(),
	})
	if err != nil {
		return "", "", err
	}

	return target.Ref, target.Path, nil
}

func (s *CronService) TriggerSchedule(ctx context.Context, sched CronSchedule) error {
	return s.triggerJob(ctx, sched.JobID, &sched)
}

func (s *CronService) dispatchRun(ctx context.Context, job *api.Job, runID, definitionHash string) error {
	job.RunId = &runID
	s.mu.Lock()
	qc := s.queueClient
	ingress := s.ingress
	s.mu.Unlock()

	req := &api.JobRequest{Job: job}
	if _, err := s.attachExecutionEnvelope(ctx, req, runID, s.clock.Now().UnixNano()); err != nil {
		s.logger.Error("Failed to attach execution envelope for cron run %s: %v", runID, err)
	}

	dispatchReq, err := s.recordExecutionPayload(ctx, runID, req, definitionHash)
	if err != nil {
		msg := err.Error()
		if dispatchErr := s.recordDispatchAttemptOutcome(ctx, runID, dal.DispatchEventFailure, &msg); dispatchErr != nil {
			s.logger.Error("Failed to record cron dispatch failure for run %s: %v", runID, dispatchErr)
		}

		return err
	}

	req = dispatchReq

	if ingress == nil {
		endpoints, err := config.CellIngressEndpoints()
		if err != nil {
			msg := err.Error()
			if dispatchErr := s.recordDispatchAttemptOutcome(ctx, runID, dal.DispatchEventFailure, &msg); dispatchErr != nil {
				s.logger.Error("Failed to record cron dispatch failure for run %s: %v", runID, dispatchErr)
			}

			return err
		}

		if database.GlobalAndCellDatabasesAreSplit() {
			qc = nil
		}

		ingress = cell.NewExecutionRouterWithOptions(config.CellID(), qc, endpoints, s.logger, cell.ExecutionRouterOptions{
			TLSConfigForEndpoint: config.CellIngressHTTPClientTLSConfig,
		})
	}

	submission, err := cell.NewExecutionSubmission(req)
	if err != nil {
		msg := err.Error()
		if dispatchErr := s.recordDispatchAttemptOutcome(ctx, runID, dal.DispatchEventFailure, &msg); dispatchErr != nil {
			s.logger.Error("Failed to record cron dispatch failure for run %s: %v", runID, dispatchErr)
		}

		return err
	}

	if err := ingress.SubmitExecution(ctx, submission); err != nil {
		msg := err.Error()
		if dispatchErr := s.recordDispatchAttemptOutcome(ctx, runID, dal.DispatchEventFailure, &msg); dispatchErr != nil {
			s.logger.Error("Failed to record cron dispatch failure for run %s: %v", runID, dispatchErr)
		}

		return err
	}

	if err := s.recordDispatchAttemptOutcome(ctx, runID, dal.DispatchEventSuccess, nil); err != nil {
		s.logger.Error("TouchDispatched after enqueue for run %s: %v", runID, err)
		msg := "touch dispatched: " + err.Error()
		s.recordDispatchEvent(ctx, runID, dal.DispatchEventFailure, &msg)
		return nil
	}

	return nil
}

func (s *CronService) attachExecutionEnvelope(ctx context.Context, req *api.JobRequest, runID string, createdAtUnixNano int64) (*cell.ExecutionEnvelope, error) {
	dispatch, err := s.runs.GetPendingExecution(ctx, runID)
	if err != nil {
		return nil, err
	}

	deadline, err := s.runs.EnsureExecutionStartDeadline(ctx, dispatch.ExecutionID, dispatchmeta.DeadlineUnixNano(s.clock.Now(), config.DispatchStartTTL()))
	if err != nil {
		return nil, err
	}

	dispatch.StartDeadlineUnixNano = deadline
	return cell.AttachExecutionEnvelopeWithActions(req, dispatch, createdAtUnixNano, s.actionResolver)
}

func (s *CronService) recordTriggerInvocation(ctx context.Context, jobID string, schedule *CronSchedule) (string, error) {
	if s.triggers == nil {
		return "", nil
	}

	var triggerID *int64
	payload := map[string]string{
		"job_id":    jobID,
		"triggered": s.clock.Now().UTC().Format(time.RFC3339Nano),
	}

	if schedule != nil {
		if schedule.TriggerID > 0 {
			v := schedule.TriggerID
			triggerID = &v
		}

		payload["schedule_id"] = strconv.FormatInt(schedule.ID, 10)
		if schedule.ScheduleID != "" {
			payload["configured_schedule_id"] = schedule.ScheduleID
		}

		payload["cron_spec"] = schedule.CronSpec
		payload["next_run_at"] = schedule.NextRunAt.UTC().Format(time.RFC3339Nano)
		if schedule.SourceRepositoryID != "" {
			payload["source_repository_id"] = schedule.SourceRepositoryID
			payload["source_ref"] = schedule.effectiveSourceRef()
			payload["source_path"] = schedule.effectiveSourcePath()
			if schedule.SourceOverrideRef != "" {
				payload["source_override_ref"] = schedule.SourceOverrideRef
			}

			if schedule.SourceOverridePath != "" {
				payload["source_override_path"] = schedule.SourceOverridePath
			}

			if schedule.SourceOverrideReason != "" {
				payload["source_override_reason"] = schedule.SourceOverrideReason
			}
		}
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal cron trigger payload: %w", err)
	}

	rec, err := s.triggers.Record(ctx, dal.TriggerInvocation{
		TriggerID:          triggerID,
		JobID:              jobID,
		TriggerType:        dal.TriggerTypeCron,
		TriggerPayloadHash: dal.PayloadHash(string(payloadJSON)),
		RequestedCells:     []string{config.CellID()},
	})

	if err != nil {
		return "", fmt.Errorf("record cron trigger invocation: %w", err)
	}

	return rec.InvocationID, nil
}

func (s *CronService) recordExecutionPayload(ctx context.Context, runID string, req *api.JobRequest, definitionHash string) (*api.JobRequest, error) {
	return cell.RecordExecutionHandoffPayload(ctx, s.runs, runID, req, definitionHash)
}

func (s *CronService) ClaimDue(ctx context.Context, scheduleID int64, observedNextRun time.Time, claimToken string, claimedUntil, now time.Time) (bool, error) {
	return s.schedules.ClaimDue(ctx, scheduleID, observedNextRun, claimToken, claimedUntil, now)
}

func (s *CronService) CompleteClaim(ctx context.Context, scheduleID int64, claimToken string, nextRun time.Time) (bool, error) {
	return s.schedules.CompleteClaim(ctx, scheduleID, claimToken, nextRun)
}

func (s *CronService) ReleaseClaim(ctx context.Context, scheduleID int64, claimToken string) {
	if err := s.schedules.ReleaseClaim(ctx, scheduleID, claimToken); err != nil {
		s.logger.Error("Failed to release cron claim for schedule %d: %v", scheduleID, err)
	}
}

func (s *CronService) ProcessSchedules(ctx context.Context) error {
	schedules, err := s.GetReadySchedules(ctx)
	if err != nil {
		return err
	}

	if len(schedules) == 0 {
		return nil
	}

	now := s.clock.Now()
	s.logger.Info("Processing %d schedule(s)", len(schedules))

	for _, sched := range schedules {
		nextRun, err := s.CalculateNextRun(sched.CronSpec, now)
		if err != nil {
			s.logger.Error("Invalid cron spec for job %s: %v", sched.JobID, err)
			continue
		}

		claimToken := s.nextClaimToken(sched)
		claimedUntil := now.Add(s.effectiveClaimTTL())
		claimed, err := s.ClaimDue(ctx, sched.ID, sched.NextRunAt, claimToken, claimedUntil, now)
		if err != nil {
			s.logger.Error("Failed to claim schedule for job %s: %v", sched.JobID, err)
			continue
		}

		if !claimed {
			s.logger.Info("Skipping job %s: schedule was already claimed or advanced", sched.JobID)
			continue
		}

		s.logger.Info("Triggering job %s (spec: %q)", sched.JobID, sched.CronSpec)

		if err := s.TriggerSchedule(ctx, sched); err != nil {
			s.logger.Error("Failed to trigger job %s after claiming schedule: %v", sched.JobID, err)
			s.ReleaseClaim(ctx, sched.ID, claimToken)
			continue
		}

		completed, err := s.CompleteClaim(ctx, sched.ID, claimToken, nextRun)
		if err != nil {
			s.logger.Error("Failed to advance next_run_at for job %s: %v", sched.JobID, err)
			continue
		}

		if !completed {
			s.logger.Error("Failed to advance next_run_at for job %s: schedule claim was lost", sched.JobID)
			continue
		}

		s.logger.Info("Job %s triggered successfully, next run at %v", sched.JobID, nextRun)
	}

	return nil
}

func (s *CronService) WaitTimeToNextMinute() time.Duration {
	now := s.clock.Now()
	nextMinute := now.Truncate(time.Minute).Add(time.Minute)
	return nextMinute.Sub(now)
}

func (s *CronService) Run(ctx context.Context) error {
	wait := s.WaitTimeToNextMinute()
	s.logger.Info("Waiting %v until next minute boundary", wait)

	select {
	case <-ctx.Done():
		return nil
	case <-time.After(wait):
	}

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	s.logger.Info("Cron service started, polling every 60 seconds")

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Cron service shutting down")
			return nil
		case <-ticker.C:
			if err := s.ProcessSchedules(ctx); err != nil {
				s.logger.Error("Schedule processing failed: %v", err)
			}
		}
	}
}
