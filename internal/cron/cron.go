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
	"vectis/internal/backoff"
	"vectis/internal/cell"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/queueclient"

	"google.golang.org/protobuf/encoding/protojson"
)

type CronSchedule struct {
	ID        int64
	TriggerID int64
	JobID     string
	CronSpec  string
	NextRunAt time.Time
}

type CronService struct {
	jobs         dal.JobsRepository
	runs         dal.RunsRepository
	schedules    dal.SchedulesRepository
	dispatch     dal.DispatchEventsRepository
	triggers     dal.TriggerInvocationsRepository
	logger       interfaces.Logger
	queueClient  interfaces.QueueService
	queueClose   func()
	ingress      cell.ExecutionIngress
	parser       cron.Parser
	clock        interfaces.Clock
	retryMetrics backoff.RetryMetrics
	instanceID   string
	claimTTL     time.Duration
	claimSeq     atomic.Uint64
	mu           sync.Mutex
}

func NewCronService(logger interfaces.Logger, db *sql.DB) *CronService {
	repos := dal.NewSQLRepositoriesWithCellID(db, config.CellID())
	s := NewCronServiceWithRepositories(logger, repos.Jobs(), repos.Runs(), repos.Schedules())
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

func (s *CronService) SetTriggerInvocations(triggers dal.TriggerInvocationsRepository) {
	s.triggers = triggers
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
			ID:        sched.ID,
			TriggerID: sched.TriggerID,
			JobID:     sched.JobID,
			CronSpec:  sched.CronSpec,
			NextRunAt: sched.NextRunAt,
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

func (s *CronService) GetJobDefinition(ctx context.Context, jobID string) (*api.Job, error) {
	job, _, _, err := s.getJobDefinitionWithVersion(ctx, jobID)
	return job, err
}

func (s *CronService) getJobDefinitionWithVersion(ctx context.Context, jobID string) (*api.Job, int, string, error) {
	definitionJSON, version, err := s.jobs.GetDefinition(ctx, jobID)
	if err != nil {
		if dal.IsNotFound(err) {
			return nil, 0, "", fmt.Errorf("job not found: %s", jobID)
		}

		return nil, 0, "", fmt.Errorf("database error: %w", err)
	}

	var job api.Job
	if err := json.Unmarshal([]byte(definitionJSON), &job); err != nil {
		return nil, 0, "", fmt.Errorf("failed to parse job definition: %w", err)
	}

	return &job, version, dal.DefinitionHash(definitionJSON), nil
}

func (s *CronService) TriggerJob(ctx context.Context, jobID string) error {
	return s.triggerJob(ctx, jobID, nil)
}

func (s *CronService) triggerJob(ctx context.Context, jobID string, schedule *CronSchedule) error {
	job, definitionVersion, definitionHash, err := s.getJobDefinitionWithVersion(ctx, jobID)
	if err != nil {
		return err
	}

	job.Id = &jobID

	invocationID, err := s.recordTriggerInvocation(ctx, jobID, schedule)
	if err != nil {
		return err
	}

	var runID string
	if schedule != nil {
		var created bool
		runID, _, created, err = s.runs.CreateScheduledRun(ctx, schedule.ID, schedule.NextRunAt, jobID, definitionVersion, dal.RunAuditMetadata{TriggerInvocationID: invocationID})
		if err != nil {
			return err
		}

		if !created {
			s.logger.Info("Reusing existing cron run %s for schedule %d at %v", runID, schedule.ID, schedule.NextRunAt)
		}
	} else {
		created, err := s.runs.CreateRunsInCellsWithAudit(ctx, jobID, nil, definitionVersion, nil, dal.RunAuditMetadata{TriggerInvocationID: invocationID})
		if err != nil {
			return err
		}

		if len(created) == 0 {
			return fmt.Errorf("%w: no cron run created", dal.ErrNotFound)
		}

		runID = created[0].RunID
	}

	return s.dispatchRun(ctx, job, runID, definitionHash)
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
	if _, err := cell.AttachPendingExecutionEnvelope(ctx, s.runs, req, runID, s.clock.Now().UnixNano()); err != nil {
		s.logger.Error("Failed to attach execution envelope for cron run %s: %v", runID, err)
	}

	s.recordDispatchEvent(ctx, runID, dal.DispatchEventAttempt, nil)
	dispatchReq, err := s.recordExecutionPayload(ctx, runID, req, definitionHash)
	if err != nil {
		msg := err.Error()
		s.recordDispatchEvent(ctx, runID, dal.DispatchEventFailure, &msg)
		return err
	}

	req = dispatchReq

	if ingress == nil {
		endpoints, err := config.CellIngressEndpoints()
		if err != nil {
			msg := err.Error()
			s.recordDispatchEvent(ctx, runID, dal.DispatchEventFailure, &msg)
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
		s.recordDispatchEvent(ctx, runID, dal.DispatchEventFailure, &msg)
		return err
	}

	if err := ingress.SubmitExecution(ctx, submission); err != nil {
		msg := err.Error()
		s.recordDispatchEvent(ctx, runID, dal.DispatchEventFailure, &msg)
		return err
	}

	if err := s.runs.TouchDispatched(ctx, runID); err != nil {
		s.logger.Error("TouchDispatched after enqueue for run %s: %v", runID, err)
		msg := "touch dispatched: " + err.Error()
		s.recordDispatchEvent(ctx, runID, dal.DispatchEventFailure, &msg)
		return nil
	}

	s.recordDispatchEvent(ctx, runID, dal.DispatchEventSuccess, nil)
	return nil
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
		payload["cron_spec"] = schedule.CronSpec
		payload["next_run_at"] = schedule.NextRunAt.UTC().Format(time.RFC3339Nano)
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
		matches, err := s.ValidateCronSpec(sched.CronSpec, now)
		if err != nil {
			s.logger.Error("Invalid cron spec for job %s: %v", sched.JobID, err)
			continue
		}

		if !matches {
			s.logger.Debug("Skipping job %s: current time %v does not match spec %q",
				sched.JobID, now, sched.CronSpec)
			continue
		}

		nextRun, err := s.CalculateNextRun(sched.CronSpec, now)
		if err != nil {
			s.logger.Error("Failed to calculate next run for job %s: %v", sched.JobID, err)
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
