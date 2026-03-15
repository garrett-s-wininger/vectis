package cron

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/registry"
)

type CronSchedule struct {
	ID        int64
	JobID     string
	CronSpec  string
	NextRunAt time.Time
}

type CronService struct {
	db          *sql.DB
	logger      interfaces.Logger
	queueClient interfaces.QueueService
	parser      cron.Parser
}

func NewCronService(logger interfaces.Logger, db *sql.DB) *CronService {
	return &CronService{
		db:     db,
		logger: logger,
		parser: cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow),
	}
}

func (s *CronService) SetQueueClient(client interfaces.QueueService) {
	s.queueClient = client
}

func (s *CronService) ConnectToQueue(ctx context.Context) error {
	registryClient, err := registry.New(ctx, s.logger)
	if err != nil {
		return fmt.Errorf("failed to connect to registry: %w", err)
	}
	defer registryClient.Close()

	queueAddr, err := registryClient.Address(ctx, api.Component_COMPONENT_QUEUE)
	if err != nil {
		return fmt.Errorf("failed to get queue address: %w", err)
	}

	conn, err := grpc.NewClient(queueAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to queue: %w", err)
	}

	s.queueClient = interfaces.NewQueueService(api.NewQueueServiceClient(conn))
	s.logger.Info("Connected to queue at %s", queueAddr)
	return nil
}

func (s *CronService) GetReadySchedules(ctx context.Context) ([]CronSchedule, error) {
	query := `
		SELECT id, job_id, cron_spec, next_run_at 
		FROM job_cron_schedules 
		WHERE next_run_at <= ?
	`

	rows, err := s.db.QueryContext(ctx, query, time.Now())
	if err != nil {
		return nil, fmt.Errorf("failed to query schedules: %w", err)
	}

	defer rows.Close()

	var schedules []CronSchedule
	for rows.Next() {
		var sched CronSchedule
		var nextRunAt string
		if err := rows.Scan(&sched.ID, &sched.JobID, &sched.CronSpec, &nextRunAt); err != nil {
			s.logger.Error("Failed to scan schedule row: %v", err)
			continue
		}

		parsedTime, err := time.Parse(time.RFC3339, nextRunAt)
		if err != nil {
			s.logger.Error("Failed to parse next_run_at time %s: %v", nextRunAt, err)
			continue
		}

		sched.NextRunAt = parsedTime
		schedules = append(schedules, sched)
	}

	return schedules, rows.Err()
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
	var definitionJSON string
	err := s.db.QueryRowContext(ctx,
		"SELECT definition_json FROM stored_jobs WHERE job_id = ?", jobID).Scan(&definitionJSON)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("job not found: %s", jobID)
		}
		return nil, fmt.Errorf("database error: %w", err)
	}

	var job api.Job
	if err := json.Unmarshal([]byte(definitionJSON), &job); err != nil {
		return nil, fmt.Errorf("failed to parse job definition: %w", err)
	}

	return &job, nil
}

func (s *CronService) TriggerJob(ctx context.Context, jobID string) error {
	job, err := s.GetJobDefinition(ctx, jobID)
	if err != nil {
		return err
	}

	job.Id = &jobID
	_, err = s.queueClient.Enqueue(ctx, job)
	return err
}

func (s *CronService) UpdateNextRun(ctx context.Context, scheduleID int64, nextRun time.Time) error {
	_, err := s.db.ExecContext(ctx,
		"UPDATE job_cron_schedules SET next_run_at = ? WHERE id = ?",
		nextRun, scheduleID)

	return err
}

func (s *CronService) ProcessSchedules(ctx context.Context) error {
	schedules, err := s.GetReadySchedules(ctx)
	if err != nil {
		return err
	}

	if len(schedules) == 0 {
		return nil
	}

	now := time.Now()
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

		s.logger.Info("Triggering job %s (spec: %q)", sched.JobID, sched.CronSpec)

		if err := s.TriggerJob(ctx, sched.JobID); err != nil {
			s.logger.Error("Failed to trigger job %s: %v", sched.JobID, err)
			// NOTE(garrett): Leave next_run_at unchanged to preserve drift audit trail
			continue
		}

		nextRun, err := s.CalculateNextRun(sched.CronSpec, now)
		if err != nil {
			s.logger.Error("Failed to calculate next run for job %s: %v", sched.JobID, err)
			continue
		}

		if err := s.UpdateNextRun(ctx, sched.ID, nextRun); err != nil {
			s.logger.Error("Failed to update next_run_at for job %s: %v", sched.JobID, err)
			continue
		}

		s.logger.Info("Job %s triggered successfully, next run at %v", sched.JobID, nextRun)
	}

	return nil
}

func (s *CronService) Run(ctx context.Context) error {
	// TODO(garrett): First trigger on actual minute boundary.
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	s.logger.Info("Cron service started, polling every 60 seconds")

	// Process immediately on start
	if err := s.ProcessSchedules(ctx); err != nil {
		s.logger.Error("Initial schedule processing failed: %v", err)
	}

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
