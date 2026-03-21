package reconciler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/runstore"
)

const MinDispatchGap = 30 * time.Second

type Service struct {
	db          *sql.DB
	logger      interfaces.Logger
	queueClient interfaces.QueueService
	clock       interfaces.Clock
	minGap      time.Duration
}

func NewService(logger interfaces.Logger, db *sql.DB, queue interfaces.QueueService, clock interfaces.Clock) *Service {
	if clock == nil {
		clock = interfaces.SystemClock{}
	}

	return &Service{
		db:          db,
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
	rows, err := s.db.QueryContext(ctx, `
		SELECT run_id, job_id
		FROM job_runs
		WHERE status = 'queued'
			AND (last_dispatched_at IS NULL OR last_dispatched_at < ?)
		ORDER BY id ASC
	`, cutoff)

	if err != nil {
		return fmt.Errorf("query queued runs: %w", err)
	}
	defer rows.Close()

	type row struct {
		runID string
		jobID string
	}

	var batch []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.runID, &r.jobID); err != nil {
			return fmt.Errorf("scan: %w", err)
		}

		batch = append(batch, r)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	for _, r := range batch {
		if err := s.dispatchOne(ctx, r.runID, r.jobID); err != nil {
			s.logger.Error("reconciler: run %s: %v", r.runID, err)
		}
	}

	return nil
}

func (s *Service) dispatchOne(ctx context.Context, runID, jobID string) error {
	var defJSON string
	err := s.db.QueryRowContext(ctx,
		`SELECT definition_json FROM stored_jobs WHERE job_id = ?`, jobID).Scan(&defJSON)

	if err != nil {
		if err == sql.ErrNoRows {
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

	if err := runstore.TouchDispatched(ctx, s.db, runID); err != nil {
		return fmt.Errorf("touch dispatched: %w", err)
	}

	s.logger.Info("reconciler: re-enqueued run %s (job %s)", runID, jobID)
	return nil
}
