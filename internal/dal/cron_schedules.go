package dal

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type SQLSchedulesRepository struct {
	db *sql.DB
}

func (r *SQLSchedulesRepository) CountCronSchedules(ctx context.Context) (int64, error) {
	var count int64
	err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM job_cron_schedules").Scan(&count)

	if err != nil {
		return 0, normalizeSQLError(err)
	}

	return count, nil
}

func (r *SQLSchedulesRepository) GetReady(ctx context.Context, at time.Time) ([]CronSchedule, error) {
	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT id, job_id, cron_spec, next_run_at
		FROM job_cron_schedules
		WHERE next_run_at <= ?
		  AND (claimed_until IS NULL OR claimed_until <= ?)
	`), at.Format(time.RFC3339), at.Format(time.RFC3339))

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []CronSchedule
	for rows.Next() {
		var sched CronSchedule
		var nextRunAt string
		if err := rows.Scan(&sched.ID, &sched.JobID, &sched.CronSpec, &nextRunAt); err != nil {
			return nil, normalizeSQLError(err)
		}

		parsedTime, err := time.Parse(time.RFC3339, nextRunAt)
		if err != nil {
			return nil, fmt.Errorf("parse next_run_at %q: %w", nextRunAt, err)
		}

		sched.NextRunAt = parsedTime
		out = append(out, sched)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return out, nil
}

func (r *SQLSchedulesRepository) ClaimDue(ctx context.Context, scheduleID int64, observedNextRun time.Time, claimToken string, claimedUntil, now time.Time) (bool, error) {
	result, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`
			UPDATE job_cron_schedules
			SET claim_token = ?, claimed_until = ?
			WHERE id = ?
			  AND next_run_at = ?
			  AND (claimed_until IS NULL OR claimed_until <= ?)
		`),
		claimToken,
		claimedUntil.Format(time.RFC3339),
		scheduleID,
		observedNextRun.Format(time.RFC3339),
		now.Format(time.RFC3339),
	)

	if err != nil {
		return false, normalizeSQLError(err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, normalizeSQLError(err)
	}

	return rows == 1, nil
}

func (r *SQLSchedulesRepository) CompleteClaim(ctx context.Context, scheduleID int64, claimToken string, nextRun time.Time) (bool, error) {
	result, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`
			UPDATE job_cron_schedules
			SET next_run_at = ?, claim_token = NULL, claimed_until = NULL
			WHERE id = ? AND claim_token = ?
		`),
		nextRun.Format(time.RFC3339),
		scheduleID,
		claimToken,
	)

	if err != nil {
		return false, normalizeSQLError(err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, normalizeSQLError(err)
	}

	return rows == 1, nil
}

func (r *SQLSchedulesRepository) ReleaseClaim(ctx context.Context, scheduleID int64, claimToken string) error {
	_, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`
			UPDATE job_cron_schedules
			SET claim_token = NULL, claimed_until = NULL
			WHERE id = ? AND claim_token = ?
		`),
		scheduleID,
		claimToken,
	)

	return normalizeSQLError(err)
}
