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
	err := r.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM cron_trigger_specs cts
		JOIN job_triggers jt ON jt.id = cts.trigger_id
		WHERE jt.enabled
	`).Scan(&count)

	if err != nil {
		return 0, normalizeSQLError(err)
	}

	return count, nil
}

func (r *SQLSchedulesRepository) CronScheduleSummary(ctx context.Context, at time.Time) (CronScheduleSummary, error) {
	atText := at.UTC().Format(time.RFC3339)
	var summary CronScheduleSummary
	var oldestDue sql.NullString
	err := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN cts.next_run_at <= ? AND (cts.claimed_until IS NULL OR cts.claimed_until <= ?) THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN cts.next_run_at <= ? AND cts.claimed_until > ? THEN 1 ELSE 0 END), 0),
			MIN(CASE WHEN cts.next_run_at <= ? THEN cts.next_run_at ELSE NULL END)
		FROM cron_trigger_specs cts
		JOIN job_triggers jt ON jt.id = cts.trigger_id
		WHERE jt.enabled
	`), atText, atText, atText, atText, atText).Scan(&summary.ScheduleCount, &summary.DueCount, &summary.ClaimedCount, &oldestDue)

	if err != nil {
		return CronScheduleSummary{}, normalizeSQLError(err)
	}

	if oldestDue.Valid && oldestDue.String != "" {
		parsed, err := time.Parse(time.RFC3339, oldestDue.String)
		if err != nil {
			return CronScheduleSummary{}, fmt.Errorf("parse oldest due cron schedule %q: %w", oldestDue.String, err)
		}
		summary.OldestDueAt = &parsed
	}

	return summary, nil
}

func (r *SQLSchedulesRepository) GetReady(ctx context.Context, at time.Time) ([]CronSchedule, error) {
	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT cts.id, cts.trigger_id, jt.job_id, cts.cron_spec, cts.next_run_at
		FROM cron_trigger_specs cts
		JOIN job_triggers jt ON jt.id = cts.trigger_id
		WHERE cts.next_run_at <= ?
		  AND (cts.claimed_until IS NULL OR cts.claimed_until <= ?)
		  AND jt.enabled
	`), at.Format(time.RFC3339), at.Format(time.RFC3339))

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []CronSchedule
	for rows.Next() {
		var sched CronSchedule
		var nextRunAt string
		if err := rows.Scan(&sched.ID, &sched.TriggerID, &sched.JobID, &sched.CronSpec, &nextRunAt); err != nil {
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
			UPDATE cron_trigger_specs
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
			UPDATE cron_trigger_specs
			SET next_run_at = ?, claim_token = NULL, claimed_until = NULL, updated_at = CURRENT_TIMESTAMP
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
			UPDATE cron_trigger_specs
			SET claim_token = NULL, claimed_until = NULL, updated_at = CURRENT_TIMESTAMP
			WHERE id = ? AND claim_token = ?
		`),
		scheduleID,
		claimToken,
	)

	return normalizeSQLError(err)
}
