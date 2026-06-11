package dal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type SQLSchedulesRepository struct {
	db *sql.DB
}

func (r *SQLSchedulesRepository) CreateCronSchedule(ctx context.Context, rec CronScheduleRecord) (CronScheduleRecord, error) {
	rec, err := normalizeCronScheduleRecord(rec)
	if err != nil {
		return CronScheduleRecord{}, err
	}

	if rec.NextRunAt.IsZero() {
		rec.NextRunAt = time.Now().UTC()
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return CronScheduleRecord{}, err
	}
	defer func() { _ = tx.Rollback() }()

	var triggerID int64
	if err := tx.QueryRowContext(ctx, rebindQueryForPgx(`
		INSERT INTO job_triggers (
			job_id,
			trigger_type,
			source_repository_id,
			source_ref,
			source_path,
			enabled
		)
		VALUES (?, ?, ?, ?, ?, ?)
		RETURNING id
	`),
		rec.JobID,
		TriggerTypeCron,
		rec.SourceRepositoryID,
		rec.SourceRef,
		rec.SourcePath,
		rec.Enabled,
	).Scan(&triggerID); err != nil {
		return CronScheduleRecord{}, normalizeSQLError(err)
	}

	if _, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO cron_trigger_specs (
			trigger_id,
			schedule_id,
			cron_spec,
			next_run_at
		)
		VALUES (?, ?, ?, ?)
	`),
		triggerID,
		rec.ScheduleID,
		rec.CronSpec,
		rec.NextRunAt.UTC().Format(time.RFC3339),
	); err != nil {
		return CronScheduleRecord{}, normalizeSQLError(err)
	}

	if err := tx.Commit(); err != nil {
		return CronScheduleRecord{}, err
	}

	return r.GetCronScheduleByScheduleID(ctx, rec.ScheduleID)
}

func (r *SQLSchedulesRepository) UpdateCronSchedule(ctx context.Context, rec CronScheduleRecord) (CronScheduleRecord, error) {
	rec, err := normalizeCronScheduleRecord(rec)
	if err != nil {
		return CronScheduleRecord{}, err
	}

	existing, err := r.GetCronScheduleByScheduleID(ctx, rec.ScheduleID)
	if err != nil {
		return CronScheduleRecord{}, err
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return CronScheduleRecord{}, err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE job_triggers
		SET
			job_id = ?,
			source_repository_id = ?,
			source_ref = ?,
			source_path = ?,
			enabled = ?,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`),
		rec.JobID,
		rec.SourceRepositoryID,
		rec.SourceRef,
		rec.SourcePath,
		rec.Enabled,
		existing.TriggerID,
	); err != nil {
		return CronScheduleRecord{}, normalizeSQLError(err)
	}

	if rec.NextRunAt.IsZero() {
		if _, err := tx.ExecContext(ctx, rebindQueryForPgx(`
			UPDATE cron_trigger_specs
			SET cron_spec = ?, updated_at = CURRENT_TIMESTAMP
			WHERE id = ?
		`), rec.CronSpec, existing.ID); err != nil {
			return CronScheduleRecord{}, normalizeSQLError(err)
		}
	} else {
		if _, err := tx.ExecContext(ctx, rebindQueryForPgx(`
			UPDATE cron_trigger_specs
			SET
				cron_spec = ?,
				next_run_at = ?,
				claim_token = NULL,
				claimed_until = NULL,
				updated_at = CURRENT_TIMESTAMP
			WHERE id = ?
		`), rec.CronSpec, rec.NextRunAt.UTC().Format(time.RFC3339), existing.ID); err != nil {
			return CronScheduleRecord{}, normalizeSQLError(err)
		}
	}

	if err := tx.Commit(); err != nil {
		return CronScheduleRecord{}, err
	}

	return r.GetCronScheduleByScheduleID(ctx, rec.ScheduleID)
}

func (r *SQLSchedulesRepository) GetCronScheduleByScheduleID(ctx context.Context, scheduleID string) (CronScheduleRecord, error) {
	scheduleID = strings.TrimSpace(scheduleID)
	if scheduleID == "" {
		return CronScheduleRecord{}, fmt.Errorf("%w: schedule_id is required", ErrNotFound)
	}

	query := `
		SELECT
			cts.id,
			cts.trigger_id,
			COALESCE(cts.schedule_id, ''),
			jt.job_id,
			cts.cron_spec,
			cts.next_run_at,
			COALESCE(jt.source_repository_id, ''),
			COALESCE(jt.source_ref, ''),
			COALESCE(jt.source_path, ''),
			jt.enabled
		FROM cron_trigger_specs cts
		JOIN job_triggers jt ON jt.id = cts.trigger_id
		WHERE cts.schedule_id = ?`

	rec, err := r.scanCronScheduleRecordRow(r.db.QueryRowContext(ctx, rebindQueryForPgx(query), scheduleID))
	if err != nil {
		if err == sql.ErrNoRows {
			return CronScheduleRecord{}, fmt.Errorf("%w: cron schedule %s", ErrNotFound, scheduleID)
		}

		return CronScheduleRecord{}, normalizeSQLError(err)
	}

	return rec, nil
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
		SELECT
			cts.id,
			cts.trigger_id,
			COALESCE(cts.schedule_id, ''),
			jt.job_id,
			cts.cron_spec,
			cts.next_run_at,
			COALESCE(jt.source_repository_id, ''),
			COALESCE(jt.source_ref, ''),
			COALESCE(jt.source_path, '')
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
		if err := rows.Scan(
			&sched.ID,
			&sched.TriggerID,
			&sched.ScheduleID,
			&sched.JobID,
			&sched.CronSpec,
			&nextRunAt,
			&sched.SourceRepositoryID,
			&sched.SourceRef,
			&sched.SourcePath,
		); err != nil {
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

func (r *SQLSchedulesRepository) scanCronScheduleRecordRow(row *sql.Row) (CronScheduleRecord, error) {
	var rec CronScheduleRecord
	var nextRunAt string
	if err := row.Scan(
		&rec.ID,
		&rec.TriggerID,
		&rec.ScheduleID,
		&rec.JobID,
		&rec.CronSpec,
		&nextRunAt,
		&rec.SourceRepositoryID,
		&rec.SourceRef,
		&rec.SourcePath,
		&rec.Enabled,
	); err != nil {
		return CronScheduleRecord{}, err
	}

	parsedTime, err := time.Parse(time.RFC3339, nextRunAt)
	if err != nil {
		return CronScheduleRecord{}, fmt.Errorf("parse next_run_at %q: %w", nextRunAt, err)
	}

	rec.NextRunAt = parsedTime
	return rec, nil
}

func normalizeCronScheduleRecord(rec CronScheduleRecord) (CronScheduleRecord, error) {
	rec.ScheduleID = strings.TrimSpace(rec.ScheduleID)
	rec.JobID = strings.TrimSpace(rec.JobID)
	rec.CronSpec = strings.TrimSpace(rec.CronSpec)
	rec.SourceRepositoryID = strings.TrimSpace(rec.SourceRepositoryID)
	rec.SourceRef = strings.TrimSpace(rec.SourceRef)
	rec.SourcePath = strings.TrimSpace(rec.SourcePath)

	if rec.ScheduleID == "" {
		return CronScheduleRecord{}, fmt.Errorf("%w: schedule_id is required", ErrConflict)
	}

	if rec.JobID == "" {
		return CronScheduleRecord{}, fmt.Errorf("%w: job_id is required", ErrConflict)
	}

	if rec.CronSpec == "" {
		return CronScheduleRecord{}, fmt.Errorf("%w: cron_spec is required", ErrConflict)
	}

	return rec, nil
}
