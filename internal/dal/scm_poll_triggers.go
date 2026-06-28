package dal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type SQLSCMPollTriggersRepository struct {
	db *sql.DB
}

func (r *SQLSCMPollTriggersRepository) GetReady(ctx context.Context, at time.Time, limit int) ([]SCMPollTriggerSpec, error) {
	if limit <= 0 {
		limit = 100
	}

	atText := at.UTC().Format(time.RFC3339)
	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT
			spts.id,
			spts.trigger_id,
			jt.job_id,
			spts.provider,
			spts.base_url,
			spts.project,
			spts.branch,
			spts.query,
			spts.interval_seconds,
			spts.next_poll_at,
			spts.cursor
		FROM scm_poll_trigger_specs spts
		JOIN job_triggers jt ON jt.id = spts.trigger_id
		WHERE spts.next_poll_at <= ?
		  AND (spts.claimed_until IS NULL OR spts.claimed_until <= ?)
		  AND jt.enabled
		ORDER BY spts.next_poll_at ASC, spts.id ASC
		LIMIT ?
	`), atText, atText, limit)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []SCMPollTriggerSpec
	for rows.Next() {
		var spec SCMPollTriggerSpec
		var intervalSeconds int64
		var nextPollAt string
		if err := rows.Scan(
			&spec.ID,
			&spec.TriggerID,
			&spec.JobID,
			&spec.Provider,
			&spec.BaseURL,
			&spec.Project,
			&spec.Branch,
			&spec.Query,
			&intervalSeconds,
			&nextPollAt,
			&spec.Cursor,
		); err != nil {
			return nil, normalizeSQLError(err)
		}

		parsed, err := time.Parse(time.RFC3339, nextPollAt)
		if err != nil {
			return nil, fmt.Errorf("parse scm next_poll_at %q: %w", nextPollAt, err)
		}

		spec.NextPollAt = parsed
		spec.Interval = time.Duration(intervalSeconds) * time.Second
		out = append(out, spec)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return out, nil
}

func (r *SQLSCMPollTriggersRepository) ClaimDue(ctx context.Context, specID int64, observedNextPoll time.Time, claimToken string, claimedUntil, now time.Time) (bool, error) {
	result, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`
			UPDATE scm_poll_trigger_specs
			SET claim_token = ?, claimed_until = ?, updated_at = CURRENT_TIMESTAMP
			WHERE id = ?
			  AND next_poll_at = ?
			  AND (claimed_until IS NULL OR claimed_until <= ?)
		`),
		claimToken,
		claimedUntil.UTC().Format(time.RFC3339),
		specID,
		observedNextPoll.UTC().Format(time.RFC3339),
		now.UTC().Format(time.RFC3339),
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

func (r *SQLSCMPollTriggersRepository) CompleteClaim(ctx context.Context, specID int64, claimToken string, nextPoll time.Time, cursor string) (bool, error) {
	result, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`
			UPDATE scm_poll_trigger_specs
			SET next_poll_at = ?, cursor = ?, claim_token = NULL, claimed_until = NULL, last_poll_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
			WHERE id = ? AND claim_token = ?
		`),
		nextPoll.UTC().Format(time.RFC3339),
		cursor,
		specID,
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

func (r *SQLSCMPollTriggersRepository) ReleaseClaim(ctx context.Context, specID int64, claimToken string) error {
	_, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`
			UPDATE scm_poll_trigger_specs
			SET claim_token = NULL, claimed_until = NULL, updated_at = CURRENT_TIMESTAMP
			WHERE id = ? AND claim_token = ?
		`),
		specID,
		claimToken,
	)

	return normalizeSQLError(err)
}

func (r *SQLSCMPollTriggersRepository) RecordEvent(ctx context.Context, event SCMTriggerEvent) (SCMTriggerEventRecord, bool, error) {
	event.TriggerID = normalizeTriggerID(event.TriggerID)
	event.EventKey = strings.TrimSpace(event.EventKey)
	if event.TriggerID <= 0 {
		return SCMTriggerEventRecord{}, false, fmt.Errorf("%w: trigger_id is required", ErrConflict)
	}

	if event.EventKey == "" {
		return SCMTriggerEventRecord{}, false, fmt.Errorf("%w: event_key is required", ErrConflict)
	}

	var runID any
	if strings.TrimSpace(event.RunID) != "" {
		runID = strings.TrimSpace(event.RunID)
	}

	result, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO scm_trigger_events (trigger_id, event_key, run_id, payload_json)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(trigger_id, event_key) DO NOTHING
	`), event.TriggerID, event.EventKey, runID, event.PayloadJSON)

	if err != nil {
		return SCMTriggerEventRecord{}, false, normalizeSQLError(err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return SCMTriggerEventRecord{}, false, normalizeSQLError(err)
	}

	rec, err := r.getEvent(ctx, event.TriggerID, event.EventKey)
	if err != nil {
		return SCMTriggerEventRecord{}, false, err
	}

	return rec, rows == 1, nil
}

func (r *SQLSCMPollTriggersRepository) getEvent(ctx context.Context, triggerID int64, eventKey string) (SCMTriggerEventRecord, error) {
	var rec SCMTriggerEventRecord
	var runID sql.NullString
	if err := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT trigger_id, event_key, run_id, payload_json
		FROM scm_trigger_events
		WHERE trigger_id = ? AND event_key = ?
	`), triggerID, eventKey).Scan(&rec.TriggerID, &rec.EventKey, &runID, &rec.PayloadJSON); err != nil {
		return SCMTriggerEventRecord{}, normalizeSQLError(err)
	}

	if runID.Valid {
		rec.RunID = &runID.String
	}

	return rec, nil
}

func normalizeTriggerID(id int64) int64 {
	if id < 0 {
		return 0
	}

	return id
}

var _ SCMPollTriggersRepository = (*SQLSCMPollTriggersRepository)(nil)
