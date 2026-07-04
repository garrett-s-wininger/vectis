package dal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

type SQLJobsRepository struct {
	db     *sql.DB
	cellID string
}

func (r *SQLJobsRepository) currentCellID() string {
	return normalizeCellID(r.cellID)
}

func (r *SQLJobsRepository) Create(ctx context.Context, jobID, definitionJSON string, namespaceID int64) error {
	return r.CreateWithTriggers(ctx, jobID, definitionJSON, namespaceID, nil)
}

func (r *SQLJobsRepository) CreateWithTriggers(ctx context.Context, jobID, definitionJSON string, namespaceID int64, triggers []JobTriggerConfig) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	initialVersion, err := nextJobDefinitionVersionTx(ctx, tx, jobID)
	if err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("INSERT INTO stored_jobs (global_id, job_id, namespace_id, current_version, home_cell) VALUES (?, ?, ?, ?, ?)"),
		newGlobalID(),
		jobID,
		namespaceID,
		initialVersion,
		r.currentCellID(),
	); err != nil {
		return normalizeSQLError(err)
	}

	if err := insertJobDefinitionTx(ctx, tx, jobID, initialVersion, definitionJSON); err != nil {
		return err
	}

	if err := r.replaceJobTriggers(ctx, tx, jobID, triggers); err != nil {
		return err
	}

	return tx.Commit()
}

func (r *SQLJobsRepository) CreateDefinitionSnapshot(ctx context.Context, jobID, definitionJSON string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	version, err := nextJobDefinitionVersionTx(ctx, tx, jobID)
	if err != nil {
		return err
	}

	if err := insertJobDefinitionTx(ctx, tx, jobID, version, definitionJSON); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx(`
			INSERT INTO stored_jobs (global_id, job_id, namespace_id, current_version, home_cell)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(job_id) DO UPDATE
			SET current_version = excluded.current_version,
			    updated_at = CURRENT_TIMESTAMP
		`),
		newGlobalID(),
		jobID,
		RootNamespaceID,
		version,
		r.currentCellID(),
	); err != nil {
		return normalizeSQLError(err)
	}

	return tx.Commit()
}

func nextJobDefinitionVersionTx(ctx context.Context, tx *sql.Tx, jobID string) (int, error) {
	var version int
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT COALESCE(MAX(version), 0) + 1 FROM job_definitions WHERE job_id = ?"),
		jobID,
	).Scan(&version); err != nil {
		return 0, normalizeSQLError(err)
	}

	return version, nil
}

func insertJobDefinitionTx(ctx context.Context, tx *sql.Tx, jobID string, version int, definitionJSON string) error {
	definitionHash := DefinitionHash(definitionJSON)
	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("INSERT INTO job_definitions (global_id, job_id, version, definition_json, definition_hash) VALUES (?, ?, ?, ?, ?)"),
		newGlobalID(),
		jobID,
		version,
		definitionJSON,
		definitionHash,
	); err != nil {
		return normalizeSQLError(err)
	}

	return nil
}

func (r *SQLJobsRepository) Delete(ctx context.Context, jobID string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if err := r.deleteAllJobTriggers(ctx, tx, jobID); err != nil {
		return err
	}

	res, err := tx.ExecContext(ctx, rebindQueryForPgx("DELETE FROM stored_jobs WHERE job_id = ?"), jobID)
	if err != nil {
		return normalizeSQLError(err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return normalizeSQLError(err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("%w: job %s", ErrNotFound, jobID)
	}

	return tx.Commit()
}

func (r *SQLJobsRepository) List(ctx context.Context, cursor int64, limit int) ([]JobRecord, int64, error) {
	query := `
		SELECT sj.id, COALESCE(sj.global_id, ''), sj.job_id, sj.namespace_id, jd.definition_json, jd.definition_hash, sj.current_version, sj.home_cell
		FROM stored_jobs sj
		JOIN job_definitions jd ON jd.job_id = sj.job_id AND jd.version = sj.current_version
	`

	args := []any{}
	if cursor > 0 {
		query += " WHERE sj.id > ?"
		args = append(args, cursor)
	}

	query += " ORDER BY sj.id ASC LIMIT ?"
	args = append(args, limit+1)

	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(query), args...)
	if err != nil {
		return nil, 0, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []JobRecord
	var lastID int64
	for rows.Next() {
		var rec JobRecord
		var id int64
		if err := rows.Scan(&id, &rec.GlobalID, &rec.JobID, &rec.NamespaceID, &rec.DefinitionJSON, &rec.DefinitionHash, &rec.Version, &rec.HomeCell); err != nil {
			return nil, 0, normalizeSQLError(err)
		}

		lastID = id
		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, normalizeSQLError(err)
	}

	var nextCursor int64
	if len(out) > limit {
		out = out[:limit]
		nextCursor = lastID
	}

	return out, nextCursor, nil
}

func (r *SQLJobsRepository) ListByNamespace(ctx context.Context, namespaceID int64) ([]JobRecord, error) {
	rows, err := r.db.QueryContext(ctx,
		rebindQueryForPgx(`
			SELECT COALESCE(sj.global_id, ''), sj.job_id, sj.namespace_id, jd.definition_json, jd.definition_hash, sj.current_version, sj.home_cell
			FROM stored_jobs sj
			JOIN job_definitions jd ON jd.job_id = sj.job_id AND jd.version = sj.current_version
			WHERE sj.namespace_id = ?
		`),
		namespaceID,
	)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []JobRecord
	for rows.Next() {
		var rec JobRecord
		if err := rows.Scan(&rec.GlobalID, &rec.JobID, &rec.NamespaceID, &rec.DefinitionJSON, &rec.DefinitionHash, &rec.Version, &rec.HomeCell); err != nil {
			return nil, normalizeSQLError(err)
		}

		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return out, nil
}

func (r *SQLJobsRepository) GetLatestDefinition(ctx context.Context, jobID string) (string, int, error) {
	definitionJSON, version, err := r.getStoredJobDefinition(ctx, jobID)
	if err == nil || !IsNotFound(err) {
		return definitionJSON, version, err
	}

	var latestJSON string
	var latestVersion int
	if err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT definition_json, version FROM job_definitions WHERE job_id = ? ORDER BY version DESC LIMIT 1"),
		jobID,
	).Scan(&latestJSON, &latestVersion); err != nil {
		if err == sql.ErrNoRows {
			return "", 0, fmt.Errorf("%w: job %s", ErrNotFound, jobID)
		}

		return "", 0, normalizeSQLError(err)
	}

	return latestJSON, latestVersion, nil
}

func (r *SQLJobsRepository) getStoredJobDefinition(ctx context.Context, jobID string) (string, int, error) {
	var definitionJSON string
	var version int
	if err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx(`
			SELECT jd.definition_json, sj.current_version
			FROM stored_jobs sj
			JOIN job_definitions jd ON jd.job_id = sj.job_id AND jd.version = sj.current_version
			WHERE sj.job_id = ?
		`),
		jobID,
	).Scan(&definitionJSON, &version); err != nil {
		if err == sql.ErrNoRows {
			return "", 0, fmt.Errorf("%w: job %s", ErrNotFound, jobID)
		}

		return "", 0, normalizeSQLError(err)
	}

	return definitionJSON, version, nil
}

func (r *SQLJobsRepository) GetNamespaceID(ctx context.Context, jobID string) (int64, error) {
	var namespaceID int64
	if err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT namespace_id FROM stored_jobs WHERE job_id = ?"),
		jobID,
	).Scan(&namespaceID); err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("%w: job %s", ErrNotFound, jobID)
		}

		return 0, normalizeSQLError(err)
	}

	return namespaceID, nil
}

func (r *SQLJobsRepository) UpdateDefinition(ctx context.Context, jobID, definitionJSON string) (int, error) {
	return r.updateDefinition(ctx, jobID, definitionJSON, nil, false)
}

func (r *SQLJobsRepository) UpdateDefinitionWithTriggers(ctx context.Context, jobID, definitionJSON string, triggers []JobTriggerConfig) (int, error) {
	return r.updateDefinition(ctx, jobID, definitionJSON, triggers, true)
}

func (r *SQLJobsRepository) updateDefinition(ctx context.Context, jobID, definitionJSON string, triggers []JobTriggerConfig, replaceTriggers bool) (int, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	var currentVersion int
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT current_version FROM stored_jobs WHERE job_id = ?"),
		jobID,
	).Scan(&currentVersion); err != nil {
		return 0, normalizeSQLError(err)
	}

	newVersion := currentVersion + 1
	if err := insertJobDefinitionTx(ctx, tx, jobID, newVersion, definitionJSON); err != nil {
		return 0, err
	}

	result, err := tx.ExecContext(ctx,
		rebindQueryForPgx("UPDATE stored_jobs SET current_version = ?, updated_at = CURRENT_TIMESTAMP WHERE job_id = ? AND current_version = ?"),
		newVersion,
		jobID,
		currentVersion,
	)
	if err != nil {
		return 0, normalizeSQLError(err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return 0, normalizeSQLError(err)
	}

	if rows == 0 {
		return 0, fmt.Errorf("%w: job %s was updated concurrently", ErrConflict, jobID)
	}

	if replaceTriggers {
		if err := r.replaceJobTriggers(ctx, tx, jobID, triggers); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return newVersion, nil
}

func (r *SQLJobsRepository) replaceJobTriggers(ctx context.Context, tx *sql.Tx, jobID string, triggers []JobTriggerConfig) error {
	if err := r.deleteJobTriggerSpecs(ctx, tx, jobID); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("UPDATE job_triggers SET enabled = ?, updated_at = CURRENT_TIMESTAMP WHERE job_id = ?"),
		false,
		jobID,
	); err != nil {
		return normalizeSQLError(err)
	}

	if len(triggers) == 0 {
		return nil
	}

	now := time.Now().UTC()
	nextRunFrom := now
	nextPollAt := now.Format(time.RFC3339)
	for _, trigger := range triggers {
		trigger = normalizeJobTriggerConfig(trigger)

		switch {
		case trigger.Manual != nil:
			if _, err := r.ensureJobTrigger(ctx, tx, jobID, TriggerTypeManual, trigger); err != nil {
				return err
			}
		case trigger.Cron != nil:
			triggerID, err := r.ensureJobTrigger(ctx, tx, jobID, TriggerTypeCron, trigger)
			if err != nil {
				return err
			}

			nextRunAt, err := nextCronRun(trigger.Cron.Spec, nextRunFrom)
			if err != nil {
				return err
			}

			if _, err := tx.ExecContext(ctx,
				rebindQueryForPgx(`
					INSERT INTO cron_trigger_specs (trigger_id, cron_spec, next_run_at)
					VALUES (?, ?, ?)
				`),
				triggerID,
				trigger.Cron.Spec,
				nextRunAt.Format(time.RFC3339),
			); err != nil {
				return normalizeSQLError(err)
			}
		case trigger.SCMPoll != nil:
			triggerID, err := r.ensureJobTrigger(ctx, tx, jobID, TriggerTypeSCMPoll, trigger)
			if err != nil {
				return err
			}

			spec := *trigger.SCMPoll
			intervalSeconds := int64(spec.Interval / time.Second)
			if intervalSeconds <= 0 {
				intervalSeconds = 60
			}

			if _, err := tx.ExecContext(ctx,
				rebindQueryForPgx(`
					INSERT INTO scm_poll_trigger_specs
						(trigger_id, provider, base_url, project, branch, query, interval_seconds, next_poll_at)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?)
				`),
				triggerID,
				spec.Provider,
				spec.BaseURL,
				spec.Project,
				spec.Branch,
				spec.Query,
				intervalSeconds,
				nextPollAt,
			); err != nil {
				return normalizeSQLError(err)
			}
		default:
			continue
		}
	}

	return nil
}

func (r *SQLJobsRepository) deleteAllJobTriggers(ctx context.Context, tx *sql.Tx, jobID string) error {
	if err := r.deleteJobTriggerSpecs(ctx, tx, jobID); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("DELETE FROM job_triggers WHERE job_id = ?"),
		jobID,
	); err != nil {
		return normalizeSQLError(err)
	}

	return nil
}

func (r *SQLJobsRepository) deleteJobTriggerSpecs(ctx context.Context, tx *sql.Tx, jobID string) error {
	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx(`
			DELETE FROM cron_trigger_specs
			WHERE trigger_id IN (
				SELECT id FROM job_triggers WHERE job_id = ?
			)
		`),
		jobID,
	); err != nil {
		return normalizeSQLError(err)
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx(`
			DELETE FROM scm_poll_trigger_specs
			WHERE trigger_id IN (
				SELECT id FROM job_triggers WHERE job_id = ?
			)
		`),
		jobID,
	); err != nil {
		return normalizeSQLError(err)
	}

	return nil
}

func (r *SQLJobsRepository) ensureJobTrigger(ctx context.Context, tx *sql.Tx, jobID, triggerType string, trigger JobTriggerConfig) (int64, error) {
	var triggerID int64
	err := tx.QueryRowContext(ctx,
		rebindQueryForPgx(`
			SELECT id
			FROM job_triggers
			WHERE job_id = ? AND trigger_type = ? AND trigger_key = ?
			ORDER BY id DESC
			LIMIT 1
		`),
		jobID,
		triggerType,
		trigger.ID,
	).Scan(&triggerID)

	if err == nil {
		if _, err := tx.ExecContext(ctx,
			rebindQueryForPgx(`
				UPDATE job_triggers
				SET display_name = ?, enabled = ?, home_cell = ?, updated_at = CURRENT_TIMESTAMP
				WHERE id = ?
			`),
			trigger.Name,
			true,
			r.currentCellID(),
			triggerID,
		); err != nil {
			return 0, normalizeSQLError(err)
		}

		return triggerID, nil
	}

	if err != sql.ErrNoRows {
		return 0, normalizeSQLError(err)
	}

	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx(`
			INSERT INTO job_triggers (job_id, trigger_type, trigger_key, display_name, enabled, home_cell)
			VALUES (?, ?, ?, ?, ?, ?)
			RETURNING id
		`),
		jobID,
		triggerType,
		trigger.ID,
		trigger.Name,
		true,
		r.currentCellID(),
	).Scan(&triggerID); err != nil {
		return 0, normalizeSQLError(err)
	}

	return triggerID, nil
}

func normalizeJobTriggerConfig(trigger JobTriggerConfig) JobTriggerConfig {
	trigger.ID = strings.TrimSpace(trigger.ID)
	trigger.Name = strings.TrimSpace(trigger.Name)
	if trigger.Cron != nil {
		spec := strings.TrimSpace(trigger.Cron.Spec)
		trigger.Cron = &JobCronTriggerConfig{Spec: spec}
	}

	if trigger.SCMPoll != nil {
		spec := *trigger.SCMPoll
		spec.Provider = strings.TrimSpace(spec.Provider)
		spec.BaseURL = strings.TrimSpace(spec.BaseURL)
		spec.Project = strings.TrimSpace(spec.Project)
		spec.Branch = strings.TrimSpace(spec.Branch)
		spec.Query = strings.TrimSpace(spec.Query)
		trigger.SCMPoll = &spec
	}

	return trigger
}

func nextCronRun(spec string, from time.Time) (time.Time, error) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(spec)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid cron spec %q: %v", ErrConflict, spec, err)
	}

	return schedule.Next(from), nil
}

func (r *SQLJobsRepository) GetDefinitionVersion(ctx context.Context, jobID string, version int) (string, error) {
	var definitionJSON string
	if err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT definition_json FROM job_definitions WHERE job_id = ? AND version = ?"),
		jobID,
		version,
	).Scan(&definitionJSON); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("%w: job %s version %d", ErrNotFound, jobID, version)
		}

		return "", normalizeSQLError(err)
	}

	return definitionJSON, nil
}

func (r *SQLJobsRepository) GetEnabledTriggerID(ctx context.Context, jobID, triggerType, triggerKey string) (int64, error) {
	jobID = strings.TrimSpace(jobID)
	triggerType = strings.TrimSpace(triggerType)
	triggerKey = strings.TrimSpace(triggerKey)
	if jobID == "" || triggerType == "" || triggerKey == "" {
		return 0, fmt.Errorf("%w: job trigger is required", ErrNotFound)
	}

	var triggerID int64
	if err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx(`
			SELECT id
			FROM job_triggers
			WHERE job_id = ? AND trigger_type = ? AND trigger_key = ? AND enabled
			ORDER BY id DESC
			LIMIT 1
		`),
		jobID,
		triggerType,
		triggerKey,
	).Scan(&triggerID); err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("%w: job %s trigger %s/%s", ErrNotFound, jobID, triggerType, triggerKey)
		}

		return 0, normalizeSQLError(err)
	}

	return triggerID, nil
}
