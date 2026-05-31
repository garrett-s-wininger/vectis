package dal

import (
	"context"
	"database/sql"
	"fmt"
)

type SQLJobsRepository struct {
	db     *sql.DB
	cellID string
}

func (r *SQLJobsRepository) currentCellID() string {
	return normalizeCellID(r.cellID)
}

func (r *SQLJobsRepository) Create(ctx context.Context, jobID, definitionJSON string, namespaceID int64) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var initialVersion int
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT COALESCE(MAX(version), 0) + 1 FROM job_definitions WHERE job_id = ?"),
		jobID,
	).Scan(&initialVersion); err != nil {
		return normalizeSQLError(err)
	}

	definitionHash := DefinitionHash(definitionJSON)
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

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("INSERT INTO job_definitions (global_id, job_id, version, definition_json, definition_hash) VALUES (?, ?, ?, ?, ?)"),
		newGlobalID(),
		jobID,
		initialVersion,
		definitionJSON,
		definitionHash,
	); err != nil {
		return normalizeSQLError(err)
	}

	return tx.Commit()
}

func (r *SQLJobsRepository) Delete(ctx context.Context, jobID string) error {
	res, err := r.db.ExecContext(ctx, rebindQueryForPgx("DELETE FROM stored_jobs WHERE job_id = ?"), jobID)
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

	return nil
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

func (r *SQLJobsRepository) GetDefinition(ctx context.Context, jobID string) (string, int, error) {
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
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	var currentVersion int
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx(`
			SELECT sj.current_version
			FROM stored_jobs sj
			JOIN job_definitions jd ON jd.job_id = sj.job_id AND jd.version = sj.current_version
			WHERE sj.job_id = ?
		`),
		jobID,
	).Scan(&currentVersion); err != nil {
		return 0, normalizeSQLError(err)
	}

	definitionHash := DefinitionHash(definitionJSON)
	newVersion := currentVersion + 1

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("INSERT INTO job_definitions (global_id, job_id, version, definition_json, definition_hash) VALUES (?, ?, ?, ?, ?)"),
		newGlobalID(),
		jobID,
		newVersion,
		definitionJSON,
		definitionHash,
	); err != nil {
		return 0, normalizeSQLError(err)
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

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return newVersion, nil
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
