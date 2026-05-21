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
		rebindQueryForPgx("INSERT INTO stored_jobs (global_id, job_id, namespace_id, definition_json, definition_hash, version, home_cell) VALUES (?, ?, ?, ?, ?, ?, ?)"),
		newGlobalID(),
		jobID,
		namespaceID,
		definitionJSON,
		definitionHash,
		initialVersion,
		r.currentCellID(),
	); err != nil {
		return normalizeSQLError(err)
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("INSERT INTO job_definitions (global_id, job_id, version, definition_json, definition_hash, home_cell) VALUES (?, ?, ?, ?, ?, ?)"),
		newGlobalID(),
		jobID,
		initialVersion,
		definitionJSON,
		definitionHash,
		r.currentCellID(),
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
	query := "SELECT id, COALESCE(global_id, ''), job_id, namespace_id, definition_json, definition_hash, version, home_cell FROM stored_jobs"
	args := []any{}
	if cursor > 0 {
		query += " WHERE id > ?"
		args = append(args, cursor)
	}

	query += " ORDER BY id ASC LIMIT ?"
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
		rebindQueryForPgx("SELECT COALESCE(global_id, ''), job_id, namespace_id, definition_json, definition_hash, version, home_cell FROM stored_jobs WHERE namespace_id = ?"),
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
		rebindQueryForPgx("SELECT definition_json, version FROM stored_jobs WHERE job_id = ?"),
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

	var currentDefinition string
	var currentHash string
	var currentVersion int
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT definition_json, definition_hash, version FROM stored_jobs WHERE job_id = ?"),
		jobID,
	).Scan(&currentDefinition, &currentHash, &currentVersion); err != nil {
		return 0, normalizeSQLError(err)
	}

	if currentHash == "" {
		currentHash = DefinitionHash(currentDefinition)
	}

	if err := insertDefinitionVersionTx(ctx, tx, jobID, currentVersion, currentDefinition, currentHash, r.currentCellID()); err != nil {
		return 0, err
	}

	definitionHash := DefinitionHash(definitionJSON)
	var newVersion int
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("UPDATE stored_jobs SET definition_json = ?, definition_hash = ?, version = version + 1, updated_at = CURRENT_TIMESTAMP WHERE job_id = ? RETURNING version"),
		definitionJSON,
		definitionHash,
		jobID,
	).Scan(&newVersion); err != nil {
		return 0, normalizeSQLError(err)
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("INSERT INTO job_definitions (global_id, job_id, version, definition_json, definition_hash, home_cell) VALUES (?, ?, ?, ?, ?, ?)"),
		newGlobalID(),
		jobID,
		newVersion,
		definitionJSON,
		definitionHash,
		r.currentCellID(),
	); err != nil {
		return 0, normalizeSQLError(err)
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return newVersion, nil
}

func insertDefinitionVersionTx(ctx context.Context, tx *sql.Tx, jobID string, version int, definitionJSON, definitionHash, cellID string) error {
	_, err := tx.ExecContext(ctx,
		rebindQueryForPgx("INSERT INTO job_definitions (global_id, job_id, version, definition_json, definition_hash, home_cell) VALUES (?, ?, ?, ?, ?, ?)"),
		newGlobalID(),
		jobID,
		version,
		definitionJSON,
		definitionHash,
		normalizeCellID(cellID),
	)

	if err != nil {
		err = normalizeSQLError(err)
		if IsConflict(err) {
			return nil
		}

		return err
	}

	return nil
}

func (r *SQLJobsRepository) GetDefinitionVersion(ctx context.Context, jobID string, version int) (string, error) {
	var definitionJSON string
	if err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT definition_json FROM job_definitions WHERE job_id = ? AND version = ?"),
		jobID,
		version,
	).Scan(&definitionJSON); err != nil {
		if err == sql.ErrNoRows {
			var currentVersion int
			if currentErr := r.db.QueryRowContext(ctx,
				rebindQueryForPgx("SELECT definition_json, version FROM stored_jobs WHERE job_id = ?"),
				jobID,
			).Scan(&definitionJSON, &currentVersion); currentErr != nil {
				if currentErr == sql.ErrNoRows {
					return "", fmt.Errorf("%w: job %s version %d", ErrNotFound, jobID, version)
				}

				return "", normalizeSQLError(currentErr)
			}

			if currentVersion == version {
				return definitionJSON, nil
			}

			return "", fmt.Errorf("%w: job %s version %d", ErrNotFound, jobID, version)
		}

		return "", normalizeSQLError(err)
	}

	return definitionJSON, nil
}
