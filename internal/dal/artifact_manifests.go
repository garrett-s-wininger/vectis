package dal

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type SQLArtifactsRepository struct {
	db     *sql.DB
	cellID string
}

func (r *SQLArtifactsRepository) Record(ctx context.Context, create ArtifactCreate) (ArtifactRecord, error) {
	create, err := normalizeArtifactCreate(create, r.cellID)
	if err != nil {
		return ArtifactRecord{}, err
	}

	now := time.Now().UnixNano()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return ArtifactRecord{}, err
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO run_artifacts (
			run_id,
			task_id,
			task_attempt_id,
			execution_id,
			cell_id,
			name,
			path,
			content_type,
			blob_key,
			blob_algorithm,
			blob_digest,
			size_bytes,
			artifact_shard_id,
			metadata_json,
			created_at,
			updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(run_id, name) DO UPDATE SET
			task_id = excluded.task_id,
			task_attempt_id = excluded.task_attempt_id,
			execution_id = excluded.execution_id,
			cell_id = excluded.cell_id,
			path = excluded.path,
			content_type = excluded.content_type,
			blob_key = excluded.blob_key,
			blob_algorithm = excluded.blob_algorithm,
			blob_digest = excluded.blob_digest,
			size_bytes = excluded.size_bytes,
			artifact_shard_id = excluded.artifact_shard_id,
			metadata_json = excluded.metadata_json,
			updated_at = excluded.updated_at
	`),
		create.RunID,
		nullableString(create.TaskID),
		nullableString(create.TaskAttemptID),
		nullableString(create.ExecutionID),
		create.CellID,
		create.Name,
		create.Path,
		create.ContentType,
		create.BlobKey,
		create.BlobAlgorithm,
		create.BlobDigest,
		create.SizeBytes,
		create.ArtifactShardID,
		nullableStringPtr(create.MetadataJSON),
		now,
		now,
	)

	if err != nil {
		return ArtifactRecord{}, normalizeSQLError(err)
	}

	rec, err := getArtifactByRunAndNameTx(ctx, tx, create.RunID, create.Name)
	if err != nil {
		return ArtifactRecord{}, err
	}

	if err := tx.Commit(); err != nil {
		return ArtifactRecord{}, err
	}

	return rec, nil
}

func (r *SQLArtifactsRepository) GetByRunAndName(ctx context.Context, runID, name string) (ArtifactRecord, error) {
	runID = strings.TrimSpace(runID)
	name = strings.TrimSpace(name)
	if runID == "" {
		return ArtifactRecord{}, fmt.Errorf("%w: run_id is required", ErrConflict)
	}

	if name == "" {
		return ArtifactRecord{}, fmt.Errorf("%w: artifact name is required", ErrConflict)
	}

	return scanArtifact(r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT id, run_id, task_id, task_attempt_id, execution_id, cell_id, name, path, content_type, blob_key, blob_algorithm, blob_digest, size_bytes, artifact_shard_id, metadata_json, created_at, updated_at
		FROM run_artifacts
		WHERE run_id = ? AND name = ?
	`), runID, name))
}

func (r *SQLArtifactsRepository) ListByRun(ctx context.Context, runID string, cursor int64, limit int) ([]ArtifactRecord, int64, error) {
	return r.ListByRunFiltered(ctx, runID, cursor, limit, ArtifactListFilter{})
}

func (r *SQLArtifactsRepository) ListByRunFiltered(ctx context.Context, runID string, cursor int64, limit int, filter ArtifactListFilter) ([]ArtifactRecord, int64, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, 0, fmt.Errorf("%w: run_id is required", ErrConflict)
	}

	if limit <= 0 {
		limit = 100
	}

	filter = normalizeArtifactListFilter(filter)
	where := []string{"run_id = ?", "id > ?"}
	args := []any{runID, cursor}
	if filter.TaskID != "" {
		where = append(where, "task_id = ?")
		args = append(args, filter.TaskID)
	}

	if filter.TaskAttemptID != "" {
		where = append(where, "task_attempt_id = ?")
		args = append(args, filter.TaskAttemptID)
	}

	if filter.ExecutionID != "" {
		where = append(where, "execution_id = ?")
		args = append(args, filter.ExecutionID)
	}

	args = append(args, limit+1)
	query := `
		SELECT id, run_id, task_id, task_attempt_id, execution_id, cell_id, name, path, content_type, blob_key, blob_algorithm, blob_digest, size_bytes, artifact_shard_id, metadata_json, created_at, updated_at
		FROM run_artifacts
		WHERE ` + strings.Join(where, " AND ") + `
		ORDER BY id ASC
		LIMIT ?
	`

	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(query), args...)
	if err != nil {
		return nil, 0, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []ArtifactRecord
	for rows.Next() {
		rec, err := scanArtifact(rows)
		if err != nil {
			return nil, 0, err
		}

		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, normalizeSQLError(err)
	}

	var nextCursor int64
	if len(out) > limit {
		nextCursor = out[limit-1].ID
		out = out[:limit]
	}

	return out, nextCursor, nil
}

func normalizeArtifactListFilter(filter ArtifactListFilter) ArtifactListFilter {
	filter.TaskID = strings.TrimSpace(filter.TaskID)
	filter.TaskAttemptID = strings.TrimSpace(filter.TaskAttemptID)
	filter.ExecutionID = strings.TrimSpace(filter.ExecutionID)
	return filter
}

func (r *SQLArtifactsRepository) GetRunUsageExcludingName(ctx context.Context, runID, name string) (ArtifactRunUsage, error) {
	runID = strings.TrimSpace(runID)
	name = strings.TrimSpace(name)
	if runID == "" {
		return ArtifactRunUsage{}, fmt.Errorf("%w: run_id is required", ErrConflict)
	}

	var usage ArtifactRunUsage
	var err error
	if name == "" {
		err = r.db.QueryRowContext(ctx, rebindQueryForPgx(`
			SELECT COUNT(*), CAST(COALESCE(SUM(size_bytes), 0) AS BIGINT)
			FROM run_artifacts
			WHERE run_id = ?
		`), runID).Scan(&usage.Count, &usage.SizeBytes)
	} else {
		err = r.db.QueryRowContext(ctx, rebindQueryForPgx(`
			SELECT COUNT(*), CAST(COALESCE(SUM(size_bytes), 0) AS BIGINT)
			FROM run_artifacts
			WHERE run_id = ? AND name <> ?
		`), runID, name).Scan(&usage.Count, &usage.SizeBytes)
	}

	if err != nil {
		return ArtifactRunUsage{}, normalizeSQLError(err)
	}

	return usage, nil
}

func getArtifactByRunAndNameTx(ctx context.Context, tx *sql.Tx, runID, name string) (ArtifactRecord, error) {
	return scanArtifact(tx.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT id, run_id, task_id, task_attempt_id, execution_id, cell_id, name, path, content_type, blob_key, blob_algorithm, blob_digest, size_bytes, artifact_shard_id, metadata_json, created_at, updated_at
		FROM run_artifacts
		WHERE run_id = ? AND name = ?
	`), runID, name))
}

type artifactScanner interface {
	Scan(dest ...any) error
}

func scanArtifact(scanner artifactScanner) (ArtifactRecord, error) {
	var rec ArtifactRecord
	var taskID, taskAttemptID, executionID, metadataJSON sql.NullString
	if err := scanner.Scan(
		&rec.ID,
		&rec.RunID,
		&taskID,
		&taskAttemptID,
		&executionID,
		&rec.CellID,
		&rec.Name,
		&rec.Path,
		&rec.ContentType,
		&rec.BlobKey,
		&rec.BlobAlgorithm,
		&rec.BlobDigest,
		&rec.SizeBytes,
		&rec.ArtifactShardID,
		&metadataJSON,
		&rec.CreatedAt,
		&rec.UpdatedAt,
	); err != nil {
		return ArtifactRecord{}, normalizeSQLError(err)
	}

	rec.TaskID = nullStringPtr(taskID)
	rec.TaskAttemptID = nullStringPtr(taskAttemptID)
	rec.ExecutionID = nullStringPtr(executionID)
	rec.MetadataJSON = nullStringPtr(metadataJSON)
	return rec, nil
}

func normalizeArtifactCreate(create ArtifactCreate, fallbackCellID string) (ArtifactCreate, error) {
	create.RunID = strings.TrimSpace(create.RunID)
	create.TaskID = strings.TrimSpace(create.TaskID)
	create.TaskAttemptID = strings.TrimSpace(create.TaskAttemptID)
	create.ExecutionID = strings.TrimSpace(create.ExecutionID)
	create.CellID = normalizeTargetCellID(create.CellID, fallbackCellID)
	create.Name = strings.TrimSpace(create.Name)
	create.Path = strings.TrimSpace(create.Path)
	create.ContentType = strings.TrimSpace(create.ContentType)
	create.BlobKey = strings.TrimSpace(create.BlobKey)
	create.BlobAlgorithm = strings.TrimSpace(create.BlobAlgorithm)
	create.BlobDigest = strings.TrimSpace(create.BlobDigest)
	create.ArtifactShardID = strings.TrimSpace(create.ArtifactShardID)
	create.MetadataJSON = trimOptionalJSON(create.MetadataJSON)

	switch {
	case create.RunID == "":
		return ArtifactCreate{}, fmt.Errorf("%w: run_id is required", ErrConflict)
	case create.Name == "":
		return ArtifactCreate{}, fmt.Errorf("%w: artifact name is required", ErrConflict)
	case create.BlobKey == "":
		return ArtifactCreate{}, fmt.Errorf("%w: artifact blob_key is required", ErrConflict)
	case create.BlobAlgorithm == "":
		return ArtifactCreate{}, fmt.Errorf("%w: artifact blob_algorithm is required", ErrConflict)
	case create.BlobDigest == "":
		return ArtifactCreate{}, fmt.Errorf("%w: artifact blob_digest is required", ErrConflict)
	case create.SizeBytes < 0:
		return ArtifactCreate{}, fmt.Errorf("%w: artifact size_bytes must be >= 0", ErrConflict)
	case create.ArtifactShardID == "":
		return ArtifactCreate{}, fmt.Errorf("%w: artifact_shard_id is required", ErrConflict)
	}

	if create.Path == "" {
		create.Path = create.Name
	}

	if create.MetadataJSON != nil && !json.Valid([]byte(*create.MetadataJSON)) {
		return ArtifactCreate{}, fmt.Errorf("%w: artifact metadata_json must be valid JSON", ErrConflict)
	}

	return create, nil
}

func trimOptionalJSON(value *string) *string {
	if value == nil {
		return nil
	}

	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}

	return &trimmed
}

func nullableStringPtr(value *string) any {
	if value == nil {
		return nil
	}

	return nullableString(*value)
}

var _ ArtifactsRepository = (*SQLArtifactsRepository)(nil)
