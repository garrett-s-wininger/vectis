package dal

import (
	"context"
	"database/sql"
	"strings"
)

type SQLCatalogStatusBackfillRepository struct {
	db *sql.DB
}

func (r *SQLCatalogStatusBackfillRepository) ListMissingRunStatusCatalogEvents(ctx context.Context, sourceCell string, limit int) ([]RunStatusUpdate, error) {
	sourceCell = normalizeCellID(sourceCell)
	if limit <= 0 {
		limit = 100
	}

	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT jr.run_id, jr.status, jr.failure_code, jr.failure_reason, jr.orphan_reason
		FROM job_runs jr
		LEFT JOIN cell_catalog_events cce
			ON cce.source_cell = ?
			AND cce.event_key = ('run:' || jr.run_id || ':' || jr.status)
		WHERE jr.owning_cell = ?
			AND jr.status IN (?, ?, ?, ?, ?, ?)
			AND cce.id IS NULL
		ORDER BY jr.id ASC
		LIMIT ?
	`),
		sourceCell,
		sourceCell,
		RunStatusRunning,
		RunStatusSucceeded,
		RunStatusFailed,
		RunStatusOrphaned,
		RunStatusCancelled,
		RunStatusAborted,
		limit,
	)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []RunStatusUpdate
	for rows.Next() {
		var update RunStatusUpdate
		var failureCode string
		var failureReason, orphanReason sql.NullString
		if err := rows.Scan(&update.RunID, &update.Status, &failureCode, &failureReason, &orphanReason); err != nil {
			return nil, normalizeSQLError(err)
		}

		update.FailureCode = failureCode
		update.Reason = firstNonEmpty(failureReason.String, orphanReason.String)
		out = append(out, update)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return out, nil
}

func (r *SQLCatalogStatusBackfillRepository) ListMissingExecutionStatusCatalogEvents(ctx context.Context, sourceCell string, limit int) ([]ExecutionStatusUpdate, error) {
	sourceCell = normalizeCellID(sourceCell)
	if limit <= 0 {
		limit = 100
	}

	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT se.execution_id, se.status
		FROM segment_executions se
		LEFT JOIN cell_catalog_events cce
			ON cce.source_cell = ?
			AND cce.event_key = ('execution:' || se.execution_id || ':' || se.status)
		WHERE se.cell_id = ?
			AND se.status IN (?, ?, ?, ?, ?, ?)
			AND cce.id IS NULL
		ORDER BY se.id ASC
		LIMIT ?
	`),
		sourceCell,
		sourceCell,
		ExecutionStatusAccepted,
		ExecutionStatusRunning,
		ExecutionStatusSucceeded,
		ExecutionStatusFailed,
		ExecutionStatusCancelled,
		ExecutionStatusAborted,
		limit,
	)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []ExecutionStatusUpdate
	for rows.Next() {
		var update ExecutionStatusUpdate
		if err := rows.Scan(&update.ExecutionID, &update.Status); err != nil {
			return nil, normalizeSQLError(err)
		}

		out = append(out, update)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return out, nil
}

func (r *SQLCatalogStatusBackfillRepository) ListMissingExecutionSecurityCatalogEvents(ctx context.Context, sourceCell string, limit int) ([]ExecutionSecurityEvent, error) {
	sourceCell = normalizeCellID(sourceCell)
	if limit <= 0 {
		limit = 100
	}

	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT ese.id, ese.event_key, ese.run_id, ese.task_id, ese.task_attempt_id, ese.execution_id, ese.event_type, ese.outcome, ese.reason, ese.provider, ese.secret_count, ese.file_count, ese.created_at
		FROM execution_security_events ese
		INNER JOIN job_runs jr ON jr.run_id = ese.run_id
		LEFT JOIN cell_catalog_events cce
			ON cce.source_cell = ?
			AND cce.event_key = ese.event_key
		WHERE jr.owning_cell = ?
			AND ese.event_key IS NOT NULL
			AND ese.event_key <> ''
			AND cce.id IS NULL
		ORDER BY ese.id ASC
		LIMIT ?
	`), sourceCell, sourceCell, limit)
	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []ExecutionSecurityEvent
	for rows.Next() {
		rec, err := scanExecutionSecurityEvent(rows)
		if err != nil {
			return nil, err
		}

		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return out, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}

	return ""
}

var _ CatalogStatusBackfillRepository = (*SQLCatalogStatusBackfillRepository)(nil)
