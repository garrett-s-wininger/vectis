package dal

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type SQLCatalogEventsRepository struct {
	db *sql.DB
}

func (r *SQLCatalogEventsRepository) Record(ctx context.Context, sourceCell, eventKey, eventType string, payload []byte) (CatalogEventRecord, bool, error) {
	sourceCell = strings.TrimSpace(sourceCell)
	eventKey = strings.TrimSpace(eventKey)
	eventType = strings.TrimSpace(eventType)
	payload = bytes.TrimSpace(payload)

	if sourceCell == "" {
		return CatalogEventRecord{}, false, fmt.Errorf("%w: source_cell is required", ErrConflict)
	}

	if eventKey == "" {
		return CatalogEventRecord{}, false, fmt.Errorf("%w: event_key is required", ErrConflict)
	}

	if eventType == "" {
		return CatalogEventRecord{}, false, fmt.Errorf("%w: event_type is required", ErrConflict)
	}

	if len(payload) == 0 {
		return CatalogEventRecord{}, false, fmt.Errorf("%w: payload is required", ErrConflict)
	}

	now := time.Now().UnixNano()
	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO cell_catalog_events
			(source_cell, event_key, event_type, payload_json, status, attempts, received_at, updated_at)
		VALUES (?, ?, ?, ?, ?, 0, ?, ?)
		ON CONFLICT(source_cell, event_key) DO NOTHING
	`), sourceCell, eventKey, eventType, string(payload), CatalogEventStatusPending, now, now)

	if err != nil {
		return CatalogEventRecord{}, false, normalizeSQLError(err)
	}

	created := false
	if rows, err := res.RowsAffected(); err == nil {
		created = rows == 1
	}

	rec, err := r.getByKey(ctx, sourceCell, eventKey)
	if err != nil {
		return CatalogEventRecord{}, false, err
	}

	return rec, created, nil
}

func (r *SQLCatalogEventsRepository) ListPending(ctx context.Context, limit int) ([]CatalogEventRecord, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT id, source_cell, event_key, event_type, payload_json, status, attempts, last_error, received_at, applied_at, updated_at
		FROM cell_catalog_events
		WHERE status = ?
		ORDER BY id ASC
		LIMIT ?
	`), CatalogEventStatusPending, limit)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []CatalogEventRecord
	for rows.Next() {
		rec, err := scanCatalogEvent(rows)
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

func (r *SQLCatalogEventsRepository) MarkApplied(ctx context.Context, id int64) error {
	now := time.Now().UnixNano()
	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE cell_catalog_events
		SET status = ?, attempts = attempts + 1, last_error = NULL, applied_at = ?, updated_at = ?
		WHERE id = ?
	`), CatalogEventStatusApplied, now, now, id)

	if err != nil {
		return normalizeSQLError(err)
	}

	return requireOneRow(res, "catalog event", id)
}

func (r *SQLCatalogEventsRepository) MarkFailed(ctx context.Context, id int64, message string) error {
	now := time.Now().UnixNano()
	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE cell_catalog_events
		SET status = ?, attempts = attempts + 1, last_error = ?, updated_at = ?
		WHERE id = ?
	`), CatalogEventStatusFailed, message, now, id)

	if err != nil {
		return normalizeSQLError(err)
	}

	return requireOneRow(res, "catalog event", id)
}

func (r *SQLCatalogEventsRepository) MarkRetryable(ctx context.Context, id int64, message string) error {
	now := time.Now().UnixNano()
	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE cell_catalog_events
		SET status = ?, attempts = attempts + 1, last_error = ?, updated_at = ?
		WHERE id = ?
	`), CatalogEventStatusPending, message, now, id)

	if err != nil {
		return normalizeSQLError(err)
	}

	return requireOneRow(res, "catalog event", id)
}

func (r *SQLCatalogEventsRepository) Summary(ctx context.Context) (CatalogEventSummary, error) {
	row := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT
			COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0),
			COUNT(*),
			MAX(received_at),
			MAX(applied_at)
		FROM cell_catalog_events
	`), CatalogEventStatusPending, CatalogEventStatusApplied, CatalogEventStatusFailed)

	var out CatalogEventSummary
	var lastReceived sql.NullInt64
	var lastApplied sql.NullInt64
	if err := row.Scan(&out.Pending, &out.Applied, &out.Failed, &out.Total, &lastReceived, &lastApplied); err != nil {
		return CatalogEventSummary{}, normalizeSQLError(err)
	}

	if lastReceived.Valid {
		out.LastReceivedUnix = &lastReceived.Int64
	}

	if lastApplied.Valid {
		out.LastAppliedUnix = &lastApplied.Int64
	}

	return out, nil
}

func (r *SQLCatalogEventsRepository) SummaryBySource(ctx context.Context) ([]CatalogEventSourceSummary, error) {
	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT
			source_cell,
			COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0),
			COUNT(*),
			MAX(received_at),
			MAX(applied_at)
		FROM cell_catalog_events
		GROUP BY source_cell
		ORDER BY source_cell ASC
	`), CatalogEventStatusPending, CatalogEventStatusApplied, CatalogEventStatusFailed)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []CatalogEventSourceSummary
	for rows.Next() {
		var rec CatalogEventSourceSummary
		var lastReceived sql.NullInt64
		var lastApplied sql.NullInt64
		if err := rows.Scan(&rec.SourceCell, &rec.Pending, &rec.Applied, &rec.Failed, &rec.Total, &lastReceived, &lastApplied); err != nil {
			return nil, normalizeSQLError(err)
		}

		if lastReceived.Valid {
			rec.LastReceivedUnix = &lastReceived.Int64
		}

		if lastApplied.Valid {
			rec.LastAppliedUnix = &lastApplied.Int64
		}

		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return out, nil
}

func (r *SQLCatalogEventsRepository) getByKey(ctx context.Context, sourceCell, eventKey string) (CatalogEventRecord, error) {
	row := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT id, source_cell, event_key, event_type, payload_json, status, attempts, last_error, received_at, applied_at, updated_at
		FROM cell_catalog_events
		WHERE source_cell = ? AND event_key = ?
	`), sourceCell, eventKey)

	return scanCatalogEvent(row)
}

type catalogEventScanner interface {
	Scan(dest ...any) error
}

func scanCatalogEvent(scanner catalogEventScanner) (CatalogEventRecord, error) {
	var rec CatalogEventRecord
	var payload string
	var lastError sql.NullString
	var appliedAt sql.NullInt64
	if err := scanner.Scan(
		&rec.ID,
		&rec.SourceCell,
		&rec.EventKey,
		&rec.EventType,
		&payload,
		&rec.Status,
		&rec.Attempts,
		&lastError,
		&rec.ReceivedAt,
		&appliedAt,
		&rec.UpdatedAt,
	); err != nil {
		return CatalogEventRecord{}, normalizeSQLError(err)
	}

	rec.Payload = []byte(payload)
	if lastError.Valid {
		rec.LastError = &lastError.String
	}

	if appliedAt.Valid {
		rec.AppliedAt = &appliedAt.Int64
	}

	return rec, nil
}

func requireOneRow(res sql.Result, resource string, id int64) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n == 0 {
		return fmt.Errorf("%w: %s %d", ErrNotFound, resource, id)
	}

	return nil
}

var _ CatalogEventsRepository = (*SQLCatalogEventsRepository)(nil)
