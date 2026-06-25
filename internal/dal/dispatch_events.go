package dal

import (
	"context"
	"database/sql"
	"time"
)

type SQLDispatchEventsRepository struct {
	db *sql.DB
}

func (r *SQLDispatchEventsRepository) Record(ctx context.Context, runID, source, eventType string, message *string) error {
	_, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`
			INSERT INTO run_dispatch_events (run_id, source, event_type, message, created_at)
			VALUES (?, ?, ?, ?, ?)
		`),
		runID,
		source,
		eventType,
		message,
		time.Now().Unix(),
	)

	return normalizeSQLError(err)
}

func (r *SQLDispatchEventsRepository) RecordDispatchSuccess(ctx context.Context, runID, source string) error {
	now := time.Now().Unix()
	_, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`
			UPDATE job_runs
			SET last_dispatched_at = ?
			WHERE run_id = ?;

			INSERT INTO run_dispatch_events (run_id, source, event_type, message, created_at)
			VALUES (?, ?, ?, NULL, ?)
		`),
		now,
		runID,
		runID,
		source,
		DispatchEventSuccess,
		now,
	)

	return normalizeSQLError(err)
}

func (r *SQLDispatchEventsRepository) ListByRun(ctx context.Context, runID string) ([]DispatchEvent, error) {
	rows, err := r.db.QueryContext(ctx,
		rebindQueryForPgx(`
			SELECT id, run_id, source, event_type, message, created_at
			FROM run_dispatch_events
			WHERE run_id = ?
			ORDER BY created_at ASC, id ASC
		`),
		runID,
	)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []DispatchEvent
	for rows.Next() {
		var rec DispatchEvent
		var message sql.NullString
		if err := rows.Scan(&rec.ID, &rec.RunID, &rec.Source, &rec.EventType, &message, &rec.CreatedAt); err != nil {
			return nil, normalizeSQLError(err)
		}

		if message.Valid {
			rec.Message = &message.String
		}

		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return out, nil
}

func (r *SQLDispatchEventsRepository) LastReconcilerActivity(ctx context.Context) (*int64, error) {
	var ts sql.NullInt64
	err := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT MAX(created_at) FROM run_dispatch_events WHERE source = ?
	`), DispatchSourceReconciler).Scan(&ts)
	if err != nil {
		return nil, normalizeSQLError(err)
	}

	if !ts.Valid {
		return nil, nil
	}

	return &ts.Int64, nil
}

var _ DispatchEventsRepository = (*SQLDispatchEventsRepository)(nil)
