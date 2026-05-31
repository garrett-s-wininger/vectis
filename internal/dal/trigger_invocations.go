package dal

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

type SQLTriggerInvocationsRepository struct {
	db *sql.DB
}

func (r *SQLTriggerInvocationsRepository) Record(ctx context.Context, invocation TriggerInvocation) (TriggerInvocationRecord, error) {
	normalized, requestedCellsJSON, err := normalizeTriggerInvocation(invocation)
	if err != nil {
		return TriggerInvocationRecord{}, err
	}

	var triggerID any
	if normalized.TriggerID != nil {
		triggerID = *normalized.TriggerID
	}

	if _, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO trigger_invocations
			(invocation_id, trigger_id, job_id, trigger_type, trigger_payload_hash, requested_cells)
		VALUES (?, ?, ?, ?, ?, ?)
	`),
		normalized.InvocationID,
		triggerID,
		normalized.JobID,
		normalized.TriggerType,
		normalized.TriggerPayloadHash,
		requestedCellsJSON,
	); err != nil {
		return TriggerInvocationRecord{}, normalizeSQLError(err)
	}

	var rec TriggerInvocationRecord
	var nullableTriggerID sql.NullInt64
	if err := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT id, invocation_id, trigger_id, job_id, trigger_type, trigger_payload_hash, requested_cells
		FROM trigger_invocations
		WHERE invocation_id = ?
	`), normalized.InvocationID).Scan(
		&rec.ID,
		&rec.InvocationID,
		&nullableTriggerID,
		&rec.JobID,
		&rec.TriggerType,
		&rec.TriggerPayloadHash,
		&rec.RequestedCellsJSON,
	); err != nil {
		return TriggerInvocationRecord{}, normalizeSQLError(err)
	}

	if nullableTriggerID.Valid {
		v := nullableTriggerID.Int64
		rec.TriggerID = &v
	}

	return rec, nil
}

func normalizeTriggerInvocation(invocation TriggerInvocation) (TriggerInvocation, string, error) {
	invocation.InvocationID = strings.TrimSpace(invocation.InvocationID)
	if invocation.InvocationID == "" {
		invocation.InvocationID = newGlobalID()
	}

	invocation.JobID = strings.TrimSpace(invocation.JobID)
	if invocation.JobID == "" {
		return TriggerInvocation{}, "", fmt.Errorf("%w: job_id is required", ErrConflict)
	}

	invocation.TriggerType = strings.TrimSpace(invocation.TriggerType)
	if invocation.TriggerType == "" {
		return TriggerInvocation{}, "", fmt.Errorf("%w: trigger_type is required", ErrConflict)
	}

	invocation.TriggerPayloadHash = strings.TrimSpace(invocation.TriggerPayloadHash)
	if invocation.TriggerPayloadHash == "" {
		invocation.TriggerPayloadHash = PayloadHash("")
	}

	requestedCells := make([]string, 0, len(invocation.RequestedCells))
	for _, cellID := range invocation.RequestedCells {
		cellID = strings.TrimSpace(cellID)
		if cellID == "" {
			continue
		}

		requestedCells = append(requestedCells, cellID)
	}
	invocation.RequestedCells = requestedCells

	requestedCellsJSON, err := json.Marshal(requestedCells)
	if err != nil {
		return TriggerInvocation{}, "", fmt.Errorf("marshal requested cells: %w", err)
	}

	return invocation, string(requestedCellsJSON), nil
}

var _ TriggerInvocationsRepository = (*SQLTriggerInvocationsRepository)(nil)
