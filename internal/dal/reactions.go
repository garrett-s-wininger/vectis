package dal

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type SQLReactionsRepository struct {
	db     *sql.DB
	cellID string
}

func (r *SQLReactionsRepository) RecordEvent(ctx context.Context, create ReactionEventCreate) (ReactionEventRecord, error) {
	eventID := strings.TrimSpace(create.EventID)
	if eventID == "" {
		eventID = newGlobalID()
	}

	source := strings.TrimSpace(create.Source)
	if source == "" {
		source = ReactionEventSourceLifecycle
	}

	eventType := strings.TrimSpace(create.EventType)
	if eventType == "" {
		return ReactionEventRecord{}, fmt.Errorf("%w: event_type is required", ErrConflict)
	}

	payload, err := normalizeJSONObject(create.PayloadJSON, "payload_json", "{}")
	if err != nil {
		return ReactionEventRecord{}, err
	}

	sourceCell := normalizeTargetCellID(create.SourceCell, r.cellID)
	createdAt := create.CreatedAt
	if createdAt <= 0 {
		createdAt = time.Now().UnixNano()
	}

	_, err = r.db.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO reaction_events
			(event_id, source, event_type, namespace_id, job_id, run_id, actor, payload_json, source_cell, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`),
		eventID,
		source,
		eventType,
		nullableInt64(create.NamespaceID),
		strings.TrimSpace(create.JobID),
		strings.TrimSpace(create.RunID),
		strings.TrimSpace(create.Actor),
		string(payload),
		sourceCell,
		createdAt,
	)

	if err != nil {
		return ReactionEventRecord{}, normalizeSQLError(err)
	}

	return r.getEvent(ctx, eventID)
}

func (r *SQLReactionsRepository) GetEvent(ctx context.Context, eventID string) (ReactionEventRecord, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return ReactionEventRecord{}, fmt.Errorf("%w: event_id is required", ErrConflict)
	}

	return r.getEvent(ctx, eventID)
}

func (r *SQLReactionsRepository) CreateTarget(ctx context.Context, create ReactionTargetCreate) (ReactionTargetRecord, error) {
	targetID := strings.TrimSpace(create.TargetID)
	if targetID == "" {
		targetID = newGlobalID()
	}

	name := strings.TrimSpace(create.Name)
	if name == "" {
		return ReactionTargetRecord{}, fmt.Errorf("%w: target name is required", ErrConflict)
	}

	kind := strings.TrimSpace(create.Kind)
	if kind == "" {
		return ReactionTargetRecord{}, fmt.Errorf("%w: target kind is required", ErrConflict)
	}

	uses := strings.TrimSpace(create.Uses)
	if uses == "" {
		return ReactionTargetRecord{}, fmt.Errorf("%w: target uses is required", ErrConflict)
	}

	config, err := normalizeJSONObject(create.ConfigJSON, "config_json", "{}")
	if err != nil {
		return ReactionTargetRecord{}, err
	}

	secretRefs, err := normalizeJSONArray(create.SecretRefsJSON, "secret_refs_json", "[]")
	if err != nil {
		return ReactionTargetRecord{}, err
	}

	now := create.CreatedAt
	if now <= 0 {
		now = time.Now().UnixNano()
	}

	_, err = r.db.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO reaction_targets
			(target_id, namespace_id, name, kind, uses, config_json, secret_refs_json, enabled, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`),
		targetID,
		nullableInt64(create.NamespaceID),
		name,
		kind,
		uses,
		string(config),
		string(secretRefs),
		true,
		now,
		now,
	)

	if err != nil {
		return ReactionTargetRecord{}, normalizeSQLError(err)
	}

	return r.getTarget(ctx, targetID)
}

func (r *SQLReactionsRepository) CreateInvocation(ctx context.Context, create ReactionInvocationCreate) (ReactionInvocationRecord, error) {
	invocationID := strings.TrimSpace(create.InvocationID)
	if invocationID == "" {
		invocationID = newGlobalID()
	}

	eventID := strings.TrimSpace(create.EventID)
	if eventID == "" {
		return ReactionInvocationRecord{}, fmt.Errorf("%w: event_id is required", ErrConflict)
	}

	targetID := strings.TrimSpace(create.TargetID)
	if targetID == "" {
		return ReactionInvocationRecord{}, fmt.Errorf("%w: target_id is required", ErrConflict)
	}

	actionUses := strings.TrimSpace(create.ActionUses)
	if actionUses == "" {
		return ReactionInvocationRecord{}, fmt.Errorf("%w: action_uses is required", ErrConflict)
	}

	descriptor, err := normalizeJSONObject(create.ActionDescriptorJSON, "action_descriptor_json", "{}")
	if err != nil {
		return ReactionInvocationRecord{}, err
	}

	targetConfig, err := normalizeJSONObject(create.TargetConfigJSON, "target_config_json", "{}")
	if err != nil {
		return ReactionInvocationRecord{}, err
	}

	maxAttempts := create.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 5
	}

	now := create.CreatedAt
	if now <= 0 {
		now = time.Now().UnixNano()
	}

	nextAttemptAt := create.NextAttemptAt
	if nextAttemptAt <= 0 {
		nextAttemptAt = now
	}

	_, err = r.db.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO reaction_invocations
			(invocation_id, event_id, target_id, status, action_uses, action_descriptor_json, action_digest, target_config_json, attempts, max_attempts, next_attempt_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
	`),
		invocationID,
		eventID,
		targetID,
		ReactionInvocationStatusPending,
		actionUses,
		string(descriptor),
		strings.TrimSpace(create.ActionDigest),
		string(targetConfig),
		maxAttempts,
		nextAttemptAt,
		now,
		now,
	)

	if err != nil {
		return ReactionInvocationRecord{}, normalizeSQLError(err)
	}

	return r.getInvocation(ctx, invocationID)
}

func (r *SQLReactionsRepository) GetInvocation(ctx context.Context, invocationID string) (ReactionInvocationRecord, error) {
	invocationID = strings.TrimSpace(invocationID)
	if invocationID == "" {
		return ReactionInvocationRecord{}, fmt.Errorf("%w: invocation_id is required", ErrConflict)
	}

	return r.getInvocation(ctx, invocationID)
}

func (r *SQLReactionsRepository) ListReadyInvocations(ctx context.Context, nowUnixNano int64, limit int) ([]ReactionInvocationRecord, error) {
	if nowUnixNano <= 0 {
		nowUnixNano = time.Now().UnixNano()
	}

	if limit <= 0 {
		limit = 100
	}

	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT id, invocation_id, event_id, target_id, status, action_uses, action_descriptor_json, action_digest,
		       target_config_json, attempts, max_attempts, next_attempt_at, claimed_by, claim_until, last_error,
		       created_at, updated_at, completed_at
		FROM reaction_invocations
		WHERE (status = ? AND next_attempt_at <= ?)
		   OR (status = ? AND claim_until IS NOT NULL AND claim_until <= ?)
		ORDER BY next_attempt_at ASC, id ASC
		LIMIT ?
	`), ReactionInvocationStatusPending, nowUnixNano, ReactionInvocationStatusRunning, nowUnixNano, limit)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []ReactionInvocationRecord
	for rows.Next() {
		rec, err := scanReactionInvocation(rows)
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

func (r *SQLReactionsRepository) MarkInvocationRunning(ctx context.Context, invocationID, owner string, claimUntilUnixNano int64) (bool, error) {
	invocationID = strings.TrimSpace(invocationID)
	if invocationID == "" {
		return false, fmt.Errorf("%w: invocation_id is required", ErrConflict)
	}

	owner = strings.TrimSpace(owner)
	if owner == "" {
		return false, fmt.Errorf("%w: owner is required", ErrConflict)
	}

	if claimUntilUnixNano <= 0 {
		return false, fmt.Errorf("%w: claim_until is required", ErrConflict)
	}

	now := time.Now().UnixNano()
	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE reaction_invocations
		SET status = ?, claimed_by = ?, claim_until = ?, attempts = attempts + 1, updated_at = ?
		WHERE invocation_id = ?
		  AND (
		    (status = ? AND next_attempt_at <= ?)
		    OR (status = ? AND claim_until IS NOT NULL AND claim_until <= ?)
		  )
	`), ReactionInvocationStatusRunning, owner, claimUntilUnixNano, now, invocationID,
		ReactionInvocationStatusPending, now, ReactionInvocationStatusRunning, now)

	if err != nil {
		return false, normalizeSQLError(err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}

	return rows == 1, nil
}

func (r *SQLReactionsRepository) MarkInvocationSucceeded(ctx context.Context, invocationID string, completedAtUnixNano int64) error {
	invocationID = strings.TrimSpace(invocationID)
	if invocationID == "" {
		return fmt.Errorf("%w: invocation_id is required", ErrConflict)
	}

	if completedAtUnixNano <= 0 {
		completedAtUnixNano = time.Now().UnixNano()
	}

	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE reaction_invocations
		SET status = ?, claimed_by = '', claim_until = NULL, last_error = NULL, completed_at = ?, updated_at = ?
		WHERE invocation_id = ?
	`), ReactionInvocationStatusSucceeded, completedAtUnixNano, completedAtUnixNano, invocationID)

	if err != nil {
		return normalizeSQLError(err)
	}

	return requireOneRowByString(res, "reaction invocation", invocationID)
}

func (r *SQLReactionsRepository) MarkInvocationFailed(ctx context.Context, invocationID, message string, nextAttemptAtUnixNano int64) error {
	invocationID = strings.TrimSpace(invocationID)
	if invocationID == "" {
		return fmt.Errorf("%w: invocation_id is required", ErrConflict)
	}

	if nextAttemptAtUnixNano <= 0 {
		nextAttemptAtUnixNano = time.Now().Add(time.Minute).UnixNano()
	}

	now := time.Now().UnixNano()
	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE reaction_invocations
		SET status = CASE WHEN attempts >= max_attempts THEN ? ELSE ? END,
		    claimed_by = '',
		    claim_until = NULL,
		    last_error = ?,
		    next_attempt_at = ?,
		    updated_at = ?
		WHERE invocation_id = ?
	`), ReactionInvocationStatusFailed, ReactionInvocationStatusPending, strings.TrimSpace(message), nextAttemptAtUnixNano, now, invocationID)

	if err != nil {
		return normalizeSQLError(err)
	}

	return requireOneRowByString(res, "reaction invocation", invocationID)
}

func (r *SQLReactionsRepository) RecordLocalMessage(ctx context.Context, create ReactionLocalMessageCreate) (ReactionLocalMessageRecord, error) {
	messageID := strings.TrimSpace(create.MessageID)
	if messageID == "" {
		messageID = newGlobalID()
	}

	eventID := strings.TrimSpace(create.EventID)
	if eventID == "" {
		return ReactionLocalMessageRecord{}, fmt.Errorf("%w: event_id is required", ErrConflict)
	}

	invocationID := strings.TrimSpace(create.InvocationID)
	if invocationID == "" {
		return ReactionLocalMessageRecord{}, fmt.Errorf("%w: invocation_id is required", ErrConflict)
	}

	mailbox := strings.TrimSpace(create.Mailbox)
	if mailbox == "" {
		mailbox = "default"
	}

	payload, err := normalizeJSONObject(create.PayloadJSON, "payload_json", "{}")
	if err != nil {
		return ReactionLocalMessageRecord{}, err
	}

	createdAt := create.CreatedAt
	if createdAt <= 0 {
		createdAt = time.Now().UnixNano()
	}

	_, err = r.db.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO reaction_local_messages
			(message_id, event_id, invocation_id, mailbox, payload_json, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`), messageID, eventID, invocationID, mailbox, string(payload), createdAt)

	if err != nil {
		return ReactionLocalMessageRecord{}, normalizeSQLError(err)
	}

	return r.getLocalMessage(ctx, messageID)
}

func (r *SQLReactionsRepository) ListLocalMessages(ctx context.Context, mailbox string, cursor int64, limit int) ([]ReactionLocalMessageRecord, int64, error) {
	mailbox = strings.TrimSpace(mailbox)
	if mailbox == "" {
		mailbox = "default"
	}

	if limit <= 0 {
		limit = 100
	}

	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT id, message_id, event_id, invocation_id, mailbox, payload_json, created_at
		FROM reaction_local_messages
		WHERE mailbox = ? AND id > ?
		ORDER BY id ASC
		LIMIT ?
	`), mailbox, cursor, limit)

	if err != nil {
		return nil, 0, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []ReactionLocalMessageRecord
	var next int64
	for rows.Next() {
		rec, err := scanReactionLocalMessage(rows)
		if err != nil {
			return nil, 0, err
		}

		next = rec.ID
		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, normalizeSQLError(err)
	}

	return out, next, nil
}

func (r *SQLReactionsRepository) getEvent(ctx context.Context, eventID string) (ReactionEventRecord, error) {
	row := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT id, event_id, source, event_type, namespace_id, job_id, run_id, actor, payload_json, source_cell, created_at
		FROM reaction_events
		WHERE event_id = ?
	`), eventID)

	return scanReactionEvent(row)
}

func (r *SQLReactionsRepository) getTarget(ctx context.Context, targetID string) (ReactionTargetRecord, error) {
	row := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT id, target_id, namespace_id, name, kind, uses, config_json, secret_refs_json, enabled, created_at, updated_at
		FROM reaction_targets
		WHERE target_id = ?
	`), targetID)

	return scanReactionTarget(row)
}

func (r *SQLReactionsRepository) getInvocation(ctx context.Context, invocationID string) (ReactionInvocationRecord, error) {
	row := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT id, invocation_id, event_id, target_id, status, action_uses, action_descriptor_json, action_digest,
		       target_config_json, attempts, max_attempts, next_attempt_at, claimed_by, claim_until, last_error,
		       created_at, updated_at, completed_at
		FROM reaction_invocations
		WHERE invocation_id = ?
	`), invocationID)

	return scanReactionInvocation(row)
}

func (r *SQLReactionsRepository) getLocalMessage(ctx context.Context, messageID string) (ReactionLocalMessageRecord, error) {
	row := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT id, message_id, event_id, invocation_id, mailbox, payload_json, created_at
		FROM reaction_local_messages
		WHERE message_id = ?
	`), messageID)

	return scanReactionLocalMessage(row)
}

type reactionScanner interface {
	Scan(dest ...any) error
}

func scanReactionEvent(scanner reactionScanner) (ReactionEventRecord, error) {
	var rec ReactionEventRecord
	var namespaceID sql.NullInt64
	var payload string
	if err := scanner.Scan(
		&rec.ID,
		&rec.EventID,
		&rec.Source,
		&rec.EventType,
		&namespaceID,
		&rec.JobID,
		&rec.RunID,
		&rec.Actor,
		&payload,
		&rec.SourceCell,
		&rec.CreatedAt,
	); err != nil {
		return ReactionEventRecord{}, normalizeSQLError(err)
	}

	if namespaceID.Valid {
		rec.NamespaceID = &namespaceID.Int64
	}

	rec.PayloadJSON = []byte(payload)
	return rec, nil
}

func scanReactionTarget(scanner reactionScanner) (ReactionTargetRecord, error) {
	var rec ReactionTargetRecord
	var namespaceID sql.NullInt64
	var config string
	var secretRefs string
	if err := scanner.Scan(
		&rec.ID,
		&rec.TargetID,
		&namespaceID,
		&rec.Name,
		&rec.Kind,
		&rec.Uses,
		&config,
		&secretRefs,
		&rec.Enabled,
		&rec.CreatedAt,
		&rec.UpdatedAt,
	); err != nil {
		return ReactionTargetRecord{}, normalizeSQLError(err)
	}

	if namespaceID.Valid {
		rec.NamespaceID = &namespaceID.Int64
	}

	rec.ConfigJSON = []byte(config)
	rec.SecretRefsJSON = []byte(secretRefs)
	return rec, nil
}

func scanReactionInvocation(scanner reactionScanner) (ReactionInvocationRecord, error) {
	var rec ReactionInvocationRecord
	var descriptor string
	var targetConfig string
	var claimUntil sql.NullInt64
	var lastError sql.NullString
	var completedAt sql.NullInt64
	if err := scanner.Scan(
		&rec.ID,
		&rec.InvocationID,
		&rec.EventID,
		&rec.TargetID,
		&rec.Status,
		&rec.ActionUses,
		&descriptor,
		&rec.ActionDigest,
		&targetConfig,
		&rec.Attempts,
		&rec.MaxAttempts,
		&rec.NextAttemptAt,
		&rec.ClaimedBy,
		&claimUntil,
		&lastError,
		&rec.CreatedAt,
		&rec.UpdatedAt,
		&completedAt,
	); err != nil {
		return ReactionInvocationRecord{}, normalizeSQLError(err)
	}

	rec.ActionDescriptorJSON = []byte(descriptor)
	rec.TargetConfigJSON = []byte(targetConfig)
	if claimUntil.Valid {
		rec.ClaimUntil = &claimUntil.Int64
	}

	if lastError.Valid {
		rec.LastError = &lastError.String
	}

	if completedAt.Valid {
		rec.CompletedAt = &completedAt.Int64
	}

	return rec, nil
}

func scanReactionLocalMessage(scanner reactionScanner) (ReactionLocalMessageRecord, error) {
	var rec ReactionLocalMessageRecord
	var payload string
	if err := scanner.Scan(
		&rec.ID,
		&rec.MessageID,
		&rec.EventID,
		&rec.InvocationID,
		&rec.Mailbox,
		&payload,
		&rec.CreatedAt,
	); err != nil {
		return ReactionLocalMessageRecord{}, normalizeSQLError(err)
	}

	rec.PayloadJSON = []byte(payload)
	return rec, nil
}

func normalizeJSONObject(raw []byte, field, fallback string) ([]byte, error) {
	payload := bytes.TrimSpace(raw)
	if len(payload) == 0 {
		payload = []byte(fallback)
	}

	if !json.Valid(payload) {
		return nil, fmt.Errorf("%w: %s must be valid JSON", ErrConflict, field)
	}

	var value any
	if err := json.Unmarshal(payload, &value); err != nil {
		return nil, fmt.Errorf("%w: %s must be valid JSON", ErrConflict, field)
	}

	if _, ok := value.(map[string]any); !ok {
		return nil, fmt.Errorf("%w: %s must be a JSON object", ErrConflict, field)
	}

	return payload, nil
}

func normalizeJSONArray(raw []byte, field, fallback string) ([]byte, error) {
	payload := bytes.TrimSpace(raw)
	if len(payload) == 0 {
		payload = []byte(fallback)
	}

	if !json.Valid(payload) {
		return nil, fmt.Errorf("%w: %s must be valid JSON", ErrConflict, field)
	}

	var value any
	if err := json.Unmarshal(payload, &value); err != nil {
		return nil, fmt.Errorf("%w: %s must be valid JSON", ErrConflict, field)
	}

	if _, ok := value.([]any); !ok {
		return nil, fmt.Errorf("%w: %s must be a JSON array", ErrConflict, field)
	}

	return payload, nil
}

func requireOneRowByString(res sql.Result, resource, id string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n == 0 {
		return fmt.Errorf("%w: %s %s", ErrNotFound, resource, id)
	}

	return nil
}

var _ ReactionsRepository = (*SQLReactionsRepository)(nil)
