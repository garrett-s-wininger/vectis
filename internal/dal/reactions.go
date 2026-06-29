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
	if err := validateReactionEventSource(source); err != nil {
		return ReactionEventRecord{}, err
	}

	eventType := strings.TrimSpace(create.EventType)
	if eventType == "" {
		return ReactionEventRecord{}, fmt.Errorf("%w: event_type is required", ErrConflict)
	}
	if err := validateReactionEventType(eventType); err != nil {
		return ReactionEventRecord{}, err
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
		ON CONFLICT(event_id) DO NOTHING
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

	event, err := r.getEvent(ctx, eventID)
	if err != nil {
		return ReactionEventRecord{}, err
	}

	if !sameReactionEvent(event, ReactionEventRecord{
		EventID:     eventID,
		Source:      source,
		EventType:   eventType,
		NamespaceID: nullableNamespaceID(create.NamespaceID),
		JobID:       strings.TrimSpace(create.JobID),
		RunID:       strings.TrimSpace(create.RunID),
		Actor:       strings.TrimSpace(create.Actor),
		PayloadJSON: payload,
		SourceCell:  sourceCell,
	}) {
		return ReactionEventRecord{}, fmt.Errorf("%w: reaction event %s already exists with different content", ErrConflict, eventID)
	}

	return event, nil
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
	if err := validateReactionTargetAction(kind, uses); err != nil {
		return ReactionTargetRecord{}, err
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

func (r *SQLReactionsRepository) GetTarget(ctx context.Context, targetID string) (ReactionTargetRecord, error) {
	targetID = strings.TrimSpace(targetID)
	if targetID == "" {
		return ReactionTargetRecord{}, fmt.Errorf("%w: target_id is required", ErrConflict)
	}

	return r.getTarget(ctx, targetID)
}

func (r *SQLReactionsRepository) SetTargetEnabled(ctx context.Context, targetID string, enabled bool) (ReactionTargetRecord, error) {
	targetID = strings.TrimSpace(targetID)
	if targetID == "" {
		return ReactionTargetRecord{}, fmt.Errorf("%w: target_id is required", ErrConflict)
	}

	now := time.Now().UnixNano()
	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE reaction_targets
		SET enabled = ?, updated_at = ?
		WHERE target_id = ?
	`), enabled, now, targetID)
	if err != nil {
		return ReactionTargetRecord{}, normalizeSQLError(err)
	}

	if err := requireOneRowByString(res, "reaction target", targetID); err != nil {
		return ReactionTargetRecord{}, err
	}

	return r.getTarget(ctx, targetID)
}

func (r *SQLReactionsRepository) CreateSubscription(ctx context.Context, create ReactionSubscriptionCreate) (ReactionSubscriptionRecord, error) {
	subscriptionID := strings.TrimSpace(create.SubscriptionID)
	if subscriptionID == "" {
		subscriptionID = newGlobalID()
	}

	targetID := strings.TrimSpace(create.TargetID)
	if targetID == "" {
		return ReactionSubscriptionRecord{}, fmt.Errorf("%w: target_id is required", ErrConflict)
	}

	name := strings.TrimSpace(create.Name)
	if name == "" {
		return ReactionSubscriptionRecord{}, fmt.Errorf("%w: subscription name is required", ErrConflict)
	}

	target, err := r.getTarget(ctx, targetID)
	if err != nil {
		return ReactionSubscriptionRecord{}, err
	}

	if create.NamespaceID > 0 && target.NamespaceID != nil && *target.NamespaceID != create.NamespaceID {
		return ReactionSubscriptionRecord{}, fmt.Errorf("%w: reaction subscription namespace does not match target namespace", ErrConflict)
	}

	eventType := strings.TrimSpace(create.EventType)
	if eventType != "" {
		if err := validateReactionEventType(eventType); err != nil {
			return ReactionSubscriptionRecord{}, err
		}
	}

	runStatus := strings.TrimSpace(create.RunStatus)
	if err := validateReactionRunStatusFilter(runStatus); err != nil {
		return ReactionSubscriptionRecord{}, err
	}

	triggerType := strings.TrimSpace(create.TriggerType)
	if err := validateReactionTriggerTypeFilter(triggerType); err != nil {
		return ReactionSubscriptionRecord{}, err
	}

	now := create.CreatedAt
	if now <= 0 {
		now = time.Now().UnixNano()
	}

	_, err = r.db.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO reaction_subscriptions
			(subscription_id, namespace_id, target_id, name, event_type, job_id, run_status, trigger_type, owning_cell, enabled, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`),
		subscriptionID,
		nullableInt64(create.NamespaceID),
		targetID,
		name,
		eventType,
		strings.TrimSpace(create.JobID),
		runStatus,
		triggerType,
		strings.TrimSpace(create.OwningCell),
		true,
		now,
		now,
	)

	if err != nil {
		return ReactionSubscriptionRecord{}, normalizeSQLError(err)
	}

	return r.getSubscription(ctx, subscriptionID)
}

func (r *SQLReactionsRepository) SetSubscriptionEnabled(ctx context.Context, subscriptionID string, enabled bool) (ReactionSubscriptionRecord, error) {
	subscriptionID = strings.TrimSpace(subscriptionID)
	if subscriptionID == "" {
		return ReactionSubscriptionRecord{}, fmt.Errorf("%w: subscription_id is required", ErrConflict)
	}

	now := time.Now().UnixNano()
	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE reaction_subscriptions
		SET enabled = ?, updated_at = ?
		WHERE subscription_id = ?
	`), enabled, now, subscriptionID)
	if err != nil {
		return ReactionSubscriptionRecord{}, normalizeSQLError(err)
	}

	if err := requireOneRowByString(res, "reaction subscription", subscriptionID); err != nil {
		return ReactionSubscriptionRecord{}, err
	}

	return r.getSubscription(ctx, subscriptionID)
}

func (r *SQLReactionsRepository) ListMatchingSubscriptions(ctx context.Context, event ReactionEventRecord) ([]ReactionSubscriptionMatch, error) {
	eventType := strings.TrimSpace(event.EventType)
	if eventType == "" {
		return nil, fmt.Errorf("%w: event_type is required", ErrConflict)
	}
	if err := validateReactionEventType(eventType); err != nil {
		return nil, err
	}

	namespaceID := int64(0)
	if event.NamespaceID != nil {
		namespaceID = *event.NamespaceID
	}

	runStatus := reactionEventPayloadString(event.PayloadJSON, "status")
	triggerType := reactionEventPayloadString(event.PayloadJSON, "trigger_type")
	sourceCell := strings.TrimSpace(event.SourceCell)

	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT
		    s.id, s.subscription_id, s.namespace_id, s.target_id, s.name, s.event_type, s.job_id, s.run_status,
		    s.trigger_type, s.owning_cell, s.enabled, s.created_at, s.updated_at,
		    t.id, t.target_id, t.namespace_id, t.name, t.kind, t.uses, t.config_json, t.secret_refs_json,
		    t.enabled, t.created_at, t.updated_at
		FROM reaction_subscriptions s
		JOIN reaction_targets t ON t.target_id = s.target_id
		WHERE s.enabled = ?
		  AND t.enabled = ?
		  AND (s.namespace_id IS NULL OR s.namespace_id = ?)
		  AND (t.namespace_id IS NULL OR t.namespace_id = ?)
		  AND (s.event_type = '' OR s.event_type = ?)
		  AND (s.job_id = '' OR s.job_id = ?)
		  AND (s.run_status = '' OR s.run_status = ?)
		  AND (s.trigger_type = '' OR s.trigger_type = ?)
		  AND (s.owning_cell = '' OR s.owning_cell = ?)
		ORDER BY s.id ASC
	`), true, true, namespaceID, namespaceID, eventType, strings.TrimSpace(event.JobID), runStatus, triggerType, sourceCell)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []ReactionSubscriptionMatch
	for rows.Next() {
		match, err := scanReactionSubscriptionMatch(rows)
		if err != nil {
			return nil, err
		}

		out = append(out, match)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return out, nil
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
	if _, err := r.getEvent(ctx, eventID); err != nil {
		return ReactionInvocationRecord{}, err
	}

	targetID := strings.TrimSpace(create.TargetID)
	if targetID == "" {
		return ReactionInvocationRecord{}, fmt.Errorf("%w: target_id is required", ErrConflict)
	}

	actionUses := strings.TrimSpace(create.ActionUses)
	if actionUses == "" {
		return ReactionInvocationRecord{}, fmt.Errorf("%w: action_uses is required", ErrConflict)
	}

	target, err := r.getTarget(ctx, targetID)
	if err != nil {
		return ReactionInvocationRecord{}, err
	}
	if err := validateReactionTargetAction(target.Kind, target.Uses); err != nil {
		return ReactionInvocationRecord{}, err
	}
	if actionUses != target.Uses {
		return ReactionInvocationRecord{}, fmt.Errorf("%w: reaction invocation action %q does not match target %s action %q", ErrConflict, actionUses, targetID, target.Uses)
	}

	descriptor, err := normalizeJSONObject(create.ActionDescriptorJSON, "action_descriptor_json", "{}")
	if err != nil {
		return ReactionInvocationRecord{}, err
	}

	targetConfig, err := normalizeJSONObject(create.TargetConfigJSON, "target_config_json", "{}")
	if err != nil {
		return ReactionInvocationRecord{}, err
	}
	if !sameReactionJSON(targetConfig, target.ConfigJSON) {
		return ReactionInvocationRecord{}, fmt.Errorf("%w: reaction invocation target_config_json does not match target %s config_json", ErrConflict, targetID)
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
		ON CONFLICT(event_id, target_id) DO NOTHING
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

	invocation, err := r.getInvocationByEventTarget(ctx, eventID, targetID)
	if err != nil {
		return ReactionInvocationRecord{}, err
	}

	if !sameReactionInvocation(invocation, ReactionInvocationRecord{
		EventID:              eventID,
		TargetID:             targetID,
		ActionUses:           actionUses,
		ActionDescriptorJSON: descriptor,
		ActionDigest:         strings.TrimSpace(create.ActionDigest),
		TargetConfigJSON:     targetConfig,
		MaxAttempts:          maxAttempts,
		NextAttemptAt:        nextAttemptAt,
	}) {
		return ReactionInvocationRecord{}, fmt.Errorf("%w: reaction invocation for event %s target %s already exists with different content", ErrConflict, eventID, targetID)
	}

	return invocation, nil
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
		   OR (status = ? AND claim_until IS NOT NULL AND claim_until <= ? AND attempts < max_attempts)
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

func (r *SQLReactionsRepository) MarkExpiredInvocationsFailed(ctx context.Context, nowUnixNano int64) (int, error) {
	if nowUnixNano <= 0 {
		nowUnixNano = time.Now().UnixNano()
	}

	now := time.Now().UnixNano()
	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE reaction_invocations
		SET status = ?,
		    claimed_by = '',
		    claim_until = NULL,
		    last_error = ?,
		    updated_at = ?
		WHERE status = ?
		  AND claim_until IS NOT NULL
		  AND claim_until <= ?
		  AND attempts >= max_attempts
	`), ReactionInvocationStatusFailed, "reaction invocation claim expired after reaching max attempts", now,
		ReactionInvocationStatusRunning, nowUnixNano)

	if err != nil {
		return 0, normalizeSQLError(err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(rows), nil
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
		    OR (status = ? AND claim_until IS NOT NULL AND claim_until <= ? AND attempts < max_attempts)
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

func (r *SQLReactionsRepository) MarkInvocationSucceeded(ctx context.Context, invocationID, owner string, completedAtUnixNano int64) error {
	invocationID = strings.TrimSpace(invocationID)
	if invocationID == "" {
		return fmt.Errorf("%w: invocation_id is required", ErrConflict)
	}

	owner = strings.TrimSpace(owner)
	if owner == "" {
		return fmt.Errorf("%w: owner is required", ErrConflict)
	}

	if completedAtUnixNano <= 0 {
		completedAtUnixNano = time.Now().UnixNano()
	}

	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE reaction_invocations
		SET status = ?, claimed_by = '', claim_until = NULL, last_error = NULL, completed_at = ?, updated_at = ?
		WHERE invocation_id = ? AND status = ? AND claimed_by = ?
	`), ReactionInvocationStatusSucceeded, completedAtUnixNano, completedAtUnixNano, invocationID, ReactionInvocationStatusRunning, owner)

	if err != nil {
		return normalizeSQLError(err)
	}

	return requireOneRowByString(res, "reaction invocation", invocationID)
}

func (r *SQLReactionsRepository) MarkInvocationFailed(ctx context.Context, invocationID, owner, message string, nextAttemptAtUnixNano int64) error {
	invocationID = strings.TrimSpace(invocationID)
	if invocationID == "" {
		return fmt.Errorf("%w: invocation_id is required", ErrConflict)
	}

	owner = strings.TrimSpace(owner)
	if owner == "" {
		return fmt.Errorf("%w: owner is required", ErrConflict)
	}

	if nextAttemptAtUnixNano <= 0 {
		nextAttemptAtUnixNano = time.Now().Add(time.Minute).UnixNano()
	}

	message = truncateReactionLastError(message)
	now := time.Now().UnixNano()
	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE reaction_invocations
		SET status = CASE WHEN attempts >= max_attempts THEN ? ELSE ? END,
		    claimed_by = '',
		    claim_until = NULL,
		    last_error = ?,
		    next_attempt_at = ?,
		    updated_at = ?
		WHERE invocation_id = ? AND status = ? AND claimed_by = ?
	`), ReactionInvocationStatusFailed, ReactionInvocationStatusPending, message, nextAttemptAtUnixNano, now, invocationID, ReactionInvocationStatusRunning, owner)

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
	invocation, err := r.getInvocation(ctx, invocationID)
	if err != nil {
		return ReactionLocalMessageRecord{}, err
	}
	if invocation.EventID != eventID {
		return ReactionLocalMessageRecord{}, fmt.Errorf("%w: reaction local message event %s does not match invocation %s event %s", ErrConflict, eventID, invocationID, invocation.EventID)
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
		ON CONFLICT(invocation_id) DO NOTHING
	`), messageID, eventID, invocationID, mailbox, string(payload), createdAt)

	if err != nil {
		return ReactionLocalMessageRecord{}, normalizeSQLError(err)
	}

	message, err := r.getLocalMessageByInvocation(ctx, invocationID)
	if err != nil {
		return ReactionLocalMessageRecord{}, err
	}

	if !sameReactionLocalMessage(message, ReactionLocalMessageRecord{
		EventID:      eventID,
		InvocationID: invocationID,
		Mailbox:      mailbox,
		PayloadJSON:  payload,
	}) {
		return ReactionLocalMessageRecord{}, fmt.Errorf("%w: reaction local message for invocation %s already exists with different content", ErrConflict, invocationID)
	}

	return message, nil
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

func (r *SQLReactionsRepository) getSubscription(ctx context.Context, subscriptionID string) (ReactionSubscriptionRecord, error) {
	row := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT id, subscription_id, namespace_id, target_id, name, event_type, job_id, run_status, trigger_type, owning_cell,
		       enabled, created_at, updated_at
		FROM reaction_subscriptions
		WHERE subscription_id = ?
	`), subscriptionID)

	return scanReactionSubscription(row)
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

func (r *SQLReactionsRepository) getInvocationByEventTarget(ctx context.Context, eventID, targetID string) (ReactionInvocationRecord, error) {
	row := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT id, invocation_id, event_id, target_id, status, action_uses, action_descriptor_json, action_digest,
		       target_config_json, attempts, max_attempts, next_attempt_at, claimed_by, claim_until, last_error,
		       created_at, updated_at, completed_at
		FROM reaction_invocations
		WHERE event_id = ? AND target_id = ?
	`), eventID, targetID)

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

func (r *SQLReactionsRepository) getLocalMessageByInvocation(ctx context.Context, invocationID string) (ReactionLocalMessageRecord, error) {
	row := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT id, message_id, event_id, invocation_id, mailbox, payload_json, created_at
		FROM reaction_local_messages
		WHERE invocation_id = ?
	`), invocationID)

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

func scanReactionSubscription(scanner reactionScanner) (ReactionSubscriptionRecord, error) {
	var rec ReactionSubscriptionRecord
	var namespaceID sql.NullInt64
	if err := scanner.Scan(
		&rec.ID,
		&rec.SubscriptionID,
		&namespaceID,
		&rec.TargetID,
		&rec.Name,
		&rec.EventType,
		&rec.JobID,
		&rec.RunStatus,
		&rec.TriggerType,
		&rec.OwningCell,
		&rec.Enabled,
		&rec.CreatedAt,
		&rec.UpdatedAt,
	); err != nil {
		return ReactionSubscriptionRecord{}, normalizeSQLError(err)
	}

	if namespaceID.Valid {
		rec.NamespaceID = &namespaceID.Int64
	}

	return rec, nil
}

func scanReactionSubscriptionMatch(scanner reactionScanner) (ReactionSubscriptionMatch, error) {
	var match ReactionSubscriptionMatch
	var subscriptionNamespaceID sql.NullInt64
	var targetNamespaceID sql.NullInt64
	var targetConfig string
	var targetSecretRefs string
	if err := scanner.Scan(
		&match.Subscription.ID,
		&match.Subscription.SubscriptionID,
		&subscriptionNamespaceID,
		&match.Subscription.TargetID,
		&match.Subscription.Name,
		&match.Subscription.EventType,
		&match.Subscription.JobID,
		&match.Subscription.RunStatus,
		&match.Subscription.TriggerType,
		&match.Subscription.OwningCell,
		&match.Subscription.Enabled,
		&match.Subscription.CreatedAt,
		&match.Subscription.UpdatedAt,
		&match.Target.ID,
		&match.Target.TargetID,
		&targetNamespaceID,
		&match.Target.Name,
		&match.Target.Kind,
		&match.Target.Uses,
		&targetConfig,
		&targetSecretRefs,
		&match.Target.Enabled,
		&match.Target.CreatedAt,
		&match.Target.UpdatedAt,
	); err != nil {
		return ReactionSubscriptionMatch{}, normalizeSQLError(err)
	}

	if subscriptionNamespaceID.Valid {
		match.Subscription.NamespaceID = &subscriptionNamespaceID.Int64
	}

	if targetNamespaceID.Valid {
		match.Target.NamespaceID = &targetNamespaceID.Int64
	}

	match.Target.ConfigJSON = []byte(targetConfig)
	match.Target.SecretRefsJSON = []byte(targetSecretRefs)
	return match, nil
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

func sameReactionEvent(left, right ReactionEventRecord) bool {
	if !sameNullableInt64(left.NamespaceID, right.NamespaceID) {
		return false
	}

	return left.EventID == right.EventID &&
		left.Source == right.Source &&
		left.EventType == right.EventType &&
		left.JobID == right.JobID &&
		left.RunID == right.RunID &&
		left.Actor == right.Actor &&
		sameReactionJSON(left.PayloadJSON, right.PayloadJSON) &&
		left.SourceCell == right.SourceCell
}

func sameReactionInvocation(left, right ReactionInvocationRecord) bool {
	return left.EventID == right.EventID &&
		left.TargetID == right.TargetID &&
		left.ActionUses == right.ActionUses &&
		sameReactionJSON(left.ActionDescriptorJSON, right.ActionDescriptorJSON) &&
		left.ActionDigest == right.ActionDigest &&
		sameReactionJSON(left.TargetConfigJSON, right.TargetConfigJSON) &&
		left.MaxAttempts == right.MaxAttempts
}

func sameReactionLocalMessage(left, right ReactionLocalMessageRecord) bool {
	return left.EventID == right.EventID &&
		left.InvocationID == right.InvocationID &&
		left.Mailbox == right.Mailbox &&
		sameReactionJSON(left.PayloadJSON, right.PayloadJSON)
}

func sameReactionJSON(left, right []byte) bool {
	leftNormalized, err := canonicalReactionJSON(left)
	if err != nil {
		return false
	}

	rightNormalized, err := canonicalReactionJSON(right)
	if err != nil {
		return false
	}

	return bytes.Equal(leftNormalized, rightNormalized)
}

func canonicalReactionJSON(raw []byte) ([]byte, error) {
	payload := bytes.TrimSpace(raw)
	if len(payload) == 0 {
		return nil, fmt.Errorf("empty reaction JSON")
	}

	var value any
	if err := json.Unmarshal(payload, &value); err != nil {
		return nil, err
	}

	return json.Marshal(value)
}

func sameNullableInt64(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}

	return *left == *right
}

func nullableNamespaceID(value int64) *int64 {
	if value <= 0 {
		return nil
	}

	return &value
}

func truncateReactionLastError(message string) string {
	message = strings.TrimSpace(message)
	runes := []rune(message)
	if len(runes) <= ReactionInvocationLastErrorMaxLength {
		return message
	}

	suffix := []rune("...(truncated)")
	keep := ReactionInvocationLastErrorMaxLength - len(suffix)
	if keep < 0 {
		keep = ReactionInvocationLastErrorMaxLength
		suffix = nil
	}

	return string(runes[:keep]) + string(suffix)
}

func reactionEventPayloadString(payload []byte, field string) string {
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 {
		return ""
	}

	var object map[string]any
	if err := json.Unmarshal(payload, &object); err != nil {
		return ""
	}

	value, ok := object[field].(string)
	if !ok {
		return ""
	}

	return strings.TrimSpace(value)
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

	normalized, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%w: %s must be valid JSON", ErrConflict, field)
	}

	return normalized, nil
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

	normalized, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%w: %s must be valid JSON", ErrConflict, field)
	}

	return normalized, nil
}

func validateReactionTargetAction(kind, uses string) error {
	switch kind {
	case ReactionTargetKindLocal:
		if uses != ReactionActionNotifyLocal {
			return fmt.Errorf("%w: reaction target kind %q requires uses %q", ErrConflict, kind, ReactionActionNotifyLocal)
		}
	case ReactionTargetKindJob:
		if uses != ReactionActionTriggerJob {
			return fmt.Errorf("%w: reaction target kind %q requires uses %q", ErrConflict, kind, ReactionActionTriggerJob)
		}
	default:
		return fmt.Errorf("%w: unsupported reaction target kind %q", ErrConflict, kind)
	}

	return nil
}

func validateReactionEventSource(source string) error {
	switch source {
	case ReactionEventSourceLifecycle, ReactionEventSourceManual:
		return nil
	default:
		return fmt.Errorf("%w: unsupported reaction event source %q", ErrConflict, source)
	}
}

func validateReactionEventType(eventType string) error {
	switch eventType {
	case ReactionEventTypeManualNotice,
		ReactionEventTypeRunCompleted,
		ReactionEventTypeDefinitionValidationFailed:
		return nil
	default:
		return fmt.Errorf("%w: unsupported reaction event type %q", ErrConflict, eventType)
	}
}

func validateReactionRunStatusFilter(status string) error {
	if status == "" {
		return nil
	}

	switch status {
	case RunStatusQueued,
		RunStatusRunning,
		RunStatusSucceeded,
		RunStatusFailed,
		RunStatusOrphaned,
		RunStatusCancelled,
		RunStatusAbandoned,
		RunStatusAborted:
		return nil
	default:
		return fmt.Errorf("%w: unsupported reaction run status filter %q", ErrConflict, status)
	}
}

func validateReactionTriggerTypeFilter(triggerType string) error {
	if triggerType == "" {
		return nil
	}

	switch triggerType {
	case TriggerTypeManual,
		TriggerTypeCron,
		TriggerTypeReplay,
		TriggerTypeWebhook:
		return nil
	default:
		return fmt.Errorf("%w: unsupported reaction trigger type filter %q", ErrConflict, triggerType)
	}
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
