// Package audit provides asynchronous structured audit logging for API operations.
package audit

import (
	"context"
	"time"
)

// Event represents a single audit log entry.
type Event struct {
	Type          string
	ActorID       int64
	TargetID      int64
	Metadata      map[string]interface{}
	IPAddress     string
	CorrelationID string
	Timestamp     time.Time
}

// Auditor persists audit events.
type Auditor interface {
	// Log records an audit event. It should not block the caller.
	// Returns an error only if the event could not be queued.
	Log(ctx context.Context, event Event) error
}

// EventType constants for audit log entries.
const (
	EventTokenCreated    = "token.created"
	EventTokenDeleted    = "token.deleted"
	EventPasswordChanged = "password.changed"
	EventUserCreated     = "user.created"
	EventUserUpdated     = "user.updated"
	EventUserDeleted     = "user.deleted"
	EventAuthSuccess     = "auth.success"
	EventAuthFailure     = "auth.failure"
)
