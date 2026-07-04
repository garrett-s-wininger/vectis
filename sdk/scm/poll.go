package scm

import "context"

// PollProvider discovers source-control events for durable SCM poll triggers.
type PollProvider interface {
	Poll(context.Context, PollSpec) (PollResult, error)
}

// PollSpec is the provider-neutral shape persisted for SCM poll triggers.
type PollSpec struct {
	TriggerID int64
	JobID     string
	Provider  string
	BaseURL   string
	Project   string
	Branch    string
	Query     string
	Cursor    string
}

// PollResult returns new provider events plus the cursor to persist after a poll.
type PollResult struct {
	Events []Event
	Cursor string
}

// Event is a provider event that can dispatch a Vectis run once per key.
type Event struct {
	Key         string
	PayloadJSON string
}
