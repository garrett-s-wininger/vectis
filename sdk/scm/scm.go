package scm

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type Provider interface {
	QueryChanges(context.Context, Query) ([]Change, error)
}

type Query struct {
	Project  string
	Branch   string
	Status   string
	ChangeID string
}

type PollOptions struct {
	Query    Query
	ChangeID string
	Timeout  time.Duration
	Interval time.Duration
}

type Change struct {
	ID              string              `json:"id"`
	Project         string              `json:"project"`
	Branch          string              `json:"branch"`
	Status          string              `json:"status"`
	ChangeID        string              `json:"change_id"`
	Number          int                 `json:"number,omitempty"`
	CurrentRevision string              `json:"current_revision"`
	Revisions       map[string]Revision `json:"revisions"`
}

type Revision struct {
	Ref string `json:"ref"`
}

func PollChange(ctx context.Context, provider Provider, opts PollOptions) (Change, error) {
	if provider == nil {
		return Change{}, fmt.Errorf("SCM provider is required")
	}

	query := opts.Query
	if query.ChangeID == "" {
		query.ChangeID = opts.ChangeID
	}

	changeID := strings.TrimSpace(opts.ChangeID)
	if changeID == "" {
		changeID = strings.TrimSpace(query.ChangeID)
	}

	if changeID == "" {
		return Change{}, fmt.Errorf("SCM poll change_id is required")
	}

	interval := opts.Interval
	if interval <= 0 {
		interval = time.Second
	}

	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	var lastCount int
	var lastErr error
	for {
		changes, err := provider.QueryChanges(ctx, query)
		if err == nil {
			lastCount = len(changes)
			if info, ok := FindChange(changes, changeID); ok {
				return info, nil
			}
		} else {
			lastErr = err
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			if lastErr != nil {
				return Change{}, fmt.Errorf("poll SCM change %s: %w", changeID, lastErr)
			}

			return Change{}, fmt.Errorf("poll SCM change %s: not found after last query returned %d change(s): %w", changeID, lastCount, ctx.Err())
		case <-timer.C:
		}
	}
}

func FindChange(changes []Change, changeID string) (Change, bool) {
	changeID = strings.TrimSpace(changeID)
	for _, change := range changes {
		if strings.TrimSpace(change.ChangeID) == changeID || strings.TrimSpace(change.ID) == changeID {
			return change, true
		}
	}

	return Change{}, false
}

func (c Change) CurrentRevisionRef() (string, string, error) {
	revision := strings.TrimSpace(c.CurrentRevision)
	if revision == "" {
		return "", "", fmt.Errorf("SCM change missing current_revision")
	}

	info, ok := c.Revisions[revision]
	if !ok || strings.TrimSpace(info.Ref) == "" {
		return "", "", fmt.Errorf("SCM change missing fetch ref for revision %s", revision)
	}

	return revision, strings.TrimSpace(info.Ref), nil
}
