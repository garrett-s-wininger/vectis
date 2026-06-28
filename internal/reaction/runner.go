package reaction

import (
	"context"
	"fmt"
	"strings"
	"time"

	"vectis/internal/dal"
)

const (
	defaultRunnerOwner = "reaction-runner"
	defaultClaimTTL    = time.Minute
	defaultRetryDelay  = time.Minute
)

type Store interface {
	GetEvent(ctx context.Context, eventID string) (dal.ReactionEventRecord, error)
	ListReadyInvocations(ctx context.Context, nowUnixNano int64, limit int) ([]dal.ReactionInvocationRecord, error)
	MarkInvocationRunning(ctx context.Context, invocationID, owner string, claimUntilUnixNano int64) (bool, error)
	MarkInvocationSucceeded(ctx context.Context, invocationID string, completedAtUnixNano int64) error
	MarkInvocationFailed(ctx context.Context, invocationID, message string, nextAttemptAtUnixNano int64) error
	RecordLocalMessage(ctx context.Context, create dal.ReactionLocalMessageCreate) (dal.ReactionLocalMessageRecord, error)
}

type ActionRequest struct {
	Event      dal.ReactionEventRecord
	Invocation dal.ReactionInvocationRecord
	Payload    []byte
}

type Action interface {
	Type() string
	ExecuteInvocation(ctx context.Context, req ActionRequest) error
}

type Runner struct {
	Store      Store
	Owner      string
	ClaimTTL   time.Duration
	RetryDelay time.Duration
	Actions    []Action
}

type RunSummary struct {
	Scanned   int
	Claimed   int
	Skipped   int
	Succeeded int
	Failed    int
}

func (r *Runner) RunOnce(ctx context.Context, limit int) (RunSummary, error) {
	if r == nil || r.Store == nil {
		return RunSummary{}, fmt.Errorf("reaction runner requires a store")
	}

	actions, err := r.actionRegistry()
	if err != nil {
		return RunSummary{}, err
	}

	now := time.Now()
	ready, err := r.Store.ListReadyInvocations(ctx, now.UnixNano(), limit)
	if err != nil {
		return RunSummary{}, err
	}

	summary := RunSummary{Scanned: len(ready)}
	for _, invocation := range ready {
		claimed, err := r.Store.MarkInvocationRunning(ctx, invocation.InvocationID, r.owner(), now.Add(r.claimTTL()).UnixNano())
		if err != nil {
			return summary, err
		}

		if !claimed {
			summary.Skipped++
			continue
		}

		summary.Claimed++
		if err := r.runClaimed(ctx, invocation, actions); err != nil {
			summary.Failed++
			if markErr := r.Store.MarkInvocationFailed(ctx, invocation.InvocationID, err.Error(), time.Now().Add(r.retryDelay()).UnixNano()); markErr != nil {
				return summary, markErr
			}
			continue
		}

		if err := r.Store.MarkInvocationSucceeded(ctx, invocation.InvocationID, time.Now().UnixNano()); err != nil {
			return summary, err
		}

		summary.Succeeded++
	}

	return summary, nil
}

func (r *Runner) runClaimed(ctx context.Context, invocation dal.ReactionInvocationRecord, actions map[string]Action) error {
	event, err := r.Store.GetEvent(ctx, invocation.EventID)
	if err != nil {
		return err
	}

	actionUses := strings.TrimSpace(invocation.ActionUses)
	action, ok := actions[actionUses]
	if !ok {
		return fmt.Errorf("unsupported reaction action %q", actionUses)
	}

	return action.ExecuteInvocation(ctx, ActionRequest{
		Event:      event,
		Invocation: invocation,
	})
}

func (r *Runner) actionRegistry() (map[string]Action, error) {
	actions := r.Actions
	if len(actions) == 0 {
		actions = []Action{&LocalNotifyAction{Store: r.Store}}
	}

	out := make(map[string]Action, len(actions))
	for _, action := range actions {
		if action == nil {
			return nil, fmt.Errorf("reaction runner action registry contains a nil action")
		}

		actionType := strings.TrimSpace(action.Type())
		if actionType == "" {
			return nil, fmt.Errorf("reaction runner action registry contains an action with empty type")
		}

		if _, exists := out[actionType]; exists {
			return nil, fmt.Errorf("reaction runner action registry contains duplicate action %q", actionType)
		}

		out[actionType] = action
	}

	return out, nil
}

func (r *Runner) owner() string {
	owner := strings.TrimSpace(r.Owner)
	if owner == "" {
		return defaultRunnerOwner
	}

	return owner
}

func (r *Runner) claimTTL() time.Duration {
	if r.ClaimTTL <= 0 {
		return defaultClaimTTL
	}

	return r.ClaimTTL
}

func (r *Runner) retryDelay() time.Duration {
	if r.RetryDelay <= 0 {
		return defaultRetryDelay
	}

	return r.RetryDelay
}
