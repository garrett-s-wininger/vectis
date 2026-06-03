package taskreduce

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"vectis/internal/dal"
)

type Outcome string

const (
	OutcomeWaiting   Outcome = "waiting"
	OutcomeSucceeded Outcome = "succeeded"
	OutcomeFailed    Outcome = "failed"
)

type Decision struct {
	Outcome Outcome
	Summary dal.RunTaskCompletion
}

type Reducer struct {
	runs dal.RunsRepository
}

func New(runs dal.RunsRepository) *Reducer {
	return &Reducer{runs: runs}
}

func Decide(summary dal.RunTaskCompletion) Decision {
	switch {
	case summary.TerminalFailed > 0:
		return Decision{Outcome: OutcomeFailed, Summary: summary}
	case summary.AllSucceeded():
		return Decision{Outcome: OutcomeSucceeded, Summary: summary}
	default:
		return Decision{Outcome: OutcomeWaiting, Summary: summary}
	}
}

func (r *Reducer) Reduce(ctx context.Context, runID string) (Decision, error) {
	if r == nil {
		return Decision{}, errors.New("task reducer is required")
	}

	if r.runs == nil {
		return Decision{}, errors.New("runs repository is required")
	}

	runID = strings.TrimSpace(runID)
	if runID == "" {
		return Decision{}, fmt.Errorf("%w: run_id is required", dal.ErrNotFound)
	}

	summary, err := r.runs.GetRunTaskCompletion(ctx, runID)
	if err != nil {
		return Decision{}, err
	}

	return Decide(summary), nil
}
