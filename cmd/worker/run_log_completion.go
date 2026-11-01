package main

import (
	"context"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/job"
)

func (w *worker) publishRunLogCompletion(parent context.Context, update dal.RunStatusUpdate) {
	if w.logClient == nil || update.RunID == "" {
		return
	}

	var outcome api.RunOutcome
	var status string
	switch update.Status {
	case dal.RunStatusSucceeded:
		outcome, status = api.RunOutcome_RUN_OUTCOME_SUCCESS, "success"
	case dal.RunStatusFailed:
		outcome, status = api.RunOutcome_RUN_OUTCOME_FAILURE, "failure"
	case dal.RunStatusCancelled, dal.RunStatusAborted, dal.RunStatusAbandoned:
		outcome, status = api.RunOutcome_RUN_OUTCOME_UNKNOWN, update.Status
	default:
		return
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer cancel()

	err := job.PublishRunLogCompletion(ctx, w.logClient, w.logger, update.RunID, status, outcome, update.RootExecutionID)
	if err != nil {
		w.logger.Warn("Publish run log completion for %s: %v", update.RunID, err)
	}
}
