package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"vectis/internal/dal"
)

type runPersistenceAcknowledger interface {
	AcknowledgeRunPersistence(ctx context.Context, runID, token string) error
}

var errRunNotExecutable = errors.New("run has completed or been deleted")

func (w *worker) checkRunCanLoadHotState(ctx context.Context, runID string) error {
	if w.store == nil {
		return nil
	}

	status, found, err := w.store.GetRunStatus(ctx, runID)
	if err != nil {
		return err
	}

	if !found || status == dal.RunStatusSucceeded || status == dal.RunStatusFailed ||
		status == dal.RunStatusCancelled || status == dal.RunStatusAborted || status == dal.RunStatusAbandoned {
		return fmt.Errorf("%w: %w: %s", dal.ErrConflict, errRunNotExecutable, runID)
	}

	return nil
}

func (w *worker) acknowledgePersistedRun(ctx context.Context, result dal.ExecutionFinalizationResult) {
	if w.store == nil || result.PersistenceToken == "" || len(result.Executions) == 0 ||
		w.executionChoreographer().RequiresDurableTaskRows() || !isTerminalFinalizationOutcome(result.Outcome) {
		return
	}

	ack, ok := w.executionChoreographer().(runPersistenceAcknowledger)
	if !ok {
		return
	}

	ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := w.acknowledgeRetirement(ackCtx, ack, dal.OrchestratorRetirement{RunID: result.RunID, PersistenceToken: result.PersistenceToken}); err != nil {
		w.logger.Warn("Run %s persistence acknowledgment will be retried: %v", result.RunID, err)
	}
}

func (w *worker) acknowledgeRetirement(ctx context.Context, ack runPersistenceAcknowledger, receipt dal.OrchestratorRetirement) error {
	if err := ack.AcknowledgeRunPersistence(ctx, receipt.RunID, receipt.PersistenceToken); err != nil {
		return err
	}

	if w.retirements != nil {
		return w.retirements.Delete(ctx, receipt.PersistenceToken)
	}

	return nil
}

func (w *worker) drainRetirements(ctx context.Context) error {
	ack, ok := w.executionChoreographer().(runPersistenceAcknowledger)
	if !ok || w.retirements == nil {
		return nil
	}

	receipts, err := w.retirements.ListDue(ctx, time.Now().Unix(), 256)
	if err != nil {
		return err
	}

	for _, receipt := range receipts {
		if err := ctx.Err(); err != nil {
			return err
		}

		ackCtx, cancel := context.WithTimeout(ctx, time.Second)
		ackErr := w.acknowledgeRetirement(ackCtx, ack, receipt)
		cancel()
		if ackErr != nil {
			if retryErr := w.retirements.RetryAfter(ctx, receipt.PersistenceToken, time.Now().Add(10*time.Second).Unix()); retryErr != nil {
				return fmt.Errorf("defer persistence acknowledgment: %w", retryErr)
			}

			w.logger.Warn("Run %s persistence acknowledgment retry failed: %v", receipt.RunID, ackErr)
		}
	}

	return nil
}

func (w *worker) runRetirementLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		batchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := w.drainRetirements(batchCtx)
		cancel()

		if err != nil && ctx.Err() == nil {
			w.logger.Warn("Run persistence receipt recovery failed: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
