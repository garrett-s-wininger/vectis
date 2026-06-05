package taskdispatch

import (
	"context"
	"errors"
	"fmt"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces"

	"google.golang.org/protobuf/encoding/protojson"
)

const defaultDrainLimit = 100

type Dispatcher struct {
	runs            dal.RunsRepository
	intents         dal.TaskDispatchIntentsRepository
	dispatch        dal.DispatchEventsRepository
	dispatchMetrics DispatchMetrics
	ingress         cell.ExecutionIngress
	clock           interfaces.Clock
}

type DispatchMetrics interface {
	RecordDispatchEvent(ctx context.Context, source, eventType, targetCell string)
}

type DrainOptions struct {
	CellID              string
	RunID               string
	Limit               int
	RetryCutoffUnixNano int64
}

type DrainResult struct {
	Listed   int
	Enqueued int
	Failed   int
}

func New(runs dal.RunsRepository, intents dal.TaskDispatchIntentsRepository, dispatch dal.DispatchEventsRepository, ingress cell.ExecutionIngress, clock interfaces.Clock) *Dispatcher {
	if clock == nil {
		clock = interfaces.SystemClock{}
	}

	return &Dispatcher{
		runs:     runs,
		intents:  intents,
		dispatch: dispatch,
		ingress:  ingress,
		clock:    clock,
	}
}

func (d *Dispatcher) SetDispatchMetrics(metrics DispatchMetrics) {
	if d == nil {
		return
	}

	d.dispatchMetrics = metrics
}

func (d *Dispatcher) Drain(ctx context.Context, opts DrainOptions) (DrainResult, error) {
	if err := d.validate(); err != nil {
		return DrainResult{}, err
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = defaultDrainLimit
	}

	pending, err := d.listPending(ctx, opts, limit)
	if err != nil {
		return DrainResult{}, fmt.Errorf("list pending task dispatch intents: %w", err)
	}

	result := DrainResult{Listed: len(pending)}
	for _, intent := range pending {
		attemptedAt := d.clock.Now().UnixNano()
		d.recordDispatchEvent(ctx, intent, dal.DispatchEventAttempt, nil)
		if err := d.dispatchOne(ctx, intent, attemptedAt); err != nil {
			result.Failed++
			d.recordDispatchEvent(ctx, intent, dal.DispatchEventFailure, err)
			if markErr := d.intents.MarkEnqueueFailed(ctx, intent.ExecutionID, attemptedAt, err.Error()); markErr != nil {
				return result, fmt.Errorf("mark task dispatch intent %s failed after dispatch error %q: %w", intent.ExecutionID, err.Error(), markErr)
			}

			continue
		}

		if err := d.intents.MarkEnqueued(ctx, intent.ExecutionID, d.clock.Now().UnixNano()); err != nil {
			return result, fmt.Errorf("mark task dispatch intent %s enqueued: %w", intent.ExecutionID, err)
		}

		if err := d.runs.TouchDispatched(ctx, intent.RunID); err != nil {
			return result, fmt.Errorf("touch task dispatch run %s dispatched: %w", intent.RunID, err)
		}

		d.recordDispatchEvent(ctx, intent, dal.DispatchEventSuccess, nil)
		result.Enqueued++
	}

	return result, nil
}

func (d *Dispatcher) HasPending(ctx context.Context, opts DrainOptions) (bool, error) {
	if err := d.validate(); err != nil {
		return false, err
	}

	pending, err := d.listPending(ctx, opts, 1)
	if err != nil {
		return false, fmt.Errorf("list pending task dispatch intents: %w", err)
	}

	return len(pending) > 0, nil
}

func (d *Dispatcher) validate() error {
	if d == nil {
		return errors.New("task dispatcher is required")
	}

	if d.runs == nil {
		return errors.New("runs repository is required")
	}

	if d.intents == nil {
		return errors.New("task dispatch intents repository is required")
	}

	if d.ingress == nil {
		return errors.New("execution ingress is required")
	}

	return nil
}

func (d *Dispatcher) retryCutoff(cutoff int64) int64 {
	if cutoff <= 0 {
		cutoff = d.clock.Now().UnixNano()
	}

	return cutoff
}

func (d *Dispatcher) listPending(ctx context.Context, opts DrainOptions, limit int) ([]dal.TaskDispatchIntent, error) {
	cutoff := d.retryCutoff(opts.RetryCutoffUnixNano)
	if opts.RunID != "" {
		return d.intents.ListPendingForRun(ctx, opts.RunID, opts.CellID, cutoff, limit)
	}

	return d.intents.ListPending(ctx, opts.CellID, cutoff, limit)
}

func (d *Dispatcher) dispatchOne(ctx context.Context, intent dal.TaskDispatchIntent, createdAtUnixNano int64) error {
	req, err := d.requestForIntent(ctx, intent, createdAtUnixNano)
	if err != nil {
		return err
	}

	submission, err := cell.NewExecutionSubmission(req)
	if err != nil {
		return fmt.Errorf("execution submission: %w", err)
	}

	if err := d.ingress.SubmitExecution(ctx, submission); err != nil {
		return fmt.Errorf("submit execution: %w", err)
	}

	return nil
}

func (d *Dispatcher) requestForIntent(ctx context.Context, intent dal.TaskDispatchIntent, createdAtUnixNano int64) (*api.JobRequest, error) {
	dispatch, err := d.runs.GetExecutionDispatch(ctx, intent.ExecutionID)
	if err != nil {
		return nil, fmt.Errorf("load execution dispatch record: %w", err)
	}

	payload, err := d.runs.GetExecutionPayloadForRun(ctx, dispatch.RunID)
	if err != nil {
		return nil, fmt.Errorf("load execution payload: %w", err)
	}

	var req api.JobRequest
	if err := protojson.Unmarshal([]byte(payload.PayloadJSON), &req); err != nil {
		return nil, fmt.Errorf("parse execution payload: %w", err)
	}

	if _, err := cell.AttachExecutionEnvelope(&req, dispatch, createdAtUnixNano); err != nil {
		return nil, fmt.Errorf("attach execution envelope: %w", err)
	}

	return &req, nil
}

func (d *Dispatcher) recordDispatchEvent(ctx context.Context, intent dal.TaskDispatchIntent, eventType string, dispatchErr error) {
	if d.dispatchMetrics != nil {
		d.dispatchMetrics.RecordDispatchEvent(ctx, dal.DispatchSourceTask, eventType, intent.CellID)
	}

	if d.dispatch == nil {
		return
	}

	message := taskDispatchEventMessage(intent, dispatchErr)
	_ = d.dispatch.Record(ctx, intent.RunID, dal.DispatchSourceTask, eventType, &message)
}

func taskDispatchEventMessage(intent dal.TaskDispatchIntent, dispatchErr error) string {
	message := fmt.Sprintf("execution_id=%s task_id=%s task_attempt_id=%s", intent.ExecutionID, intent.TaskID, intent.TaskAttemptID)
	if dispatchErr != nil {
		message = fmt.Sprintf("%s error=%s", message, dispatchErr.Error())
	}

	const maxMessageLen = 1000
	if len(message) > maxMessageLen {
		return message[:maxMessageLen]
	}

	return message
}
