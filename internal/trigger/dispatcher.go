package trigger

import (
	"context"
	"fmt"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/action/actionregistry"
	"vectis/internal/cell"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/dispatchmeta"
	"vectis/internal/interfaces"
)

type Dispatcher struct {
	Runs           dal.RunsRepository
	Dispatch       dal.DispatchEventsRepository
	QueueClient    interfaces.QueueService
	Ingress        cell.ExecutionIngress
	Logger         interfaces.Logger
	Clock          interfaces.Clock
	ActionResolver actionregistry.Resolver
	Source         string
	SourceInstance string
}

func (d Dispatcher) DispatchRun(ctx context.Context, job *api.Job, runID, definitionHash string) error {
	if d.Runs == nil {
		return fmt.Errorf("trigger dispatcher runs repository is required")
	}

	runID = strings.TrimSpace(runID)
	if runID == "" {
		return fmt.Errorf("trigger dispatcher run id is required")
	}

	if job == nil {
		return fmt.Errorf("trigger dispatcher job is required")
	}

	job.RunId = &runID
	req := &api.JobRequest{Job: job}
	if _, err := d.attachExecutionEnvelope(ctx, req, runID, d.clock().Now().UnixNano()); err != nil {
		d.logger().Error("Failed to attach execution envelope for trigger run %s: %v", runID, err)
	}

	d.recordDispatchEvent(ctx, runID, dal.DispatchEventAttempt, nil)
	dispatchReq, err := cell.RecordExecutionHandoffPayload(ctx, d.Runs, runID, req, definitionHash)
	if err != nil {
		msg := err.Error()
		d.recordDispatchEvent(ctx, runID, dal.DispatchEventFailure, &msg)
		return err
	}

	ingress, err := d.executionIngress()
	if err != nil {
		msg := err.Error()
		d.recordDispatchEvent(ctx, runID, dal.DispatchEventFailure, &msg)
		return err
	}

	submission, err := cell.NewExecutionSubmission(dispatchReq)
	if err != nil {
		msg := err.Error()
		d.recordDispatchEvent(ctx, runID, dal.DispatchEventFailure, &msg)
		return err
	}

	if err := ingress.SubmitExecution(ctx, submission); err != nil {
		msg := err.Error()
		d.recordDispatchEvent(ctx, runID, dal.DispatchEventFailure, &msg)
		return err
	}

	if err := d.Runs.TouchDispatched(ctx, runID); err != nil {
		d.logger().Error("TouchDispatched after enqueue for run %s: %v", runID, err)
		msg := "touch dispatched: " + err.Error()
		d.recordDispatchEvent(ctx, runID, dal.DispatchEventFailure, &msg)
		return nil
	}

	d.recordDispatchEvent(ctx, runID, dal.DispatchEventSuccess, nil)
	return nil
}

func (d Dispatcher) attachExecutionEnvelope(ctx context.Context, req *api.JobRequest, runID string, createdAtUnixNano int64) (*cell.ExecutionEnvelope, error) {
	dispatch, err := d.Runs.GetPendingExecution(ctx, runID)
	if err != nil {
		return nil, err
	}

	deadline, err := d.Runs.EnsureExecutionStartDeadline(ctx, dispatch.ExecutionID, dispatchmeta.DeadlineUnixNano(d.clock().Now(), config.DispatchStartTTL()))
	if err != nil {
		return nil, err
	}

	dispatch.StartDeadlineUnixNano = deadline
	return cell.AttachExecutionEnvelopeWithActions(req, dispatch, createdAtUnixNano, d.ActionResolver)
}

func (d Dispatcher) executionIngress() (cell.ExecutionIngress, error) {
	if d.Ingress != nil {
		return d.Ingress, nil
	}

	endpoints, err := config.CellIngressEndpoints()
	if err != nil {
		return nil, err
	}

	queueClient := d.QueueClient
	if database.GlobalAndCellDatabasesAreSplit() {
		queueClient = nil
	}

	return cell.NewExecutionRouterWithOptions(config.CellID(), queueClient, endpoints, d.logger(), cell.ExecutionRouterOptions{
		TLSConfigForEndpoint: config.CellIngressHTTPClientTLSConfig,
	}), nil
}

func (d Dispatcher) recordDispatchEvent(ctx context.Context, runID, eventType string, message *string) {
	if d.Dispatch == nil {
		return
	}

	source := strings.TrimSpace(d.Source)
	if source == "" {
		source = "trigger"
	}

	if err := d.Dispatch.RecordWithInstance(ctx, runID, source, d.SourceInstance, eventType, message); err != nil {
		d.logger().Error("Failed to record trigger dispatch event for run %s: %v", runID, err)
	}
}

func (d Dispatcher) logger() interfaces.Logger {
	if d.Logger != nil {
		return d.Logger
	}

	return interfaces.NewLogger("trigger")
}

func (d Dispatcher) clock() interfaces.Clock {
	if d.Clock != nil {
		return d.Clock
	}

	return interfaces.SystemClock{}
}
