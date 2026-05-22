package cell

import (
	"context"
	"errors"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/queueclient"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type ExecutionSubmission struct {
	Request  *api.JobRequest
	Envelope *ExecutionEnvelope
}

type ExecutionIngress interface {
	SubmitExecution(ctx context.Context, submission ExecutionSubmission) error
}

type QueueExecutionIngress struct {
	queue  interfaces.QueueService
	logger interfaces.Logger
}

func NewExecutionSubmission(req *api.JobRequest) (ExecutionSubmission, error) {
	if req == nil {
		return ExecutionSubmission{}, errors.New("job request is required")
	}

	env, _, err := ExecutionEnvelopeFromRequest(req)
	if err != nil {
		return ExecutionSubmission{}, err
	}

	return ExecutionSubmission{
		Request:  req,
		Envelope: env,
	}, nil
}

func (s ExecutionSubmission) TargetCellID() string {
	if s.Envelope != nil {
		return s.Envelope.CellID
	}

	return dal.DefaultCellID
}

func NewQueueExecutionIngress(queue interfaces.QueueService, logger interfaces.Logger) QueueExecutionIngress {
	return QueueExecutionIngress{
		queue:  queue,
		logger: logger,
	}
}

func SubmitToQueue(ctx context.Context, queue interfaces.QueueService, req *api.JobRequest, logger interfaces.Logger) error {
	submission, err := NewExecutionSubmission(req)
	if err != nil {
		return err
	}

	return NewQueueExecutionIngress(queue, logger).SubmitExecution(ctx, submission)
}

func (i QueueExecutionIngress) SubmitExecution(ctx context.Context, submission ExecutionSubmission) error {
	if submission.Request == nil {
		return errors.New("job request is required")
	}

	trace.SpanFromContext(ctx).SetAttributes(attribute.String("vectis.cell.target_id", submission.TargetCellID()))
	return queueclient.EnqueueWithRetry(ctx, i.queue, submission.Request, i.logger)
}

var _ ExecutionIngress = QueueExecutionIngress{}
