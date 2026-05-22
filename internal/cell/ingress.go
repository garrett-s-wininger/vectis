package cell

import (
	"context"
	"errors"
	"fmt"
	"strings"

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

var ErrCellNotRoutable = errors.New("cell not routable")

type QueueExecutionIngress struct {
	queue  interfaces.QueueService
	logger interfaces.Logger
}

type StaticExecutionRouter struct {
	routes map[string]ExecutionIngress
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

func NewStaticExecutionRouter(routes map[string]ExecutionIngress) StaticExecutionRouter {
	copied := make(map[string]ExecutionIngress, len(routes))
	for cellID, ingress := range routes {
		cellID = normalizeRouteCellID(cellID)
		if ingress == nil {
			continue
		}

		copied[cellID] = ingress
	}

	return StaticExecutionRouter{routes: copied}
}

func (r StaticExecutionRouter) SubmitExecution(ctx context.Context, submission ExecutionSubmission) error {
	if submission.Request == nil {
		return errors.New("job request is required")
	}

	targetCellID := normalizeRouteCellID(submission.TargetCellID())
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("vectis.cell.target_id", targetCellID))

	ingress := r.routes[targetCellID]
	if ingress == nil {
		return fmt.Errorf("%w: %s", ErrCellNotRoutable, targetCellID)
	}

	return ingress.SubmitExecution(ctx, submission)
}

func NewQueueExecutionIngress(queue interfaces.QueueService, logger interfaces.Logger) QueueExecutionIngress {
	return QueueExecutionIngress{
		queue:  queue,
		logger: logger,
	}
}

func SubmitToLocalQueue(ctx context.Context, localCellID string, queue interfaces.QueueService, req *api.JobRequest, logger interfaces.Logger) error {
	submission, err := NewExecutionSubmission(req)
	if err != nil {
		return err
	}

	router := NewStaticExecutionRouter(map[string]ExecutionIngress{
		localCellID: NewQueueExecutionIngress(queue, logger),
	})

	return router.SubmitExecution(ctx, submission)
}

func (i QueueExecutionIngress) SubmitExecution(ctx context.Context, submission ExecutionSubmission) error {
	if submission.Request == nil {
		return errors.New("job request is required")
	}

	trace.SpanFromContext(ctx).SetAttributes(attribute.String("vectis.cell.ingress", "queue"))
	return queueclient.EnqueueWithRetry(ctx, i.queue, submission.Request, i.logger)
}

func IsCellNotRoutable(err error) bool {
	return errors.Is(err, ErrCellNotRoutable)
}

func normalizeRouteCellID(cellID string) string {
	cellID = strings.TrimSpace(cellID)
	if cellID == "" {
		return dal.DefaultCellID
	}

	return cellID
}

var _ ExecutionIngress = QueueExecutionIngress{}
var _ ExecutionIngress = StaticExecutionRouter{}
