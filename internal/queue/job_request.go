package queue

import (
	"context"
	"strconv"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/observability"

	"google.golang.org/protobuf/proto"
)

func cloneJobRequest(req *api.JobRequest) *api.JobRequest {
	if req == nil {
		return nil
	}

	cloned, ok := proto.Clone(req).(*api.JobRequest)
	if !ok {
		return nil
	}

	return cloned
}

func cloneJobRequestForEnqueue(ctx context.Context, req *api.JobRequest, acceptedAt time.Time) *api.JobRequest {
	cloned := cloneJobRequest(req)
	if cloned == nil {
		cloned = &api.JobRequest{}
	}

	if cloned.Metadata == nil {
		cloned.Metadata = map[string]string{}
	}

	cloned.Metadata[observability.JobEnqueueAcceptedUnixNanoKey] = strconv.FormatInt(acceptedAt.UnixNano(), 10)
	observability.InjectJobTraceContext(ctx, cloned)
	return cloned
}

func cloneJobRequestWithDeliveryID(req *api.JobRequest, deliveryID string) *api.JobRequest {
	cloned := cloneJobRequest(req)
	ensureJobRequestDeliveryID(cloned, deliveryID)
	return cloned
}

func ensureJobRequestDeliveryID(req *api.JobRequest, deliveryID string) {
	if req == nil || deliveryID == "" {
		return
	}

	if req.Job == nil {
		req.Job = &api.Job{}
	}

	req.Job.DeliveryId = &deliveryID
}
