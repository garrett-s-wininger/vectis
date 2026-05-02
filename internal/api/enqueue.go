package api

import (
	"context"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/queueclient"
)

func enqueueWithRetry(ctx context.Context, q interfaces.QueueService, req *api.JobRequest, log interfaces.Logger) error {
	return queueclient.EnqueueWithRetry(ctx, q, req, log)
}
