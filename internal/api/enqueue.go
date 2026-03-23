package api

import (
	"context"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/queueclient"
)

func enqueueWithRetry(ctx context.Context, q interfaces.QueueService, job *api.Job, log interfaces.Logger) error {
	return queueclient.EnqueueWithRetry(ctx, q, job, log)
}
