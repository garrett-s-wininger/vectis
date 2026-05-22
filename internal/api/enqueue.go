package api

import (
	"context"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/interfaces"
)

func enqueueWithRetry(ctx context.Context, q interfaces.QueueService, req *api.JobRequest, log interfaces.Logger) error {
	return cell.SubmitToQueue(ctx, q, req, log)
}
