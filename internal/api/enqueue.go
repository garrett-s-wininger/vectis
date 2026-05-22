package api

import (
	"context"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/config"
	"vectis/internal/interfaces"
)

func enqueueWithRetry(ctx context.Context, q interfaces.QueueService, req *api.JobRequest, log interfaces.Logger) error {
	return cell.SubmitToLocalQueue(ctx, config.CellID(), q, req, log)
}
