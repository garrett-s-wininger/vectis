package workercore

import (
	"context"
	"fmt"
	"strings"

	"vectis/internal/job"
)

type InProcessCore struct {
	executor *job.Executor
}

func NewInProcessCore(executor *job.Executor) *InProcessCore {
	if executor == nil {
		executor = job.NewExecutor()
	}

	return &InProcessCore{executor: executor}
}

func (c *InProcessCore) ExecuteTask(ctx context.Context, req ExecuteTaskRequest) error {
	if c == nil || c.executor == nil {
		return fmt.Errorf("worker execution core is not configured")
	}

	if req.Job == nil {
		return fmt.Errorf("worker execution core requires a job")
	}

	if strings.TrimSpace(req.TaskKey) == "" {
		return fmt.Errorf("worker execution core requires a task key")
	}

	if req.LogClient == nil {
		return fmt.Errorf("worker execution core requires a log client")
	}

	if req.Logger == nil {
		return fmt.Errorf("worker execution core requires a logger")
	}

	return c.executor.ExecuteTaskWithOptions(ctx, req.Job, req.TaskKey, req.LogClient, req.Logger, job.ExecuteOptions{
		WorkloadIdentity:  req.WorkloadIdentity,
		ArtifactPublisher: req.ArtifactPublisher,
		ActionLocks:       req.ActionLocks,
		ActionResolver:    req.ActionResolver,
	})
}

var _ Core = (*InProcessCore)(nil)
