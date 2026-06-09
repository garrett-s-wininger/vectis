package workercore

import (
	"context"
	"fmt"
	"strings"

	"vectis/internal/job"
)

type ExecutorCore struct {
	executor *job.Executor
}

func NewExecutorCore(executor *job.Executor) *ExecutorCore {
	if executor == nil {
		executor = job.NewExecutor()
	}

	return &ExecutorCore{executor: executor}
}

func (c *ExecutorCore) ExecuteTask(ctx context.Context, req ExecuteTaskRequest) error {
	if c == nil || c.executor == nil {
		return fmt.Errorf("worker execution core is not configured")
	}

	if req.Job == nil {
		return fmt.Errorf("worker execution core requires a job")
	}

	if strings.TrimSpace(req.TaskKey) == "" {
		return fmt.Errorf("worker execution core requires a task key")
	}

	if req.Session == nil {
		return fmt.Errorf("worker execution core requires a task session")
	}

	logClient := req.Session.LogClient()
	if logClient == nil {
		return fmt.Errorf("worker execution core requires a log client")
	}

	logger := req.Session.Logger()
	if logger == nil {
		return fmt.Errorf("worker execution core requires a logger")
	}

	return c.executor.ExecuteTaskWithOptions(ctx, req.Job, req.TaskKey, logClient, logger, job.ExecuteOptions{
		WorkloadIdentity:  req.Session.WorkloadIdentity(),
		ArtifactPublisher: req.Session.ArtifactPublisher(),
		ActionLocks:       req.Session.ActionLocks(),
		ActionResolver:    req.Session.ActionResolver(),
		SecretFiles:       req.Session.SecretFiles(),
	})
}

func (c *ExecutorCore) CancelTask(context.Context, CancelTaskRequest) error {
	return nil
}

var _ Core = (*ExecutorCore)(nil)
var _ CancellableCore = (*ExecutorCore)(nil)
