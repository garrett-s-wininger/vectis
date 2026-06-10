package workercore

import (
	"context"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
	"vectis/internal/interfaces"
	"vectis/internal/workloadidentity"
)

// Core is the worker-side execution core boundary. The worker shell owns queue,
// lease, cancel, and finalization invariants; the core owns how a claimed task
// is actually executed.
type Core interface {
	ExecuteTask(ctx context.Context, req ExecuteTaskRequest) error
}

type ExecuteTaskRequest struct {
	Job               *api.Job
	TaskKey           string
	LogClient         interfaces.LogClient
	Logger            interfaces.Logger
	WorkloadIdentity  *workloadidentity.Identity
	ArtifactPublisher action.ArtifactPublisher
	ActionLocks       []actionregistry.ActionLock
	ActionResolver    actionregistry.Resolver
}
