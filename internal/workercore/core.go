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
	Job     *api.Job
	TaskKey string
	Session TaskSession
}

// TaskSession is the shell-owned execution handle passed to a core for one
// claimed task. Keeping shell capabilities behind this handle gives a future
// out-of-process core one narrow surface to map onto UDS/RPC calls.
type TaskSession interface {
	Logger() interfaces.Logger
	LogClient() interfaces.LogClient
	ArtifactPublisher() action.ArtifactPublisher
	WorkloadIdentity() *workloadidentity.Identity
	ActionLocks() []actionregistry.ActionLock
	ActionResolver() actionregistry.Resolver
}

type TaskSessionOptions struct {
	Logger            interfaces.Logger
	LogClient         interfaces.LogClient
	ArtifactPublisher action.ArtifactPublisher
	WorkloadIdentity  *workloadidentity.Identity
	ActionLocks       []actionregistry.ActionLock
	ActionResolver    actionregistry.Resolver
}

func NewTaskSession(opts TaskSessionOptions) TaskSession {
	return taskSession{
		logger:            opts.Logger,
		logClient:         opts.LogClient,
		artifactPublisher: opts.ArtifactPublisher,
		workloadIdentity:  opts.WorkloadIdentity,
		actionLocks:       actionregistry.CloneActionLocks(opts.ActionLocks),
		actionResolver:    opts.ActionResolver,
	}
}

type taskSession struct {
	logger            interfaces.Logger
	logClient         interfaces.LogClient
	artifactPublisher action.ArtifactPublisher
	workloadIdentity  *workloadidentity.Identity
	actionLocks       []actionregistry.ActionLock
	actionResolver    actionregistry.Resolver
}

func (s taskSession) Logger() interfaces.Logger {
	return s.logger
}

func (s taskSession) LogClient() interfaces.LogClient {
	return s.logClient
}

func (s taskSession) ArtifactPublisher() action.ArtifactPublisher {
	return s.artifactPublisher
}

func (s taskSession) WorkloadIdentity() *workloadidentity.Identity {
	return s.workloadIdentity
}

func (s taskSession) ActionLocks() []actionregistry.ActionLock {
	return actionregistry.CloneActionLocks(s.actionLocks)
}

func (s taskSession) ActionResolver() actionregistry.Resolver {
	return s.actionResolver
}
