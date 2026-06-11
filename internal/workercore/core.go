package workercore

import (
	"context"
	"fmt"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
	"vectis/internal/interfaces"
	"vectis/internal/workloadidentity"
	workersdk "vectis/sdk/workercore"
)

// Core is the worker-side execution core boundary. The worker shell owns queue,
// lease, cancel, and finalization invariants; the core owns how a claimed task
// is actually executed.
type Core interface {
	ExecuteTask(ctx context.Context, req ExecuteTaskRequest) error
}

type CancellableCore interface {
	CancelTask(ctx context.Context, req CancelTaskRequest) error
}

type ExecuteTaskRequest struct {
	Job     *api.Job
	TaskKey string
	Session TaskSession
}

type CancelTaskRequest struct {
	SessionID string
	RunID     string
	TaskKey   string
	Reason    string
}

type TaskResultError struct {
	Outcome    api.RunOutcome
	ReasonCode string
	Message    string
}

func (e *TaskResultError) Error() string {
	if e == nil {
		return ""
	}

	outcome := strings.TrimPrefix(e.Outcome.String(), "RUN_OUTCOME_")
	if outcome == "" || outcome == api.RunOutcome_RUN_OUTCOME_UNSPECIFIED.String() {
		outcome = "UNKNOWN"
	}

	reason := strings.TrimSpace(e.ReasonCode)
	message := strings.TrimSpace(e.Message)
	switch {
	case reason != "" && message != "":
		return fmt.Sprintf("remote worker core task %s (%s): %s", strings.ToLower(outcome), reason, message)
	case reason != "":
		return fmt.Sprintf("remote worker core task %s (%s)", strings.ToLower(outcome), reason)
	case message != "":
		return fmt.Sprintf("remote worker core task %s: %s", strings.ToLower(outcome), message)
	default:
		return fmt.Sprintf("remote worker core task %s", strings.ToLower(outcome))
	}
}

func NewTaskResultError(outcome api.RunOutcome, reasonCode, message string) *TaskResultError {
	if outcome == api.RunOutcome_RUN_OUTCOME_UNSPECIFIED {
		outcome = api.RunOutcome_RUN_OUTCOME_UNKNOWN
	}

	return &TaskResultError{
		Outcome:    outcome,
		ReasonCode: normalizeTaskResultReason(reasonCode, outcome),
		Message:    strings.TrimSpace(message),
	}
}

func normalizeTaskResultReason(reasonCode string, outcome api.RunOutcome) string {
	reasonCode = strings.TrimSpace(reasonCode)
	if reasonCode != "" {
		return reasonCode
	}

	switch outcome {
	case api.RunOutcome_RUN_OUTCOME_FAILURE:
		return workersdk.ReasonExecutionFailed
	case api.RunOutcome_RUN_OUTCOME_UNKNOWN:
		return workersdk.ReasonUnknown
	default:
		return ""
	}
}

// TaskSession is the shell-owned execution handle passed to a core for one
// claimed task. Keeping shell capabilities behind this handle gives a future
// out-of-process core one narrow surface to map onto UDS/RPC calls.
type TaskSession interface {
	SessionID() string
	ShellEndpoint() string
	Logger() interfaces.Logger
	LogClient() interfaces.LogClient
	ArtifactPublisher() action.ArtifactPublisher
	WorkloadIdentity() *workloadidentity.Identity
	ActionLocks() []actionregistry.ActionLock
	ActionResolver() actionregistry.Resolver
}

type TaskSessionOptions struct {
	SessionID         string
	ShellEndpoint     string
	Logger            interfaces.Logger
	LogClient         interfaces.LogClient
	ArtifactPublisher action.ArtifactPublisher
	WorkloadIdentity  *workloadidentity.Identity
	ActionLocks       []actionregistry.ActionLock
	ActionResolver    actionregistry.Resolver
}

func NewTaskSession(opts TaskSessionOptions) TaskSession {
	return taskSession{
		sessionID:         opts.SessionID,
		shellEndpoint:     opts.ShellEndpoint,
		logger:            opts.Logger,
		logClient:         opts.LogClient,
		artifactPublisher: opts.ArtifactPublisher,
		workloadIdentity:  opts.WorkloadIdentity,
		actionLocks:       actionregistry.CloneActionLocks(opts.ActionLocks),
		actionResolver:    opts.ActionResolver,
	}
}

type taskSession struct {
	sessionID         string
	shellEndpoint     string
	logger            interfaces.Logger
	logClient         interfaces.LogClient
	artifactPublisher action.ArtifactPublisher
	workloadIdentity  *workloadidentity.Identity
	actionLocks       []actionregistry.ActionLock
	actionResolver    actionregistry.Resolver
}

func (s taskSession) SessionID() string {
	return s.sessionID
}

func (s taskSession) ShellEndpoint() string {
	return s.shellEndpoint
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
