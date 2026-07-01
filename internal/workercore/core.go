package workercore

import (
	"context"
	"fmt"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
	"vectis/internal/interfaces"
	"vectis/internal/secrets"
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

type CheckoutCacheWarmer interface {
	WarmCheckoutCache(ctx context.Context, req WarmCheckoutCacheRequest) (WarmCheckoutCacheResult, error)
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

type WarmCheckoutCacheRequest struct {
	RemoteURLs []string
	Remotes    []CheckoutCacheRemote
}

type WarmCheckoutCacheResult struct {
	Warmed   int
	Failures []CheckoutCacheWarmFailure
}

type CheckoutCacheRemote struct {
	RemoteURL          string
	FallbackRemoteURLs []string
}

type CheckoutCacheWarmFailure struct {
	RemoteURL string
	Message   string
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
	RunID() string
	ShellEndpoint() string
	Logger() interfaces.Logger
	LogClient() interfaces.LogClient
	ArtifactPublisher() action.ArtifactPublisher
	WorkloadIdentity() *workloadidentity.Identity
	ActionLocks() []actionregistry.ActionLock
	ActionResolver() actionregistry.Resolver
	SecretFiles() []secrets.FileMaterial
	CheckoutCacheRemoteURLs() []string
	CheckoutCacheRemotes() []CheckoutCacheRemote
}

type TaskSessionOptions struct {
	SessionID               string
	RunID                   string
	ShellEndpoint           string
	Logger                  interfaces.Logger
	LogClient               interfaces.LogClient
	ArtifactPublisher       action.ArtifactPublisher
	WorkloadIdentity        *workloadidentity.Identity
	ActionLocks             []actionregistry.ActionLock
	ActionResolver          actionregistry.Resolver
	SecretFiles             []secrets.FileMaterial
	CheckoutCacheRemoteURLs []string
	CheckoutCacheRemotes    []CheckoutCacheRemote
}

func NewTaskSession(opts TaskSessionOptions) TaskSession {
	runID := strings.TrimSpace(opts.RunID)
	if runID == "" && opts.WorkloadIdentity != nil {
		runID = opts.WorkloadIdentity.RunID
	}

	checkoutCacheRemotes := cloneCheckoutCacheRemotes(opts.CheckoutCacheRemotes)
	checkoutCacheRemoteURLs := cloneStringSlice(opts.CheckoutCacheRemoteURLs)
	if len(checkoutCacheRemotes) == 0 {
		checkoutCacheRemotes = checkoutCacheRemotesFromURLs(checkoutCacheRemoteURLs)
	}

	checkoutCacheRemoteURLs = uniqueCheckoutCacheRemoteURLs(append(checkoutCacheRemoteURLs, checkoutCacheRemoteURLsFromRemotes(checkoutCacheRemotes)...))

	return taskSession{
		sessionID:               opts.SessionID,
		runID:                   runID,
		shellEndpoint:           opts.ShellEndpoint,
		logger:                  opts.Logger,
		logClient:               opts.LogClient,
		artifactPublisher:       opts.ArtifactPublisher,
		workloadIdentity:        opts.WorkloadIdentity,
		actionLocks:             actionregistry.CloneActionLocks(opts.ActionLocks),
		actionResolver:          opts.ActionResolver,
		secretFiles:             cloneSecretFiles(opts.SecretFiles),
		checkoutCacheRemoteURLs: checkoutCacheRemoteURLs,
		checkoutCacheRemotes:    checkoutCacheRemotes,
	}
}

type taskSession struct {
	sessionID               string
	runID                   string
	shellEndpoint           string
	logger                  interfaces.Logger
	logClient               interfaces.LogClient
	artifactPublisher       action.ArtifactPublisher
	workloadIdentity        *workloadidentity.Identity
	actionLocks             []actionregistry.ActionLock
	actionResolver          actionregistry.Resolver
	secretFiles             []secrets.FileMaterial
	checkoutCacheRemoteURLs []string
	checkoutCacheRemotes    []CheckoutCacheRemote
}

func (s taskSession) SessionID() string {
	return s.sessionID
}

func (s taskSession) RunID() string {
	return s.runID
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

func (s taskSession) SecretFiles() []secrets.FileMaterial {
	return cloneSecretFiles(s.secretFiles)
}

func (s taskSession) CheckoutCacheRemoteURLs() []string {
	return cloneStringSlice(s.checkoutCacheRemoteURLs)
}

func (s taskSession) CheckoutCacheRemotes() []CheckoutCacheRemote {
	return cloneCheckoutCacheRemotes(s.checkoutCacheRemotes)
}

func cloneSecretFiles(files []secrets.FileMaterial) []secrets.FileMaterial {
	if len(files) == 0 {
		return nil
	}

	out := make([]secrets.FileMaterial, len(files))
	for i, file := range files {
		out[i] = file
		out[i].Data = append([]byte(nil), file.Data...)
	}

	return out
}

func cloneStringSlice(in []string) []string {
	if len(in) == 0 {
		return nil
	}

	return append([]string(nil), in...)
}

func cloneCheckoutCacheRemotes(in []CheckoutCacheRemote) []CheckoutCacheRemote {
	if len(in) == 0 {
		return nil
	}

	out := make([]CheckoutCacheRemote, 0, len(in))
	for _, remote := range in {
		remote.RemoteURL = strings.TrimSpace(remote.RemoteURL)
		remote.FallbackRemoteURLs = cloneStringSlice(remote.FallbackRemoteURLs)
		if remote.RemoteURL == "" && len(remote.FallbackRemoteURLs) == 0 {
			continue
		}

		out = append(out, remote)
	}

	return out
}

func checkoutCacheRemotesFromURLs(remoteURLs []string) []CheckoutCacheRemote {
	if len(remoteURLs) == 0 {
		return nil
	}

	remotes := make([]CheckoutCacheRemote, 0, len(remoteURLs))
	for _, remoteURL := range remoteURLs {
		remoteURL = strings.TrimSpace(remoteURL)
		if remoteURL == "" {
			continue
		}

		remotes = append(remotes, CheckoutCacheRemote{RemoteURL: remoteURL})
	}

	return remotes
}

func checkoutCacheRemoteURLsFromRemotes(remotes []CheckoutCacheRemote) []string {
	if len(remotes) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(remotes))
	out := make([]string, 0, len(remotes))
	for _, remote := range remotes {
		for _, remoteURL := range append([]string{remote.RemoteURL}, remote.FallbackRemoteURLs...) {
			remoteURL = strings.TrimSpace(remoteURL)
			if remoteURL == "" {
				continue
			}

			if _, ok := seen[remoteURL]; ok {
				continue
			}

			seen[remoteURL] = struct{}{}
			out = append(out, remoteURL)
		}
	}

	return out
}
