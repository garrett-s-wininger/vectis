package job

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
	"vectis/internal/action/builtins"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
	"vectis/internal/taskgraph"
	"vectis/internal/workloadidentity"

	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// LogStreamWaiter is implemented by the background log stream and allows tests
// to synchronize on flush completion without sleeps.
type LogStreamWaiter interface {
	WaitForDone(timeout time.Duration) error
}

type Executor struct {
	registry            *builtins.Registry
	hostProcessExecutor interfaces.ExecExecutor
	vmProcessExecutor   interfaces.ExecExecutor
	defaultIsolation    string
	workspaceRoot       string

	// TestLogStreamHook is a test-only channel that receives the LogStreamWaiter
	// created during ExecuteJob. It allows tests to wait for background flush
	// completion without sleeps.
	TestLogStreamHook chan LogStreamWaiter
}

type ExecuteOptions struct {
	WorkloadIdentity *workloadidentity.Identity
	ActionLocks      []actionregistry.ActionLock
	ActionResolver   actionregistry.Resolver
	ProcessExecutor  interfaces.ExecExecutor
}

// ExecutorOption configures a job executor.
type ExecutorOption func(*Executor)

// WithProcessExecutor sets the command execution backend used by built-in
// host-isolated actions during a run. Nil preserves the default host process
// executor.
func WithProcessExecutor(processExecutor interfaces.ExecExecutor) ExecutorOption {
	return func(e *Executor) {
		if processExecutor != nil {
			e.hostProcessExecutor = processExecutor
		}
	}
}

// WithVMProcessExecutor sets the command execution backend used by VM-isolated
// actions. Nil leaves VM isolation unavailable.
func WithVMProcessExecutor(processExecutor interfaces.ExecExecutor) ExecutorOption {
	return func(e *Executor) {
		if processExecutor != nil {
			e.vmProcessExecutor = processExecutor
		}
	}
}

// WithDefaultIsolation sets the isolation level inherited by nodes that do not
// declare one explicitly. Empty preserves the existing default.
func WithDefaultIsolation(isolation string) ExecutorOption {
	return func(e *Executor) {
		if normalized := action.NormalizeIsolation(isolation); normalized != "" {
			e.defaultIsolation = normalized
		}
	}
}

// WithWorkspaceRoot sets the parent directory for automatically-created run
// workspaces. Empty preserves os.TempDir().
func WithWorkspaceRoot(root string) ExecutorOption {
	return func(e *Executor) {
		e.workspaceRoot = strings.TrimSpace(root)
	}
}

func NewExecutor(options ...ExecutorOption) *Executor {
	e := &Executor{
		registry:            builtins.NewRegistry(),
		hostProcessExecutor: interfaces.NewDirectExecutor(),
		defaultIsolation:    action.IsolationHost,
	}

	for _, option := range options {
		if option != nil {
			option(e)
		}
	}

	return e
}

func (e *Executor) ResolveProcessExecutor(isolation string) (interfaces.ExecExecutor, string, error) {
	effective := action.NormalizeIsolation(isolation)
	if effective == "" {
		effective = action.NormalizeIsolation(e.defaultIsolation)
	}

	if effective == "" {
		effective = action.IsolationHost
	}

	switch effective {
	case action.IsolationHost:
		if e.hostProcessExecutor == nil {
			return nil, "", fmt.Errorf("host isolation requested but no host process executor is configured")
		}

		return e.hostProcessExecutor, action.IsolationHost, nil
	case action.IsolationVM:
		if e.vmProcessExecutor == nil {
			return nil, "", fmt.Errorf("vm isolation requested but no VM process executor is configured")
		}

		return e.vmProcessExecutor, action.IsolationVM, nil
	default:
		return nil, "", fmt.Errorf("unsupported isolation level %q", isolation)
	}
}

func sanitizeJobIDForPrefix(id string) string {
	return strings.ReplaceAll(strings.ReplaceAll(id, "/", "-"), string(filepath.Separator), "-")
}

func (e *Executor) ExecuteJob(ctx context.Context, job *api.Job, logClient interfaces.LogClient, logger interfaces.Logger) (err error) {
	return e.ExecuteJobWithOptions(ctx, job, logClient, logger, ExecuteOptions{})
}

func (e *Executor) ExecuteJobWithOptions(ctx context.Context, job *api.Job, logClient interfaces.LogClient, logger interfaces.Logger, opts ExecuteOptions) (err error) {
	return e.execute(ctx, job, logClient, logger, "", "", false, opts)
}

func (e *Executor) ExecuteJobInWorkspace(ctx context.Context, job *api.Job, logClient interfaces.LogClient, logger interfaces.Logger, workspace string) (err error) {
	if workspace == "" {
		return fmt.Errorf("workspace is required")
	}

	return e.execute(ctx, job, logClient, logger, workspace, "", false, ExecuteOptions{})
}

func (e *Executor) ExecuteTask(ctx context.Context, job *api.Job, taskKey string, logClient interfaces.LogClient, logger interfaces.Logger) (err error) {
	return e.ExecuteTaskWithOptions(ctx, job, taskKey, logClient, logger, ExecuteOptions{})
}

func (e *Executor) ExecuteTaskWithOptions(ctx context.Context, job *api.Job, taskKey string, logClient interfaces.LogClient, logger interfaces.Logger, opts ExecuteOptions) (err error) {
	return e.execute(ctx, job, logClient, logger, "", taskKey, true, opts)
}

func (e *Executor) ExecuteTaskInWorkspace(ctx context.Context, job *api.Job, taskKey string, logClient interfaces.LogClient, logger interfaces.Logger, workspace string) (err error) {
	if workspace == "" {
		return fmt.Errorf("workspace is required")
	}

	return e.execute(ctx, job, logClient, logger, workspace, taskKey, true, ExecuteOptions{})
}

func (e *Executor) execute(ctx context.Context, job *api.Job, logClient interfaces.LogClient, logger interfaces.Logger, workspace, taskKey string, taskScoped bool, opts ExecuteOptions) (err error) {
	if job.GetRoot() == nil {
		return fmt.Errorf("job has no root node")
	}

	if job.GetId() == "" {
		return fmt.Errorf("job has no id")
	}

	if job.GetRunId() == "" {
		return fmt.Errorf("job has no run id")
	}

	node := job.GetRoot()
	ports := taskgraph.ChildPorts(node)
	if taskScoped {
		node, err = findTaskNode(job, taskKey)
		if err != nil {
			return err
		}

		ports = taskgraph.LocalPortsForTask(node)
	}

	cleanupWorkspace := false
	if workspace == "" {
		prefix := "vectis-" + sanitizeJobIDForPrefix(job.GetRunId()) + "-"
		workspaceRoot := e.workspaceRoot
		if workspaceRoot == "" {
			workspaceRoot = os.TempDir()
		}

		workspace, err = os.MkdirTemp(workspaceRoot, prefix)
		if err != nil {
			return fmt.Errorf("failed to create workspace: %w", err)
		}

		cleanupWorkspace = true
	}

	if cleanupWorkspace {
		defer func() {
			if err := os.RemoveAll(workspace); err != nil {
				logger.Error("Failed to remove workspace %s: %v", workspace, err)
			}
		}()
	}

	if err := os.MkdirAll(filepath.Join(workspace, ".tmp"), 0o700); err != nil {
		return fmt.Errorf("failed to create workspace temp dir: %w", err)
	}

	descriptorResolver := opts.ActionResolver
	if descriptorResolver == nil {
		descriptorResolver = e.registry
	}

	actionVerifier, err := newActionLockVerifier(descriptorResolver, opts.ActionLocks)
	if err != nil {
		return fmt.Errorf("initialize action lock verifier: %w", err)
	}

	logger.Info("Created workspace: %s", workspace)

	logStream, err := newDurableLogStream(logClient, logger, job.GetRunId())
	if err != nil {
		return fmt.Errorf("failed to initialize durable log stream: %w", err)
	}

	if e.TestLogStreamHook != nil {
		select {
		case e.TestLogStreamHook <- logStream:
		default:
		}
	}

	defer func() {
		if closeErr := logStream.CloseSend(); closeErr != nil {
			logger.Warn("Log stream flush incomplete for run %s: %v", job.GetRunId(), closeErr)
		}
	}()

	defaultIsolation := action.NormalizeIsolation(job.GetDefaultIsolation())
	if defaultIsolation == "" {
		defaultIsolation = e.defaultIsolation
	}

	processExecutor, isolation, err := e.ResolveProcessExecutor(defaultIsolation)
	if err != nil {
		return err
	}

	actionProcessExecutor := opts.ProcessExecutor
	if actionProcessExecutor == nil {
		actionProcessExecutor = processExecutor
	}

	actionResolver, err := newExecutableActionResolver(e.registry, descriptorResolver, opts.ActionLocks, actionProcessExecutor)
	if err != nil {
		return fmt.Errorf("initialize action resolver: %w", err)
	}

	state := &action.ExecutionState{
		JobID:     job.GetId(),
		RunID:     job.GetRunId(),
		Workspace: workspace,
		ProcessEnv: action.SanitizedProcessEnv(
			workspace,
			os.Environ(),
		),
		Logger:                  logger,
		LogClient:               logClient,
		LogStream:               logStream,
		Resolver:                actionResolver,
		Verifier:                actionVerifier,
		NodePaths:               executionNodePaths(job.GetRoot()),
		Workload:                opts.WorkloadIdentity,
		ProcessExecutor:         processExecutor,
		ProcessExecutorResolver: e,
		Isolation:               isolation,
	}

	sendLog(state, api.Stream_STREAM_CONTROL, `{"event":"start"}`)
	if taskScoped {
		logger.Info("Starting task execution: %s task %s", job.GetId(), taskKey)
		sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Starting task execution: %s task %s", job.GetId(), taskKey))
	} else {
		logger.Info("Starting job execution: %s", job.GetId())
		sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Starting job execution: %s", job.GetId()))
	}

	result := e.executeNodeWithPorts(ctx, node, state, ports)

	if result.Status == action.StatusFailure {
		if ctx.Err() != nil {
			sendCompletionLog(state, api.Stream_STREAM_CONTROL, `{"event":"completed","status":"aborted"}`, api.RunOutcome_RUN_OUTCOME_UNKNOWN)
			if result.Error == nil {
				return ctx.Err()
			}

			return result.Error
		}

		logger.Error("Job failed: %v", result.Error)
		sendCompletionLog(state, api.Stream_STREAM_CONTROL, `{"event":"completed","status":"failure"}`, api.RunOutcome_RUN_OUTCOME_FAILURE)
		return result.Error
	}

	sendCompletionLog(state, api.Stream_STREAM_CONTROL, `{"event":"completed","status":"success"}`, api.RunOutcome_RUN_OUTCOME_SUCCESS)
	return nil
}

func (e *Executor) executeNode(ctx context.Context, node *api.Node, state *action.ExecutionState) action.Result {
	return e.executeNodeWithPorts(ctx, node, state, taskgraph.ChildPorts(node))
}

func (e *Executor) executeNodeWithPorts(ctx context.Context, node *api.Node, state *action.ExecutionState, ports map[string][]*api.Node) action.Result {
	if node == nil {
		return action.NewFailureResult(fmt.Errorf("nil node"))
	}

	nodeCtx, span := observability.Tracer("vectis/job").Start(ctx, "run.action.execute", trace.WithSpanKind(trace.SpanKindInternal))
	span.SetAttributes(observability.JobRunAttrs(state.JobID, state.RunID)...)
	span.SetAttributes(
		attribute.String("action.type", node.GetUses()),
		attribute.String("action.node.id", node.GetId()),
		attribute.Bool("action.has_children", len(ports) > 0),
	)
	if state.Workload != nil {
		span.SetAttributes(attribute.String("vectis.workload.spiffe_id", state.Workload.SPIFFEID))
	}
	defer span.End()

	restoreIsolation, err := state.ApplyNodeIsolation(node)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "resolve isolation")
		return action.NewFailureResult(
			&action.ExecutionError{
				NodeID:  node.GetId(),
				Action:  node.GetUses(),
				Message: "failed to resolve isolation",
				Cause:   err,
			},
		)
	}
	defer restoreIsolation()
	span.SetAttributes(attribute.String("action.isolation", state.Isolation))

	if state.Verifier != nil {
		if err := state.Verifier.VerifyAction(node, state.NodePath(node)); err != nil {
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, "verify action lock")
			return action.NewFailureResult(&action.ExecutionError{
				NodeID:  node.GetId(),
				Action:  node.GetUses(),
				Message: "failed to verify action lock",
				Cause:   err,
			})
		}
	}

	resolver := state.Resolver
	if resolver == nil {
		resolver = e.registry
	}

	nodeImpl, err := resolver.Resolve(node.GetUses())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "resolve action")
		return action.NewFailureResult(
			&action.ExecutionError{
				NodeID:  node.GetId(),
				Action:  node.GetUses(),
				Message: "failed to resolve node",
				Cause:   err,
			},
		)
	}

	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Executing node: %s", nodeImpl.Type()))

	inputs, err := action.ResolveNodeInputs(state, node, action.InputSchema(nodeImpl))
	if err != nil {
		wrapped := &action.ExecutionError{
			NodeID:  node.GetId(),
			Action:  node.GetUses(),
			Message: "failed to resolve node inputs",
			Cause:   err,
		}

		span.RecordError(wrapped)
		span.SetStatus(otelcodes.Error, "resolve inputs")
		return action.NewFailureResult(wrapped)
	}

	result := nodeImpl.Execute(nodeCtx, state, inputs, action.Ports(ports))
	if result.Status == action.StatusFailure && result.Error != nil {
		span.RecordError(result.Error)
		span.SetStatus(otelcodes.Error, "action failed")
		return result
	}

	if result.Status == action.StatusSuccess {
		state.RecordOutputs(node.GetId(), result.Outputs)
	}

	return result
}

func executionNodePaths(root *api.Node) map[*api.Node]string {
	paths := map[*api.Node]string{}
	collectExecutionNodePaths(paths, root, "root")
	return paths
}

func collectExecutionNodePaths(paths map[*api.Node]string, node *api.Node, path string) {
	if node == nil {
		return
	}

	paths[node] = path
	for _, ref := range taskgraph.ChildRefs(node, path) {
		collectExecutionNodePaths(paths, ref.Node, ref.Path)
	}
}

func findTaskNode(job *api.Job, taskKey string) (*api.Node, error) {
	taskKey = strings.TrimSpace(taskKey)
	if taskKey == "" {
		return nil, fmt.Errorf("task key is required")
	}

	if taskKey == dal.RootTaskKey {
		return job.GetRoot(), nil
	}

	if node := findNodeByID(job.GetRoot(), taskKey); node != nil {
		return node, nil
	}

	return nil, fmt.Errorf("task node %q not found", taskKey)
}

func findNodeByID(node *api.Node, id string) *api.Node {
	if node == nil {
		return nil
	}

	if strings.TrimSpace(node.GetId()) == id {
		return node
	}

	for _, child := range taskgraph.AllChildren(node) {
		if found := findNodeByID(child, id); found != nil {
			return found
		}
	}

	return nil
}

func sendLog(state *action.ExecutionState, streamType api.Stream, message string) {
	sendLogChunk(state, streamType, message, api.RunOutcome_RUN_OUTCOME_UNSPECIFIED)
}

func sendCompletionLog(state *action.ExecutionState, streamType api.Stream, message string, outcome api.RunOutcome) {
	sendLogChunk(state, streamType, message, outcome)
}

func sendLogChunk(state *action.ExecutionState, streamType api.Stream, message string, outcome api.RunOutcome) {
	if state.LogStream == nil {
		return
	}

	seq := state.NextSequence()
	chunk := &api.LogChunk{
		RunId:     &state.RunID,
		Data:      []byte(message),
		Sequence:  &seq,
		Stream:    &streamType,
		Completed: outcome.Enum(),
		Timestamp: timestamppb.Now(),
	}

	if err := state.LogStream.Send(chunk); err != nil {
		state.Logger.Error("Failed to send log chunk: %v", err)
	}
}
