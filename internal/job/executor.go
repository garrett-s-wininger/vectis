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
	registry *builtins.Registry

	// TestLogStreamHook is a test-only channel that receives the LogStreamWaiter
	// created during ExecuteJob. It allows tests to wait for background flush
	// completion without sleeps.
	TestLogStreamHook chan LogStreamWaiter
}

type ExecuteOptions struct {
	WorkloadIdentity *workloadidentity.Identity
}

func NewExecutor() *Executor {
	return &Executor{
		registry: builtins.NewRegistry(),
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
		workspace, err = os.MkdirTemp(os.TempDir(), prefix)
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

	state := &action.ExecutionState{
		JobID:     job.GetId(),
		RunID:     job.GetRunId(),
		Workspace: workspace,
		ProcessEnv: action.SanitizedProcessEnv(
			workspace,
			os.Environ(),
		),
		Logger:    logger,
		LogClient: logClient,
		LogStream: logStream,
		Resolver:  e.registry,
		Workload:  opts.WorkloadIdentity,
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

	nodeImpl, err := e.registry.Resolve(node.GetUses())
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

	result := nodeImpl.Execute(nodeCtx, state, taskgraph.ActionInputs(node.GetWith()), action.Ports(ports))
	if result.Status == action.StatusFailure && result.Error != nil {
		span.RecordError(result.Error)
		span.SetStatus(otelcodes.Error, "action failed")
	}

	return result
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
