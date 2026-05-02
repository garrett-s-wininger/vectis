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
	"vectis/internal/interfaces"
	"vectis/internal/observability"

	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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

func NewExecutor() *Executor {
	return &Executor{
		registry: builtins.NewRegistry(),
	}
}

func sanitizeJobIDForPrefix(id string) string {
	return strings.ReplaceAll(strings.ReplaceAll(id, "/", "-"), string(filepath.Separator), "-")
}

func (e *Executor) ExecuteJob(ctx context.Context, job *api.Job, logClient interfaces.LogClient, logger interfaces.Logger) (err error) {
	if job.GetRoot() == nil {
		return fmt.Errorf("job has no root node")
	}

	if job.GetId() == "" {
		return fmt.Errorf("job has no id")
	}

	if job.GetRunId() == "" {
		return fmt.Errorf("job has no run id")
	}

	prefix := "vectis-" + sanitizeJobIDForPrefix(job.GetRunId()) + "-"
	workspace, err := os.MkdirTemp(os.TempDir(), prefix)
	if err != nil {
		return fmt.Errorf("failed to create workspace: %w", err)
	}

	defer func() {
		if err := os.RemoveAll(workspace); err != nil {
			logger.Error("Failed to remove workspace %s: %v", workspace, err)
		}
	}()

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
		Logger:    logger,
		LogClient: logClient,
		LogStream: logStream,
		Resolver:  e.registry,
	}

	logger.Info("Starting job execution: %s", job.GetId())

	sendLog(state, api.Stream_STREAM_CONTROL, `{"event":"start"}`)
	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Starting job execution: %s", job.GetId()))

	result := e.executeNode(ctx, job.GetRoot(), state)

	if result.Status == action.StatusFailure {
		logger.Error("Job failed: %v", result.Error)
		sendLog(state, api.Stream_STREAM_CONTROL, `{"event":"completed","status":"failure"}`)
		return result.Error
	}

	sendLog(state, api.Stream_STREAM_CONTROL, `{"event":"completed","status":"success"}`)
	return nil
}

func (e *Executor) executeNode(ctx context.Context, node *api.Node, state *action.ExecutionState) action.Result {
	if node == nil {
		return action.NewFailureResult(fmt.Errorf("nil node"))
	}

	nodeCtx, span := observability.Tracer("vectis/job").Start(ctx, "run.action.execute", trace.WithSpanKind(trace.SpanKindInternal))
	span.SetAttributes(observability.JobRunAttrs(state.JobID, state.RunID)...)
	span.SetAttributes(
		attribute.String("action.type", node.GetUses()),
		attribute.String("action.node.id", node.GetId()),
		attribute.Bool("action.has_children", len(node.GetSteps()) > 0),
	)
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

	inputs := make(map[string]any)
	for k, v := range node.GetWith() {
		inputs[k] = v
	}

	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Executing node: %s", nodeImpl.Type()))

	result := nodeImpl.Execute(nodeCtx, state, inputs, node.GetSteps())
	if result.Status == action.StatusFailure && result.Error != nil {
		span.RecordError(result.Error)
		span.SetStatus(otelcodes.Error, "action failed")
	}

	return result
}

func sendLog(state *action.ExecutionState, streamType api.Stream, message string) {
	if state.LogStream == nil {
		return
	}

	seq := state.NextSequence()
	chunk := &api.LogChunk{
		RunId:    &state.RunID,
		Data:     []byte(message),
		Sequence: &seq,
		Stream:   &streamType,
	}

	if err := state.LogStream.Send(chunk); err != nil {
		state.Logger.Error("Failed to send log chunk: %v", err)
	}
}
