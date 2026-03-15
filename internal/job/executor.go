package job

import (
	"context"
	"fmt"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/builtins"
	"vectis/internal/interfaces"
)

type Executor struct {
	registry *builtins.Registry
}

func NewExecutor() *Executor {
	return &Executor{
		registry: builtins.NewRegistry(),
	}
}

func (e *Executor) ExecuteJob(ctx context.Context, job *api.Job, logClient interfaces.LogClient, logger interfaces.Logger) error {
	if job.GetRoot() == nil {
		return fmt.Errorf("job has no root node")
	}

	logStream, err := logClient.StreamLogs(ctx)
	if err != nil {
		return fmt.Errorf("failed to create log stream: %w", err)
	}

	state := &action.ExecutionState{
		JobID:     job.GetId(),
		Logger:    logger,
		LogClient: logClient,
		LogStream: logStream,
	}

	logger.Info("Starting job execution: %s", job.GetId())
	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Starting job execution: %s", job.GetId()))

	result := e.executeNode(ctx, job.GetRoot(), state)
	logStream.CloseSend()

	if result.Status == action.StatusFailure {
		logger.Error("Job failed: %v", result.Error)
		return result.Error
	}

	logger.Info("Job completed successfully: %s", job.GetId())
	return nil
}

func (e *Executor) executeNode(ctx context.Context, node *api.Node, state *action.ExecutionState) action.Result {
	if node == nil {
		return action.NewFailureResult(fmt.Errorf("nil node"))
	}

	nodeImpl, err := e.registry.Resolve(node.GetUses())
	if err != nil {
		return action.NewFailureResult(
			&action.ExecutionError{
				NodeID:  node.GetId(),
				Action:  node.GetUses(),
				Message: "failed to resolve node",
				Cause:   err,
			},
		)
	}

	inputs := make(map[string]interface{})
	for k, v := range node.GetWith() {
		inputs[k] = v
	}

	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Executing node: %s", nodeImpl.Type()))

	return nodeImpl.Execute(ctx, state, inputs, node.GetSteps())
}

func sendLog(state *action.ExecutionState, streamType api.Stream, message string) {
	if state.LogStream == nil {
		return
	}

	seq := state.NextSequence()
	chunk := &api.LogChunk{
		JobId:    &state.JobID,
		Data:     []byte(message),
		Sequence: &seq,
		Stream:   &streamType,
	}

	if err := state.LogStream.Send(chunk); err != nil {
		state.Logger.Error("Failed to send log chunk: %v", err)
	}
}
