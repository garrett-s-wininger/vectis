package builtins

import (
	"context"
	"fmt"
	"io"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/interfaces"
)

type ShellAction struct {
	executor interfaces.CommandExecutor
}

func NewShellAction(executor interfaces.CommandExecutor) *ShellAction {
	if executor == nil {
		executor = interfaces.NewOSExecutor()
	}
	return &ShellAction{
		executor: executor,
	}
}

func (s *ShellAction) Type() string {
	return "builtins/shell"
}

func (s *ShellAction) Execute(ctx context.Context, state *action.ExecutionState, inputs map[string]any, _ []*api.Node) action.Result {
	commandStr, ok := inputs["command"].(string)
	if !ok || commandStr == "" {
		return action.NewFailureResult(fmt.Errorf("shell action requires 'command' input"))
	}

	state.Logger.Info("Executing shell command: %s", commandStr)
	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("$ %s", commandStr))

	process, err := s.executor.Start(ctx, commandStr)
	if err != nil {
		return action.NewFailureResult(fmt.Errorf("failed to start command: %w", err))
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		streamOutput(process.Stdout(), state, api.Stream_STREAM_STDOUT)
	}()

	go func() {
		defer wg.Done()
		streamOutput(process.Stderr(), state, api.Stream_STREAM_STDERR)
	}()

	// NOTE(garrett): We drain stdout/stderr fully before waiting on the process to
	// avoid reading from pipes that may be closed by Wait.
	wg.Wait()
	cmdErr := process.Wait()

	if cmdErr != nil {
		state.Logger.Error("Command failed: %v", cmdErr)
		sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("Command failed: %v", cmdErr))
		return action.NewFailureResult(fmt.Errorf("command failed: %w", cmdErr))
	}

	state.Logger.Info("Command completed successfully")
	sendLog(state, api.Stream_STREAM_STDOUT, "Command completed successfully")
	return action.NewSuccessResult(nil)
}

func streamOutput(reader io.Reader, state *action.ExecutionState, streamType api.Stream) {
	buf := make([]byte, 4096)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			data := buf[:n]
			seq := state.NextSequence()
			chunk := &api.LogChunk{
				JobId:    &state.JobID,
				Data:     data,
				Sequence: &seq,
				Stream:   &streamType,
			}

			if err := state.LogStream.Send(chunk); err != nil {
				state.Logger.Error("Failed to send log chunk: %v", err)
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			state.Logger.Error("Error reading output: %v", err)
			break
		}
	}
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
