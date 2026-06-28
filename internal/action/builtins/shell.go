package builtins

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/interfaces"
)

const ShellOutputsField = "outputs"

type ShellAction struct {
	executor interfaces.ExecExecutor
}

func NewShellAction(executor interfaces.ExecExecutor) *ShellAction {
	return &ShellAction{
		executor: executor,
	}
}

func (s *ShellAction) ValidateWith(with map[string]string) []action.FieldError {
	errs := action.ValidateWithSpec(with, s.InputSchema())

	if _, ok := with[ShellOutputsField]; ok && strings.TrimSpace(with[ShellOutputsField]) == "" {
		errs = append(errs, action.FieldError{Field: ShellOutputsField, Message: "must not be empty"})
	}

	return errs
}

func (s *ShellAction) InputSchema() []action.FieldSpec {
	return []action.FieldSpec{
		{Name: "command", Type: action.FieldString, Required: true},
		{Name: ShellOutputsField, Type: action.FieldString},
	}
}

func (s *ShellAction) Type() string {
	return "builtins/shell"
}

func (s *ShellAction) Execute(ctx context.Context, state *action.ExecutionState, inputs map[string]any, _ action.Ports) action.Result {
	commandStr, ok := inputs["command"].(string)
	if !ok || commandStr == "" {
		return action.NewFailureResult(fmt.Errorf("shell action requires 'command' input"))
	}

	state.Logger.Info("Executing shell command: %s", commandStr)
	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("$ %s", commandStr))

	process, err := s.processExecutor(state).Start(ctx, "sh", []string{"-c", commandStr}, state.Workspace, state.CommandEnv())
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
		if ctx.Err() != nil {
			return action.NewFailureResult(fmt.Errorf("command cancelled: %w", cmdErr))
		}

		state.Logger.Error("Command failed: %v", cmdErr)
		sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("Command failed: %v", cmdErr))
		return action.NewFailureResult(fmt.Errorf("command failed: %w", cmdErr))
	}

	state.Logger.Info("Command completed successfully")
	sendLog(state, api.Stream_STREAM_STDOUT, "Command completed successfully")

	outputs, err := readShellOutputsFile(state.Workspace, inputs)
	if err != nil {
		state.Logger.Error("Failed to read shell outputs: %v", err)
		sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("Failed to read shell outputs: %v", err))
		return action.NewFailureResult(err)
	}

	return action.NewSuccessResult(outputs)
}

func readShellOutputsFile(workspace string, inputs map[string]any) (map[string]any, error) {
	raw, exists := inputs[ShellOutputsField]
	if !exists {
		return map[string]any{}, nil
	}

	outputPath, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("shell outputs must be a path string")
	}

	fullPath, err := workspaceRelativePath(workspace, outputPath)
	if err != nil {
		return nil, fmt.Errorf("shell outputs path: %w", err)
	}

	data, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("read shell outputs file %q: %w", outputPath, err)
	}

	var outputs map[string]any
	if err := json.Unmarshal(data, &outputs); err != nil {
		return nil, fmt.Errorf("parse shell outputs file %q: %w", outputPath, err)
	}

	if outputs == nil {
		return nil, fmt.Errorf("shell outputs file %q must contain a JSON object", outputPath)
	}

	return outputs, nil
}

func workspaceRelativePath(workspace, rawPath string) (string, error) {
	rawPath = strings.TrimSpace(rawPath)
	if rawPath == "" {
		return "", fmt.Errorf("is required")
	}

	if filepath.IsAbs(rawPath) {
		return "", fmt.Errorf("must be relative to the workspace")
	}

	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return "", fmt.Errorf("workspace is required")
	}

	root, err := filepath.Abs(workspace)
	if err != nil {
		return "", fmt.Errorf("resolve workspace: %w", err)
	}

	fullPath, err := filepath.Abs(filepath.Join(root, filepath.Clean(rawPath)))
	if err != nil {
		return "", fmt.Errorf("resolve path: %w", err)
	}

	rel, err := filepath.Rel(root, fullPath)
	if err != nil {
		return "", fmt.Errorf("resolve path relative to workspace: %w", err)
	}

	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("must stay inside the workspace")
	}

	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("resolve workspace symlinks: %w", err)
	}

	realPath, err := filepath.EvalSymlinks(fullPath)
	if err != nil {
		return "", fmt.Errorf("resolve path symlinks: %w", err)
	}

	realRel, err := filepath.Rel(realRoot, realPath)
	if err != nil {
		return "", fmt.Errorf("resolve real path relative to workspace: %w", err)
	}

	if realRel == ".." || strings.HasPrefix(realRel, ".."+string(os.PathSeparator)) || filepath.IsAbs(realRel) {
		return "", fmt.Errorf("must stay inside the workspace")
	}

	return realPath, nil
}

func (s *ShellAction) processExecutor(state *action.ExecutionState) interfaces.ExecExecutor {
	if s.executor != nil {
		return s.executor
	}

	if state != nil && state.ProcessExecutor != nil {
		return state.ProcessExecutor
	}

	return interfaces.NewDirectExecutor()
}

func streamOutput(reader io.Reader, state *action.ExecutionState, streamType api.Stream) {
	buf := make([]byte, 4096)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			data := buf[:n]
			seq := state.NextSequence()
			chunk := &api.LogChunk{
				RunId:    &state.RunID,
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
		RunId:    &state.RunID,
		Data:     []byte(message),
		Sequence: &seq,
		Stream:   &streamType,
	}

	if err := state.LogStream.Send(chunk); err != nil {
		state.Logger.Error("Failed to send log chunk: %v", err)
	}
}
