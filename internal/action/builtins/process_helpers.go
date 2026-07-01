package builtins

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/interfaces"
)

const OutputsField = "outputs"

func readOutputsFile(label, workspace string, inputs map[string]any) (map[string]any, error) {
	raw, exists := inputs[OutputsField]
	if !exists {
		return map[string]any{}, nil
	}

	outputPath, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("%s outputs must be a path string", label)
	}

	fullPath, err := workspaceRelativePath(workspace, outputPath)
	if err != nil {
		return nil, fmt.Errorf("%s outputs path: %w", label, err)
	}

	data, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("read %s outputs file %q: %w", label, outputPath, err)
	}

	var outputs map[string]any
	if err := json.Unmarshal(data, &outputs); err != nil {
		return nil, fmt.Errorf("parse %s outputs file %q: %w", label, outputPath, err)
	}

	if outputs == nil {
		return nil, fmt.Errorf("%s outputs file %q must contain a JSON object", label, outputPath)
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

func processExecutor(preferred interfaces.ExecExecutor, state *action.ExecutionState) interfaces.ExecExecutor {
	if preferred != nil {
		return preferred
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
