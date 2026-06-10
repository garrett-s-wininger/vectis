package custom

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
	"vectis/internal/interfaces"
)

type ProcessAction struct {
	descriptor actionregistry.Descriptor
	executor   interfaces.ExecExecutor
}

func NewProcessAction(descriptor actionregistry.Descriptor, executor interfaces.ExecExecutor) *ProcessAction {
	if executor == nil {
		executor = interfaces.NewDirectExecutor()
	}
	return &ProcessAction{descriptor: descriptor, executor: executor}
}

func (a *ProcessAction) Type() string {
	return a.descriptor.CanonicalName
}

func (a *ProcessAction) ValidateWith(with map[string]string) []action.FieldError {
	return a.descriptor.InputSchema.ValidateWith(with)
}

func (a *ProcessAction) InputSchema() []action.FieldSpec {
	return a.descriptor.InputSchema.ActionFieldSpecs()
}

func (a *ProcessAction) PortSchema() []action.PortSpec {
	out := make([]action.PortSpec, 0, len(a.descriptor.PortSchema))
	for _, port := range a.descriptor.PortSchema {
		out = append(out, port.ActionPortSpec())
	}
	return out
}

func (a *ProcessAction) LocalOnly() bool {
	return a.descriptor.LocalOnly
}

func (a *ProcessAction) Execute(ctx context.Context, state *action.ExecutionState, inputs map[string]any, ports action.Ports) action.Result {
	if a.descriptor.Runtime != actionregistry.RuntimeProcess {
		return action.NewFailureResult(fmt.Errorf("custom action %s uses unsupported runtime %q", a.Type(), a.descriptor.Runtime))
	}

	if len(ports) > 0 {
		return action.NewFailureResult(fmt.Errorf("custom process action %s does not support child ports", a.Type()))
	}

	command := processCommand(a.descriptor.RuntimeConfig)
	if command == "" {
		return action.NewFailureResult(fmt.Errorf("custom process action %s requires runtime_config.command", a.Type()))
	}

	workDir := processWorkDir(a.descriptor, state)
	env := processEnv(a.descriptor, state, inputs)

	logLine(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("$ %s", command))
	process, err := a.executor.Start(ctx, "sh", []string{"-c", command}, workDir, env)
	if err != nil {
		return action.NewFailureResult(fmt.Errorf("failed to start custom process action %s: %w", a.Type(), err))
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
	wg.Wait()

	if err := process.Wait(); err != nil {
		if ctx.Err() != nil {
			return action.NewFailureResult(fmt.Errorf("custom process action %s cancelled: %w", a.Type(), err))
		}

		logLine(state, api.Stream_STREAM_STDERR, fmt.Sprintf("Custom action failed: %v", err))
		return action.NewFailureResult(fmt.Errorf("custom process action %s failed: %w", a.Type(), err))
	}

	logLine(state, api.Stream_STREAM_STDOUT, "Custom action completed successfully")
	return action.NewSuccessResult(nil)
}

func processCommand(config map[string]string) string {
	if command := strings.TrimSpace(config["command"]); command != "" {
		return command
	}

	return strings.TrimSpace(config["entrypoint"])
}

func processWorkDir(descriptor actionregistry.Descriptor, state *action.ExecutionState) string {
	workspace := "."
	if state != nil && strings.TrimSpace(state.Workspace) != "" {
		workspace = strings.TrimSpace(state.Workspace)
	}

	base := workspace
	if descriptor.Source == actionregistry.SourceLocalFilesystem && strings.TrimSpace(descriptor.SourcePath) != "" {
		base = strings.TrimSpace(descriptor.SourcePath)
	}

	if configured := strings.TrimSpace(descriptor.RuntimeConfig["working_directory"]); configured != "" {
		if filepath.IsAbs(configured) {
			return configured
		}

		return filepath.Join(base, configured)
	}

	return base
}

func processEnv(descriptor actionregistry.Descriptor, state *action.ExecutionState, inputs map[string]any) []string {
	var env []string
	if state == nil {
		env = action.SanitizedProcessEnv("", nil)
	} else {
		env = state.CommandEnv()
	}

	env = action.AppendEnv(env, "VECTIS_ACTION_NAME", descriptor.CanonicalName)
	env = action.AppendEnv(env, "VECTIS_ACTION_VERSION", descriptor.Version)
	env = action.AppendEnv(env, "VECTIS_ACTION_DIGEST", descriptor.Digest)
	if state != nil {
		env = action.AppendEnv(env, "VECTIS_WORKSPACE", state.Workspace)
	}

	keys := make([]string, 0, len(inputs))
	for key := range inputs {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	for _, key := range keys {
		envKey := inputEnvKey(key)
		if envKey == "" {
			continue
		}

		env = action.AppendEnv(env, envKey, fmt.Sprint(inputs[key]))
	}

	return env
}

func inputEnvKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return ""
	}

	var b strings.Builder
	b.WriteString("VECTIS_INPUT_")
	wrote := false
	for _, r := range key {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r - 'a' + 'A')
			wrote = true
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
			wrote = true
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			wrote = true
		default:
			b.WriteByte('_')
		}
	}

	if !wrote {
		return ""
	}

	return b.String()
}

func streamOutput(reader io.Reader, state *action.ExecutionState, streamType api.Stream) {
	if reader == nil || state == nil || state.LogStream == nil {
		return
	}

	buf := make([]byte, 4096)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			seq := state.NextSequence()
			chunk := &api.LogChunk{
				RunId:    &state.RunID,
				Data:     append([]byte(nil), buf[:n]...),
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

func logLine(state *action.ExecutionState, streamType api.Stream, message string) {
	if state == nil || state.LogStream == nil {
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
