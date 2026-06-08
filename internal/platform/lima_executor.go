package platform

import (
	"context"
	"fmt"
	"os/exec"
	"path"
	"path/filepath"
	"strings"

	"vectis/internal/interfaces"
)

const defaultLimaPath = "limactl"

const limaWorkspaceCommand = `workspace=$1
shift
mkdir -p "$workspace"
cd "$workspace"
exec "$@"`

// LimaExecutorOptions configures command execution through a Lima VM.
type LimaExecutorOptions struct {
	Instance           string
	LimactlPath        string
	GuestWorkspaceRoot string
	Start              bool
	PreserveEnv        bool
}

// LimaExecutor runs action commands through limactl shell.
type LimaExecutor struct {
	instance           string
	limactlPath        string
	guestWorkspaceRoot string
	start              bool
	preserveEnv        bool
}

// NewLimaExecutor creates a Lima-backed process executor.
func NewLimaExecutor(options LimaExecutorOptions) (*LimaExecutor, error) {
	instance := strings.TrimSpace(options.Instance)
	if instance == "" {
		return nil, fmt.Errorf("lima instance is required")
	}

	limactlPath := strings.TrimSpace(options.LimactlPath)
	if limactlPath == "" {
		limactlPath = defaultLimaPath
	}

	return &LimaExecutor{
		instance:           instance,
		limactlPath:        limactlPath,
		guestWorkspaceRoot: strings.TrimSpace(options.GuestWorkspaceRoot),
		start:              options.Start,
		preserveEnv:        options.PreserveEnv,
	}, nil
}

func (e *LimaExecutor) Start(ctx context.Context, commandPath string, args []string, workDir string, env []string) (interfaces.Process, error) {
	if strings.TrimSpace(commandPath) == "" {
		return nil, fmt.Errorf("lima command path is required")
	}

	cmd := exec.CommandContext(ctx, e.limactlPath, e.commandArgsWithEnv(commandPath, args, workDir, env)...)
	return interfaces.StartProcess(cmd)
}

func (e *LimaExecutor) commandArgs(commandPath string, args []string, workDir string) []string {
	return e.commandArgsWithEnv(commandPath, args, workDir, nil)
}

func (e *LimaExecutor) commandArgsWithEnv(commandPath string, args []string, workDir string, env []string) []string {
	guestCommand := guestCommandWithEnv(commandPath, args, env, e.preserveEnv)

	limactlArgs := []string{"--tty=false", "shell"}
	if e.start {
		limactlArgs = append(limactlArgs, "--start")
	}

	if e.preserveEnv {
		limactlArgs = append(limactlArgs, "--preserve-env")
	}

	if guestWorkDir := e.guestWorkDir(workDir); guestWorkDir != "" {
		limactlArgs = append(limactlArgs, e.instance, "--", "sh", "-c", limaWorkspaceCommand, "vectis-lima-workspace", guestWorkDir)
		limactlArgs = append(limactlArgs, guestCommand...)
		return limactlArgs
	}

	if workDir != "" {
		limactlArgs = append(limactlArgs, "--workdir", workDir)
	}

	limactlArgs = append(limactlArgs, e.instance, "--")
	limactlArgs = append(limactlArgs, guestCommand...)

	return limactlArgs
}

func guestCommandWithEnv(commandPath string, args []string, env []string, preserveEnv bool) []string {
	command := append([]string{commandPath}, args...)
	if len(env) == 0 {
		return command
	}

	out := []string{"env"}
	if !preserveEnv {
		out = append(out, "-i")
	}
	out = append(out, env...)
	out = append(out, command...)
	return out
}

func (e *LimaExecutor) guestWorkDir(workDir string) string {
	if e.guestWorkspaceRoot == "" || strings.TrimSpace(workDir) == "" {
		return ""
	}

	name := filepath.Base(workDir)
	if name == "." || name == string(filepath.Separator) || name == "" {
		name = "workspace"
	}

	return path.Join(e.guestWorkspaceRoot, name)
}
