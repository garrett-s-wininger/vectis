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

type limaVirtualMachineConfig struct {
	instance           string
	limactlPath        string
	guestWorkspaceRoot string
	start              bool
	preserveEnv        bool
}

type limaVirtualMachine struct {
	instance           string
	limactlPath        string
	guestWorkspaceRoot string
	start              bool
	preserveEnv        bool
}

func newLimaVirtualMachine(options limaVirtualMachineConfig) (*limaVirtualMachine, error) {
	instance := strings.TrimSpace(options.instance)
	if instance == "" {
		return nil, fmt.Errorf("lima instance is required")
	}

	limactlPath := strings.TrimSpace(options.limactlPath)
	if limactlPath == "" {
		limactlPath = defaultLimaPath
	}

	return &limaVirtualMachine{
		instance:           instance,
		limactlPath:        limactlPath,
		guestWorkspaceRoot: strings.TrimSpace(options.guestWorkspaceRoot),
		start:              options.start,
		preserveEnv:        options.preserveEnv,
	}, nil
}

func (e *limaVirtualMachine) StartCommand(ctx context.Context, command VirtualMachineCommand) (interfaces.Process, error) {
	if strings.TrimSpace(command.Path) == "" {
		return nil, fmt.Errorf("lima command path is required")
	}

	cmd := exec.CommandContext(ctx, e.limactlPath, e.commandArgsWithEnv(command.Path, command.Args, command.WorkDir, command.Env)...)
	return interfaces.StartProcess(cmd)
}

func (e *limaVirtualMachine) commandArgs(commandPath string, args []string, workDir string) []string {
	return e.commandArgsWithEnv(commandPath, args, workDir, nil)
}

func (e *limaVirtualMachine) commandArgsWithEnv(commandPath string, args []string, workDir string, env []string) []string {
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

func (e *limaVirtualMachine) guestWorkDir(workDir string) string {
	if e.guestWorkspaceRoot == "" || strings.TrimSpace(workDir) == "" {
		return ""
	}

	name := filepath.Base(workDir)
	if name == "." || name == string(filepath.Separator) || name == "" {
		name = "workspace"
	}

	return path.Join(e.guestWorkspaceRoot, name)
}
