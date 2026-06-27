package platform

import (
	"context"
	"errors"
	"fmt"
	"io"
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

type limaVirtualMachineManager struct {
	limactlPath string
	stdout      io.Writer
	stderr      io.Writer
}

func newLimaVirtualMachineManager(config VirtualMachineManagerConfig) (*limaVirtualMachineManager, error) {
	limactlPath := strings.TrimSpace(config.ProviderPath)
	if limactlPath == "" {
		limactlPath = defaultLimaPath
	}

	return &limaVirtualMachineManager{
		limactlPath: limactlPath,
		stdout:      config.Stdout,
		stderr:      config.Stderr,
	}, nil
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

func (m *limaVirtualMachineManager) Provider() string {
	return VirtualMachineProviderLima
}

func (m *limaVirtualMachineManager) CheckAvailable() error {
	if _, err := exec.LookPath(m.limactlPath); err != nil {
		return fmt.Errorf("limactl is not installed; install Lima, then rerun this command")
	}

	return nil
}

func (m *limaVirtualMachineManager) InstanceExists(ctx context.Context, instance string) (bool, error) {
	cmd := exec.CommandContext(ctx, m.limactlPath, "list", instance) //#nosec G204
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return false, nil
		}

		return false, fmt.Errorf("limactl list %s: %w", instance, err)
	}

	return true, nil
}

func (m *limaVirtualMachineManager) InstanceStatus(ctx context.Context, instance string) (string, error) {
	var stdout, stderr strings.Builder
	cmd := exec.CommandContext(ctx, m.limactlPath, "list", instance, "--format", "{{.Name}}\t{{.Status}}") //#nosec G204
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("read Lima status for %s: %w: %s", instance, err, strings.TrimSpace(stderr.String()))
	}

	status, err := parseLimaStatus(stdout.String(), instance)
	if err != nil {
		return "", err
	}

	return status, nil
}

func parseLimaStatus(output, instance string) (string, error) {
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		fields := strings.Split(line, "\t")
		if len(fields) != 2 {
			continue
		}

		if fields[0] == instance {
			status := strings.TrimSpace(fields[1])
			if status == "" {
				return "unknown", nil
			}

			return status, nil
		}
	}

	return "", fmt.Errorf("Lima instance %q was not present in status output", instance)
}

func (m *limaVirtualMachineManager) Create(ctx context.Context, instance, template string) error {
	return m.run(ctx, nil, "create", "--name="+instance, "template:"+template)
}

func (m *limaVirtualMachineManager) Start(ctx context.Context, instance string) error {
	return m.run(ctx, nil, "start", instance)
}

func (m *limaVirtualMachineManager) Stop(ctx context.Context, instance string) error {
	return m.run(ctx, nil, "stop", instance)
}

func (m *limaVirtualMachineManager) Delete(ctx context.Context, instance string) error {
	return m.run(ctx, nil, "delete", "--force", instance)
}

func (m *limaVirtualMachineManager) CopyDir(ctx context.Context, localDir, instance, remoteDir string) error {
	return m.run(ctx, nil, "copy", "-r", localDir, instance+":"+remoteDir)
}

func (m *limaVirtualMachineManager) CopyDirFrom(ctx context.Context, instance, remoteDir, localDir string) error {
	return m.run(ctx, nil, "copy", "-r", instance+":"+remoteDir, localDir)
}

func (m *limaVirtualMachineManager) Shell(ctx context.Context, instance string, stdin io.Reader, args ...string) error {
	return m.run(ctx, stdin, append([]string{"shell", instance}, args...)...)
}

func (m *limaVirtualMachineManager) run(ctx context.Context, stdin io.Reader, args ...string) error {
	cmd := exec.CommandContext(ctx, m.limactlPath, args...) //#nosec G204
	cmd.Stdin = stdin
	cmd.Stdout = m.stdout
	cmd.Stderr = m.stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("limactl %s failed: %w", strings.Join(args, " "), err)
	}

	return nil
}

func (e *limaVirtualMachine) StartCommand(ctx context.Context, command VirtualMachineCommand) (interfaces.Process, error) {
	if strings.TrimSpace(command.Path) == "" {
		return nil, fmt.Errorf("lima command path is required")
	}

	// #nosec G204 -- limactl path and guest command are explicit VM execution configuration.
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
