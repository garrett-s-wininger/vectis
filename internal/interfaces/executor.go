package interfaces

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"vectis/internal/actionlauncher"
)

type CommandExecutor interface {
	Start(ctx context.Context, command string, workDir string, env []string) (Process, error)
}

type ExecExecutor interface {
	Start(ctx context.Context, path string, args []string, workDir string, env []string) (Process, error)
}

type Process interface {
	Wait() error
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
}

func init() {
	actionlauncher.MaybeRun()
}

// StartProviderProcess adapts provider wrapper commands to the Process
// interface. Host worker action code should use ExecExecutor so it flows
// through the action launcher contract.
func StartProviderProcess(cmd *exec.Cmd) (Process, error) {
	return startProcess(cmd)
}

// startProcess starts cmd with worker-safe process defaults and adapts it to
// the Process interface. A nil Stdin intentionally lets os/exec connect the
// child to the null device, and ExtraFiles are inherited only when the caller
// explicitly configured them.
func startProcess(cmd *exec.Cmd) (Process, error) {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	configureCommandProcessIsolation(cmd)

	registerActiveProcess(cmd)
	if err := cmd.Start(); err != nil {
		unregisterActiveProcess(cmd)
		return nil, fmt.Errorf("failed to start command: %w", err)
	}

	process := &osProcess{
		cmd:    cmd,
		stdout: stdout,
		stderr: stderr,
	}

	return process, nil
}

func NewDirectExecutor() *DirectExecutor {
	return &DirectExecutor{}
}

type DirectExecutor struct{}

func (e *DirectExecutor) Start(ctx context.Context, path string, args []string, workDir string, env []string) (Process, error) {
	return startExecProcess(ctx, path, args, workDir, env)
}

type OSExecutor struct{}

func NewOSExecutor() *OSExecutor {
	return &OSExecutor{}
}

func (e *OSExecutor) Start(ctx context.Context, command string, workDir string, env []string) (Process, error) {
	return startExecProcess(ctx, "sh", []string{"-c", command}, workDir, env)
}

func startExecProcess(ctx context.Context, path string, args []string, workDir string, env []string) (Process, error) {
	secureWorkDir, err := secureLaunchWorkDir(workDir)
	if err != nil {
		return nil, err
	}

	resolvedPath, err := resolveExecutablePath(path)
	if err != nil {
		return nil, err
	}

	launcherCommand, err := actionlauncher.Command(actionlauncher.LaunchSpec{
		Path:    resolvedPath,
		Args:    args,
		WorkDir: secureWorkDir,
		Env:     env,
	})

	if err != nil {
		return nil, err
	}

	cmd := exec.CommandContext(ctx, launcherCommand.Path, launcherCommand.Args...)
	cmd.Dir = launcherCommand.WorkDir
	cmd.Env = launcherCommand.Env
	return startProcess(cmd)
}

func resolveExecutablePath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("command path is required")
	}

	if strings.ContainsRune(path, os.PathSeparator) {
		return path, nil
	}

	resolved, err := exec.LookPath(path)
	if err != nil {
		return "", fmt.Errorf("resolve command path %q: %w", path, err)
	}

	return resolved, nil
}

func secureLaunchWorkDir(workDir string) (string, error) {
	if strings.TrimSpace(workDir) == "" {
		return "", fmt.Errorf("work directory is required")
	}

	info, err := os.Lstat(workDir)
	if err != nil {
		return "", fmt.Errorf("stat work directory: %w", err)
	}

	if info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("work directory must not be a symlink")
	}

	if !info.IsDir() {
		return "", fmt.Errorf("work directory is not a directory")
	}

	absWorkDir, err := filepath.Abs(workDir)
	if err != nil {
		return "", fmt.Errorf("resolve work directory: %w", err)
	}

	realWorkDir, err := filepath.EvalSymlinks(absWorkDir)
	if err != nil {
		return "", fmt.Errorf("resolve real work directory: %w", err)
	}

	info, err = os.Stat(realWorkDir)
	if err != nil {
		return "", fmt.Errorf("stat real work directory: %w", err)
	}

	if !info.IsDir() {
		return "", fmt.Errorf("real work directory is not a directory")
	}

	return realWorkDir, nil
}

type osProcess struct {
	cmd    *exec.Cmd
	stdout io.ReadCloser
	stderr io.ReadCloser
}

func (p *osProcess) Wait() error {
	defer unregisterActiveProcess(p.cmd)
	return p.cmd.Wait()
}

func (p *osProcess) Stdout() io.ReadCloser {
	return p.stdout
}

func (p *osProcess) Stderr() io.ReadCloser {
	return p.stderr
}
