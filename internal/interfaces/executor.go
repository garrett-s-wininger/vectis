package interfaces

import (
	"context"
	"fmt"
	"io"
	"os/exec"
)

type CommandExecutor interface {
	Start(ctx context.Context, command string, workDir string) (Process, error)
}

type Process interface {
	Wait() error
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
}

type OSExecutor struct{}

func NewOSExecutor() *OSExecutor {
	return &OSExecutor{}
}

func (e *OSExecutor) Start(ctx context.Context, command string, workDir string) (Process, error) {
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.Dir = workDir

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start command: %w", err)
	}

	return &osProcess{
		cmd:    cmd,
		stdout: stdout,
		stderr: stderr,
	}, nil
}

type osProcess struct {
	cmd    *exec.Cmd
	stdout io.ReadCloser
	stderr io.ReadCloser
}

func (p *osProcess) Wait() error {
	return p.cmd.Wait()
}

func (p *osProcess) Stdout() io.ReadCloser {
	return p.stdout
}

func (p *osProcess) Stderr() io.ReadCloser {
	return p.stderr
}
