package mocks

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"

	"vectis/internal/interfaces"
)

type MockExecutor struct {
	mu       sync.Mutex
	commands []string
	process  interfaces.Process
	err      error
}

func NewMockExecutor() *MockExecutor {
	return &MockExecutor{
		commands: make([]string, 0),
	}
}

func (m *MockExecutor) SetProcess(process interfaces.Process) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.process = process
}

func (m *MockExecutor) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

func (m *MockExecutor) GetCommands() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.commands))
	copy(result, m.commands)
	return result
}

func (m *MockExecutor) Start(ctx context.Context, command string) (interfaces.Process, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.commands = append(m.commands, command)

	if m.err != nil {
		return nil, m.err
	}

	if m.process == nil {
		return nil, errors.New("no process configured in mock")
	}

	return m.process, nil
}

type MockProcess struct {
	mu         sync.Mutex
	waitErr    error
	stdoutData string
	stderrData string
	waitCalled bool
}

func NewMockProcess() *MockProcess {
	return &MockProcess{}
}

func (m *MockProcess) SetWaitError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waitErr = err
}

func (m *MockProcess) SetStdout(data string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stdoutData = data
}

func (m *MockProcess) SetStderr(data string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stderrData = data
}

func (m *MockProcess) WaitCalled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.waitCalled
}

func (m *MockProcess) Wait() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waitCalled = true
	return m.waitErr
}

func (m *MockProcess) Stdout() io.ReadCloser {
	m.mu.Lock()
	defer m.mu.Unlock()
	return io.NopCloser(strings.NewReader(m.stdoutData))
}

func (m *MockProcess) Stderr() io.ReadCloser {
	m.mu.Lock()
	defer m.mu.Unlock()
	return io.NopCloser(strings.NewReader(m.stderrData))
}
