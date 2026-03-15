package mocks

import (
	"fmt"
	"io"
	"sync"

	"vectis/internal/interfaces"
)

type LogCall struct {
	Level   interfaces.Level
	Message string
}

type MockLogger struct {
	mu    sync.Mutex
	calls []LogCall
	out   io.Writer
}

func NewMockLogger() *MockLogger {
	return &MockLogger{
		calls: make([]LogCall, 0),
	}
}

func (m *MockLogger) WithOutput(w io.Writer) interfaces.Logger {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &MockLogger{
		calls: m.calls,
		out:   w,
	}
}

func (m *MockLogger) GetCalls() []LogCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]LogCall, len(m.calls))
	copy(result, m.calls)
	return result
}

func (m *MockLogger) GetDebugCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []string
	for _, call := range m.calls {
		if call.Level == interfaces.LevelDebug {
			result = append(result, call.Message)
		}
	}
	return result
}

func (m *MockLogger) GetInfoCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []string
	for _, call := range m.calls {
		if call.Level == interfaces.LevelInfo {
			result = append(result, call.Message)
		}
	}
	return result
}

func (m *MockLogger) GetWarnCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []string
	for _, call := range m.calls {
		if call.Level == interfaces.LevelWarn {
			result = append(result, call.Message)
		}
	}
	return result
}

func (m *MockLogger) GetErrorCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []string
	for _, call := range m.calls {
		if call.Level == interfaces.LevelError {
			result = append(result, call.Message)
		}
	}
	return result
}

func (m *MockLogger) GetFatalCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []string
	for _, call := range m.calls {
		if call.Level == interfaces.LevelFatal {
			result = append(result, call.Message)
		}
	}
	return result
}

func (m *MockLogger) log(level interfaces.Level, msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()

	message := msg
	if len(args) > 0 {
		message = fmt.Sprintf(msg, args...)
	}

	m.calls = append(m.calls, LogCall{Level: level, Message: message})

	if m.out != nil {
		fmt.Fprintln(m.out, message)
	}
}

func (m *MockLogger) Debug(msg string, args ...any) {
	m.log(interfaces.LevelDebug, msg, args...)
}

func (m *MockLogger) Info(msg string, args ...any) {
	m.log(interfaces.LevelInfo, msg, args...)
}

func (m *MockLogger) Warn(msg string, args ...any) {
	m.log(interfaces.LevelWarn, msg, args...)
}

func (m *MockLogger) Error(msg string, args ...any) {
	m.log(interfaces.LevelError, msg, args...)
}

func (m *MockLogger) Fatal(msg string, args ...any) {
	m.log(interfaces.LevelFatal, msg, args...)
	if len(args) > 0 {
		panic("Fatal: " + fmt.Sprintf(msg, args...))
	}
	panic("Fatal: " + msg)
}

var _ interfaces.Logger = (*MockLogger)(nil)
