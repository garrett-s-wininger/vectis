package interfaces_test

import (
	"bytes"
	"strings"
	"testing"

	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
)

func TestMockLogger_Debug(t *testing.T) {
	logger := mocks.NewMockLogger()

	logger.Debug("test debug message")
	logger.Debug("debug with %s", "args")

	calls := logger.GetDebugCalls()
	if len(calls) != 2 {
		t.Errorf("expected 2 debug calls, got %d", len(calls))
	}

	if !strings.Contains(calls[0], "test debug message") {
		t.Errorf("expected first call to contain 'test debug message', got: %s", calls[0])
	}

	if !strings.Contains(calls[1], "debug with args") {
		t.Errorf("expected second call to contain 'debug with args', got: %s", calls[1])
	}
}

func TestMockLogger_Info(t *testing.T) {
	logger := mocks.NewMockLogger()

	logger.Info("test info message")
	logger.Info("info with %s", "args")

	calls := logger.GetInfoCalls()
	if len(calls) != 2 {
		t.Errorf("expected 2 info calls, got %d", len(calls))
	}

	if !strings.Contains(calls[0], "test info message") {
		t.Errorf("expected first call to contain 'test info message', got: %s", calls[0])
	}

	if !strings.Contains(calls[1], "info with args") {
		t.Errorf("expected second call to contain 'info with args', got: %s", calls[1])
	}
}

func TestMockLogger_Warn(t *testing.T) {
	logger := mocks.NewMockLogger()

	logger.Warn("test warn message")
	logger.Warn("warn with %s", "args")

	calls := logger.GetWarnCalls()
	if len(calls) != 2 {
		t.Errorf("expected 2 warn calls, got %d", len(calls))
	}

	if !strings.Contains(calls[0], "test warn message") {
		t.Errorf("expected first call to contain 'test warn message', got: %s", calls[0])
	}
}

func TestMockLogger_Error(t *testing.T) {
	logger := mocks.NewMockLogger()

	logger.Error("test error message")
	logger.Error("error with %s", "args")

	calls := logger.GetErrorCalls()
	if len(calls) != 2 {
		t.Errorf("expected 2 error calls, got %d", len(calls))
	}

	if !strings.Contains(calls[0], "test error message") {
		t.Errorf("expected first call to contain 'test error message', got: %s", calls[0])
	}
}

func TestMockLogger_Fatal(t *testing.T) {
	logger := mocks.NewMockLogger()

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected Fatal to panic")
		} else {
			panicMsg, ok := r.(string)
			if !ok {
				t.Error("expected panic message to be a string")
				return
			}
			if !strings.Contains(panicMsg, "test fatal message") {
				t.Errorf("expected panic message to contain 'test fatal message', got: %s", panicMsg)
			}
		}
	}()

	logger.Fatal("test fatal message")
	t.Error("should not reach here after Fatal")
}

func TestMockLogger_AllLevels(t *testing.T) {
	logger := mocks.NewMockLogger()

	logger.Debug("debug")
	logger.Info("info")
	logger.Warn("warn")
	logger.Error("error")

	allCalls := logger.GetCalls()
	if len(allCalls) != 4 {
		t.Errorf("expected 4 total calls, got %d", len(allCalls))
	}

	expectedLevels := []interfaces.Level{
		interfaces.LevelDebug,
		interfaces.LevelInfo,
		interfaces.LevelWarn,
		interfaces.LevelError,
	}

	for i, call := range allCalls {
		if call.Level != expectedLevels[i] {
			t.Errorf("expected call %d to have level %s, got %s", i, expectedLevels[i], call.Level)
		}
	}
}

func TestMockLogger_WithOutput(t *testing.T) {
	logger := mocks.NewMockLogger()
	var buf bytes.Buffer

	newLogger := logger.WithOutput(&buf)
	newLogger.Info("test output")

	if !strings.Contains(buf.String(), "test output") {
		t.Errorf("expected output buffer to contain 'test output', got: %s", buf.String())
	}
}

func TestStderrLogger_WithOutput(t *testing.T) {
	logger := interfaces.NewLogger("test")
	var buf bytes.Buffer

	newLogger := logger.WithOutput(&buf)
	newLogger.Info("test message")

	if !strings.Contains(buf.String(), "test message") {
		t.Errorf("expected output buffer to contain 'test message', got: %s", buf.String())
	}

	if !strings.Contains(buf.String(), "[test]") {
		t.Errorf("expected output buffer to contain component name '[test]', got: %s", buf.String())
	}
}

func TestStderrLogger_AllLevels(t *testing.T) {
	logger := interfaces.NewLogger("test")
	var buf bytes.Buffer
	newLogger := logger.WithOutput(&buf)

	newLogger.Debug("debug message")
	newLogger.Info("info message")
	newLogger.Warn("warn message")
	newLogger.Error("error message")

	output := buf.String()

	if !strings.Contains(output, "DEBUG") {
		t.Error("expected output to contain DEBUG level")
	}
	if !strings.Contains(output, "INFO") {
		t.Error("expected output to contain INFO level")
	}
	if !strings.Contains(output, "WARN") {
		t.Error("expected output to contain WARN level")
	}
	if !strings.Contains(output, "ERROR") {
		t.Error("expected output to contain ERROR level")
	}
}

func TestMockLogger_ErrorArgument(t *testing.T) {
	logger := mocks.NewMockLogger()

	testErr := &testError{msg: "test error"}
	logger.Error("got error: %v", testErr)

	calls := logger.GetErrorCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 error call, got %d", len(calls))
	}

	if !strings.Contains(calls[0], "test error") {
		t.Errorf("expected error message to contain error text, got: %s", calls[0])
	}
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}
