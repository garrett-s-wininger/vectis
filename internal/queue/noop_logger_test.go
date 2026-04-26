package queue

import (
	"io"

	"vectis/internal/interfaces"
)

type noopLogger struct{}

func (n noopLogger) SetLevel(level interfaces.Level) {}

func (n noopLogger) Debug(msg string, args ...any) {}
func (n noopLogger) Info(msg string, args ...any)  {}
func (n noopLogger) Warn(msg string, args ...any)  {}
func (n noopLogger) Error(msg string, args ...any) {}
func (n noopLogger) Fatal(msg string, args ...any) {}

func (n noopLogger) WithOutput(w io.Writer) interfaces.Logger       { return n }
func (n noopLogger) WithField(string, string) interfaces.Logger     { return n }
func (n noopLogger) WithFields(map[string]string) interfaces.Logger { return n }
