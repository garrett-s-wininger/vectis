package log

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type Level string

const (
	LevelDebug Level = "DEBUG"
	LevelInfo  Level = "INFO"
	LevelWarn  Level = "WARN"
	LevelError Level = "ERROR"
)

type Logger struct {
	component string
	out       io.Writer
	mu        sync.Mutex
}

func New(component string) *Logger {
	return &Logger{
		component: component,
		out:       os.Stderr,
	}
}

func (l *Logger) WithOutput(w io.Writer) *Logger {
	return &Logger{
		component: l.component,
		out:       w,
	}
}

func (l *Logger) format(level Level, msg string, args ...any) string {
	ts := time.Now().UTC().Format(time.RFC3339)
	if len(args) == 0 {
		return fmt.Sprintf("%s %s [%s] %s\n", ts, level, l.component, msg)
	}

	return fmt.Sprintf("%s %s [%s] %s\n", ts, level, l.component, fmt.Sprintf(msg, args...))
}

func (l *Logger) write(level Level, msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	_, _ = io.WriteString(l.out, l.format(level, msg, args...))
}

func (l *Logger) Debug(msg string, args ...any) {
	l.write(LevelDebug, msg, args...)
}

func (l *Logger) Info(msg string, args ...any) {
	l.write(LevelInfo, msg, args...)
}

func (l *Logger) Warn(msg string, args ...any) {
	l.write(LevelWarn, msg, args...)
}

func (l *Logger) Error(msg string, args ...any) {
	l.write(LevelError, msg, args...)
}

func (l *Logger) Fatal(msg string, args ...any) {
	l.write(LevelError, msg, args...)
	os.Exit(1)
}
