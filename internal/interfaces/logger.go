package interfaces

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
	LevelFatal Level = "FATAL"
)

type Logger interface {
	SetLevel(level Level)
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	Fatal(msg string, args ...any)
	WithOutput(w io.Writer) Logger
}

type StderrLogger struct {
	component string
	out       io.Writer
	mu        sync.Mutex
	minLevel  Level
}

func NewLogger(component string) Logger {
	return &StderrLogger{
		component: component,
		out:       os.Stderr,
		minLevel:  LevelInfo,
	}
}

func (l *StderrLogger) WithOutput(w io.Writer) Logger {
	return &StderrLogger{
		component: l.component,
		out:       w,
		minLevel:  l.minLevel,
	}
}

func (l *StderrLogger) SetLevel(level Level) {
	l.minLevel = level
}

func (l *StderrLogger) levelPriority(level Level) int {
	switch level {
	case LevelDebug:
		return 0
	case LevelInfo:
		return 1
	case LevelWarn:
		return 2
	case LevelError:
		return 3
	case LevelFatal:
		return 4
	}
	return 1
}

func (l *StderrLogger) shouldLog(level Level) bool {
	return l.levelPriority(level) >= l.levelPriority(l.minLevel)
}

func (l *StderrLogger) format(level Level, msg string, args ...any) string {
	ts := time.Now().UTC().Format(time.RFC3339)
	if len(args) == 0 {
		return ts + " " + string(level) + " [" + l.component + "] " + msg + "\n"
	}
	return ts + " " + string(level) + " [" + l.component + "] " + fmt.Sprintf(msg, args...) + "\n"
}

func (l *StderrLogger) write(level Level, msg string, args ...any) {
	if !l.shouldLog(level) {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	io.WriteString(l.out, l.format(level, msg, args...))
}

func (l *StderrLogger) Debug(msg string, args ...any) {
	l.write(LevelDebug, msg, args...)
}

func (l *StderrLogger) Info(msg string, args ...any) {
	l.write(LevelInfo, msg, args...)
}

func (l *StderrLogger) Warn(msg string, args ...any) {
	l.write(LevelWarn, msg, args...)
}

func (l *StderrLogger) Error(msg string, args ...any) {
	l.write(LevelError, msg, args...)
}

func (l *StderrLogger) Fatal(msg string, args ...any) {
	l.write(LevelFatal, msg, args...)
	os.Exit(1)
}

func ParseLevel(levelStr string) (Level, error) {
	switch levelStr {
	case "debug":
		return LevelDebug, nil
	case "info":
		return LevelInfo, nil
	case "warn":
		return LevelWarn, nil
	case "error":
		return LevelError, nil
	default:
		return LevelInfo, fmt.Errorf("invalid log level: %s (must be debug, info, warn, or error)", levelStr)
	}
}
