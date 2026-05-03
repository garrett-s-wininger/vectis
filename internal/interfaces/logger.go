package interfaces

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	WithField(key string, value string) Logger
	WithFields(fields map[string]string) Logger
}

type StderrLogger struct {
	component string
	out       io.Writer
	mu        sync.Mutex
	minLevel  Level
	fields    map[string]string
	jsonFmt   bool
}

const defaultAsyncLoggerBuffer = 4096

func NewLogger(component string) Logger {
	jsonFmt := os.Getenv("VECTIS_LOG_FORMAT") == "json"
	return &StderrLogger{
		component: component,
		out:       os.Stderr,
		minLevel:  LevelInfo,
		fields:    nil,
		jsonFmt:   jsonFmt,
	}
}

func NewAsyncLogger(component string) *AsyncLogger {
	return NewAsyncLoggerWithBuffer(component, asyncLoggerBufferSize())
}

func NewAsyncLoggerWithBuffer(component string, bufferSize int) *AsyncLogger {
	if bufferSize <= 0 {
		bufferSize = defaultAsyncLoggerBuffer
	}

	core := &asyncLoggerCore{
		queue: make(chan asyncLogEntry, bufferSize),
		done:  make(chan struct{}),
	}
	core.wg.Add(1)
	go core.run()

	return &AsyncLogger{
		inner: NewLogger(component),
		core:  core,
	}
}

func asyncLoggerBufferSize() int {
	raw := strings.TrimSpace(os.Getenv("VECTIS_LOG_BUFFER_SIZE"))
	if raw == "" {
		return defaultAsyncLoggerBuffer
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return defaultAsyncLoggerBuffer
	}

	return n
}

func (l *StderrLogger) WithOutput(w io.Writer) Logger {
	return &StderrLogger{
		component: l.component,
		out:       w,
		minLevel:  l.minLevel,
		fields:    l.copyFields(),
		jsonFmt:   l.jsonFmt,
	}
}

func (l *StderrLogger) WithField(key string, value string) Logger {
	fields := l.copyFields()
	fields[key] = value
	return &StderrLogger{
		component: l.component,
		out:       l.out,
		minLevel:  l.minLevel,
		fields:    fields,
		jsonFmt:   l.jsonFmt,
	}
}

func (l *StderrLogger) WithFields(m map[string]string) Logger {
	fields := l.copyFields()
	maps.Copy(fields, m)
	return &StderrLogger{
		component: l.component,
		out:       l.out,
		minLevel:  l.minLevel,
		fields:    fields,
		jsonFmt:   l.jsonFmt,
	}
}

func (l *StderrLogger) copyFields() map[string]string {
	if len(l.fields) == 0 {
		return make(map[string]string)
	}
	out := make(map[string]string, len(l.fields))
	maps.Copy(out, l.fields)
	return out
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
	if l.jsonFmt {
		return l.formatJSON(level, msg, args...)
	}
	return l.formatText(level, msg, args...)
}

func (l *StderrLogger) formatText(level Level, msg string, args ...any) string {
	ts := time.Now().UTC().Format(time.RFC3339)
	var msgStr string
	if len(args) == 0 {
		msgStr = msg
	} else {
		msgStr = fmt.Sprintf(msg, args...)
	}
	var line strings.Builder
	line.WriteString(ts + " " + string(level) + " [" + l.component + "] " + msgStr)
	if len(l.fields) > 0 {
		first := true
		for k, v := range l.fields {
			if first {
				line.WriteString(" |")
				first = false
			}
			line.WriteString(" " + k + "=" + v)
		}
	}
	return line.String() + "\n"
}

func (l *StderrLogger) formatJSON(level Level, msg string, args ...any) string {
	entry := make(map[string]any, 4+len(l.fields))
	entry["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	entry["level"] = string(level)
	entry["component"] = l.component
	if len(args) == 0 {
		entry["msg"] = msg
	} else {
		entry["msg"] = fmt.Sprintf(msg, args...)
	}
	for k, v := range l.fields {
		entry[k] = v
	}
	b, _ := json.Marshal(entry)
	return string(b) + "\n"
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

type asyncLogEntry struct {
	logger Logger
	level  Level
	msg    string
	args   []any
}

type asyncLoggerCore struct {
	queue   chan asyncLogEntry
	done    chan struct{}
	wg      sync.WaitGroup
	close   sync.Once
	stopped atomic.Bool
	dropped atomic.Uint64
}

func (c *asyncLoggerCore) run() {
	defer c.wg.Done()

	for {
		select {
		case entry := <-c.queue:
			writeLogEntry(entry)
		case <-c.done:
			for {
				select {
				case entry := <-c.queue:
					writeLogEntry(entry)
				default:
					return
				}
			}
		}
	}
}

func (c *asyncLoggerCore) enqueue(entry asyncLogEntry) {
	if c.stopped.Load() {
		return
	}

	select {
	case c.queue <- entry:
	default:
		c.dropped.Add(1)
	}
}

func (c *asyncLoggerCore) Close() error {
	c.close.Do(func() {
		c.stopped.Store(true)
		close(c.done)
		c.wg.Wait()
	})
	return nil
}

func writeLogEntry(entry asyncLogEntry) {
	switch entry.level {
	case LevelDebug:
		entry.logger.Debug(entry.msg, entry.args...)
	case LevelInfo:
		entry.logger.Info(entry.msg, entry.args...)
	case LevelWarn:
		entry.logger.Warn(entry.msg, entry.args...)
	case LevelError:
		entry.logger.Error(entry.msg, entry.args...)
	case LevelFatal:
		entry.logger.Fatal(entry.msg, entry.args...)
	}
}

type AsyncLogger struct {
	inner Logger
	core  *asyncLoggerCore
}

func (l *AsyncLogger) SetLevel(level Level) {
	l.inner.SetLevel(level)
}

func (l *AsyncLogger) Debug(msg string, args ...any) {
	l.core.enqueue(asyncLogEntry{logger: l.inner, level: LevelDebug, msg: msg, args: args})
}

func (l *AsyncLogger) Info(msg string, args ...any) {
	l.core.enqueue(asyncLogEntry{logger: l.inner, level: LevelInfo, msg: msg, args: args})
}

func (l *AsyncLogger) Warn(msg string, args ...any) {
	l.core.enqueue(asyncLogEntry{logger: l.inner, level: LevelWarn, msg: msg, args: args})
}

func (l *AsyncLogger) Error(msg string, args ...any) {
	l.core.enqueue(asyncLogEntry{logger: l.inner, level: LevelError, msg: msg, args: args})
}

func (l *AsyncLogger) Fatal(msg string, args ...any) {
	_ = l.Close()
	l.inner.Fatal(msg, args...)
}

func (l *AsyncLogger) WithOutput(w io.Writer) Logger {
	return &AsyncLogger{
		inner: l.inner.WithOutput(w),
		core:  l.core,
	}
}

func (l *AsyncLogger) WithField(key string, value string) Logger {
	return &AsyncLogger{
		inner: l.inner.WithField(key, value),
		core:  l.core,
	}
}

func (l *AsyncLogger) WithFields(fields map[string]string) Logger {
	return &AsyncLogger{
		inner: l.inner.WithFields(fields),
		core:  l.core,
	}
}

func (l *AsyncLogger) Close() error {
	if l == nil || l.core == nil {
		return nil
	}
	return l.core.Close()
}

func (l *AsyncLogger) Shutdown(ctx context.Context) error {
	done := make(chan error, 1)
	go func() {
		done <- l.Close()
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *AsyncLogger) Dropped() uint64 {
	if l == nil || l.core == nil {
		return 0
	}
	return l.core.dropped.Load()
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
