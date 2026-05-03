package observability

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

const defaultAsyncSlogBuffer = 4096

type asyncSlogEntry struct {
	ctx    context.Context
	record slog.Record
	target slog.Handler
}

type asyncSlogCore struct {
	queue   chan asyncSlogEntry
	done    chan struct{}
	wg      sync.WaitGroup
	close   sync.Once
	stopped atomic.Bool
	dropped atomic.Uint64
}

type AsyncSlogHandler struct {
	target slog.Handler
	core   *asyncSlogCore
}

func NewAsyncSlogHandler(target slog.Handler, bufferSize int) *AsyncSlogHandler {
	if bufferSize <= 0 {
		bufferSize = defaultAsyncSlogBuffer
	}

	core := &asyncSlogCore{
		queue: make(chan asyncSlogEntry, bufferSize),
		done:  make(chan struct{}),
	}
	core.wg.Add(1)
	go core.run()

	return &AsyncSlogHandler{
		target: target,
		core:   core,
	}
}

func (h *AsyncSlogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.target.Enabled(ctx, level)
}

func (h *AsyncSlogHandler) Handle(ctx context.Context, record slog.Record) error {
	if h.core.stopped.Load() {
		return nil
	}

	record = record.Clone()
	select {
	case h.core.queue <- asyncSlogEntry{ctx: ctx, record: record, target: h.target}:
	default:
		h.core.dropped.Add(1)
	}

	return nil
}

func (h *AsyncSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &AsyncSlogHandler{
		target: h.target.WithAttrs(attrs),
		core:   h.core,
	}
}

func (h *AsyncSlogHandler) WithGroup(name string) slog.Handler {
	return &AsyncSlogHandler{
		target: h.target.WithGroup(name),
		core:   h.core,
	}
}

func (h *AsyncSlogHandler) Close() error {
	if h == nil || h.core == nil {
		return nil
	}

	h.core.close.Do(func() {
		h.core.stopped.Store(true)
		close(h.core.done)
		h.core.wg.Wait()
	})

	return nil
}

func (h *AsyncSlogHandler) Dropped() uint64 {
	if h == nil || h.core == nil {
		return 0
	}

	return h.core.dropped.Load()
}

func (c *asyncSlogCore) run() {
	defer c.wg.Done()

	for {
		select {
		case entry := <-c.queue:
			_ = entry.target.Handle(entry.ctx, entry.record)
		case <-c.done:
			for {
				select {
				case entry := <-c.queue:
					_ = entry.target.Handle(entry.ctx, entry.record)
				default:
					return
				}
			}
		}
	}
}
