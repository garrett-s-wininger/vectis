package audit

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrBufferFull = errors.New("audit buffer full")
	ErrStopped    = errors.New("audit auditor stopped")
)

// Repository defines the persistence layer for audit events.
type Repository interface {
	InsertAuditEvents(ctx context.Context, events []Event) error
}

// Metrics records observable audit loss and durability signals.
type Metrics interface {
	RecordDropped(ctx context.Context, eventType string)
	RecordFlushFailure(ctx context.Context, eventCount int)
}

// AsyncAuditor buffers audit events and flushes them asynchronously.
type AsyncAuditor struct {
	repo          Repository
	logger        *slog.Logger
	metrics       Metrics
	buffer        chan Event
	batchSize     int
	flushInterval time.Duration
	done          chan struct{}
	wg            sync.WaitGroup
	stopped       atomic.Bool
	stopOnce      sync.Once

	droppedCount   atomic.Int64
	flushFailCount atomic.Int64
}

// NewAsyncAuditor creates an asynchronous auditor.
// Events are buffered in memory and flushed in batches.
func NewAsyncAuditor(repo Repository, logger *slog.Logger) *AsyncAuditor {
	return NewAsyncAuditorWithMetrics(repo, logger, nil)
}

// NewAsyncAuditorWithMetrics creates an asynchronous auditor with metrics.
func NewAsyncAuditorWithMetrics(repo Repository, logger *slog.Logger, metrics Metrics) *AsyncAuditor {
	return NewAsyncAuditorWithOptionsAndMetrics(repo, logger, 100, 1*time.Second, metrics)
}

// NewAsyncAuditorWithOptions creates an asynchronous auditor with configurable batch size and flush interval.
func NewAsyncAuditorWithOptions(repo Repository, logger *slog.Logger, batchSize int, flushInterval time.Duration) *AsyncAuditor {
	return NewAsyncAuditorWithOptionsAndMetrics(repo, logger, batchSize, flushInterval, nil)
}

// NewAsyncAuditorWithOptionsAndMetrics creates an asynchronous auditor with configurable flushing and metrics.
func NewAsyncAuditorWithOptionsAndMetrics(repo Repository, logger *slog.Logger, batchSize int, flushInterval time.Duration, metrics Metrics) *AsyncAuditor {
	return NewAsyncAuditorWithBufferAndMetrics(repo, logger, batchSize, flushInterval, 1000, metrics)
}

// NewAsyncAuditorWithBuffer creates an asynchronous auditor with full control over buffering.
func NewAsyncAuditorWithBuffer(repo Repository, logger *slog.Logger, batchSize int, flushInterval time.Duration, bufferSize int) *AsyncAuditor {
	return NewAsyncAuditorWithBufferAndMetrics(repo, logger, batchSize, flushInterval, bufferSize, nil)
}

// NewAsyncAuditorWithBufferAndMetrics creates an asynchronous auditor with full control over buffering and metrics.
func NewAsyncAuditorWithBufferAndMetrics(repo Repository, logger *slog.Logger, batchSize int, flushInterval time.Duration, bufferSize int, metrics Metrics) *AsyncAuditor {
	if logger == nil {
		logger = slog.Default()
	}

	if batchSize <= 0 {
		batchSize = 100
	}

	if flushInterval <= 0 {
		flushInterval = 1 * time.Second
	}

	if bufferSize <= 0 {
		bufferSize = 1000
	}

	a := &AsyncAuditor{
		repo:          repo,
		logger:        logger,
		metrics:       metrics,
		buffer:        make(chan Event, bufferSize),
		batchSize:     batchSize,
		flushInterval: flushInterval,
		done:          make(chan struct{}),
	}

	a.wg.Add(1)
	go a.flushLoop()

	return a
}

// Stop shuts down the background flush goroutine and drains pending events.
func (a *AsyncAuditor) Stop() {
	a.stopOnce.Do(func() {
		a.stopped.Store(true)
		close(a.done)
		a.wg.Wait()
	})

	// Drain any events enqueued after the goroutine exited
	for {
		select {
		case event := <-a.buffer:
			a.flush([]Event{event})
		default:
			return
		}
	}
}

func (a *AsyncAuditor) DroppedCount() int64 {
	return a.droppedCount.Load()
}

// Log emits an audit event according to its durability policy.
func (a *AsyncAuditor) Log(ctx context.Context, event Event) error {
	event = normalizeEvent(event)

	switch event.Durability {
	case DurabilityDisabled:
		return nil
	case DurabilityFailClosed:
		return a.insertSync(ctx, []Event{event})
	case DurabilityDurableBestEffort:
		if err := a.enqueue(event); err == nil {
			return nil
		}

		if err := a.insertSync(ctx, []Event{event}); err != nil {
			if a.metrics != nil {
				a.metrics.RecordFlushFailure(ctx, 1)
			}

			a.logger.Error("failed to synchronously persist durable best-effort audit event", "event_type", event.Type, "error", err)
		}

		return nil
	default:
		if err := a.enqueue(event); err != nil {
			if a.metrics != nil {
				a.metrics.RecordDropped(ctx, event.Type)
			}

			a.logger.Warn("audit event dropped", "event_type", event.Type, "error", err)
		}
		return nil
	}
}

func normalizeEvent(event Event) Event {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	if event.Durability == "" {
		event.Durability = DefaultPolicy().DurabilityFor(event.Type)
	}

	return event
}

func (a *AsyncAuditor) enqueue(event Event) error {
	if a.stopped.Load() {
		return ErrStopped
	}

	select {
	case a.buffer <- event:
		return nil
	default:
		a.droppedCount.Add(1)

		a.logger.Warn("audit event dropped: buffer full", "event_type", event.Type)
		return ErrBufferFull
	}
}

func (a *AsyncAuditor) flushLoop() {
	defer a.wg.Done()

	ticker := time.NewTicker(a.flushInterval)
	defer ticker.Stop()

	batch := make([]Event, 0, a.batchSize)

	for {
		select {
		case event := <-a.buffer:
			batch = append(batch, event)
			if len(batch) >= a.batchSize {
				a.flush(batch)
				batch = make([]Event, 0, a.batchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				a.flush(batch)
				batch = make([]Event, 0, a.batchSize)
			}
		case <-a.done:
			// Drain remaining events from buffer
			for {
				select {
				case event := <-a.buffer:
					batch = append(batch, event)
					if len(batch) >= a.batchSize {
						a.flush(batch)
						batch = make([]Event, 0, a.batchSize)
					}
				default:
					if len(batch) > 0 {
						a.flush(batch)
					}
					return
				}
			}
		}
	}
}

// FlushFailureCount returns the number of audit flush failures since startup.
func (a *AsyncAuditor) FlushFailureCount() int64 {
	return a.flushFailCount.Load()
}

func (a *AsyncAuditor) flush(events []Event) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := a.repo.InsertAuditEvents(ctx, events); err != nil {
		a.flushFailCount.Add(1)

		if a.metrics != nil {
			a.metrics.RecordFlushFailure(ctx, len(events))
		}

		a.logger.Error("failed to flush audit events", "error", err, "count", len(events))
	}
}

func (a *AsyncAuditor) insertSync(ctx context.Context, events []Event) error {
	if ctx == nil {
		ctx = context.Background()
	}

	syncCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return a.repo.InsertAuditEvents(syncCtx, events)
}

// NoOpAuditor is an auditor that discards all events.
type NoOpAuditor struct{}

func (NoOpAuditor) Log(ctx context.Context, event Event) error { return nil }
