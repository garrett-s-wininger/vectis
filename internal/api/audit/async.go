package audit

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Repository defines the persistence layer for audit events.
type Repository interface {
	InsertAuditEvents(ctx context.Context, events []Event) error
}

// AsyncAuditor buffers audit events and flushes them asynchronously.
type AsyncAuditor struct {
	repo          Repository
	logger        *slog.Logger
	buffer        chan Event
	batchSize     int
	flushInterval time.Duration
	done          chan struct{}
	wg            sync.WaitGroup
	stopped       atomic.Bool
}

// NewAsyncAuditor creates an asynchronous auditor.
// Events are buffered in memory and flushed in batches.
func NewAsyncAuditor(repo Repository, logger *slog.Logger) *AsyncAuditor {
	if logger == nil {
		logger = slog.Default()
	}

	a := &AsyncAuditor{
		repo:          repo,
		logger:        logger,
		buffer:        make(chan Event, 1000),
		batchSize:     100,
		flushInterval: 1 * time.Second,
		done:          make(chan struct{}),
	}

	a.wg.Add(1)
	go a.flushLoop()

	return a
}

// Stop shuts down the background flush goroutine and drains pending events.
func (a *AsyncAuditor) Stop() {
	a.stopped.Store(true)
	close(a.done)
	a.wg.Wait()

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

// Log queues an audit event for asynchronous persistence.
// If the buffer is full, the event is dropped and a warning is logged.
func (a *AsyncAuditor) Log(ctx context.Context, event Event) error {
	if a.stopped.Load() {
		return nil
	}

	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	select {
	case a.buffer <- event:
		return nil
	default:
		a.logger.Warn("audit event dropped: buffer full", "event_type", event.Type)
		return nil // Don't block the caller
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

func (a *AsyncAuditor) flush(events []Event) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := a.repo.InsertAuditEvents(ctx, events); err != nil {
		a.logger.Error("failed to flush audit events", "error", err, "count", len(events))
	}
}

// NoOpAuditor is an auditor that discards all events.
type NoOpAuditor struct{}

func (NoOpAuditor) Log(ctx context.Context, event Event) error { return nil }
