package audit

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockRepo struct {
	mu     sync.Mutex
	events [][]Event
}

func (m *mockRepo) InsertAuditEvents(_ context.Context, events []Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, append([]Event(nil), events...))

	return nil
}

func (m *mockRepo) totalEvents() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0

	for _, batch := range m.events {
		total += len(batch)
	}

	return total
}

func waitFor(t *testing.T, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)

	for time.Now().Before(deadline) {
		if pred() {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout waiting for condition")
}

func TestAsyncAuditor_LogAndFlush(t *testing.T) {
	repo := &mockRepo{}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	a := NewAsyncAuditorWithBuffer(repo, logger, 10, 20*time.Millisecond, 10)
	defer a.Stop()

	ctx := context.Background()
	if err := a.Log(ctx, Event{Type: EventAuthSuccess, ActorID: 1}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool { return repo.totalEvents() == 1 })
}

func TestAsyncAuditor_BatchFlush(t *testing.T) {
	repo := &mockRepo{}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	a := NewAsyncAuditorWithBuffer(repo, logger, 10, 50*time.Millisecond, 100)
	defer a.Stop()

	ctx := context.Background()
	for i := 0; i < 25; i++ {
		if err := a.Log(ctx, Event{Type: EventAuthSuccess, ActorID: int64(i)}); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, func() bool { return repo.totalEvents() == 25 })
}

func TestAsyncAuditor_StopDrainsEvents(t *testing.T) {
	repo := &mockRepo{}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	a := NewAsyncAuditorWithBuffer(repo, logger, 100, time.Hour, 10)

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := a.Log(ctx, Event{Type: EventAuthSuccess, ActorID: int64(i)}); err != nil {
			t.Fatal(err)
		}
	}

	a.Stop()

	if repo.totalEvents() != 5 {
		t.Fatalf("expected 5 events after drain, got %d", repo.totalEvents())
	}
}

func TestAsyncAuditor_StopIdempotent(t *testing.T) {
	repo := &mockRepo{}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	a := NewAsyncAuditorWithBuffer(repo, logger, 10, 20*time.Millisecond, 10)

	ctx := context.Background()
	_ = a.Log(ctx, Event{Type: EventAuthSuccess, ActorID: 1})

	// Multiple stops should not panic
	a.Stop()
	a.Stop()
	a.Stop()
}

func TestAsyncAuditor_BufferFullDropsEvent(t *testing.T) {
	// Use a repo that blocks long enough for us to overflow the tiny buffer.
	blockRepo := &slowMockRepo{delay: 200 * time.Millisecond}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	a := NewAsyncAuditorWithBuffer(blockRepo, logger, 5, 500*time.Millisecond, 5)
	defer a.Stop()

	ctx := context.Background()

	// Send more events than the buffer can hold while flush is blocked.
	// The flush will sleep for 200ms per call, giving us time to overflow.
	for i := 0; i < 20; i++ {
		_ = a.Log(ctx, Event{Type: EventAuthSuccess, ActorID: int64(i)})
	}

	// Should not panic; some events may be dropped. We only wait long enough
	// to confirm the flush goroutine is making progress.
	waitFor(t, func() bool { return blockRepo.calls.Load() > 0 })
}

func TestAsyncAuditor_StoppedEventDropped(t *testing.T) {
	repo := &mockRepo{}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	a := NewAsyncAuditorWithBuffer(repo, logger, 10, 20*time.Millisecond, 10)
	a.Stop()

	ctx := context.Background()
	if err := a.Log(ctx, Event{Type: EventAuthSuccess, ActorID: 1}); err != nil {
		t.Fatal(err)
	}

	if repo.totalEvents() != 0 {
		t.Fatalf("expected 0 events after stop, got %d", repo.totalEvents())
	}
}

func TestAsyncAuditor_FlushErrorLogged(t *testing.T) {
	repo := &errMockRepo{err: errors.New("db down")}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	a := NewAsyncAuditorWithBuffer(repo, logger, 10, 20*time.Millisecond, 10)
	defer a.Stop()

	ctx := context.Background()
	_ = a.Log(ctx, Event{Type: EventAuthSuccess, ActorID: 1})

	// Wait for at least one flush attempt to happen
	waitFor(t, func() bool { return repo.calls.Load() > 0 })
}

type slowMockRepo struct {
	delay time.Duration
	calls atomic.Int32
}

func (m *slowMockRepo) InsertAuditEvents(_ context.Context, _ []Event) error {
	m.calls.Add(1)
	time.Sleep(m.delay)
	return nil
}

type errMockRepo struct {
	err   error
	calls atomic.Int32
}

func (m *errMockRepo) InsertAuditEvents(_ context.Context, _ []Event) error {
	m.calls.Add(1)
	return m.err
}
