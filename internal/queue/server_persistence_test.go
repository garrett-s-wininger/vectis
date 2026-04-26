package queue

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"
)

func TestQueuePersistence_RestorePendingOrder(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	svc, err := NewQueueServiceWithOptions(mocks.NopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  8,
	}, nil)
	if err != nil {
		t.Fatalf("create persisted queue: %v", err)
	}

	for _, id := range []string{"job-1", "job-2", "job-3"} {
		jobID := id
		if _, err := svc.Enqueue(ctx, &api.Job{Id: &jobID}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	if _, err := svc.Dequeue(ctx, &api.Empty{}); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	jobID := "job-4"
	if _, err := svc.Enqueue(ctx, &api.Job{Id: &jobID}); err != nil {
		t.Fatalf("enqueue job-4: %v", err)
	}

	restarted, err := NewQueueServiceWithOptions(mocks.NopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  8,
	}, nil)

	if err != nil {
		t.Fatalf("restart queue: %v", err)
	}

	for _, want := range []string{"job-2", "job-3", "job-4"} {
		got, err := restarted.TryDequeue(ctx, &api.Empty{})
		if err != nil {
			t.Fatalf("trydequeue %s: %v", want, err)
		}
		if got == nil || got.GetId() != want {
			t.Fatalf("expected %s, got %#v", want, got)
		}
	}
}

func TestQueuePersistence_RestoreFromSnapshot(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	svc, err := NewQueueServiceWithOptions(mocks.NopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  1,
	}, nil)

	if err != nil {
		t.Fatalf("create persisted queue: %v", err)
	}

	for _, id := range []string{"job-a", "job-b"} {
		jobID := id
		if _, err := svc.Enqueue(ctx, &api.Job{Id: &jobID}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	_, _ = svc.Dequeue(ctx, &api.Empty{})

	restarted, err := NewQueueServiceWithOptions(mocks.NopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  1,
	}, nil)

	if err != nil {
		t.Fatalf("restart queue: %v", err)
	}

	got, err := restarted.TryDequeue(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("trydequeue: %v", err)
	}

	if got == nil || got.GetId() != "job-b" {
		t.Fatalf("expected job-b after restart, got %#v", got)
	}
}

func TestQueuePersistence_SnapshotTruncatesWAL(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	svc, err := NewQueueServiceWithOptions(mocks.NopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  1,
		WALRetainTail:  2,
	}, nil)

	if err != nil {
		t.Fatalf("create persisted queue: %v", err)
	}

	jobID := "job-1"
	if _, err := svc.Enqueue(ctx, &api.Job{Id: &jobID}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	segments := listWALSegments(t, dir)
	if len(segments) == 0 || len(segments) > 2 {
		t.Fatalf("expected 1-2 wal segments after compaction, got %d", len(segments))
	}
}

func TestQueuePersistence_ExpiredRequeueSurvivesRestartBeforeSnapshot(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  1000,
		DeliveryTTL:    20 * time.Millisecond,
		WALSegmentMax:  256,
		WALRetainTail:  2,
	}, nil)

	if err != nil {
		t.Fatalf("create persisted queue: %v", err)
	}

	job1 := "job-1"
	if _, err := svc.Enqueue(ctx, &api.Job{Id: &job1}); err != nil {
		t.Fatalf("enqueue job-1: %v", err)
	}

	if _, err := svc.Dequeue(ctx, &api.Empty{}); err != nil {
		t.Fatalf("dequeue job-1: %v", err)
	}

	time.Sleep(30 * time.Millisecond)

	job2 := "job-2"
	if _, err := svc.Enqueue(ctx, &api.Job{Id: &job2}); err != nil {
		t.Fatalf("enqueue job-2: %v", err)
	}

	restarted, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  1000,
		DeliveryTTL:    20 * time.Millisecond,
		WALSegmentMax:  256,
		WALRetainTail:  2,
	}, nil)
	if err != nil {
		t.Fatalf("restart queue: %v", err)
	}

	first, err := restarted.TryDequeue(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("first dequeue after restart: %v", err)
	}
	if first == nil || first.GetId() != "job-1" {
		t.Fatalf("expected first replayed job-1, got %#v", first)
	}

	second, err := restarted.TryDequeue(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("second dequeue after restart: %v", err)
	}
	if second == nil || second.GetId() != "job-2" {
		t.Fatalf("expected second job-2, got %#v", second)
	}
}

func listWALSegments(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}

	segments := make([]string, 0)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasPrefix(e.Name(), walSegmentPrefix) {
			segments = append(segments, filepath.Join(dir, e.Name()))
		}
	}

	return segments
}
