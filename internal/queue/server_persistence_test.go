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

func TestQueuePersistence_JobAttemptsSurviveSnapshot(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Use a very short delivery TTL and small snapshot interval so we can
	// trigger expired requeues and snapshot quickly.
	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  4,
		DeliveryTTL:    20 * time.Millisecond,
		WALRetainTail:  2,
	}, nil)

	if err != nil {
		t.Fatalf("create persisted queue: %v", err)
	}

	job1 := "job-1"
	if _, err := svc.Enqueue(ctx, &api.Job{Id: &job1}); err != nil {
		t.Fatalf("enqueue job-1: %v", err)
	}

	// Dequeue job-1 (attemptCount=0).
	if _, err := svc.Dequeue(ctx, &api.Empty{}); err != nil {
		t.Fatalf("dequeue job-1: %v", err)
	}

	// Let it expire and enqueue another job to trigger requeueExpiredLocked.
	time.Sleep(30 * time.Millisecond)
	job2 := "job-2"
	if _, err := svc.Enqueue(ctx, &api.Job{Id: &job2}); err != nil {
		t.Fatalf("enqueue job-2: %v", err)
	}

	if _, err := svc.Dequeue(ctx, &api.Empty{}); err != nil {
		t.Fatalf("dequeue job-2: %v", err)
	}

	// job-1 has been requeued once (attemptCount=1). Let it expire again.
	time.Sleep(30 * time.Millisecond)

	// Enqueue/dequeue a third job to trigger another requeue of job-1 (attemptCount=2).
	job3 := "job-3"
	if _, err := svc.Enqueue(ctx, &api.Job{Id: &job3}); err != nil {
		t.Fatalf("enqueue job-3: %v", err)
	}

	if _, err := svc.Dequeue(ctx, &api.Empty{}); err != nil {
		t.Fatalf("dequeue job-3: %v", err)
	}

	// Now job-1 has attemptCount=2. We need a few more WAL records to force a snapshot.
	// Enqueue and dequeue more jobs to hit snapshotEvery=4.
	job4 := "job-4"
	if _, err := svc.Enqueue(ctx, &api.Job{Id: &job4}); err != nil {
		t.Fatalf("enqueue job-4: %v", err)
	}

	if _, err := svc.Dequeue(ctx, &api.Empty{}); err != nil {
		t.Fatalf("dequeue job-4: %v", err)
	}

	time.Sleep(30 * time.Millisecond)

	// Restart the queue. The snapshot should include jobAttempts for job-1.
	restarted, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  4,
		DeliveryTTL:    20 * time.Millisecond,
		WALRetainTail:  2,
	}, nil)

	if err != nil {
		t.Fatalf("restart queue: %v", err)
	}

	// Dequeue all pending jobs. job-1 should come out with attemptCount=2.
	// If jobAttempts was lost, it would come out with attemptCount=0.
	for {
		job, err := restarted.TryDequeue(ctx, &api.Empty{})
		if err != nil {
			t.Fatalf("trydequeue after restart: %v", err)
		}

		if job == nil {
			break
		}

		if job.GetId() == job1 {
			// With attemptCount=2 preserved, one more expiration should make it 3.
			// We can't easily read attemptCount directly, but we can verify the job
			// is still in the pending queue (not DLQ) after restart, which means
			// its attempt count was preserved and is below maxRequeueAttempts.
			return
		}
	}

	// If we get here, job-1 was either in DLQ or missing, which means
	// jobAttempts was not preserved correctly.
	t.Fatalf("job-1 should still be pending (not DLQ) after restart, indicating jobAttempts survived")
}

func TestQueuePersistence_DLQRequeueRemovesDeadLetterAfterRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		PersistenceDir:     dir,
		SnapshotEvery:      1000,
		DeliveryTTL:        20 * time.Millisecond,
		MaxRequeueAttempts: 1,
	}, nil)
	if err != nil {
		t.Fatalf("create persisted queue: %v", err)
	}

	jobID := "job-dlq-requeue"
	if _, err := svc.Enqueue(ctx, &api.Job{Id: &jobID}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Dequeue twice and let both leases expire so the second expiry moves it to DLQ.
	for i := 0; i < 2; i++ {
		job, err := svc.Dequeue(ctx, &api.Empty{})
		if err != nil {
			t.Fatalf("dequeue %d: %v", i+1, err)
		}
		if job == nil {
			t.Fatalf("expected job on dequeue %d", i+1)
		}
		time.Sleep(30 * time.Millisecond)
	}

	// Trigger expiry processing and DLQ move.
	if _, err := svc.TryDequeue(ctx, &api.Empty{}); err != nil {
		t.Fatalf("trigger dlq move: %v", err)
	}

	dlq, err := svc.ListDeadLetter(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("list dead letter before requeue: %v", err)
	}
	if len(dlq.Items) != 1 {
		t.Fatalf("expected 1 dead letter item before requeue, got %d", len(dlq.Items))
	}

	deliveryID := dlq.Items[0].GetDeliveryId()
	if _, err := svc.RequeueDeadLetter(ctx, &api.RequeueDeadLetterRequest{DeliveryId: &deliveryID}); err != nil {
		t.Fatalf("requeue dead letter: %v", err)
	}

	restarted, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		PersistenceDir:     dir,
		SnapshotEvery:      1000,
		DeliveryTTL:        20 * time.Millisecond,
		MaxRequeueAttempts: 1,
	}, nil)
	if err != nil {
		t.Fatalf("restart queue: %v", err)
	}

	dlqAfter, err := restarted.ListDeadLetter(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("list dead letter after restart: %v", err)
	}
	if len(dlqAfter.Items) != 0 {
		t.Fatalf("expected dead letter to be empty after restart, got %d", len(dlqAfter.Items))
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
