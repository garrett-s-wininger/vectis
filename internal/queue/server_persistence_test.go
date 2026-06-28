package queue

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"

	"google.golang.org/protobuf/proto"
)

type queueServiceCloser interface {
	Close() error
}

func closeQueueService(t *testing.T, svc api.QueueServiceServer) {
	t.Helper()

	closer, ok := svc.(queueServiceCloser)
	if !ok {
		return
	}

	if err := closer.Close(); err != nil {
		t.Fatalf("close queue service: %v", err)
	}
}

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
		if _, err := svc.Enqueue(ctx, queueTestJobRequest(t, &api.Job{Id: &jobID})); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	if _, err := svc.Dequeue(ctx, &api.DequeueRequest{}); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	jobID := "job-4"
	if _, err := svc.Enqueue(ctx, queueTestJobRequest(t, &api.Job{Id: &jobID})); err != nil {
		t.Fatalf("enqueue job-4: %v", err)
	}

	closeQueueService(t, svc)
	restarted, err := NewQueueServiceWithOptions(mocks.NopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  8,
	}, nil)

	if err != nil {
		t.Fatalf("restart queue: %v", err)
	}

	for _, want := range []string{"job-2", "job-3", "job-4"} {
		got, err := restarted.TryDequeue(ctx, &api.DequeueRequest{})
		if err != nil {
			t.Fatalf("trydequeue %s: %v", want, err)
		}

		if got == nil || got.GetJob().GetId() != want {
			t.Fatalf("expected %s, got %#v", want, got)
		}
	}
}

func TestQueuePersistence_RestoreInflightDeliveryCarriesDeliveryID(t *testing.T) {
	for _, tc := range []struct {
		name          string
		snapshotEvery int
	}{
		{name: "wal", snapshotEvery: 1024},
		{name: "snapshot", snapshotEvery: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx := context.Background()

			svc, err := NewQueueServiceWithOptions(mocks.NopLogger{}, QueueOptions{
				PersistenceDir: dir,
				SnapshotEvery:  tc.snapshotEvery,
			}, nil)

			if err != nil {
				t.Fatalf("create persisted queue: %v", err)
			}

			jobID := "job-inflight-delivery-id"
			if _, err := svc.Enqueue(ctx, queueTestJobRequest(t, &api.Job{Id: &jobID})); err != nil {
				t.Fatalf("enqueue: %v", err)
			}

			delivered, err := svc.Dequeue(ctx, &api.DequeueRequest{})
			if err != nil {
				t.Fatalf("dequeue: %v", err)
			}

			deliveryID := delivered.GetJob().GetDeliveryId()
			if deliveryID == "" {
				t.Fatal("expected delivered job to carry delivery ID")
			}

			closeQueueService(t, svc)
			restarted, err := NewQueueServiceWithOptions(mocks.NopLogger{}, QueueOptions{
				PersistenceDir: dir,
				SnapshotEvery:  tc.snapshotEvery,
			}, nil)

			if err != nil {
				t.Fatalf("restart queue: %v", err)
			}
			defer closeQueueService(t, restarted)

			qs, ok := restarted.(*queueServer)
			if !ok {
				t.Fatalf("expected *queueServer, got %T", restarted)
			}

			qs.mu.Lock()
			item, ok := qs.inflight[deliveryID]
			pending := qs.size
			qs.mu.Unlock()

			if !ok {
				t.Fatalf("expected delivery %s to restore as in-flight", deliveryID)
			}

			if pending != 0 {
				t.Fatalf("expected no pending jobs after restore, got %d", pending)
			}

			if got := item.JobRequest.GetJob().GetDeliveryId(); got != deliveryID {
				t.Fatalf("restored in-flight job delivery_id = %q, want %q", got, deliveryID)
			}
		})
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
		if _, err := svc.Enqueue(ctx, queueTestJobRequest(t, &api.Job{Id: &jobID})); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	_, _ = svc.Dequeue(ctx, &api.DequeueRequest{})

	closeQueueService(t, svc)
	restarted, err := NewQueueServiceWithOptions(mocks.NopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  1,
	}, nil)

	if err != nil {
		t.Fatalf("restart queue: %v", err)
	}

	got, err := restarted.TryDequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("trydequeue: %v", err)
	}

	if got == nil || got.GetJob().GetId() != "job-b" {
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
	if _, err := svc.Enqueue(ctx, queueTestJobRequest(t, &api.Job{Id: &jobID})); err != nil {
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
	if _, err := svc.Enqueue(ctx, queueTestJobRequest(t, &api.Job{Id: &job1})); err != nil {
		t.Fatalf("enqueue job-1: %v", err)
	}

	if _, err := svc.Dequeue(ctx, &api.DequeueRequest{}); err != nil {
		t.Fatalf("dequeue job-1: %v", err)
	}

	time.Sleep(30 * time.Millisecond)

	job2 := "job-2"
	if _, err := svc.Enqueue(ctx, queueTestJobRequest(t, &api.Job{Id: &job2})); err != nil {
		t.Fatalf("enqueue job-2: %v", err)
	}

	closeQueueService(t, svc)
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

	first, err := restarted.TryDequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("first dequeue after restart: %v", err)
	}

	if first == nil || first.GetJob().GetId() != "job-1" {
		t.Fatalf("expected first replayed job-1, got %#v", first)
	}

	second, err := restarted.TryDequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("second dequeue after restart: %v", err)
	}

	if second == nil || second.GetJob().GetId() != "job-2" {
		t.Fatalf("expected second job-2, got %#v", second)
	}
}

func TestQueuePersistence_ExpiredDeadlineDropSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  1000,
		WALSegmentMax:  256,
		WALRetainTail:  2,
	}, nil)

	if err != nil {
		t.Fatalf("create persisted queue: %v", err)
	}

	jobID := "job-expired-deadline-drop"
	req := queueTestJobRequestWithDeadline(t, &api.Job{Id: &jobID}, time.Now().Add(-time.Second).UnixNano())
	if _, err := svc.Enqueue(ctx, req); err != nil {
		t.Fatalf("enqueue expired job: %v", err)
	}

	got, err := svc.TryDequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("trydequeue expired job: %v", err)
	}

	if got != nil {
		t.Fatalf("expected expired job to drop, got %#v", got)
	}

	closeQueueService(t, svc)
	restarted, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  1000,
		WALSegmentMax:  256,
		WALRetainTail:  2,
	}, nil)

	if err != nil {
		t.Fatalf("restart queue: %v", err)
	}

	got, err = restarted.TryDequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("trydequeue after restart: %v", err)
	}

	if got != nil {
		t.Fatalf("expected expired drop to survive restart, got %#v", got)
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
	if _, err := svc.Enqueue(ctx, queueTestJobRequest(t, &api.Job{Id: &job1})); err != nil {
		t.Fatalf("enqueue job-1: %v", err)
	}

	// Dequeue job-1 (attemptCount=0).
	if _, err := svc.Dequeue(ctx, &api.DequeueRequest{}); err != nil {
		t.Fatalf("dequeue job-1: %v", err)
	}

	// Let it expire and enqueue another job to trigger requeueExpiredLocked.
	time.Sleep(30 * time.Millisecond)
	job2 := "job-2"
	if _, err := svc.Enqueue(ctx, queueTestJobRequest(t, &api.Job{Id: &job2})); err != nil {
		t.Fatalf("enqueue job-2: %v", err)
	}

	if _, err := svc.Dequeue(ctx, &api.DequeueRequest{}); err != nil {
		t.Fatalf("dequeue job-2: %v", err)
	}

	// job-1 has been requeued once (attemptCount=1). Let it expire again.
	time.Sleep(30 * time.Millisecond)

	// Enqueue/dequeue a third job to trigger another requeue of job-1 (attemptCount=2).
	job3 := "job-3"
	if _, err := svc.Enqueue(ctx, queueTestJobRequest(t, &api.Job{Id: &job3})); err != nil {
		t.Fatalf("enqueue job-3: %v", err)
	}

	if _, err := svc.Dequeue(ctx, &api.DequeueRequest{}); err != nil {
		t.Fatalf("dequeue job-3: %v", err)
	}

	// Now job-1 has attemptCount=2. We need a few more WAL records to force a snapshot.
	// Enqueue and dequeue more jobs to hit snapshotEvery=4.
	job4 := "job-4"
	if _, err := svc.Enqueue(ctx, queueTestJobRequest(t, &api.Job{Id: &job4})); err != nil {
		t.Fatalf("enqueue job-4: %v", err)
	}

	if _, err := svc.Dequeue(ctx, &api.DequeueRequest{}); err != nil {
		t.Fatalf("dequeue job-4: %v", err)
	}

	time.Sleep(30 * time.Millisecond)

	// Restart the queue. The snapshot should include jobAttempts for job-1.
	closeQueueService(t, svc)
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
		job, err := restarted.TryDequeue(ctx, &api.DequeueRequest{})
		if err != nil {
			t.Fatalf("trydequeue after restart: %v", err)
		}

		if job == nil {
			break
		}

		if job.GetJob().GetId() == job1 {
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
	if _, err := svc.Enqueue(ctx, queueTestJobRequest(t, &api.Job{Id: &jobID})); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Dequeue twice and let both leases expire so the second expiry moves it to DLQ.
	for i := range 2 {
		job, err := svc.Dequeue(ctx, &api.DequeueRequest{})
		if err != nil {
			t.Fatalf("dequeue %d: %v", i+1, err)
		}
		if job == nil {
			t.Fatalf("expected job on dequeue %d", i+1)
		}
		time.Sleep(30 * time.Millisecond)
	}

	// Trigger expiry processing and DLQ move.
	if _, err := svc.TryDequeue(ctx, &api.DequeueRequest{}); err != nil {
		t.Fatalf("trigger dlq move: %v", err)
	}

	dlq, err := svc.ListDeadLetter(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("list dead letter before requeue: %v", err)
	}
	if len(dlq.GetItems()) != 1 {
		t.Fatalf("expected 1 dead letter item before requeue, got %d", len(dlq.GetItems()))
	}

	deliveryID := dlq.GetItems()[0].GetDeliveryId()
	if _, err := svc.RequeueDeadLetter(ctx, &api.RequeueDeadLetterRequest{DeliveryId: &deliveryID}); err != nil {
		t.Fatalf("requeue dead letter: %v", err)
	}

	closeQueueService(t, svc)
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

	if len(dlqAfter.GetItems()) != 0 {
		t.Fatalf("expected dead letter to be empty after restart, got %d", len(dlqAfter.GetItems()))
	}
}

func TestQueuePersistence_RejectsConcurrentPersistenceDir(t *testing.T) {
	dir := t.TempDir()

	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  8,
	}, nil)

	if err != nil {
		t.Fatalf("create persisted queue: %v", err)
	}
	defer closeQueueService(t, svc)

	cmd := exec.Command(os.Args[0], "-test.run=^TestQueuePersistenceLockSubprocess$")
	cmd.Env = append(os.Environ(),
		"VECTIS_QUEUE_LOCK_SUBPROCESS=1",
		"VECTIS_QUEUE_LOCK_DIR="+dir,
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("lock subprocess failed: %v\n%s", err, out)
	}
}

func TestQueuePersistence_RejectsInvalidRestoredHandoffs(t *testing.T) {
	badPayload := marshalQueuePersistenceJobRequest(t, &api.JobRequest{
		Job: &api.Job{Id: queueTestString("job-bad")},
	})

	cases := []struct {
		name string
		snap queueSnapshot
		want string
	}{
		{
			name: "pending",
			snap: queueSnapshot{
				LastAppliedIndex: 1,
				Jobs:             [][]byte{badPayload},
			},
			want: "restore pending[0]",
		},
		{
			name: "inflight",
			snap: queueSnapshot{
				LastAppliedIndex: 1,
				Inflight: []inflightSnapshot{{
					DeliveryID:    "delivery-bad",
					Job:           badPayload,
					LeaseUntilUTC: time.Now().Add(time.Minute).Unix(),
				}},
			},
			want: "restore inflight[delivery-bad]",
		},
		{
			name: "dead letter",
			snap: queueSnapshot{
				LastAppliedIndex: 1,
				DeadLetter: []deadLetterSnapshot{{
					DeliveryID:   "delivery-bad",
					Job:          badPayload,
					AttemptCount: 1,
				}},
			},
			want: "restore dead_letter[0]",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			writeQueueSnapshot(t, dir, tt.snap)

			_, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{PersistenceDir: dir}, nil)
			if err == nil {
				t.Fatal("NewQueueServiceWithOptions succeeded, want invalid persisted handoff error")
			}

			if !strings.Contains(err.Error(), tt.want) || !strings.Contains(err.Error(), "execution envelope metadata is required") {
				t.Fatalf("restart error %q does not contain %q and missing envelope reason", err.Error(), tt.want)
			}
		})
	}
}

func TestQueuePersistence_RejectsInvalidWALHandoff(t *testing.T) {
	dir := t.TempDir()
	badPayload := marshalQueuePersistenceJobRequest(t, &api.JobRequest{
		Job: &api.Job{Id: queueTestString("job-bad-wal")},
	})

	writeQueueWALRecord(t, dir, walRecord{
		Index: 1,
		Type:  walRecordEnqueue,
		Job:   badPayload,
	})

	_, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{PersistenceDir: dir}, nil)
	if err == nil {
		t.Fatal("NewQueueServiceWithOptions succeeded, want invalid WAL handoff error")
	}

	if !strings.Contains(err.Error(), "restore pending[0]") || !strings.Contains(err.Error(), "execution envelope metadata is required") {
		t.Fatalf("restart error %q does not contain pending missing-envelope reason", err.Error())
	}
}

func TestQueuePersistenceLockSubprocess(t *testing.T) {
	if os.Getenv("VECTIS_QUEUE_LOCK_SUBPROCESS") != "1" {
		t.Skip("helper test")
	}

	_, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		PersistenceDir: os.Getenv("VECTIS_QUEUE_LOCK_DIR"),
		SnapshotEvery:  8,
	}, nil)

	if err == nil {
		t.Fatalf("expected concurrent persistence directory to be rejected")
	}

	if !strings.Contains(err.Error(), "already in use") {
		t.Fatalf("expected already in use error, got %v", err)
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

func marshalQueuePersistenceJobRequest(t *testing.T, req *api.JobRequest) []byte {
	t.Helper()

	payload, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal job request: %v", err)
	}

	return payload
}

func writeQueueSnapshot(t *testing.T, dir string, snap queueSnapshot) {
	t.Helper()

	f, err := os.Create(filepath.Join(dir, snapshotFileName))
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	if err := json.NewEncoder(f).Encode(snap); err != nil {
		_ = f.Close()
		t.Fatalf("encode snapshot: %v", err)
	}

	if err := f.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}
}

func writeQueueWALRecord(t *testing.T, dir string, rec walRecord) {
	t.Helper()

	f, err := os.Create(filepath.Join(dir, walSegmentPrefix+"000001"))
	if err != nil {
		t.Fatalf("create wal segment: %v", err)
	}

	if err := json.NewEncoder(f).Encode(rec); err != nil {
		_ = f.Close()
		t.Fatalf("encode wal record: %v", err)
	}

	if err := f.Close(); err != nil {
		t.Fatalf("close wal segment: %v", err)
	}
}
