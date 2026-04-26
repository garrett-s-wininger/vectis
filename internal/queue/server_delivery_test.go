package queue

import (
	"context"
	"testing"
	"time"

	api "vectis/api/gen/go"
)

func TestQueueDelivery_AckPreventsRedelivery(t *testing.T) {
	ctx := context.Background()
	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{DeliveryTTL: 20 * time.Millisecond}, nil)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	jobID := "job-ack"
	if _, err := svc.Enqueue(ctx, &api.Job{Id: &jobID}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	job, err := svc.Dequeue(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if job.GetDeliveryId() == "" {
		t.Fatalf("expected delivery id")
	}

	deliveryID := job.GetDeliveryId()
	if _, err := svc.Ack(ctx, &api.AckRequest{DeliveryId: &deliveryID}); err != nil {
		t.Fatalf("ack: %v", err)
	}

	time.Sleep(30 * time.Millisecond)
	got, err := svc.TryDequeue(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("trydequeue: %v", err)
	}
	if got != nil {
		t.Fatalf("expected no redelivery after ack, got %s", got.GetId())
	}
}

func TestQueueDelivery_UnackedLeaseExpiryRequeues(t *testing.T) {
	ctx := context.Background()
	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{DeliveryTTL: 20 * time.Millisecond}, nil)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	jobID := "job-requeue"
	if _, err := svc.Enqueue(ctx, &api.Job{Id: &jobID}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	first, err := svc.Dequeue(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("first dequeue: %v", err)
	}
	if first.GetDeliveryId() == "" {
		t.Fatalf("expected first delivery id")
	}
	firstDeliveryID := first.GetDeliveryId()

	time.Sleep(30 * time.Millisecond)
	second, err := svc.TryDequeue(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("second dequeue: %v", err)
	}
	if second == nil {
		t.Fatalf("expected redelivery after lease expiry")
	}
	if second.GetId() != jobID {
		t.Fatalf("expected redelivered job %s, got %s", jobID, second.GetId())
	}
	if second.GetDeliveryId() == "" {
		t.Fatalf("expected second delivery id")
	}
	if second.GetDeliveryId() == firstDeliveryID {
		t.Fatalf("expected new delivery id on redelivery")
	}
}

func TestQueueDelivery_DLQAfterMaxAttempts(t *testing.T) {
	ctx := context.Background()
	// maxRequeueAttempts=2 means the job will be requeued twice (attempts 1 and 2),
	// then moved to DLQ on the 3rd expiry (attempt 3).
	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{DeliveryTTL: 20 * time.Millisecond, MaxRequeueAttempts: 2}, nil)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	jobID := "job-dlq"
	if _, err := svc.Enqueue(ctx, &api.Job{Id: &jobID}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Dequeue and let expire 3 times.
	for i := 0; i < 3; i++ {
		job, err := svc.Dequeue(ctx, &api.Empty{})
		if err != nil {
			t.Fatalf("dequeue %d: %v", i+1, err)
		}

		if job == nil {
			t.Fatalf("expected job on dequeue %d", i+1)
		}

		time.Sleep(30 * time.Millisecond)
	}

	// One more TryDequeue to trigger requeueExpiredLocked, which should move to DLQ.
	_, err = svc.TryDequeue(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("final trydequeue: %v", err)
	}

	// Job should now be in DLQ.
	dlq, err := svc.ListDeadLetter(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("list dead letter: %v", err)
	}

	if len(dlq.Items) != 1 {
		t.Fatalf("expected 1 dead letter item, got %d", len(dlq.Items))
	}

	if dlq.Items[0].GetJob().GetId() != jobID {
		t.Fatalf("expected dead letter job %s, got %s", jobID, dlq.Items[0].GetJob().GetId())
	}

	if dlq.Items[0].GetAttemptCount() != 3 {
		t.Fatalf("expected attempt count 3, got %d", dlq.Items[0].GetAttemptCount())
	}

	// Queue should be empty.
	got, err := svc.TryDequeue(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("trydequeue after dlq: %v", err)
	}

	if got != nil {
		t.Fatalf("expected empty queue after dlq, got %s", got.GetId())
	}
}

func TestQueueDelivery_DLQSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	jobID := "job-dlq-restart"
	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		DeliveryTTL:        20 * time.Millisecond,
		MaxRequeueAttempts: 1,
		PersistenceDir:     dir,
		SnapshotEvery:      8,
	}, nil)

	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	if _, err := svc.Enqueue(ctx, &api.Job{Id: &jobID}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Dequeue and let expire twice.
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

	// Final TryDequeue triggers DLQ move.
	_, err = svc.TryDequeue(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("final trydequeue: %v", err)
	}

	// Verify DLQ before restart.
	dlq, err := svc.ListDeadLetter(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("list dead letter before restart: %v", err)
	}

	if len(dlq.Items) != 1 {
		t.Fatalf("expected 1 dead letter item before restart, got %d", len(dlq.Items))
	}

	// Restart.
	svc2, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		DeliveryTTL:        20 * time.Millisecond,
		MaxRequeueAttempts: 1,
		PersistenceDir:     dir,
		SnapshotEvery:      8,
	}, nil)

	if err != nil {
		t.Fatalf("restart queue: %v", err)
	}

	dlq2, err := svc2.ListDeadLetter(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("list dead letter after restart: %v", err)
	}

	if len(dlq2.Items) != 1 {
		t.Fatalf("expected 1 dead letter item after restart, got %d", len(dlq2.Items))
	}

	if dlq2.Items[0].GetJob().GetId() != jobID {
		t.Fatalf("expected dead letter job %s after restart, got %s", jobID, dlq2.Items[0].GetJob().GetId())
	}

	if dlq2.Items[0].GetAttemptCount() != 2 {
		t.Fatalf("expected attempt count 2 after restart, got %d", dlq2.Items[0].GetAttemptCount())
	}
}
