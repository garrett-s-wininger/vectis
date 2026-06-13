package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/chaos"
)

func TestQueueChaos_EnqueuePersistenceFailureLeavesQueueEmptyAndRecovers(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	faults := chaos.NewScript()
	faults.FailNext(ChaosPointEnqueuePersist, nil)

	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  2,
		Faults:         faults,
	}, nil)

	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	jobID := "job-enqueue-chaos"
	if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: &api.Job{Id: &jobID}}); err == nil {
		t.Fatal("expected injected enqueue persistence failure")
	} else if !errors.Is(err, chaos.ErrInjected) {
		t.Fatalf("expected injected enqueue failure, got %v", err)
	}

	pending, inflight, dlq := MetricsSnapshot(svc)
	if pending != 0 || inflight != 0 || dlq != 0 {
		t.Fatalf("failed enqueue mutated queue: pending=%d inflight=%d dlq=%d", pending, inflight, dlq)
	}

	if got, err := svc.TryDequeue(ctx, &api.DequeueRequest{}); err != nil {
		t.Fatalf("trydequeue after failed enqueue: %v", err)
	} else if got != nil {
		t.Fatalf("expected empty queue after failed enqueue, got %#v", got)
	}

	if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: &api.Job{Id: &jobID}}); err != nil {
		t.Fatalf("enqueue after recovery: %v", err)
	}

	closeQueueService(t, svc)
	restarted, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  2,
	}, nil)
	if err != nil {
		t.Fatalf("restart queue: %v", err)
	}
	defer closeQueueService(t, restarted)

	got, err := restarted.TryDequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("trydequeue after restart: %v", err)
	}

	if got == nil || got.GetJob().GetId() != jobID {
		t.Fatalf("expected recovered job %s after restart, got %#v", jobID, got)
	}
}

func TestQueueChaos_DeliverPersistenceFailureLeavesJobPending(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	faults := chaos.NewScript()
	faults.FailNext(ChaosPointDeliverPersist, nil)

	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  2,
		Faults:         faults,
	}, nil)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	defer closeQueueService(t, svc)

	jobID := "job-deliver-chaos"
	if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: &api.Job{Id: &jobID}}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if _, err := svc.TryDequeue(ctx, &api.DequeueRequest{}); err == nil {
		t.Fatal("expected injected delivery persistence failure")
	} else if !errors.Is(err, chaos.ErrInjected) {
		t.Fatalf("expected injected delivery failure, got %v", err)
	}

	pending, inflight, dlq := MetricsSnapshot(svc)
	if pending != 1 || inflight != 0 || dlq != 0 {
		t.Fatalf("failed delivery mutated queue: pending=%d inflight=%d dlq=%d", pending, inflight, dlq)
	}

	got, err := svc.TryDequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("trydequeue after recovery: %v", err)
	}

	if got == nil || got.GetJob().GetId() != jobID {
		t.Fatalf("expected pending job %s after recovery, got %#v", jobID, got)
	}
}

func TestQueueChaos_AckPersistenceFailureKeepsDeliveryInflightAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	faults := chaos.NewScript()
	faults.FailNext(ChaosPointAckPersist, nil)

	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  2,
		DeliveryTTL:    time.Hour,
		Faults:         faults,
	}, nil)

	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	jobID := "job-ack-chaos"
	if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: &api.Job{Id: &jobID}}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, err := svc.TryDequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("trydequeue: %v", err)
	}

	if got == nil {
		t.Fatal("expected delivery")
	}

	deliveryID := got.GetJob().GetDeliveryId()
	if deliveryID == "" {
		t.Fatal("expected delivery id")
	}

	if _, err := svc.Ack(ctx, &api.AckRequest{DeliveryId: &deliveryID}); err == nil {
		t.Fatal("expected injected ack persistence failure")
	} else if !errors.Is(err, chaos.ErrInjected) {
		t.Fatalf("expected injected ack failure, got %v", err)
	}

	pending, inflight, dlq := MetricsSnapshot(svc)
	if pending != 0 || inflight != 1 || dlq != 0 {
		t.Fatalf("failed ack changed delivery state: pending=%d inflight=%d dlq=%d", pending, inflight, dlq)
	}

	closeQueueService(t, svc)
	restarted, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  2,
		DeliveryTTL:    time.Hour,
	}, nil)

	if err != nil {
		t.Fatalf("restart queue: %v", err)
	}

	if _, err := restarted.Ack(ctx, &api.AckRequest{DeliveryId: &deliveryID}); err != nil {
		t.Fatalf("ack after restart: %v", err)
	}

	closeQueueService(t, restarted)
	restartedAgain, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  2,
		DeliveryTTL:    time.Hour,
	}, nil)
	if err != nil {
		t.Fatalf("second restart queue: %v", err)
	}
	defer closeQueueService(t, restartedAgain)

	pending, inflight, dlq = MetricsSnapshot(restartedAgain)
	if pending != 0 || inflight != 0 || dlq != 0 {
		t.Fatalf("acked delivery survived restart: pending=%d inflight=%d dlq=%d", pending, inflight, dlq)
	}
}

func TestQueueChaos_ExpiredRequeueFailureDoesNotBurnAttempt(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	ttl := 20 * time.Millisecond
	faults := chaos.NewScript()
	faults.FailNext(ChaosPointExpiredRequeue, nil)

	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		PersistenceDir:     dir,
		SnapshotEvery:      100,
		DeliveryTTL:        ttl,
		MaxRequeueAttempts: 1,
		Faults:             faults,
	}, nil)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	defer closeQueueService(t, svc)

	jobID := "job-requeue-chaos"
	if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: &api.Job{Id: &jobID}}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if got, err := svc.TryDequeue(ctx, &api.DequeueRequest{}); err != nil {
		t.Fatalf("initial trydequeue: %v", err)
	} else if got == nil || got.GetJob().GetId() != jobID {
		t.Fatalf("expected initial delivery of %s, got %#v", jobID, got)
	}

	deliveryWait(ttl)
	if _, err := svc.TryDequeue(ctx, &api.DequeueRequest{}); err == nil {
		t.Fatal("expected injected expired-requeue persistence failure")
	} else if !errors.Is(err, chaos.ErrInjected) {
		t.Fatalf("expected injected expired-requeue failure, got %v", err)
	}

	pending, inflight, dlq := MetricsSnapshot(svc)
	if pending != 0 || inflight != 1 || dlq != 0 {
		t.Fatalf("failed requeue mutated delivery state: pending=%d inflight=%d dlq=%d", pending, inflight, dlq)
	}

	got, err := svc.TryDequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("trydequeue after recovery: %v", err)
	}

	if got == nil || got.GetJob().GetId() != jobID {
		t.Fatalf("expected requeued job %s after recovery, got %#v", jobID, got)
	}

	dlqAfter, err := svc.ListDeadLetter(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("list dead letter: %v", err)
	}

	if len(dlqAfter.Items) != 0 {
		t.Fatalf("requeue failure burned attempt and moved job to DLQ: %+v", dlqAfter.Items)
	}
}

func TestQueueChaos_DeadLetterPersistenceFailureDoesNotMoveInMemory(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	ttl := 20 * time.Millisecond
	faults := chaos.NewScript()
	faults.FailNext(ChaosPointDeadLetter, nil)

	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		PersistenceDir:     dir,
		SnapshotEvery:      100,
		DeliveryTTL:        ttl,
		MaxRequeueAttempts: 1,
		Faults:             faults,
	}, nil)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	defer closeQueueService(t, svc)

	jobID := "job-dlq-chaos"
	if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: &api.Job{Id: &jobID}}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	for attempt := 1; attempt <= 2; attempt++ {
		got, err := svc.TryDequeue(ctx, &api.DequeueRequest{})
		if err != nil {
			t.Fatalf("trydequeue attempt %d: %v", attempt, err)
		}

		if got == nil || got.GetJob().GetId() != jobID {
			t.Fatalf("expected delivery attempt %d of %s, got %#v", attempt, jobID, got)
		}

		deliveryWait(ttl)
	}

	if _, err := svc.TryDequeue(ctx, &api.DequeueRequest{}); err == nil {
		t.Fatal("expected injected dead-letter persistence failure")
	} else if !errors.Is(err, chaos.ErrInjected) {
		t.Fatalf("expected injected dead-letter failure, got %v", err)
	}

	pending, inflight, dlq := MetricsSnapshot(svc)
	if pending != 0 || inflight != 1 || dlq != 0 {
		t.Fatalf("failed DLQ move mutated delivery state: pending=%d inflight=%d dlq=%d", pending, inflight, dlq)
	}

	if _, err := svc.TryDequeue(ctx, &api.DequeueRequest{}); err != nil {
		t.Fatalf("trydequeue after DLQ persistence recovery: %v", err)
	}

	dlqAfter, err := svc.ListDeadLetter(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("list dead letter after recovery: %v", err)
	}

	if len(dlqAfter.Items) != 1 {
		t.Fatalf("expected durable DLQ move after recovery, got %d item(s)", len(dlqAfter.Items))
	}
}
