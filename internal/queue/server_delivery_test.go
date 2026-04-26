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
