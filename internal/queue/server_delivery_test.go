package queue

import (
	"context"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/cell"
	"vectis/internal/dispatchmeta"
	"vectis/internal/queueid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// deliveryWait sleeps long enough for a lease with the given TTL to expire.
// It uses a 3x multiplier to avoid flakes on slow CI runners.
func deliveryWait(ttl time.Duration) {
	time.Sleep(ttl * 3)
}

func queueTestString(value string) *string {
	return &value
}

func queueTestNode(id, uses string) *api.Node {
	return &api.Node{
		Id:   queueTestString(id),
		Uses: queueTestString(uses),
	}
}

func TestQueueDelivery_AckPreventsRedelivery(t *testing.T) {
	ctx := context.Background()
	ttl := 30 * time.Millisecond
	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{DeliveryTTL: ttl}, nil)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	jobID := "job-ack"
	if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: &api.Job{Id: &jobID}}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	job, err := svc.Dequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	if job.GetJob().GetDeliveryId() == "" {
		t.Fatalf("expected delivery id")
	}

	deliveryID := job.GetJob().GetDeliveryId()
	if _, err := svc.Ack(ctx, &api.AckRequest{DeliveryId: &deliveryID}); err != nil {
		t.Fatalf("ack: %v", err)
	}

	deliveryWait(ttl)
	got, err := svc.TryDequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("trydequeue: %v", err)
	}

	if got != nil {
		t.Fatalf("expected no redelivery after ack, got %s", got.GetJob().GetId())
	}
}

func TestQueueDelivery_IncludesInstanceID(t *testing.T) {
	ctx := context.Background()
	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{InstanceID: "queue-a"}, nil)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	jobID := "job-instance-id"
	if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: &api.Job{Id: &jobID}}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, err := svc.Dequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	instanceID, _, ok := queueid.Decode(got.GetJob().GetDeliveryId())
	if !ok {
		t.Fatalf("expected delivery id to include queue instance: %q", got.GetJob().GetDeliveryId())
	}

	if instanceID != "queue-a" {
		t.Fatalf("expected instance queue-a, got %q", instanceID)
	}
}

func TestQueueDelivery_TryDequeueSkipsUnsupportedIsolation(t *testing.T) {
	ctx := context.Background()
	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{}, nil)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	vmJobID := "job-vm-first"
	hostJobID := "job-host-second"
	vmDefault := action.IsolationVM
	if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: &api.Job{
		Id:               &vmJobID,
		DefaultIsolation: &vmDefault,
		Root:             queueTestNode("root-vm", "builtins/shell"),
	}}); err != nil {
		t.Fatalf("enqueue vm job: %v", err)
	}

	if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: &api.Job{
		Id:   &hostJobID,
		Root: queueTestNode("root-host", "builtins/shell"),
	}}); err != nil {
		t.Fatalf("enqueue host job: %v", err)
	}

	hostOnly, err := svc.TryDequeue(ctx, &api.DequeueRequest{SupportedIsolation: []string{action.IsolationHost}})
	if err != nil {
		t.Fatalf("host trydequeue filtered: %v", err)
	}

	if hostOnly == nil || hostOnly.GetJob().GetId() != hostJobID {
		t.Fatalf("expected host worker to skip VM job and receive %s, got %#v", hostJobID, hostOnly)
	}

	vmCapable, err := svc.TryDequeue(ctx, &api.DequeueRequest{SupportedIsolation: []string{action.IsolationHost, action.IsolationVM}})
	if err != nil {
		t.Fatalf("vm trydequeue filtered: %v", err)
	}

	if vmCapable == nil || vmCapable.GetJob().GetId() != vmJobID {
		t.Fatalf("expected VM-capable worker to receive skipped job %s, got %#v", vmJobID, vmCapable)
	}
}

func TestQueueDelivery_TryDequeueRejectsInvalidSupportedIsolation(t *testing.T) {
	ctx := context.Background()
	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{}, nil)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	jobID := "job-vm"
	vmDefault := action.IsolationVM
	if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: &api.Job{
		Id:               &jobID,
		DefaultIsolation: &vmDefault,
		Root:             queueTestNode("root-vm", "builtins/shell"),
	}}); err != nil {
		t.Fatalf("enqueue vm job: %v", err)
	}

	got, err := svc.TryDequeue(ctx, &api.DequeueRequest{SupportedIsolation: []string{"container"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("TryDequeue error = %v, want InvalidArgument", err)
	}

	if got != nil {
		t.Fatalf("invalid dequeue request returned job: %#v", got)
	}

	vmCapable, err := svc.TryDequeue(ctx, &api.DequeueRequest{SupportedIsolation: []string{action.IsolationVM}})
	if err != nil {
		t.Fatalf("valid trydequeue after invalid request: %v", err)
	}

	if vmCapable == nil || vmCapable.GetJob().GetId() != jobID {
		t.Fatalf("expected job to remain pending after invalid request, got %#v", vmCapable)
	}
}

func TestQueueDelivery_TryDequeueChecksInheritedNodeIsolation(t *testing.T) {
	ctx := context.Background()
	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{}, nil)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	jobID := "job-child-vm"
	vmIsolation := action.IsolationVM
	root := queueTestNode("root", "builtins/sequence")
	root.Steps = []*api.Node{{
		Id:        queueTestString("child"),
		Uses:      queueTestString("builtins/shell"),
		Isolation: &vmIsolation,
	}}

	if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: &api.Job{Id: &jobID, Root: root}}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	hostOnly, err := svc.TryDequeue(ctx, &api.DequeueRequest{SupportedIsolation: []string{action.IsolationHost}})
	if err != nil {
		t.Fatalf("host trydequeue filtered: %v", err)
	}

	if hostOnly != nil {
		t.Fatalf("expected host-only worker not to receive VM child job, got %#v", hostOnly)
	}

	vmCapable, err := svc.TryDequeue(ctx, &api.DequeueRequest{SupportedIsolation: []string{action.IsolationHost, action.IsolationVM}})
	if err != nil {
		t.Fatalf("vm trydequeue filtered: %v", err)
	}

	if vmCapable == nil || vmCapable.GetJob().GetId() != jobID {
		t.Fatalf("expected VM-capable worker to receive %s, got %#v", jobID, vmCapable)
	}
}

func TestQueueDelivery_TryDequeueUsesExecutionTaskKey(t *testing.T) {
	ctx := context.Background()
	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{}, nil)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	jobID := "job-task-host-override"
	vmDefault := action.IsolationVM
	hostIsolation := action.IsolationHost
	root := queueTestNode("root", "builtins/sequence")
	root.Steps = []*api.Node{{
		Id:        queueTestString("host-child"),
		Uses:      queueTestString("builtins/shell"),
		Isolation: &hostIsolation,
	}}

	req := &api.JobRequest{
		Job: &api.Job{
			Id:               &jobID,
			DefaultIsolation: &vmDefault,
			Root:             root,
		},
		Metadata: map[string]string{
			cell.ExecutionTaskKeyMetadataKey: "host-child",
		},
	}

	if _, err := svc.Enqueue(ctx, req); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	hostOnly, err := svc.TryDequeue(ctx, &api.DequeueRequest{SupportedIsolation: []string{action.IsolationHost}})
	if err != nil {
		t.Fatalf("host trydequeue filtered: %v", err)
	}

	if hostOnly == nil || hostOnly.GetJob().GetId() != jobID {
		t.Fatalf("expected host-only worker to receive host task override %s, got %#v", jobID, hostOnly)
	}
}

func TestQueueDelivery_UnackedLeaseExpiryRequeues(t *testing.T) {
	ctx := context.Background()
	ttl := 30 * time.Millisecond
	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{DeliveryTTL: ttl}, nil)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	jobID := "job-requeue"
	if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: &api.Job{Id: &jobID}}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	first, err := svc.Dequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("first dequeue: %v", err)
	}

	if first.GetJob().GetDeliveryId() == "" {
		t.Fatalf("expected first delivery id")
	}

	firstDeliveryID := first.GetJob().GetDeliveryId()

	deliveryWait(ttl)
	second, err := svc.TryDequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("second dequeue: %v", err)
	}

	if second == nil {
		t.Fatalf("expected redelivery after lease expiry")
	}

	if second.GetJob().GetId() != jobID {
		t.Fatalf("expected redelivered job %s, got %s", jobID, second.GetJob().GetId())
	}

	if second.GetJob().GetDeliveryId() == "" {
		t.Fatalf("expected second delivery id")
	}

	if second.GetJob().GetDeliveryId() == firstDeliveryID {
		t.Fatalf("expected new delivery id on redelivery")
	}
}

func TestQueueDelivery_ExpiredPendingDeadlineDropsBeforeDelivery(t *testing.T) {
	ctx := context.Background()
	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{}, nil)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	expiredJobID := "job-expired-pending"
	expiredReq := &api.JobRequest{Job: &api.Job{Id: &expiredJobID}}
	dispatchmeta.StampStartDeadline(expiredReq, time.Now().Add(-time.Second).UnixNano())
	if _, err := svc.Enqueue(ctx, expiredReq); err != nil {
		t.Fatalf("enqueue expired job: %v", err)
	}

	nextJobID := "job-after-expired"
	if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: &api.Job{Id: &nextJobID}}); err != nil {
		t.Fatalf("enqueue next job: %v", err)
	}

	got, err := svc.Dequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	if got == nil || got.GetJob().GetId() != nextJobID {
		t.Fatalf("expected expired job to drop and next job to deliver, got %#v", got)
	}

	dlq, err := svc.ListDeadLetter(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("list dead letter: %v", err)
	}

	if len(dlq.Items) != 0 {
		t.Fatalf("expired dispatch deadline should not enter DLQ, got %+v", dlq.Items)
	}
}

func TestQueueDelivery_ExpiredFilteredPendingDeadlineDropsBeforeDelivery(t *testing.T) {
	ctx := context.Background()
	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{}, nil)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	vmJobID := "job-vm-head"
	vmDefault := action.IsolationVM
	if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: &api.Job{
		Id:               &vmJobID,
		DefaultIsolation: &vmDefault,
		Root:             queueTestNode("root-vm", "builtins/shell"),
	}}); err != nil {
		t.Fatalf("enqueue vm job: %v", err)
	}

	expiredJobID := "job-expired-filtered"
	expiredReq := &api.JobRequest{Job: &api.Job{
		Id:   &expiredJobID,
		Root: queueTestNode("root-expired", "builtins/shell"),
	}}

	dispatchmeta.StampStartDeadline(expiredReq, time.Now().Add(-time.Second).UnixNano())
	if _, err := svc.Enqueue(ctx, expiredReq); err != nil {
		t.Fatalf("enqueue expired filtered job: %v", err)
	}

	validHostJobID := "job-valid-host"
	if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: &api.Job{
		Id:   &validHostJobID,
		Root: queueTestNode("root-host", "builtins/shell"),
	}}); err != nil {
		t.Fatalf("enqueue valid host job: %v", err)
	}

	hostOnly, err := svc.TryDequeue(ctx, &api.DequeueRequest{SupportedIsolation: []string{action.IsolationHost}})
	if err != nil {
		t.Fatalf("host trydequeue filtered: %v", err)
	}

	if hostOnly == nil || hostOnly.GetJob().GetId() != validHostJobID {
		t.Fatalf("expected expired filtered job to drop and valid host job to deliver, got %#v", hostOnly)
	}

	vmCapable, err := svc.TryDequeue(ctx, &api.DequeueRequest{SupportedIsolation: []string{action.IsolationHost, action.IsolationVM}})
	if err != nil {
		t.Fatalf("vm trydequeue filtered: %v", err)
	}

	if vmCapable == nil || vmCapable.GetJob().GetId() != vmJobID {
		t.Fatalf("expected vm job to remain queued for vm-capable worker, got %#v", vmCapable)
	}

	dlq, err := svc.ListDeadLetter(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("list dead letter: %v", err)
	}

	if len(dlq.Items) != 0 {
		t.Fatalf("expired filtered dispatch deadline should not enter DLQ, got %+v", dlq.Items)
	}
}

func TestQueueDelivery_ExpiredInflightDeadlineDropsInsteadOfRequeue(t *testing.T) {
	ctx := context.Background()
	ttl := 20 * time.Millisecond
	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{DeliveryTTL: ttl}, nil)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	jobID := "job-expired-inflight"
	req := &api.JobRequest{Job: &api.Job{Id: &jobID}}
	dispatchmeta.StampStartDeadline(req, time.Now().Add(10*time.Millisecond).UnixNano())
	if _, err := svc.Enqueue(ctx, req); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	first, err := svc.TryDequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("first trydequeue: %v", err)
	}

	if first == nil || first.GetJob().GetId() != jobID {
		t.Fatalf("expected first delivery of %s, got %#v", jobID, first)
	}

	deliveryWait(ttl)
	second, err := svc.TryDequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("second trydequeue: %v", err)
	}

	if second != nil {
		t.Fatalf("expected expired in-flight delivery to drop instead of requeue, got %#v", second)
	}

	dlq, err := svc.ListDeadLetter(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("list dead letter: %v", err)
	}

	if len(dlq.Items) != 0 {
		t.Fatalf("expired in-flight dispatch should not enter DLQ, got %+v", dlq.Items)
	}
}

func TestQueueDelivery_DLQAfterMaxAttempts(t *testing.T) {
	ctx := context.Background()
	ttl := 30 * time.Millisecond

	// maxRequeueAttempts=2 means the job will be requeued twice (attempts 1 and 2),
	// then moved to DLQ on the 3rd expiry (attempt 3).
	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{DeliveryTTL: ttl, MaxRequeueAttempts: 2}, nil)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	jobID := "job-dlq"
	if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: &api.Job{Id: &jobID}}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Dequeue and let expire 3 times.
	for i := range 3 {
		job, err := svc.Dequeue(ctx, &api.DequeueRequest{})
		if err != nil {
			t.Fatalf("dequeue %d: %v", i+1, err)
		}

		if job == nil {
			t.Fatalf("expected job on dequeue %d", i+1)
		}

		deliveryWait(ttl)
	}

	// One more TryDequeue to trigger requeueExpiredLocked, which should move to DLQ.
	_, err = svc.TryDequeue(ctx, &api.DequeueRequest{})
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

	if dlq.Items[0].GetJobRequest().GetJob().GetId() != jobID {
		t.Fatalf("expected dead letter job %s, got %s", jobID, dlq.Items[0].GetJobRequest().GetJob().GetId())
	}

	if dlq.Items[0].GetAttemptCount() != 3 {
		t.Fatalf("expected attempt count 3, got %d", dlq.Items[0].GetAttemptCount())
	}

	// Queue should be empty.
	got, err := svc.TryDequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("trydequeue after dlq: %v", err)
	}

	if got != nil {
		t.Fatalf("expected empty queue after dlq, got %s", got.GetJob().GetId())
	}
}

func TestQueueDelivery_DLQSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	ttl := 30 * time.Millisecond

	jobID := "job-dlq-restart"
	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		DeliveryTTL:        ttl,
		MaxRequeueAttempts: 1,
		PersistenceDir:     dir,
		SnapshotEvery:      8,
	}, nil)

	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: &api.Job{Id: &jobID}}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Dequeue and let expire twice.
	for i := range 2 {
		job, err := svc.Dequeue(ctx, &api.DequeueRequest{})
		if err != nil {
			t.Fatalf("dequeue %d: %v", i+1, err)
		}

		if job == nil {
			t.Fatalf("expected job on dequeue %d", i+1)
		}

		deliveryWait(ttl)
	}

	// Final TryDequeue triggers DLQ move.
	_, err = svc.TryDequeue(ctx, &api.DequeueRequest{})
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
	closeQueueService(t, svc)
	svc2, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		DeliveryTTL:        ttl,
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

	if dlq2.Items[0].GetJobRequest().GetJob().GetId() != jobID {
		t.Fatalf("expected dead letter job %s after restart, got %s", jobID, dlq2.Items[0].GetJobRequest().GetJob().GetId())
	}

	if dlq2.Items[0].GetAttemptCount() != 2 {
		t.Fatalf("expected attempt count 2 after restart, got %d", dlq2.Items[0].GetAttemptCount())
	}
}
