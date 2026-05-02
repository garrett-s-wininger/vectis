//go:build integration

package queue_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/queue"
	"vectis/internal/testutil/grpctest"

	"google.golang.org/grpc"
)

func setupQueueClient(t *testing.T) api.QueueServiceClient {
	t.Helper()

	logger := mocks.NewMockLogger()
	queueService := queue.NewQueueService(logger)

	_, _, conn := grpctest.SetupGRPCServer(t, func(s *grpc.Server) {
		api.RegisterQueueServiceServer(s, queueService)
	})

	return api.NewQueueServiceClient(conn)
}

func TestIntegrationQueue_EnqueueDequeueRoundTrip(t *testing.T) {
	client := setupQueueClient(t)

	ctx := context.Background()

	jobID := "test-job-1"
	uses := "builtins/shell"
	job := &api.Job{
		Id: &jobID,
		Root: &api.Node{
			Id:   &jobID,
			Uses: &uses,
			With: map[string]string{"command": "echo hello"},
		},
	}

	_, err := client.Enqueue(ctx, &api.JobRequest{Job: job})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	got, err := client.Dequeue(ctx, &api.Empty{})
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}

	if got.GetJob().GetId() != jobID {
		t.Errorf("expected job ID %q, got %q", jobID, got.GetJob().GetId())
	}

	if got.GetJob().GetRoot().GetUses() != "builtins/shell" {
		t.Errorf("expected action builtins/shell, got %q", got.GetJob().GetRoot().GetUses())
	}
}

func TestIntegrationQueue_DequeueBlocksUntilJobAvailable(t *testing.T) {
	client := setupQueueClient(t)
	ctx := context.Background()

	dequeueDone := make(chan *api.JobRequest, 1)
	go func() {
		job, err := client.Dequeue(ctx, &api.Empty{})
		if err != nil {
			t.Errorf("dequeue failed: %v", err)
			return
		}
		dequeueDone <- job
	}()

	jobID := "blocking-test"
	job := &api.Job{Id: &jobID}
	_, err := client.Enqueue(ctx, &api.JobRequest{Job: job})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	select {
	case got := <-dequeueDone:
		if got.GetJob().GetId() != jobID {
			t.Errorf("expected job ID %q, got %q", jobID, got.GetJob().GetId())
		}
	case <-time.After(2 * time.Second):
		t.Error("dequeue did not return within timeout after enqueue")
	}
}

func TestIntegrationQueue_DequeueContextCancellation(t *testing.T) {
	client := setupQueueClient(t)
	ctx, cancel := context.WithCancel(context.Background())

	errChan := make(chan error, 1)
	go func() {
		_, err := client.Dequeue(ctx, &api.Empty{})
		errChan <- err
	}()

	cancel()

	select {
	case err := <-errChan:
		if err == nil {
			t.Error("expected error after context cancellation, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Error("dequeue did not return within timeout after context cancellation")
	}
}

func TestIntegrationQueue_ConcurrentEnqueueDequeue(t *testing.T) {
	client := setupQueueClient(t)

	ctx := context.Background()
	numJobs := 100
	numWorkers := 10

	var enqueueWg sync.WaitGroup
	for i := range numJobs {
		enqueueWg.Add(1)

		go func(id int) {
			defer enqueueWg.Done()
			jobID := fmt.Sprintf("job-%d", id)
			job := &api.Job{Id: &jobID}
			_, err := client.Enqueue(ctx, &api.JobRequest{Job: job})
			if err != nil {
				t.Errorf("enqueue %d failed: %v", id, err)
			}
		}(i)
	}

	enqueueWg.Wait()
	receivedJobs := make(map[string]bool)
	var mu sync.Mutex
	var dequeueWg sync.WaitGroup

	for range numWorkers {
		dequeueWg.Go(func() {
			for {
				ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				job, err := client.Dequeue(ctx, &api.Empty{})
				cancel()

				if err != nil {
					return
				}

				mu.Lock()
				receivedJobs[job.GetJob().GetId()] = true
				mu.Unlock()
			}
		})
	}

	dequeueWg.Wait()

	if len(receivedJobs) != numJobs {
		t.Errorf("expected %d unique jobs, got %d", numJobs, len(receivedJobs))
	}
}

func TestIntegrationQueue_MultipleDequeueWaiters(t *testing.T) {
	client := setupQueueClient(t)

	ctx := context.Background()
	numWaiters := 5

	type result struct {
		id      int
		success bool
	}

	results := make(chan result, numWaiters)

	for i := range numWaiters {
		go func(id int) {
			timeoutCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			_, err := client.Dequeue(timeoutCtx, &api.Empty{})
			results <- result{id: id, success: err == nil}
		}(i)
	}

	jobID := "single-job"
	job := &api.Job{Id: &jobID}
	_, err := client.Enqueue(ctx, &api.JobRequest{Job: job})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	successCount := 0
	timeout := time.AfterFunc(3*time.Second, func() {
		close(results)
	})

	for res := range results {
		if res.success {
			successCount++
		}
		if len(results) == 0 {
			timeout.Stop()
			break
		}
	}

	if successCount != 1 {
		t.Errorf("expected exactly 1 successful dequeue, got %d", successCount)
	}
}

func TestIntegrationQueue_DequeueEmptyQueue(t *testing.T) {
	client := setupQueueClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := client.Dequeue(ctx, &api.Empty{})
	if err == nil {
		t.Error("expected error when dequeuing from empty queue with timeout")
	}
}

func TestIntegrationQueue_EnqueueMultipleDequeueOrder(t *testing.T) {
	client := setupQueueClient(t)

	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("job-%d", i)
		job := &api.Job{Id: &id}
		_, err := client.Enqueue(ctx, &api.JobRequest{Job: job})

		if err != nil {
			t.Fatalf("enqueue %d failed: %v", i, err)
		}
	}

	for i := 1; i <= 3; i++ {
		job, err := client.Dequeue(ctx, &api.Empty{})
		if err != nil {
			t.Fatalf("dequeue %d failed: %v", i, err)
		}

		expectedID := fmt.Sprintf("job-%d", i)
		if job.GetJob().GetId() != expectedID {
			t.Errorf("expected job ID %s, got %s", expectedID, job.GetJob().GetId())
		}
	}
}
