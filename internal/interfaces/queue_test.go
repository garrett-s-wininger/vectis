package interfaces_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"
)

func TestMockQueueClient_Enqueue(t *testing.T) {
	client := mocks.NewMockQueueClient()

	job := &api.Job{
		Id: stringPtr("test-job-1"),
	}

	err := client.Enqueue(context.Background(), job)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	jobs := client.GetJobs()
	if len(jobs) != 1 {
		t.Errorf("expected 1 job, got %d", len(jobs))
	}

	if jobs[0].GetId() != "test-job-1" {
		t.Errorf("expected job id 'test-job-1', got '%s'", jobs[0].GetId())
	}
}

func TestMockQueueClient_EnqueueError(t *testing.T) {
	client := mocks.NewMockQueueClient()
	expectedErr := errors.New("enqueue failed")
	client.SetEnqueueError(expectedErr)

	job := &api.Job{
		Id: stringPtr("test-job"),
	}

	err := client.Enqueue(context.Background(), job)
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestMockQueueClient_Dequeue(t *testing.T) {
	client := mocks.NewMockQueueClient()

	job1 := &api.Job{Id: stringPtr("job-1")}
	job2 := &api.Job{Id: stringPtr("job-2")}
	client.AddJob(job1)
	client.AddJob(job2)

	dequeued1, err := client.Dequeue(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if dequeued1.GetId() != "job-1" {
		t.Errorf("expected job-1, got %s", dequeued1.GetId())
	}

	dequeued2, err := client.Dequeue(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if dequeued2.GetId() != "job-2" {
		t.Errorf("expected job-2, got %s", dequeued2.GetId())
	}

	jobs := client.GetJobs()
	if len(jobs) != 0 {
		t.Errorf("expected empty queue, got %d jobs", len(jobs))
	}
}

func TestMockQueueClient_DequeueEmpty(t *testing.T) {
	client := mocks.NewMockQueueClient()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := client.Dequeue(ctx)
	if err == nil {
		t.Error("expected error for empty queue")
	}
}

func TestMockQueueClient_DequeueError(t *testing.T) {
	client := mocks.NewMockQueueClient()
	expectedErr := errors.New("dequeue failed")
	client.SetDequeueError(expectedErr)

	_, err := client.Dequeue(context.Background())
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestMockQueueClient_Close(t *testing.T) {
	client := mocks.NewMockQueueClient()

	if client.IsClosed() {
		t.Error("expected client to be open initially")
	}

	err := client.Close()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if !client.IsClosed() {
		t.Error("expected client to be closed")
	}
}

func TestMockQueueClient_ConcurrentAccess(t *testing.T) {
	client := mocks.NewMockQueueClient()

	for i := 0; i < 10; i++ {
		client.AddJob(&api.Job{Id: stringPtr(fmt.Sprintf("job-%d", i))})
	}

	done := make(chan bool, 20)

	for i := 0; i < 10; i++ {
		go func(id int) {
			job := &api.Job{Id: stringPtr(fmt.Sprintf("concurrent-%d", id))}
			client.Enqueue(context.Background(), job)
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		go func() {
			client.Dequeue(context.Background())
			done <- true
		}()
	}

	for i := 0; i < 20; i++ {
		<-done
	}

	_ = client.GetJobs()
}

func stringPtr(s string) *string {
	return &s
}
