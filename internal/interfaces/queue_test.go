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

func wrapJob(job *api.Job) *api.JobRequest { return &api.JobRequest{Job: job} }

func TestMockQueueClient_Enqueue(t *testing.T) {
	client := mocks.NewMockQueueClient()

	jobID := "test-job-1"
	job := &api.Job{
		Id: &jobID,
	}

	err := client.Enqueue(context.Background(), wrapJob(job))
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

	jobID := "test-job"
	job := &api.Job{
		Id: &jobID,
	}

	err := client.Enqueue(context.Background(), wrapJob(job))
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestMockQueueClient_Dequeue(t *testing.T) {
	client := mocks.NewMockQueueClient()

	jobID1 := "job-1"
	jobID2 := "job-2"
	job1 := &api.Job{Id: &jobID1}
	job2 := &api.Job{Id: &jobID2}
	client.AddJob(job1)
	client.AddJob(job2)

	dequeued1, err := client.Dequeue(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if dequeued1.GetJob().GetId() != "job-1" {
		t.Errorf("expected job-1, got %s", dequeued1.GetJob().GetId())
	}

	dequeued2, err := client.Dequeue(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if dequeued2.GetJob().GetId() != "job-2" {
		t.Errorf("expected job-2, got %s", dequeued2.GetJob().GetId())
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

	for i := range 10 {
		jobID := fmt.Sprintf("job-%d", i)
		client.AddJob(&api.Job{Id: &jobID})
	}

	done := make(chan bool, 20)

	for i := range 10 {
		go func(id int) {
			jobID := fmt.Sprintf("concurrent-%d", id)
			job := &api.Job{Id: &jobID}
			client.Enqueue(context.Background(), wrapJob(job))
			done <- true
		}(i)
	}

	for range 10 {
		go func() {
			client.Dequeue(context.Background())
			done <- true
		}()
	}

	for range 20 {
		<-done
	}

	_ = client.GetJobs()
}
