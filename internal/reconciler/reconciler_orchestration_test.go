package reconciler

import (
	"context"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
)

func TestService_Process_ReenqueuesQueuedRun_Orchestration(t *testing.T) {
	ctx := context.Background()
	jobsRepo := mocks.NewMockJobsRepository()
	jobsRepo.Definitions["job-a"] = `{"id":"job-a","root":{"uses":"builtins/shell","with":{"command":"echo x"}}}`

	runsRepo := mocks.NewMockRunsRepository()
	runsRepo.QueuedRuns = []dal.QueuedRun{{RunID: "run-1", JobID: "job-a"}}

	q := mocks.NewMockQueueService()
	svc := NewServiceWithRepositories(interfaces.NewLogger("test"), jobsRepo, runsRepo, q, interfaces.SystemClock{})
	svc.SetMinDispatchGap(1 * time.Millisecond)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process: %v", err)
	}

	enqueued := q.GetJobs()
	if len(enqueued) != 1 {
		t.Fatalf("expected 1 enqueued job, got %d", len(enqueued))
	}

	if enqueued[0].GetId() != "job-a" || enqueued[0].GetRunId() != "run-1" {
		t.Fatalf("unexpected enqueue payload: id=%q run=%q", enqueued[0].GetId(), enqueued[0].GetRunId())
	}

	if len(runsRepo.TouchedRunIDs) != 1 || runsRepo.TouchedRunIDs[0] != "run-1" {
		t.Fatalf("expected run-1 touch, got %+v", runsRepo.TouchedRunIDs)
	}
}

func TestService_Process_SkipsEphemeralWithoutStoredJob_Orchestration(t *testing.T) {
	ctx := context.Background()
	jobsRepo := mocks.NewMockJobsRepository()
	runsRepo := mocks.NewMockRunsRepository()
	runsRepo.QueuedRuns = []dal.QueuedRun{{RunID: "run-ephemeral", JobID: "missing"}}

	q := mocks.NewMockQueueService()
	svc := NewServiceWithRepositories(interfaces.NewLogger("test"), jobsRepo, runsRepo, q, interfaces.SystemClock{})
	svc.SetMinDispatchGap(1 * time.Millisecond)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process: %v", err)
	}

	if got := len(q.GetJobs()); got != 0 {
		t.Fatalf("expected no jobs enqueued, got %d", got)
	}

	if got := len(runsRepo.TouchedRunIDs); got != 0 {
		t.Fatalf("expected no touched runs, got %+v", runsRepo.TouchedRunIDs)
	}
}
