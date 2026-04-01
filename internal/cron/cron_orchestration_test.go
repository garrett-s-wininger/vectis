package cron_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"vectis/internal/cron"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
)

func TestCronService_ProcessSchedules_OrchestrationUsesRepos(t *testing.T) {
	logger := mocks.NewMockLogger()
	queue := mocks.NewMockQueueService()
	clock := mocks.NewMockClock()
	clock.SetNow(time.Date(2026, 3, 21, 12, 10, 0, 0, time.UTC))

	jobs := mocks.NewMockJobsRepository()
	jobs.Definitions["job-1"] = `{"id":"job-1","root":{"uses":"builtins/shell"}}`

	runs := mocks.NewMockRunsRepository()
	runs.CreateRunID = "run-1"
	runs.CreateRunIndex = 1

	schedules := mocks.NewMockSchedulesRepository()
	schedules.Ready = []dal.CronSchedule{{
		ID:       42,
		JobID:    "job-1",
		CronSpec: "* * * * *",
	}}

	svc := cron.NewCronServiceWithRepositories(logger, jobs, runs, schedules)
	svc.SetQueueClient(queue)
	svc.SetClock(clock)

	if err := svc.ProcessSchedules(context.Background()); err != nil {
		t.Fatalf("ProcessSchedules: %v", err)
	}

	if len(queue.GetJobs()) != 1 {
		t.Fatalf("expected one enqueued job, got %d", len(queue.GetJobs()))
	}

	touched := runs.SnapshotTouchedRunIDs()
	if len(touched) != 1 || touched[0] != "run-1" {
		t.Fatalf("expected touch for run-1, got %+v", touched)
	}

	if len(schedules.UpdateCalls) != 1 || schedules.UpdateCalls[0].ID != 42 {
		t.Fatalf("expected one update call for schedule 42, got %+v", schedules.UpdateCalls)
	}
}

func TestCronService_ProcessSchedules_QueueErrorDoesNotUpdateNextRun(t *testing.T) {
	logger := mocks.NewMockLogger()
	queue := mocks.NewMockQueueService()
	queue.SetEnqueueError(fmt.Errorf("queue unavailable"))
	clock := mocks.NewMockClock()
	clock.SetNow(time.Date(2026, 3, 21, 12, 10, 0, 0, time.UTC))

	jobs := mocks.NewMockJobsRepository()
	jobs.Definitions["job-1"] = `{"id":"job-1","root":{"uses":"builtins/shell"}}`

	runs := mocks.NewMockRunsRepository()
	runs.CreateRunID = "run-1"
	runs.CreateRunIndex = 1

	schedules := mocks.NewMockSchedulesRepository()
	schedules.Ready = []dal.CronSchedule{{
		ID:       42,
		JobID:    "job-1",
		CronSpec: "* * * * *",
	}}

	svc := cron.NewCronServiceWithRepositories(logger, jobs, runs, schedules)
	svc.SetQueueClient(queue)
	svc.SetClock(clock)

	if err := svc.ProcessSchedules(context.Background()); err != nil {
		t.Fatalf("ProcessSchedules should continue on enqueue failures, got: %v", err)
	}

	if len(schedules.UpdateCalls) != 0 {
		t.Fatalf("expected no schedule update on enqueue failure, got %+v", schedules.UpdateCalls)
	}
}
