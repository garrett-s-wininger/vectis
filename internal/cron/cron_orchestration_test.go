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
	jobs.DefinitionVersions["job-1"] = 4

	runs := mocks.NewMockRunsRepository()
	runs.CreateRunID = "run-1"
	runs.CreateRunIndex = 1

	schedules := mocks.NewMockSchedulesRepository()
	schedules.Ready = []dal.CronSchedule{{
		ID:        42,
		JobID:     "job-1",
		CronSpec:  "* * * * *",
		NextRunAt: clock.Now().Add(-1 * time.Minute),
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

	lastCreateJobID, lastDefVersion := runs.SnapshotLastCreate()
	if lastCreateJobID != "job-1" || lastDefVersion != 4 {
		t.Fatalf("expected cron run for job-1 definition version 4, got job=%q version=%d", lastCreateJobID, lastDefVersion)
	}

	lastScheduleID, lastScheduledFor := runs.SnapshotLastScheduled()
	if lastScheduleID != 42 || !lastScheduledFor.Equal(schedules.Ready[0].NextRunAt) {
		t.Fatalf("expected scheduled run for schedule 42 at %v, got schedule=%d at %v", schedules.Ready[0].NextRunAt, lastScheduleID, lastScheduledFor)
	}

	touched := runs.SnapshotTouchedRunIDs()
	if len(touched) != 1 || touched[0] != "run-1" {
		t.Fatalf("expected touch for run-1, got %+v", touched)
	}

	if len(schedules.ClaimDueCalls) != 1 || schedules.ClaimDueCalls[0].ID != 42 {
		t.Fatalf("expected one claim call for schedule 42, got %+v", schedules.ClaimDueCalls)
	}

	if len(schedules.CompleteClaimCalls) != 1 || schedules.CompleteClaimCalls[0].ID != 42 {
		t.Fatalf("expected one complete call for schedule 42, got %+v", schedules.CompleteClaimCalls)
	}
}

func TestCronService_ProcessSchedules_QueueErrorReleasesScheduleClaim(t *testing.T) {
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
		ID:        42,
		JobID:     "job-1",
		CronSpec:  "* * * * *",
		NextRunAt: clock.Now().Add(-1 * time.Minute),
	}}

	svc := cron.NewCronServiceWithRepositories(logger, jobs, runs, schedules)
	svc.SetQueueClient(queue)
	svc.SetClock(clock)

	if err := svc.ProcessSchedules(context.Background()); err != nil {
		t.Fatalf("ProcessSchedules should continue on enqueue failures, got: %v", err)
	}

	if len(schedules.ClaimDueCalls) != 1 {
		t.Fatalf("expected schedule to be claimed before enqueue, got %+v", schedules.ClaimDueCalls)
	}

	if len(schedules.ReleaseClaimCalls) != 1 || schedules.ReleaseClaimCalls[0].ID != 42 {
		t.Fatalf("expected schedule claim to be released on enqueue failure, got %+v", schedules.ReleaseClaimCalls)
	}

	if len(schedules.CompleteClaimCalls) != 0 {
		t.Fatalf("expected no schedule completion on enqueue failure, got %+v", schedules.CompleteClaimCalls)
	}
}

func TestCronService_ProcessSchedules_StaleScheduleDoesNotTrigger(t *testing.T) {
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
	schedules.ClaimDueOK = false
	schedules.Ready = []dal.CronSchedule{{
		ID:        42,
		JobID:     "job-1",
		CronSpec:  "* * * * *",
		NextRunAt: clock.Now().Add(-1 * time.Minute),
	}}

	svc := cron.NewCronServiceWithRepositories(logger, jobs, runs, schedules)
	svc.SetQueueClient(queue)
	svc.SetClock(clock)

	if err := svc.ProcessSchedules(context.Background()); err != nil {
		t.Fatalf("ProcessSchedules: %v", err)
	}

	if len(queue.GetJobs()) != 0 {
		t.Fatalf("expected no enqueued jobs for stale schedule, got %d", len(queue.GetJobs()))
	}

	if len(schedules.ClaimDueCalls) != 1 {
		t.Fatalf("expected one claim attempt, got %+v", schedules.ClaimDueCalls)
	}
}
