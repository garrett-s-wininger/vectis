package cron_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"vectis/internal/cron"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	sourcepkg "vectis/internal/source"
	"vectis/internal/testutil/dbtest"
)

func configureMockCronSource(t *testing.T, svc *cron.CronService) {
	t.Helper()

	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	if _, err := repos.Sources().CreateRepository(context.Background(), dal.SourceRepositoryRecord{
		RepositoryID: "source-repo",
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: t.TempDir(),
		DefaultRef:   "main",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("create source repository: %v", err)
	}

	svc.SetSources(repos.Sources())
	svc.SetDefinitionResolverFactory(func(rec dal.SourceRepositoryRecord) (sourcepkg.DefinitionResolver, error) {
		return &recordingCronDefinitionResolver{}, nil
	})
}

func TestCronService_ProcessSchedules_OrchestrationUsesRepos(t *testing.T) {
	logger := mocks.NewMockLogger()
	queue := mocks.NewMockQueueService()
	clock := mocks.NewMockClock()
	clock.SetNow(time.Date(2026, 3, 21, 12, 10, 0, 0, time.UTC))

	jobs := mocks.NewMockJobsRepository()
	loaded, err := (&recordingCronDefinitionResolver{}).ResolveDefinition(context.Background(), sourcepkg.DefinitionRequest{
		Ref:  "main",
		Path: ".vectis/jobs/job-1.json",
	})

	if err != nil {
		t.Fatalf("resolve test definition: %v", err)
	}

	runs := mocks.NewMockRunsRepository()
	runs.CreateRunID = "run-1"
	runs.CreateRunIndex = 1
	runs.CreateRunCreated = true
	runs.PendingExecution = dal.ExecutionDispatchRecord{
		DefinitionVersion: 1,
		DefinitionHash:    dal.DefinitionHash(loaded.DefinitionJSON),
	}

	schedules := mocks.NewMockSchedulesRepository()
	schedules.Ready = []dal.CronSchedule{{
		ID:                 42,
		JobID:              "job-1",
		CronSpec:           "* * * * *",
		NextRunAt:          clock.Now().Add(-1 * time.Minute),
		SourceRepositoryID: "source-repo",
		SourceRef:          "main",
		SourcePath:         ".vectis/jobs/job-1.json",
	}}

	svc := cron.NewCronServiceWithRepositories(logger, jobs, runs, schedules)
	svc.SetQueueClient(queue)
	svc.SetClock(clock)
	svc.SetInstanceID("cron-a")
	svc.SetClaimTTL(2 * time.Minute)
	configureMockCronSource(t, svc)

	if err := svc.ProcessSchedules(context.Background()); err != nil {
		t.Fatalf("ProcessSchedules: %v", err)
	}

	if len(queue.GetJobs()) != 1 {
		t.Fatalf("expected one enqueued job, got %d; errors=%v", len(queue.GetJobs()), logger.GetErrorCalls())
	}

	lastCreateJobID, lastDefVersion := runs.SnapshotLastCreate()
	if lastCreateJobID != "job-1" || lastDefVersion != 1 {
		t.Fatalf("expected source cron run for job-1 definition version 1, got job=%q version=%d", lastCreateJobID, lastDefVersion)
	}

	if runs.LastSourceRecord.RepositoryID != "source-repo" || runs.LastSourceRecord.DefinitionPath != ".vectis/jobs/job-1.json" {
		t.Fatalf("expected source cron provenance on scheduled run, got %+v", runs.LastSourceRecord)
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

	claim := schedules.ClaimDueCalls[0]
	if !strings.HasPrefix(claim.ClaimToken, "cron-a:42:") {
		t.Fatalf("expected claim token to include cron instance and schedule, got %q", claim.ClaimToken)
	}

	if !claim.ClaimedUntil.Equal(clock.Now().Add(2 * time.Minute)) {
		t.Fatalf("expected custom claim ttl, got claimed_until=%v", claim.ClaimedUntil)
	}

	if len(schedules.CompleteClaimCalls) != 1 || schedules.CompleteClaimCalls[0].ID != 42 {
		t.Fatalf("expected one complete call for schedule 42, got %+v", schedules.CompleteClaimCalls)
	}

	if schedules.CompleteClaimCalls[0].ClaimToken != claim.ClaimToken {
		t.Fatalf("expected complete to use claim token %q, got %q", claim.ClaimToken, schedules.CompleteClaimCalls[0].ClaimToken)
	}
}

func TestCronService_ProcessSchedules_QueueErrorReleasesScheduleClaim(t *testing.T) {
	logger := mocks.NewMockLogger()
	queue := mocks.NewMockQueueService()
	queue.SetEnqueueError(fmt.Errorf("queue unavailable"))
	clock := mocks.NewMockClock()
	clock.SetNow(time.Date(2026, 3, 21, 12, 10, 0, 0, time.UTC))

	jobs := mocks.NewMockJobsRepository()

	runs := mocks.NewMockRunsRepository()
	runs.CreateRunID = "run-1"
	runs.CreateRunIndex = 1

	schedules := mocks.NewMockSchedulesRepository()
	schedules.Ready = []dal.CronSchedule{{
		ID:                 42,
		JobID:              "job-1",
		CronSpec:           "* * * * *",
		NextRunAt:          clock.Now().Add(-1 * time.Minute),
		SourceRepositoryID: "source-repo",
		SourceRef:          "main",
		SourcePath:         ".vectis/jobs/job-1.json",
	}}

	svc := cron.NewCronServiceWithRepositories(logger, jobs, runs, schedules)
	svc.SetQueueClient(queue)
	svc.SetClock(clock)
	configureMockCronSource(t, svc)

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

func TestCronService_ProcessSchedules_TwoInstancesOnlyOneClaimsDueTick(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	jobID := "cron-ha"
	now := time.Date(2026, 3, 21, 12, 10, 0, 0, time.UTC)
	repos := dal.NewSQLRepositories(db)
	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "source-repo",
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: t.TempDir(),
		DefaultRef:   "main",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("create source repository: %v", err)
	}

	insertCronTestSourceSchedule(t, db, jobID, "source-repo", "main", ".vectis/jobs/cron-ha.json", "* * * * *", now)

	queue := mocks.NewMockQueueService()
	newService := func(instanceID string) *cron.CronService {
		clock := mocks.NewMockClock()
		clock.SetNow(now)
		svc := cron.NewCronService(mocks.NewMockLogger(), db)
		svc.SetQueueClient(queue)
		svc.SetClock(clock)
		svc.SetInstanceID(instanceID)
		svc.SetClaimTTL(time.Minute)
		svc.SetDefinitionResolverFactory(func(rec dal.SourceRepositoryRecord) (sourcepkg.DefinitionResolver, error) {
			return &recordingCronDefinitionResolver{}, nil
		})
		return svc
	}

	services := []*cron.CronService{
		newService("cron-a"),
		newService("cron-b"),
	}

	start := make(chan struct{})
	errs := make(chan error, len(services))
	var wg sync.WaitGroup
	for _, svc := range services {
		wg.Go(func() {
			<-start
			errs <- svc.ProcessSchedules(ctx)
		})
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("ProcessSchedules: %v", err)
		}
	}

	enqueued := queue.GetJobs()
	if len(enqueued) != 1 {
		t.Fatalf("expected one enqueue across two cron instances, got %d", len(enqueued))
	}

	if enqueued[0].GetId() != jobID {
		t.Fatalf("expected enqueued job %q, got %q", jobID, enqueued[0].GetId())
	}

	var runCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM job_runs WHERE job_id = ?", jobID).Scan(&runCount); err != nil {
		t.Fatalf("count job runs: %v", err)
	}

	if runCount != 1 {
		t.Fatalf("expected one durable run across two cron instances, got %d", runCount)
	}

	var fireCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cron_schedule_fires").Scan(&fireCount); err != nil {
		t.Fatalf("count schedule fires: %v", err)
	}

	if fireCount != 1 {
		t.Fatalf("expected one schedule fire across two cron instances, got %d", fireCount)
	}

	nextRunAt := queryCronTestNextRun(t, db, jobID)
	if want := now.Add(time.Minute).Format(time.RFC3339); nextRunAt != want {
		t.Fatalf("expected schedule advanced to %s, got %s", want, nextRunAt)
	}
}
