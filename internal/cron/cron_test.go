package cron_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"vectis/internal/cell"
	"vectis/internal/cron"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func setupTestCronService(t *testing.T) (*cron.CronService, *mocks.MockLogger, *mocks.MockQueueService, *sql.DB) {
	db := dbtest.NewTestDB(t)
	logger := mocks.NewMockLogger()
	queueService := mocks.NewMockQueueService()

	service := cron.NewCronService(logger, db)
	service.SetQueueClient(queueService)

	return service, logger, queueService, db
}

func insertCronTestJob(t *testing.T, db *sql.DB, jobID, definitionJSON string) {
	t.Helper()
	if err := dal.NewSQLRepositories(db).Jobs().Create(context.Background(), jobID, definitionJSON, 1); err != nil {
		t.Fatalf("insert stored job: %v", err)
	}
}

func insertCronTestSchedule(t *testing.T, db *sql.DB, jobID, cronSpec string, nextRun time.Time) int64 {
	t.Helper()

	result, err := db.Exec("INSERT INTO job_triggers (job_id, trigger_type) VALUES (?, ?)", jobID, "cron")
	if err != nil {
		t.Fatalf("insert cron trigger: %v", err)
	}

	triggerID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("cron trigger id: %v", err)
	}

	result, err = db.Exec("INSERT INTO cron_trigger_specs (trigger_id, cron_spec, next_run_at) VALUES (?, ?, ?)", triggerID, cronSpec, nextRun.Format(time.RFC3339))
	if err != nil {
		t.Fatalf("insert cron trigger spec: %v", err)
	}

	specID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("cron trigger spec id: %v", err)
	}

	return specID
}

func queryCronTestNextRun(t *testing.T, db *sql.DB, jobID string) string {
	t.Helper()

	var nextRunStr string
	if err := db.QueryRow(`
		SELECT cts.next_run_at
		FROM cron_trigger_specs cts
		JOIN job_triggers jt ON jt.id = cts.trigger_id
		WHERE jt.job_id = ?
	`, jobID).Scan(&nextRunStr); err != nil {
		t.Fatalf("query next_run_at: %v", err)
	}

	return nextRunStr
}

func TestCronService_ValidateCronSpec_Matches(t *testing.T) {
	service, _, _, _ := setupTestCronService(t)

	testTime := time.Date(2026, 3, 15, 14, 30, 0, 0, time.UTC)
	matches, err := service.ValidateCronSpec("* * * * *", testTime)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !matches {
		t.Error("expected '*' spec to match any time")
	}
}

func TestCronService_ValidateCronSpec_SpecificMinute(t *testing.T) {
	service, _, _, _ := setupTestCronService(t)

	testTime := time.Date(2026, 3, 15, 14, 30, 0, 0, time.UTC)
	matches, err := service.ValidateCronSpec("30 * * * *", testTime)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !matches {
		t.Error("expected '30 * * * *' to match at 14:30")
	}

	testTime2 := time.Date(2026, 3, 15, 14, 31, 0, 0, time.UTC)
	matches2, err := service.ValidateCronSpec("30 * * * *", testTime2)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if matches2 {
		t.Error("expected '30 * * * *' NOT to match at 14:31")
	}
}

func TestCronService_ValidateCronSpec_InvalidSpec(t *testing.T) {
	service, _, _, _ := setupTestCronService(t)

	testTime := time.Now()
	_, err := service.ValidateCronSpec("invalid-cron-spec", testTime)
	if err == nil {
		t.Error("expected error for invalid cron spec")
	}

	if !strings.Contains(err.Error(), "invalid cron spec") {
		t.Errorf("expected error message to contain 'invalid cron spec', got: %v", err)
	}
}

func TestCronService_CalculateNextRun(t *testing.T) {
	service, _, _, _ := setupTestCronService(t)

	from := time.Date(2026, 3, 15, 14, 30, 0, 0, time.UTC)
	nextRun, err := service.CalculateNextRun("* * * * *", from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := time.Date(2026, 3, 15, 14, 31, 0, 0, time.UTC)
	if !nextRun.Equal(expected) {
		t.Errorf("expected next run at %v, got %v", expected, nextRun)
	}
}

func TestCronService_CalculateNextRun_Hourly(t *testing.T) {
	service, _, _, _ := setupTestCronService(t)

	from := time.Date(2026, 3, 15, 14, 30, 0, 0, time.UTC)
	nextRun, err := service.CalculateNextRun("0 * * * *", from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := time.Date(2026, 3, 15, 15, 0, 0, 0, time.UTC)
	if !nextRun.Equal(expected) {
		t.Errorf("expected next run at %v, got %v", expected, nextRun)
	}
}

func TestCronService_CalculateNextRun_InvalidSpec(t *testing.T) {
	service, _, _, _ := setupTestCronService(t)

	_, err := service.CalculateNextRun("not-a-cron-spec", time.Now())
	if err == nil {
		t.Error("expected error for invalid cron spec")
	}
}

func TestCronService_GetReadySchedules_Empty(t *testing.T) {
	service, _, _, _ := setupTestCronService(t)

	schedules, err := service.GetReadySchedules(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if len(schedules) != 0 {
		t.Errorf("expected 0 schedules, got %d", len(schedules))
	}
}

func TestCronService_GetReadySchedules_WithReadyJobs(t *testing.T) {
	service, _, _, db := setupTestCronService(t)

	insertCronTestJob(t, db, "test-job", `{"id": "test-job"}`)

	pastTime := time.Now().Add(-24 * time.Hour)
	insertCronTestSchedule(t, db, "test-job", "* * * * *", pastTime)

	schedules, err := service.GetReadySchedules(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(schedules) != 1 {
		t.Fatalf("expected 1 schedule, got %d", len(schedules))
	}

	if schedules[0].JobID != "test-job" {
		t.Errorf("expected job ID 'test-job', got %s", schedules[0].JobID)
	}

	if schedules[0].CronSpec != "* * * * *" {
		t.Errorf("expected cron spec '* * * * *', got %s", schedules[0].CronSpec)
	}
}

func TestCronService_GetReadySchedules_FutureJobsNotIncluded(t *testing.T) {
	service, _, _, db := setupTestCronService(t)

	insertCronTestJob(t, db, "future-job", `{"id": "future-job"}`)

	futureTime := time.Now().Add(1 * time.Hour)
	insertCronTestSchedule(t, db, "future-job", "* * * * *", futureTime)

	schedules, err := service.GetReadySchedules(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(schedules) != 0 {
		t.Errorf("expected 0 schedules (future job should not be ready), got %d", len(schedules))
	}
}

func TestCronService_GetJobDefinition_Success(t *testing.T) {
	service, _, _, db := setupTestCronService(t)

	jobDef := `{"id": "test-job", "root": {"uses": "builtins/shell", "with": {"command": "echo hello"}}}`
	insertCronTestJob(t, db, "test-job", jobDef)

	job, err := service.GetJobDefinition(context.Background(), "test-job")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if job.GetId() != "test-job" {
		t.Errorf("expected job ID 'test-job', got %s", job.GetId())
	}
}

func TestCronService_GetJobDefinition_NotFound(t *testing.T) {
	service, _, _, _ := setupTestCronService(t)

	_, err := service.GetJobDefinition(context.Background(), "nonexistent-job")
	if err == nil {
		t.Error("expected error for nonexistent job")
	}

	if !strings.Contains(err.Error(), "job not found") {
		t.Errorf("expected 'job not found' error, got: %v", err)
	}
}

func TestCronService_ClaimAndCompleteSchedule(t *testing.T) {
	service, _, _, db := setupTestCronService(t)

	insertCronTestJob(t, db, "update-test", `{"id": "update-test"}`)

	oldTime := time.Now().Add(-1 * time.Hour)
	scheduleID := insertCronTestSchedule(t, db, "update-test", "* * * * *", oldTime)

	newTime := time.Now().Add(1 * time.Hour)
	claimToken := "claim-1"
	claimedUntil := time.Now().Add(5 * time.Minute)
	claimed, err := service.ClaimDue(context.Background(), scheduleID, oldTime, claimToken, claimedUntil, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !claimed {
		t.Fatal("expected schedule claim to succeed")
	}

	claimed, err = service.ClaimDue(context.Background(), scheduleID, oldTime, "claim-2", claimedUntil, time.Now())
	if err != nil {
		t.Fatalf("unexpected duplicate claim error: %v", err)
	}

	if claimed {
		t.Fatal("expected duplicate schedule claim to fail")
	}

	completed, err := service.CompleteClaim(context.Background(), scheduleID, claimToken, newTime)
	if err != nil {
		t.Fatalf("unexpected complete error: %v", err)
	}

	if !completed {
		t.Fatal("expected schedule completion to succeed")
	}

	var storedTime string
	db.QueryRow("SELECT next_run_at FROM cron_trigger_specs WHERE id = ?", scheduleID).Scan(&storedTime)
	parsedTime, _ := time.Parse(time.RFC3339, storedTime)

	// NOTE(garrett):Allow small time difference due to formatting
	if parsedTime.Sub(newTime) > time.Second {
		t.Errorf("expected next_run_at to be updated to %v, got %v", newTime, parsedTime)
	}
}

func TestCronService_ProcessSchedules_NoReadySchedules(t *testing.T) {
	service, logger, queueService, _ := setupTestCronService(t)

	err := service.ProcessSchedules(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 0 {
		t.Errorf("expected 0 jobs enqueued, got %d", len(jobs))
	}

	infoCalls := logger.GetInfoCalls()
	for _, msg := range infoCalls {
		if strings.Contains(msg, "Processing") {
			t.Error("should not log 'Processing' when no schedules ready")
		}
	}
}

func TestCronService_ProcessSchedules_TriggerAndUpdate(t *testing.T) {
	service, logger, queueService, db := setupTestCronService(t)

	jobDef := `{"id": "cron-job", "root": {"uses": "builtins/shell", "with": {"command": "echo test"}}}`
	insertCronTestJob(t, db, "cron-job", jobDef)

	pastTime := time.Now().Add(-24 * time.Hour)
	insertCronTestSchedule(t, db, "cron-job", "* * * * *", pastTime)

	err := service.ProcessSchedules(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job enqueued, got %d", len(jobs))
	}

	if jobs[0].GetId() != "cron-job" {
		t.Errorf("expected job ID 'cron-job', got %s", jobs[0].GetId())
	}

	var nextRunStr string
	nextRunStr = queryCronTestNextRun(t, db, "cron-job")
	nextRun, _ := time.Parse(time.RFC3339, nextRunStr)

	if !nextRun.After(pastTime) {
		t.Errorf("expected next_run_at to be updated to future time, got %v", nextRun)
	}

	infoCalls := logger.GetInfoCalls()
	hasTriggerMsg := false
	hasSuccessMsg := false
	for _, msg := range infoCalls {
		if strings.Contains(msg, "Processing 1 schedule") {
			hasTriggerMsg = true
		}
		if strings.Contains(msg, "Job cron-job triggered successfully") {
			hasSuccessMsg = true
		}
	}
	if !hasTriggerMsg {
		t.Errorf("expected 'Processing 1 schedule' log, got: %v", infoCalls)
	}
	if !hasSuccessMsg {
		t.Errorf("expected success log, got: %v", infoCalls)
	}
}

func TestCronService_ProcessSchedules_InvalidCronSpec(t *testing.T) {
	service, logger, queueService, db := setupTestCronService(t)

	insertCronTestJob(t, db, "invalid-cron-job", `{"id": "invalid-cron-job"}`)

	pastTime := time.Now().Add(-24 * time.Hour)
	insertCronTestSchedule(t, db, "invalid-cron-job", "invalid-spec", pastTime)

	err := service.ProcessSchedules(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 0 {
		t.Errorf("expected 0 jobs enqueued (invalid cron should be skipped), got %d", len(jobs))
	}

	errorCalls := logger.GetErrorCalls()
	hasInvalidCronError := false
	for _, msg := range errorCalls {
		if strings.Contains(msg, "Invalid cron spec") {
			hasInvalidCronError = true
			break
		}
	}

	if !hasInvalidCronError {
		t.Errorf("expected 'Invalid cron spec' error log, got: %v", errorCalls)
	}
}

func TestCronService_ProcessSchedules_QueueError(t *testing.T) {
	service, logger, queueService, db := setupTestCronService(t)

	insertCronTestJob(t, db, "queue-error-job", `{"id": "queue-error-job"}`)

	pastTime := time.Now().Add(-24 * time.Hour)
	insertCronTestSchedule(t, db, "queue-error-job", "* * * * *", pastTime)

	queueService.SetEnqueueError(errors.New("queue unavailable"))

	err := service.ProcessSchedules(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	errorCalls := logger.GetErrorCalls()
	hasQueueError := false
	for _, msg := range errorCalls {
		if strings.Contains(msg, "Failed to trigger job") {
			hasQueueError = true
			break
		}
	}

	if !hasQueueError {
		t.Errorf("expected 'Failed to trigger job' error log, got: %v", errorCalls)
	}

	var nextRunStr string
	nextRunStr = queryCronTestNextRun(t, db, "queue-error-job")
	if nextRunStr != pastTime.Format(time.RFC3339) {
		t.Error("expected next_run_at to remain unchanged after queue error")
	}
}

func TestCronService_ProcessSchedules_TimeNotMatching(t *testing.T) {
	service, logger, queueService, db := setupTestCronService(t)

	insertCronTestJob(t, db, "future-job", `{"id": "future-job"}`)

	midnight := time.Now().Truncate(24 * time.Hour)
	insertCronTestSchedule(t, db, "future-job", "0 0 * * *", midnight.Add(-1*time.Hour))

	err := service.ProcessSchedules(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 0 {
		t.Errorf("expected 0 jobs enqueued (time doesn't match), got %d", len(jobs))
	}

	debugCalls := logger.GetDebugCalls()
	hasSkipMsg := false
	for _, msg := range debugCalls {
		if strings.Contains(msg, "Skipping job") {
			hasSkipMsg = true
			break
		}
	}

	if !hasSkipMsg {
		t.Errorf("expected 'Skipping job' debug log, got: %v", debugCalls)
	}
}

func TestCronService_TriggerJob_JobNotFound(t *testing.T) {
	service, _, _, _ := setupTestCronService(t)

	err := service.TriggerJob(context.Background(), "nonexistent-job")
	if err == nil {
		t.Error("expected error for nonexistent job")
	}
}

func TestCronService_TriggerJob_Success(t *testing.T) {
	service, _, queueService, db := setupTestCronService(t)

	jobDef := `{"id": "trigger-test", "root": {"uses": "builtins/shell"}}`
	insertCronTestJob(t, db, "trigger-test", jobDef)

	err := service.TriggerJob(context.Background(), "trigger-test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job enqueued, got %d", len(jobs))
	}

	if jobs[0].GetId() != "trigger-test" {
		t.Errorf("expected job ID 'trigger-test', got %s", jobs[0].GetId())
	}

	reqs := queueService.GetJobRequests()
	envelopeJSON := reqs[0].GetMetadata()[cell.ExecutionEnvelopeMetadataKey]
	if envelopeJSON == "" {
		t.Fatal("expected execution envelope metadata")
	}

	env, err := cell.DecodeExecutionEnvelope([]byte(envelopeJSON))
	if err != nil {
		t.Fatalf("decode execution envelope: %v", err)
	}

	if env.Job.GetId() != "trigger-test" || env.Job.GetRunId() != jobs[0].GetRunId() {
		t.Fatalf("unexpected envelope job identity: job=%q run=%q", env.Job.GetId(), env.Job.GetRunId())
	}

	if env.ExecutionID == "" || env.SegmentID == "" || env.CellID != dal.DefaultCellID {
		t.Fatalf("unexpected envelope target: execution=%q segment=%q cell=%q", env.ExecutionID, env.SegmentID, env.CellID)
	}

	runRec, err := dal.NewSQLRepositories(db).Runs().GetRun(context.Background(), jobs[0].GetRunId())
	if err != nil {
		t.Fatalf("get cron run: %v", err)
	}

	if runRec.TriggerInvocationID == nil {
		t.Fatal("expected cron trigger invocation id on run")
	}

	if runRec.TriggerType == nil || *runRec.TriggerType != dal.TriggerTypeCron {
		t.Fatalf("trigger type: got %+v want %q", runRec.TriggerType, dal.TriggerTypeCron)
	}

	if runRec.ExecutionPayloadHash == "" {
		t.Fatal("expected cron execution payload hash on run")
	}
}

func TestCronService_TriggerSchedule_ReusesRunForDuplicateTick(t *testing.T) {
	service, _, queueService, db := setupTestCronService(t)
	ctx := context.Background()

	jobDef := `{"id": "scheduled-idempotent", "root": {"uses": "builtins/shell"}}`
	insertCronTestJob(t, db, "scheduled-idempotent", jobDef)

	scheduledFor := time.Date(2026, 3, 21, 12, 10, 0, 0, time.UTC)
	scheduleID := insertCronTestSchedule(t, db, "scheduled-idempotent", "* * * * *", scheduledFor)

	sched := cron.CronSchedule{
		ID:        scheduleID,
		JobID:     "scheduled-idempotent",
		CronSpec:  "* * * * *",
		NextRunAt: scheduledFor,
	}

	if err := service.TriggerSchedule(ctx, sched); err != nil {
		t.Fatalf("trigger schedule first: %v", err)
	}

	if err := service.TriggerSchedule(ctx, sched); err != nil {
		t.Fatalf("trigger schedule duplicate: %v", err)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 2 {
		t.Fatalf("expected duplicate handoff attempts for the same run, got %d jobs", len(jobs))
	}

	runID := jobs[0].GetRunId()
	if runID == "" {
		t.Fatal("expected first enqueued job to have run_id")
	}

	if jobs[1].GetRunId() != runID {
		t.Fatalf("expected duplicate scheduled tick to reuse run_id %q, got %q", runID, jobs[1].GetRunId())
	}

	var runCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM job_runs WHERE job_id = ?", "scheduled-idempotent").Scan(&runCount); err != nil {
		t.Fatalf("count job runs: %v", err)
	}

	if runCount != 1 {
		t.Fatalf("expected one job_runs row for duplicate scheduled tick, got %d", runCount)
	}

	var fireCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cron_schedule_fires WHERE schedule_id = ?", scheduleID).Scan(&fireCount); err != nil {
		t.Fatalf("count schedule fires: %v", err)
	}

	if fireCount != 1 {
		t.Fatalf("expected one cron_schedule_fires row for duplicate scheduled tick, got %d", fireCount)
	}
}

func TestCronService_WaitTimeToNextMinute_AtSecondZero(t *testing.T) {
	logger := mocks.NewMockLogger()
	clock := mocks.NewMockClock()
	clock.SetNow(time.Date(2026, 3, 15, 14, 30, 0, 0, time.UTC))

	db := dbtest.NewTestDB(t)
	jobs := dal.NewSQLRepositories(db).Jobs()
	runs := dal.NewSQLRepositories(db).Runs()
	schedules := dal.NewSQLRepositories(db).Schedules()

	service := cron.NewCronServiceWithRepositories(logger, jobs, runs, schedules)
	service.SetClock(clock)

	wait := service.WaitTimeToNextMinute()
	expected := 60 * time.Second

	if wait != expected {
		t.Errorf("expected wait time %v, got %v", expected, wait)
	}
}

func TestCronService_WaitTimeToNextMinute_AtSecond30(t *testing.T) {
	logger := mocks.NewMockLogger()
	clock := mocks.NewMockClock()
	clock.SetNow(time.Date(2026, 3, 15, 14, 30, 30, 0, time.UTC))

	db := dbtest.NewTestDB(t)
	jobs := dal.NewSQLRepositories(db).Jobs()
	runs := dal.NewSQLRepositories(db).Runs()
	schedules := dal.NewSQLRepositories(db).Schedules()

	service := cron.NewCronServiceWithRepositories(logger, jobs, runs, schedules)
	service.SetClock(clock)

	wait := service.WaitTimeToNextMinute()
	expected := 30 * time.Second

	if wait != expected {
		t.Errorf("expected wait time %v, got %v", expected, wait)
	}
}

func TestCronService_WaitTimeToNextMinute_AtSecond59(t *testing.T) {
	logger := mocks.NewMockLogger()
	clock := mocks.NewMockClock()
	clock.SetNow(time.Date(2026, 3, 15, 14, 30, 59, 500000000, time.UTC))

	db := dbtest.NewTestDB(t)
	jobs := dal.NewSQLRepositories(db).Jobs()
	runs := dal.NewSQLRepositories(db).Runs()
	schedules := dal.NewSQLRepositories(db).Schedules()

	service := cron.NewCronServiceWithRepositories(logger, jobs, runs, schedules)
	service.SetClock(clock)

	wait := service.WaitTimeToNextMinute()
	expected := 500 * time.Millisecond

	if wait != expected {
		t.Errorf("expected wait time %v, got %v", expected, wait)
	}
}
