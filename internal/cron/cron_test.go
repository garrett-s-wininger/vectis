package cron_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

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

	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)",
		"test-job", `{"id": "test-job"}`)

	pastTime := time.Now().Add(-24 * time.Hour).Format(time.RFC3339)
	db.Exec("INSERT INTO job_cron_schedules (job_id, cron_spec, next_run_at) VALUES (?, ?, ?)",
		"test-job", "* * * * *", pastTime)

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

	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)",
		"future-job", `{"id": "future-job"}`)

	futureTime := time.Now().Add(1 * time.Hour).Format(time.RFC3339)
	db.Exec("INSERT INTO job_cron_schedules (job_id, cron_spec, next_run_at) VALUES (?, ?, ?)",
		"future-job", "* * * * *", futureTime)

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
	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)",
		"test-job", jobDef)

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

func TestCronService_UpdateNextRun(t *testing.T) {
	service, _, _, db := setupTestCronService(t)

	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)",
		"update-test", `{"id": "update-test"}`)

	oldTime := time.Now().Add(-1 * time.Hour)
	result, _ := db.Exec("INSERT INTO job_cron_schedules (job_id, cron_spec, next_run_at) VALUES (?, ?, ?)",
		"update-test", "* * * * *", oldTime.Format(time.RFC3339))

	scheduleID, _ := result.LastInsertId()

	newTime := time.Now().Add(1 * time.Hour)
	err := service.UpdateNextRun(context.Background(), scheduleID, newTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var storedTime string
	db.QueryRow("SELECT next_run_at FROM job_cron_schedules WHERE id = ?", scheduleID).Scan(&storedTime)
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
	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)",
		"cron-job", jobDef)

	pastTime := time.Now().Add(-24 * time.Hour)
	db.Exec("INSERT INTO job_cron_schedules (job_id, cron_spec, next_run_at) VALUES (?, ?, ?)",
		"cron-job", "* * * * *", pastTime.Format(time.RFC3339))

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
	db.QueryRow("SELECT next_run_at FROM job_cron_schedules WHERE job_id = ?", "cron-job").Scan(&nextRunStr)
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

	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)",
		"invalid-cron-job", `{"id": "invalid-cron-job"}`)

	pastTime := time.Now().Add(-24 * time.Hour).Format(time.RFC3339)
	db.Exec("INSERT INTO job_cron_schedules (job_id, cron_spec, next_run_at) VALUES (?, ?, ?)",
		"invalid-cron-job", "invalid-spec", pastTime)

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

	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)",
		"queue-error-job", `{"id": "queue-error-job"}`)

	pastTime := time.Now().Add(-24 * time.Hour)
	db.Exec("INSERT INTO job_cron_schedules (job_id, cron_spec, next_run_at) VALUES (?, ?, ?)",
		"queue-error-job", "* * * * *", pastTime.Format(time.RFC3339))

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
	db.QueryRow("SELECT next_run_at FROM job_cron_schedules WHERE job_id = ?", "queue-error-job").Scan(&nextRunStr)
	if nextRunStr != pastTime.Format(time.RFC3339) {
		t.Error("expected next_run_at to remain unchanged after queue error")
	}
}

func TestCronService_ProcessSchedules_TimeNotMatching(t *testing.T) {
	service, logger, queueService, db := setupTestCronService(t)

	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)",
		"future-job", `{"id": "future-job"}`)

	midnight := time.Now().Truncate(24 * time.Hour)
	db.Exec("INSERT INTO job_cron_schedules (job_id, cron_spec, next_run_at) VALUES (?, ?, ?)",
		"future-job", "0 0 * * *", midnight.Add(-1*time.Hour).Format(time.RFC3339))

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
	service, logger, _, _ := setupTestCronService(t)

	err := service.TriggerJob(context.Background(), "nonexistent-job")
	if err == nil {
		t.Error("expected error for nonexistent job")
	}

	errorCalls := logger.GetErrorCalls()
	_ = errorCalls
}

func TestCronService_TriggerJob_Success(t *testing.T) {
	service, _, queueService, db := setupTestCronService(t)

	jobDef := `{"id": "trigger-test", "root": {"uses": "builtins/shell"}}`
	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)",
		"trigger-test", jobDef)

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
