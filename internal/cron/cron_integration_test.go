package cron_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"vectis/internal/cron"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func setupCronIntegrationTest(t *testing.T) (*cron.CronService, *sql.DB, *mocks.MockQueueService, *mocks.MockClock) {
	t.Helper()
	db := dbtest.NewTestDB(t)

	logger := mocks.NewMockLogger()
	queueService := mocks.NewMockQueueService()
	mockClock := mocks.NewMockClock()

	service := cron.NewCronService(logger, db)
	service.SetQueueClient(queueService)
	service.SetClock(mockClock)

	return service, db, queueService, mockClock
}

func TestIntegrationCron_TriggerJob_FullFlow(t *testing.T) {
	service, db, queueService, mockClock := setupCronIntegrationTest(t)
	ctx := context.Background()

	testTime := time.Date(2026, 3, 15, 14, 30, 0, 0, time.UTC)
	mockClock.SetNow(testTime)

	jobID := "test-cron-job"
	jobDef := `{"id": "test-cron-job", "root": {"uses": "builtins/shell", "with": {"command": "echo hello"}}}`
	_, err := db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", jobID, jobDef)
	if err != nil {
		t.Fatalf("failed to insert job: %v", err)
	}

	nextRun := testTime.Add(-1 * time.Minute)
	_, err = db.Exec("INSERT INTO job_cron_schedules (job_id, cron_spec, next_run_at) VALUES (?, ?, ?)",
		jobID, "* * * * *", nextRun.Format(time.RFC3339))

	if err != nil {
		t.Fatalf("failed to insert schedule: %v", err)
	}

	if err := service.ProcessSchedules(ctx); err != nil {
		t.Fatalf("process schedules failed: %v", err)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job enqueued, got %d", len(jobs))
	}

	if jobs[0].GetId() != jobID {
		t.Errorf("expected job ID %q, got %q", jobID, jobs[0].GetId())
	}

	var newNextRun time.Time
	err = db.QueryRow("SELECT next_run_at FROM job_cron_schedules WHERE job_id = ?", jobID).Scan(&newNextRun)
	if err != nil {
		t.Fatalf("failed to query next_run_at: %v", err)
	}

	if !newNextRun.After(testTime) {
		t.Errorf("expected next_run_at to be after %v, got %v", testTime, newNextRun)
	}
}

func TestIntegrationCron_QueueError_NoUpdate(t *testing.T) {
	service, db, queueService, mockClock := setupCronIntegrationTest(t)
	ctx := context.Background()

	testTime := time.Date(2026, 3, 15, 14, 30, 0, 0, time.UTC)
	mockClock.SetNow(testTime)

	jobID := "failing-job"
	jobDef := `{"id": "failing-job"}`
	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", jobID, jobDef)

	originalNextRun := testTime.Add(-1 * time.Hour)
	db.Exec("INSERT INTO job_cron_schedules (job_id, cron_spec, next_run_at) VALUES (?, ?, ?)",
		jobID, "* * * * *", originalNextRun.Format(time.RFC3339))

	queueService.SetEnqueueError(fmt.Errorf("queue unavailable"))

	err := service.ProcessSchedules(ctx)
	if err != nil {
		t.Logf("process schedules returned error (expected): %v", err)
	}

	var currentNextRun time.Time
	db.QueryRow("SELECT next_run_at FROM job_cron_schedules WHERE job_id = ?", jobID).Scan(&currentNextRun)

	if !currentNextRun.Equal(originalNextRun) {
		t.Errorf("next_run_at should not be updated on queue error: expected %v, got %v",
			originalNextRun, currentNextRun)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 0 {
		t.Errorf("expected 0 jobs enqueued on error, got %d", len(jobs))
	}
}

func TestIntegrationCron_MultipleSchedules(t *testing.T) {
	service, db, queueService, mockClock := setupCronIntegrationTest(t)
	ctx := context.Background()

	testTime := time.Date(2026, 3, 15, 14, 30, 0, 0, time.UTC)
	mockClock.SetNow(testTime)

	for i := 1; i <= 5; i++ {
		jobID := fmt.Sprintf("multi-job-%d", i)
		jobDef := fmt.Sprintf(`{"id": "%s"}`, jobID)
		db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", jobID, jobDef)

		nextRun := testTime.Add(-time.Duration(i) * time.Minute)
		db.Exec("INSERT INTO job_cron_schedules (job_id, cron_spec, next_run_at) VALUES (?, ?, ?)",
			jobID, "* * * * *", nextRun.Format(time.RFC3339))
	}

	if err := service.ProcessSchedules(ctx); err != nil {
		t.Fatalf("process schedules failed: %v", err)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 5 {
		t.Errorf("expected 5 jobs enqueued, got %d", len(jobs))
	}

	for i := 1; i <= 5; i++ {
		jobID := fmt.Sprintf("multi-job-%d", i)
		var newNextRun time.Time
		err := db.QueryRow("SELECT next_run_at FROM job_cron_schedules WHERE job_id = ?", jobID).Scan(&newNextRun)
		if err != nil {
			t.Errorf("failed to query next_run_at for %s: %v", jobID, err)
			continue
		}

		if !newNextRun.After(testTime) {
			t.Errorf("job %s: expected next_run_at to be after %v, got %v", jobID, testTime, newNextRun)
		}
	}
}

func TestIntegrationCron_InvalidJobDefinition(t *testing.T) {
	service, db, queueService, mockClock := setupCronIntegrationTest(t)
	ctx := context.Background()

	testTime := time.Date(2026, 3, 15, 14, 30, 0, 0, time.UTC)
	mockClock.SetNow(testTime)

	jobID := "invalid-json-job"
	jobDef := `{"id": "invalid-json-job", "root": {broken json}}`
	db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", jobID, jobDef)

	nextRun := testTime.Add(-1 * time.Minute)
	db.Exec("INSERT INTO job_cron_schedules (job_id, cron_spec, next_run_at) VALUES (?, ?, ?)",
		jobID, "* * * * *", nextRun.Format(time.RFC3339))

	err := service.ProcessSchedules(ctx)
	if err != nil {
		t.Logf("process schedules returned error (expected for invalid JSON): %v", err)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 0 {
		t.Errorf("expected 0 jobs (invalid JSON), got %d", len(jobs))
	}

	var currentNextRun time.Time
	db.QueryRow("SELECT next_run_at FROM job_cron_schedules WHERE job_id = ?", jobID).Scan(&currentNextRun)

	expectedNextRun := testTime.Add(-1 * time.Minute)
	if !currentNextRun.Equal(expectedNextRun) {
		t.Errorf("next_run_at should not be updated on parse error: expected %v, got %v",
			expectedNextRun, currentNextRun)
	}
}
