//go:build integration

package cron_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"vectis/internal/cron"
	"vectis/internal/dal"
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

func insertCronIntegrationJob(t *testing.T, db *sql.DB, jobID, definitionJSON string) {
	t.Helper()
	if err := dal.NewSQLRepositories(db).Jobs().Create(context.Background(), jobID, definitionJSON, 1); err != nil {
		t.Fatalf("failed to insert job: %v", err)
	}
}

func insertCronIntegrationSchedule(t *testing.T, db *sql.DB, jobID, cronSpec string, nextRun time.Time) int64 {
	t.Helper()

	result, err := db.Exec("INSERT INTO job_triggers (job_id, trigger_type) VALUES (?, ?)", jobID, "cron")
	if err != nil {
		t.Fatalf("failed to insert trigger: %v", err)
	}

	triggerID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("failed to get trigger id: %v", err)
	}

	result, err = db.Exec("INSERT INTO cron_trigger_specs (trigger_id, cron_spec, next_run_at) VALUES (?, ?, ?)",
		triggerID, cronSpec, nextRun.Format(time.RFC3339))
	if err != nil {
		t.Fatalf("failed to insert schedule: %v", err)
	}

	specID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("failed to get schedule id: %v", err)
	}

	return specID
}

func queryCronIntegrationNextRun(t *testing.T, db *sql.DB, jobID string) time.Time {
	t.Helper()

	var nextRunStr string
	err := db.QueryRow(`
		SELECT cts.next_run_at
		FROM cron_trigger_specs cts
		JOIN job_triggers jt ON jt.id = cts.trigger_id
		WHERE jt.job_id = ?
	`, jobID).Scan(&nextRunStr)
	if err != nil {
		t.Fatalf("failed to query next_run_at: %v", err)
	}

	nextRun, err := time.Parse(time.RFC3339, nextRunStr)
	if err != nil {
		t.Fatalf("failed to parse next_run_at %q: %v", nextRunStr, err)
	}

	return nextRun
}

func TestIntegrationCron_TriggerJob_FullFlow(t *testing.T) {
	service, db, queueService, mockClock := setupCronIntegrationTest(t)
	ctx := context.Background()

	testTime := time.Date(2026, 3, 15, 14, 30, 0, 0, time.UTC)
	mockClock.SetNow(testTime)

	jobID := "test-cron-job"
	jobDef := `{"id": "test-cron-job", "root": {"uses": "builtins/shell", "with": {"command": "echo hello"}}}`
	insertCronIntegrationJob(t, db, jobID, jobDef)

	nextRun := testTime.Add(-1 * time.Minute)
	insertCronIntegrationSchedule(t, db, jobID, "* * * * *", nextRun)

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

	runID := jobs[0].GetRunId()
	if runID == "" {
		t.Error("expected run_id to be set on enqueued job")
	}

	var dbStatus string
	var runIndex int
	var invocationID, payloadHash string
	err := db.QueryRow("SELECT status, run_index, trigger_invocation_id, execution_payload_hash FROM job_runs WHERE job_id = ? AND run_id = ?", jobID, runID).Scan(&dbStatus, &runIndex, &invocationID, &payloadHash)
	if err != nil {
		t.Fatalf("expected job_runs row for cron-triggered job: %v", err)
	}

	if dbStatus != "queued" {
		t.Errorf("expected job_runs.status queued, got %s", dbStatus)
	}

	if runIndex != 1 {
		t.Errorf("expected run_index 1, got %d", runIndex)
	}

	if invocationID == "" {
		t.Fatal("expected cron trigger invocation id")
	}

	if payloadHash == "" {
		t.Fatal("expected cron execution payload hash")
	}

	var triggerType, payloadJSON string
	if err := db.QueryRow("SELECT trigger_type, requested_cells FROM trigger_invocations WHERE invocation_id = ?", invocationID).Scan(&triggerType, &payloadJSON); err != nil {
		t.Fatalf("query trigger invocation: %v", err)
	}

	if triggerType != dal.TriggerTypeCron {
		t.Fatalf("trigger type: got %q want %q", triggerType, dal.TriggerTypeCron)
	}

	if !strings.Contains(payloadJSON, dal.DefaultCellID) {
		t.Fatalf("expected cron requested cells to include default cell, got %s", payloadJSON)
	}

	var executionPayload string
	if err := db.QueryRow("SELECT payload_json FROM execution_payloads WHERE payload_hash = ?", payloadHash).Scan(&executionPayload); err != nil {
		t.Fatalf("query execution payload: %v", err)
	}

	if !strings.Contains(executionPayload, runID) || !strings.Contains(executionPayload, jobID) {
		t.Fatalf("execution payload should contain run/job identity, got %s", executionPayload)
	}

	newNextRun := queryCronIntegrationNextRun(t, db, jobID)

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
	insertCronIntegrationJob(t, db, jobID, jobDef)

	originalNextRun := testTime.Add(-1 * time.Hour)
	insertCronIntegrationSchedule(t, db, jobID, "* * * * *", originalNextRun)

	queueService.SetEnqueueError(fmt.Errorf("queue unavailable"))

	err := service.ProcessSchedules(ctx)
	if err != nil {
		t.Logf("process schedules returned error (expected): %v", err)
	}

	currentNextRun := queryCronIntegrationNextRun(t, db, jobID)

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
		insertCronIntegrationJob(t, db, jobID, jobDef)

		nextRun := testTime.Add(-time.Duration(i) * time.Minute)
		insertCronIntegrationSchedule(t, db, jobID, "* * * * *", nextRun)
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
		newNextRun := queryCronIntegrationNextRun(t, db, jobID)

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
	insertCronIntegrationJob(t, db, jobID, jobDef)

	nextRun := testTime.Add(-1 * time.Minute)
	insertCronIntegrationSchedule(t, db, jobID, "* * * * *", nextRun)

	err := service.ProcessSchedules(ctx)
	if err != nil {
		t.Logf("process schedules returned error (expected for invalid JSON): %v", err)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 0 {
		t.Errorf("expected 0 jobs (invalid JSON), got %d", len(jobs))
	}

	currentNextRun := queryCronIntegrationNextRun(t, db, jobID)

	expectedNextRun := testTime.Add(-1 * time.Minute)
	if !currentNextRun.Equal(expectedNextRun) {
		t.Errorf("next_run_at should not be updated on parse error: expected %v, got %v",
			expectedNextRun, currentNextRun)
	}
}
