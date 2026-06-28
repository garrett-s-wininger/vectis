package cron_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"vectis/internal/cell"
	"vectis/internal/cron"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	sourcepkg "vectis/internal/source"
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

func insertCronTestSourceSchedule(t *testing.T, db *sql.DB, jobID, repositoryID, ref, path, cronSpec string, nextRun time.Time) int64 {
	t.Helper()

	result, err := db.Exec(
		"INSERT INTO job_triggers (job_id, trigger_type, source_repository_id, source_ref, source_path) VALUES (?, ?, ?, ?, ?)",
		jobID,
		"cron",
		repositoryID,
		ref,
		path,
	)
	if err != nil {
		t.Fatalf("insert source cron trigger: %v", err)
	}

	triggerID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("source cron trigger id: %v", err)
	}

	result, err = db.Exec("INSERT INTO cron_trigger_specs (trigger_id, cron_spec, next_run_at) VALUES (?, ?, ?)", triggerID, cronSpec, nextRun.Format(time.RFC3339))
	if err != nil {
		t.Fatalf("insert source cron trigger spec: %v", err)
	}

	specID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("source cron trigger spec id: %v", err)
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

	pastTime := time.Now().Add(-24 * time.Hour)
	insertCronTestSourceSchedule(t, db, "test-job", "source-repo", "main", "", "* * * * *", pastTime)

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

	if schedules[0].SourceRepositoryID != "source-repo" {
		t.Errorf("expected source repository 'source-repo', got %s", schedules[0].SourceRepositoryID)
	}
}

func TestCronService_GetReadySchedules_FutureJobsNotIncluded(t *testing.T) {
	service, _, _, db := setupTestCronService(t)

	futureTime := time.Now().Add(1 * time.Hour)
	insertCronTestSourceSchedule(t, db, "future-job", "source-repo", "main", "", "* * * * *", futureTime)

	schedules, err := service.GetReadySchedules(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(schedules) != 0 {
		t.Errorf("expected 0 schedules (future job should not be ready), got %d", len(schedules))
	}
}

func TestCronService_ClaimAndCompleteSchedule(t *testing.T) {
	service, _, _, db := setupTestCronService(t)

	oldTime := time.Now().Add(-1 * time.Hour)
	scheduleID := insertCronTestSourceSchedule(t, db, "update-test", "source-repo", "main", "", "* * * * *", oldTime)

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

func TestCronService_ProcessSchedulesRejectsLegacyScheduleWithoutSourceRepository(t *testing.T) {
	service, logger, queueService, db := setupTestCronService(t)

	pastTime := time.Now().Add(-24 * time.Hour).Truncate(time.Second).UTC()
	insertCronTestSchedule(t, db, "cron-job", "* * * * *", pastTime)

	err := service.ProcessSchedules(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 0 {
		t.Fatalf("expected no job enqueued for legacy schedule, got %d", len(jobs))
	}

	nextRunStr := queryCronTestNextRun(t, db, "cron-job")
	if nextRunStr != pastTime.Format(time.RFC3339) {
		t.Fatalf("expected next_run_at to remain unchanged for legacy schedule, got %s", nextRunStr)
	}

	infoCalls := logger.GetInfoCalls()
	hasTriggerMsg := false
	for _, msg := range infoCalls {
		if strings.Contains(msg, "Processing 1 schedule") {
			hasTriggerMsg = true
		}
	}
	if !hasTriggerMsg {
		t.Errorf("expected 'Processing 1 schedule' log, got: %v", infoCalls)
	}

	errorCalls := logger.GetErrorCalls()
	hasSourceRequired := false
	for _, msg := range errorCalls {
		if strings.Contains(msg, "Failed to trigger job") && strings.Contains(msg, "requires a source repository schedule") {
			hasSourceRequired = true
			break
		}
	}
	if !hasSourceRequired {
		t.Errorf("expected source repository schedule error log, got: %v", errorCalls)
	}
}

func TestCronService_ProcessSchedules_TriggersSourceRepositorySchedule(t *testing.T) {
	repoPath := initCronGitRepo(t)
	writeCronJobDefinitionAndCommit(t, repoPath, "source-cron", "source cron definition")
	commit := cronGitOutput(t, repoPath, "rev-parse", "HEAD")
	blob := cronGitOutput(t, repoPath, "rev-parse", "HEAD:.vectis/jobs/build.json")

	service, _, queueService, db := setupTestCronService(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "source-repo",
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: repoPath,
		DefaultRef:   "HEAD",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("create source repository: %v", err)
	}

	pastTime := time.Now().Add(-24 * time.Hour)
	insertCronTestSourceSchedule(t, db, "build", "source-repo", "", "", "* * * * *", pastTime)

	if err := service.ProcessSchedules(ctx); err != nil {
		t.Fatalf("process source schedule: %v", err)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 source job enqueued, got %d", len(jobs))
	}

	if jobs[0].GetId() != "build" ||
		jobs[0].GetRoot().GetWith()["script"] != "source-cron" ||
		jobs[0].GetRunId() == "" {
		t.Fatalf("source cron job mismatch: %+v", jobs[0])
	}

	definitionJSON, err := repos.Jobs().GetDefinitionVersion(ctx, "build", 1)
	if err != nil {
		t.Fatalf("get source cron definition snapshot: %v", err)
	}

	if !strings.Contains(definitionJSON, "source-cron") {
		t.Fatalf("source cron definition snapshot mismatch: %s", definitionJSON)
	}

	sourceRec, err := repos.Sources().GetDefinitionSource(ctx, "build", 1)
	if err != nil {
		t.Fatalf("get source cron provenance: %v", err)
	}

	if sourceRec.RepositoryID != "source-repo" ||
		sourceRec.RequestedRef != "HEAD" ||
		sourceRec.ResolvedCommit != commit ||
		sourceRec.DefinitionPath != ".vectis/jobs/build.json" ||
		sourceRec.BlobSHA != blob {
		t.Fatalf("source cron provenance mismatch: %+v", sourceRec)
	}

	nextRunStr := queryCronTestNextRun(t, db, "build")
	nextRun, err := time.Parse(time.RFC3339, nextRunStr)
	if err != nil {
		t.Fatalf("parse source cron next_run_at: %v", err)
	}
	if !nextRun.After(pastTime) {
		t.Fatalf("expected source cron next_run_at to advance, got %v", nextRun)
	}
}

func TestCronService_ProcessSchedules_UsesDefinitionResolverFactory(t *testing.T) {
	service, _, queueService, db := setupTestCronService(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)

	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "source-repo",
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: filepath.Join(t.TempDir(), "not-a-git-checkout"),
		DefaultRef:   "main",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("create source repository: %v", err)
	}

	pastTime := time.Now().Add(-24 * time.Hour)
	insertCronTestSourceSchedule(t, db, "build", "source-repo", "", ".vectis/jobs/custom.json", "* * * * *", pastTime)

	var resolverRequest sourcepkg.DefinitionRequest
	var factoryRepo dal.SourceRepositoryRecord
	factoryCalls := 0
	service.SetDefinitionResolverFactory(func(rec dal.SourceRepositoryRecord) (sourcepkg.DefinitionResolver, error) {
		factoryCalls++
		factoryRepo = rec
		return &recordingCronDefinitionResolver{request: &resolverRequest}, nil
	})

	if err := service.ProcessSchedules(ctx); err != nil {
		t.Fatalf("process source schedule with resolver factory: %v", err)
	}

	if factoryCalls != 1 || factoryRepo.RepositoryID != "source-repo" {
		t.Fatalf("resolver factory calls=%d repo=%+v", factoryCalls, factoryRepo)
	}

	if resolverRequest.Ref != "main" || resolverRequest.Path != ".vectis/jobs/custom.json" {
		t.Fatalf("resolver request mismatch: %+v", resolverRequest)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 source job enqueued, got %d", len(jobs))
	}

	if jobs[0].GetId() != "build" || jobs[0].GetRoot().GetWith()["script"] != "factory-cron" {
		t.Fatalf("source cron resolver job mismatch: %+v", jobs[0])
	}

	sourceRec, err := repos.Sources().GetDefinitionSource(ctx, "build", 1)
	if err != nil {
		t.Fatalf("get source cron resolver provenance: %v", err)
	}

	if sourceRec.RepositoryID != "source-repo" ||
		sourceRec.RequestedRef != "main" ||
		sourceRec.ResolvedCommit != "0123456789abcdef0123456789abcdef01234567" ||
		sourceRec.DefinitionPath != ".vectis/jobs/custom.json" ||
		sourceRec.BlobSHA != "abcdef0123456789abcdef0123456789abcdef01" {
		t.Fatalf("source cron resolver provenance mismatch: %+v", sourceRec)
	}
}

func TestCronService_ProcessSchedules_UsesSourceScheduleOverride(t *testing.T) {
	repoPath := initCronGitRepo(t)
	writeCronJobDefinitionAndCommit(t, repoPath, "base-cron", "base definition")
	cronGit(t, repoPath, "checkout", "-b", "hotfix/build")
	writeCronFileAndCommit(t, repoPath, ".vectis/jobs/build-hotfix.json", `{
		"root": {"id": "root", "uses": "builtins/script", "with":{"script": "hotfix-cron"}}
	}`+"\n", "hotfix definition")

	hotfixCommit := cronGitOutput(t, repoPath, "rev-parse", "hotfix/build")
	hotfixBlob := cronGitOutput(t, repoPath, "rev-parse", "hotfix/build:.vectis/jobs/build-hotfix.json")

	service, _, queueService, db := setupTestCronService(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)

	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "source-repo",
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: repoPath,
		DefaultRef:   "HEAD",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("create source repository: %v", err)
	}

	pastTime := time.Now().Add(-24 * time.Hour)
	if _, err := repos.Schedules().CreateCronSchedule(ctx, dal.CronScheduleRecord{
		ScheduleID:         "nightly-build",
		JobID:              "build",
		CronSpec:           "* * * * *",
		NextRunAt:          pastTime,
		SourceRepositoryID: "source-repo",
		Enabled:            true,
	}); err != nil {
		t.Fatalf("create source schedule: %v", err)
	}

	if _, err := repos.Schedules().SetSourceCronScheduleOverride(ctx, "nightly-build", dal.SourceScheduleOverride{
		Ref:    "hotfix/build",
		Path:   ".vectis/jobs/build-hotfix.json",
		Reason: "production hotfix",
	}); err != nil {
		t.Fatalf("set source schedule override: %v", err)
	}

	if err := service.ProcessSchedules(ctx); err != nil {
		t.Fatalf("process source schedule override: %v", err)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 source job enqueued, got %d", len(jobs))
	}

	if jobs[0].GetRoot().GetWith()["script"] != "hotfix-cron" {
		t.Fatalf("expected hotfix source cron command, got %+v", jobs[0])
	}

	sourceRec, err := repos.Sources().GetDefinitionSource(ctx, "build", 1)
	if err != nil {
		t.Fatalf("get source cron override provenance: %v", err)
	}

	if sourceRec.RequestedRef != "hotfix/build" ||
		sourceRec.ResolvedCommit != hotfixCommit ||
		sourceRec.DefinitionPath != ".vectis/jobs/build-hotfix.json" ||
		sourceRec.BlobSHA != hotfixBlob {
		t.Fatalf("source cron override provenance mismatch: %+v", sourceRec)
	}
}

func TestCronService_ProcessSchedulesRejectsInvalidSourceScheduleReference(t *testing.T) {
	for _, tc := range []struct {
		name       string
		jobID      string
		ref        string
		path       string
		defaultRef string
	}{
		{
			name:  "invalid schedule ref",
			jobID: "build",
			ref:   "HEAD~1",
		},
		{
			name:  "invalid repository default ref",
			jobID: "build",
			ref:   "",
			path:  "",
			// This value cannot be persisted through the API/config path, but cron
			// still needs to reject old or manually repaired rows at execution time.
			defaultRef: "main..hotfix",
		},
		{
			name:  "invalid schedule path",
			jobID: "build",
			path:  "../build.json",
		},
		{
			name:  "invalid derived path",
			jobID: "team/build",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, logger, queueService, db := setupTestCronService(t)
			ctx := context.Background()
			repos := dal.NewSQLRepositories(db)

			defaultRef := tc.defaultRef
			if defaultRef == "" {
				defaultRef = "HEAD"
			}

			if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
				RepositoryID: "source-repo",
				SourceKind:   dal.SourceKindLocalCheckout,
				CheckoutPath: t.TempDir(),
				DefaultRef:   defaultRef,
				Enabled:      true,
			}); err != nil {
				t.Fatalf("create source repository: %v", err)
			}

			pastTime := time.Date(2026, 5, 1, 8, 30, 0, 0, time.UTC)
			insertCronTestSourceSchedule(t, db, tc.jobID, "source-repo", tc.ref, tc.path, "* * * * *", pastTime)

			if err := service.ProcessSchedules(ctx); err != nil {
				t.Fatalf("process source schedule: %v", err)
			}

			jobs := queueService.GetJobs()
			if len(jobs) != 0 {
				t.Fatalf("expected no job to be enqueued for invalid source reference, got %d", len(jobs))
			}

			errorCalls := logger.GetErrorCalls()
			foundInvalidReference := false
			for _, msg := range errorCalls {
				if strings.Contains(msg, "Failed to trigger job") && strings.Contains(msg, "invalid source reference") {
					foundInvalidReference = true
					break
				}
			}

			if !foundInvalidReference {
				t.Fatalf("expected invalid source reference error log, got: %v", errorCalls)
			}

			nextRunStr := queryCronTestNextRun(t, db, tc.jobID)
			if nextRunStr != pastTime.Format(time.RFC3339) {
				t.Fatalf("expected next_run_at to remain unchanged after invalid source reference, got %s", nextRunStr)
			}
		})
	}
}

func TestCronService_ProcessSchedules_InvalidCronSpec(t *testing.T) {
	service, logger, queueService, db := setupTestCronService(t)

	pastTime := time.Now().Add(-24 * time.Hour)
	insertCronTestSourceSchedule(t, db, "invalid-cron-job", "source-repo", "main", "", "invalid-spec", pastTime)

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
	ctx := context.Background()

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

	service.SetDefinitionResolverFactory(func(rec dal.SourceRepositoryRecord) (sourcepkg.DefinitionResolver, error) {
		return &recordingCronDefinitionResolver{}, nil
	})

	pastTime := time.Now().Add(-24 * time.Hour).Truncate(time.Second).UTC()
	insertCronTestSourceSchedule(t, db, "queue-error-job", "source-repo", "main", ".vectis/jobs/build.json", "* * * * *", pastTime)

	queueService.SetEnqueueError(errors.New("queue unavailable"))

	err := service.ProcessSchedules(ctx)
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

	nextRunStr := queryCronTestNextRun(t, db, "queue-error-job")
	if nextRunStr != pastTime.Format(time.RFC3339) {
		t.Error("expected next_run_at to remain unchanged after queue error")
	}
}

func TestCronService_ProcessSchedules_CatchesUpDueScheduleWhenCurrentMinuteDoesNotMatch(t *testing.T) {
	service, _, queueService, db := setupTestCronService(t)
	ctx := context.Background()
	clock := mocks.NewMockClock()
	now := time.Date(2026, 3, 15, 12, 34, 0, 0, time.UTC)
	clock.SetNow(now)
	service.SetClock(clock)

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

	service.SetDefinitionResolverFactory(func(rec dal.SourceRepositoryRecord) (sourcepkg.DefinitionResolver, error) {
		return &recordingCronDefinitionResolver{}, nil
	})

	scheduledFor := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	insertCronTestSourceSchedule(t, db, "daily-job", "source-repo", "main", "", "0 0 * * *", scheduledFor)

	err := service.ProcessSchedules(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 caught-up job enqueued, got %d", len(jobs))
	}

	if jobs[0].GetId() != "daily-job" || jobs[0].GetRunId() == "" {
		t.Fatalf("caught-up cron job mismatch: %+v", jobs[0])
	}

	nextRunStr := queryCronTestNextRun(t, db, "daily-job")
	wantNextRun := time.Date(2026, 3, 16, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
	if nextRunStr != wantNextRun {
		t.Fatalf("expected next_run_at to advance to %s, got %s", wantNextRun, nextRunStr)
	}
}

func TestCronService_TriggerJobRequiresSourceSchedule(t *testing.T) {
	service, _, _, _ := setupTestCronService(t)

	err := service.TriggerJob(context.Background(), "manual-job")
	if err == nil || !strings.Contains(err.Error(), "requires a source repository schedule") {
		t.Fatalf("expected source repository schedule error, got %v", err)
	}
}

func TestCronService_TriggerSchedule_SourceSuccess(t *testing.T) {
	service, _, queueService, db := setupTestCronService(t)
	ctx := context.Background()
	service.SetInstanceID("cron-a")

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

	service.SetDefinitionResolverFactory(func(rec dal.SourceRepositoryRecord) (sourcepkg.DefinitionResolver, error) {
		return &recordingCronDefinitionResolver{}, nil
	})

	scheduledFor := time.Date(2026, 3, 21, 12, 10, 0, 0, time.UTC)
	scheduleID := insertCronTestSourceSchedule(t, db, "trigger-test", "source-repo", "main", ".vectis/jobs/trigger.json", "* * * * *", scheduledFor)
	sched := cron.CronSchedule{
		ID:                 scheduleID,
		ScheduleID:         "manual-trigger",
		JobID:              "trigger-test",
		CronSpec:           "* * * * *",
		NextRunAt:          scheduledFor,
		SourceRepositoryID: "source-repo",
		SourceRef:          "main",
		SourcePath:         ".vectis/jobs/trigger.json",
	}

	if err := service.TriggerSchedule(ctx, sched); err != nil {
		t.Fatalf("trigger source schedule: %v", err)
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

	runRec, err := repos.Runs().GetRun(ctx, jobs[0].GetRunId())
	if err != nil {
		t.Fatalf("get cron run: %v", err)
	}

	if runRec.TriggerInvocationID == nil {
		t.Fatal("expected cron trigger invocation id on run")
	}

	if runRec.TriggerType == nil || *runRec.TriggerType != dal.TriggerTypeCron {
		t.Fatalf("trigger type: got %+v want %q", runRec.TriggerType, dal.TriggerTypeCron)
	}

	if runRec.TriggerSourceInstance == nil || *runRec.TriggerSourceInstance != "cron-a" {
		t.Fatalf("trigger source instance: got %+v want cron-a", runRec.TriggerSourceInstance)
	}

	if runRec.ExecutionPayloadHash == "" {
		t.Fatal("expected cron execution payload hash on run")
	}
}

func TestCronService_TriggerSchedule_ReusesRunForDuplicateTick(t *testing.T) {
	service, _, queueService, db := setupTestCronService(t)
	ctx := context.Background()
	clock := mocks.NewMockClock()
	clock.SetNow(time.Date(2026, 3, 21, 12, 9, 30, 0, time.UTC))
	service.SetClock(clock)

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

	service.SetDefinitionResolverFactory(func(rec dal.SourceRepositoryRecord) (sourcepkg.DefinitionResolver, error) {
		return &recordingCronDefinitionResolver{}, nil
	})

	scheduledFor := time.Date(2026, 3, 21, 12, 10, 0, 0, time.UTC)
	scheduleID := insertCronTestSourceSchedule(t, db, "scheduled-idempotent", "source-repo", "main", ".vectis/jobs/scheduled.json", "* * * * *", scheduledFor)

	sched := cron.CronSchedule{
		ID:                 scheduleID,
		ScheduleID:         "nightly-build",
		JobID:              "scheduled-idempotent",
		CronSpec:           "* * * * *",
		NextRunAt:          scheduledFor,
		SourceRepositoryID: "source-repo",
		SourceRef:          "main",
		SourcePath:         ".vectis/jobs/scheduled.json",
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

	runRec, err := repos.Runs().GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get scheduled run: %v", err)
	}

	triggerPayload := map[string]string{
		"configured_schedule_id": "nightly-build",
		"cron_spec":              "* * * * *",
		"job_id":                 "scheduled-idempotent",
		"next_run_at":            scheduledFor.Format(time.RFC3339Nano),
		"schedule_id":            strconv.FormatInt(scheduleID, 10),
		"source_path":            ".vectis/jobs/scheduled.json",
		"source_ref":             "main",
		"source_repository_id":   "source-repo",
		"triggered":              clock.Now().UTC().Format(time.RFC3339Nano),
	}

	triggerPayloadJSON, err := json.Marshal(triggerPayload)
	if err != nil {
		t.Fatalf("marshal expected trigger payload: %v", err)
	}

	wantTriggerPayloadHash := dal.PayloadHash(string(triggerPayloadJSON))
	if runRec.TriggerPayloadHash == nil || *runRec.TriggerPayloadHash != wantTriggerPayloadHash {
		t.Fatalf("trigger payload hash: got %+v want %q", runRec.TriggerPayloadHash, wantTriggerPayloadHash)
	}

	var fireCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cron_schedule_fires WHERE schedule_id = ?", scheduleID).Scan(&fireCount); err != nil {
		t.Fatalf("count schedule fires: %v", err)
	}

	if fireCount != 1 {
		t.Fatalf("expected one cron_schedule_fires row for duplicate scheduled tick, got %d", fireCount)
	}
}

func TestCronServiceFault_CompleteClaimFailureRetriesSameScheduledRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	logger := mocks.NewMockLogger()
	queueService := mocks.NewMockQueueService()
	clock := mocks.NewMockClock()
	now := time.Date(2026, 3, 21, 12, 10, 0, 0, time.UTC)
	clock.SetNow(now)

	completeErr := errors.New("schedule store unavailable")
	schedules := &failOnceCompleteClaimSchedulesRepository{
		SchedulesRepository: repos.Schedules(),
		err:                 completeErr,
	}

	service := cron.NewCronServiceWithRepositories(logger, repos.Jobs(), repos.Runs(), schedules)
	service.SetQueueClient(queueService)
	service.SetClock(clock)
	service.SetClaimTTL(time.Minute)
	service.SetSources(repos.Sources())
	if _, err := repos.Sources().CreateRepository(context.Background(), dal.SourceRepositoryRecord{
		RepositoryID: "source-repo",
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: t.TempDir(),
		DefaultRef:   "main",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("create source repository: %v", err)
	}

	service.SetDefinitionResolverFactory(func(rec dal.SourceRepositoryRecord) (sourcepkg.DefinitionResolver, error) {
		return &recordingCronDefinitionResolver{}, nil
	})

	scheduleID := insertCronTestSourceSchedule(t, db, "scheduled-complete-fault", "source-repo", "main", ".vectis/jobs/scheduled-complete-fault.json", "* * * * *", now)

	if err := service.ProcessSchedules(context.Background()); err != nil {
		t.Fatalf("process schedules with injected complete failure: %v", err)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected first trigger to enqueue one job, got %d; errors=%v", len(jobs), logger.GetErrorCalls())
	}

	runID := jobs[0].GetRunId()
	if runID == "" {
		t.Fatal("expected first enqueued job to have run_id")
	}

	if ready, err := service.GetReadySchedules(context.Background()); err != nil {
		t.Fatalf("get ready during active failed claim: %v", err)
	} else if len(ready) != 0 {
		t.Fatalf("schedule should stay hidden until failed claim expires, got %+v", ready)
	}

	clock.SetNow(now.Add(2 * time.Minute))
	if err := service.ProcessSchedules(context.Background()); err != nil {
		t.Fatalf("process schedules after failed claim expiry: %v", err)
	}

	jobs = queueService.GetJobs()
	if len(jobs) != 2 {
		t.Fatalf("expected retry to enqueue the same scheduled run again, got %d jobs", len(jobs))
	}

	if jobs[1].GetRunId() != runID {
		t.Fatalf("expected retry to reuse run_id %q, got %q", runID, jobs[1].GetRunId())
	}

	var runCount int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM job_runs WHERE job_id = ?", "scheduled-complete-fault").Scan(&runCount); err != nil {
		t.Fatalf("count job runs: %v", err)
	}

	if runCount != 1 {
		t.Fatalf("expected one job_runs row after retry, got %d", runCount)
	}

	var fireCount int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM cron_schedule_fires WHERE schedule_id = ?", scheduleID).Scan(&fireCount); err != nil {
		t.Fatalf("count schedule fires: %v", err)
	}

	if fireCount != 1 {
		t.Fatalf("expected one cron_schedule_fires row after retry, got %d", fireCount)
	}

	nextRunStr := queryCronTestNextRun(t, db, "scheduled-complete-fault")
	nextRun, err := time.Parse(time.RFC3339, nextRunStr)
	if err != nil {
		t.Fatalf("parse next run: %v", err)
	}

	if !nextRun.After(now) {
		t.Fatalf("expected schedule to advance after retry, got %v", nextRun)
	}
}

func TestCronServiceRestoreSkew_ExpiredClaimWithoutRunIsRecoveredByLaterPass(t *testing.T) {
	service, _, queueService, db := setupTestCronService(t)
	ctx := context.Background()
	clock := mocks.NewMockClock()
	now := time.Date(2026, 3, 21, 12, 10, 0, 0, time.UTC)
	clock.SetNow(now)
	service.SetClock(clock)
	service.SetClaimTTL(time.Minute)
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
	service.SetDefinitionResolverFactory(func(rec dal.SourceRepositoryRecord) (sourcepkg.DefinitionResolver, error) {
		return &recordingCronDefinitionResolver{}, nil
	})

	scheduleID := insertCronTestSourceSchedule(t, db, "scheduled-claim-restore", "source-repo", "main", ".vectis/jobs/scheduled-claim-restore.json", "* * * * *", now)

	claimed, err := service.ClaimDue(ctx, scheduleID, now, "crashed-before-trigger", now.Add(time.Minute), now)
	if err != nil {
		t.Fatalf("claim schedule before simulated crash: %v", err)
	}

	if !claimed {
		t.Fatal("expected simulated crashed worker to claim schedule")
	}

	if err := service.ProcessSchedules(ctx); err != nil {
		t.Fatalf("process schedules during active crashed claim: %v", err)
	}

	if got := len(queueService.GetJobs()); got != 0 {
		t.Fatalf("active crashed claim should hide schedule, got %d queued jobs", got)
	}

	var runCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM job_runs WHERE job_id = ?", "scheduled-claim-restore").Scan(&runCount); err != nil {
		t.Fatalf("count runs before claim expiry: %v", err)
	}

	if runCount != 0 {
		t.Fatalf("simulated crash before trigger should not create a run, got %d", runCount)
	}

	clock.SetNow(now.Add(2 * time.Minute))
	if err := service.ProcessSchedules(ctx); err != nil {
		t.Fatalf("process schedules after crashed claim expiry: %v", err)
	}

	jobs := queueService.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected expired claim to be recovered with one queued job, got %d", len(jobs))
	}

	runID := jobs[0].GetRunId()
	if runID == "" {
		t.Fatal("expected recovered cron handoff to include run_id")
	}

	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM job_runs WHERE job_id = ?", "scheduled-claim-restore").Scan(&runCount); err != nil {
		t.Fatalf("count runs after claim expiry: %v", err)
	}

	if runCount != 1 {
		t.Fatalf("expected one durable run after expired claim recovery, got %d", runCount)
	}

	var fireCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cron_schedule_fires WHERE schedule_id = ?", scheduleID).Scan(&fireCount); err != nil {
		t.Fatalf("count schedule fires after claim expiry: %v", err)
	}

	if fireCount != 1 {
		t.Fatalf("expected one schedule fire after expired claim recovery, got %d", fireCount)
	}

	var claimToken sql.NullString
	var claimedUntil sql.NullString
	var nextRunStr string
	if err := db.QueryRowContext(ctx, `
		SELECT claim_token, claimed_until, next_run_at
		FROM cron_trigger_specs
		WHERE id = ?
	`, scheduleID).Scan(&claimToken, &claimedUntil, &nextRunStr); err != nil {
		t.Fatalf("query recovered schedule claim fields: %v", err)
	}

	if claimToken.Valid || claimedUntil.Valid {
		t.Fatalf("expected recovered schedule claim to be cleared, got token=%v until=%v", claimToken, claimedUntil)
	}

	nextRun, err := time.Parse(time.RFC3339, nextRunStr)
	if err != nil {
		t.Fatalf("parse recovered next_run_at: %v", err)
	}

	if !nextRun.After(now) {
		t.Fatalf("expected recovered schedule to advance, got %v", nextRun)
	}
}

type failOnceCompleteClaimSchedulesRepository struct {
	dal.SchedulesRepository
	err    error
	failed bool
}

func (r *failOnceCompleteClaimSchedulesRepository) CompleteClaim(ctx context.Context, scheduleID int64, claimToken string, nextRun time.Time) (bool, error) {
	if !r.failed {
		r.failed = true
		return false, r.err
	}

	return r.SchedulesRepository.CompleteClaim(ctx, scheduleID, claimToken, nextRun)
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

func initCronGitRepo(t *testing.T) string {
	t.Helper()

	repo := t.TempDir()
	cronGit(t, repo, "init")
	cronGit(t, repo, "config", "user.name", "Vectis Test")
	cronGit(t, repo, "config", "user.email", "vectis@example.invalid")
	cronGit(t, repo, "config", "commit.gpgsign", "false")

	return repo
}

func writeCronJobDefinitionAndCommit(t *testing.T, repo, command, message string) {
	t.Helper()

	writeCronFileAndCommit(t, repo, ".vectis/jobs/build.json", `{
		"root": {"id": "root", "uses": "builtins/script", "with":{"script": "`+command+`"}}
	}`+"\n", message)
}

func writeCronFileAndCommit(t *testing.T, repo, name, content, message string) {
	t.Helper()

	path := filepath.Join(repo, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}

	cronGit(t, repo, "add", name)
	cronGit(t, repo, "commit", "-m", message)
}

func cronGitOutput(t *testing.T, repo string, args ...string) string {
	t.Helper()

	cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}

	return strings.TrimSpace(string(out))
}

func cronGit(t *testing.T, repo string, args ...string) {
	t.Helper()
	_ = cronGitOutput(t, repo, args...)
}

type recordingCronDefinitionResolver struct {
	request *sourcepkg.DefinitionRequest
}

func (r *recordingCronDefinitionResolver) ResolveDefinition(_ context.Context, req sourcepkg.DefinitionRequest) (sourcepkg.Definition, error) {
	if r.request != nil {
		*r.request = req
	}

	return sourcepkg.ParseDefinitionFile(sourcepkg.File{
		Path:     req.Path,
		Revision: sourcepkg.Revision{Commit: "0123456789abcdef0123456789abcdef01234567"},
		BlobSHA:  "abcdef0123456789abcdef0123456789abcdef01",
		Content: []byte(`{
			"root": {"id": "root", "uses": "builtins/script", "with":{"script": "factory-cron"}}
		}`),
	}, req.Ref, req.Validation)
}
