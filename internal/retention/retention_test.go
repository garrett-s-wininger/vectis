package retention

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"vectis/internal/testutil/dbtest"
)

func TestSQLCleanerPreviewDoesNotMutate(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	now := fixedNow()
	seedRetentionRows(t, db, now)

	cleaner := NewSQLCleaner(db)
	report, err := cleaner.Preview(ctx, testPolicy(), now)
	if err != nil {
		t.Fatalf("preview: %v", err)
	}

	if !report.DryRun {
		t.Fatal("preview report should be dry-run")
	}

	if report.Counts.TerminalRuns != 3 {
		t.Fatalf("terminal run candidates: got %d want 3", report.Counts.TerminalRuns)
	}

	if report.Counts.RunDispatchEvents != 3 {
		t.Fatalf("dispatch event candidates: got %d want 3", report.Counts.RunDispatchEvents)
	}

	if report.Counts.RunTasks != 3 || report.Counts.TaskAttempts != 3 || report.Counts.RunSegments != 3 || report.Counts.SegmentExecutions != 3 || report.Counts.TaskDispatchIntents != 3 {
		t.Fatalf("task cascade candidates: %+v", report.Counts)
	}

	if report.Counts.JobDefinitions != 4 {
		t.Fatalf("job definition candidates: got %d want 4", report.Counts.JobDefinitions)
	}

	if report.Counts.IdempotencyKeys != 1 {
		t.Fatalf("idempotency candidates: got %d want 1", report.Counts.IdempotencyKeys)
	}

	if report.Counts.AuditLog != 1 {
		t.Fatalf("audit candidates: got %d want 1", report.Counts.AuditLog)
	}

	assertCount(t, db, `SELECT COUNT(*) FROM job_runs`, 6)
	assertCount(t, db, `SELECT COUNT(*) FROM run_tasks`, 4)
	assertCount(t, db, `SELECT COUNT(*) FROM task_attempts`, 4)
	assertCount(t, db, `SELECT COUNT(*) FROM run_segments`, 4)
	assertCount(t, db, `SELECT COUNT(*) FROM segment_executions`, 4)
	assertCount(t, db, `SELECT COUNT(*) FROM task_dispatch_intents`, 4)
	assertCount(t, db, `SELECT COUNT(*) FROM audit_log WHERE event_type = 'retention.cleanup'`, 0)
}

func TestSQLCleanerApplyDeletesOnlyEligibleState(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	now := fixedNow()
	seedRetentionRows(t, db, now)

	cleaner := NewSQLCleaner(db)
	report, err := cleaner.Apply(ctx, testPolicy(), now)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	if report.DryRun {
		t.Fatal("apply report should not be dry-run")
	}
	if !report.AuditEventInserted {
		t.Fatal("cleanup should insert an audit event")
	}

	if report.Counts.TerminalRuns != 3 {
		t.Fatalf("deleted terminal runs: got %d want 3", report.Counts.TerminalRuns)
	}

	if report.Counts.JobDefinitions != 4 {
		t.Fatalf("deleted job definitions: got %d want 4", report.Counts.JobDefinitions)
	}

	if report.Counts.RunTasks != 3 || report.Counts.TaskAttempts != 3 || report.Counts.RunSegments != 3 || report.Counts.SegmentExecutions != 3 || report.Counts.TaskDispatchIntents != 3 {
		t.Fatalf("deleted task cascade counts mismatch: %+v", report.Counts)
	}

	assertCount(t, db, `SELECT COUNT(*) FROM job_runs WHERE run_id IN ('old-success', 'old-failed', 'old-aborted')`, 0)
	assertCount(t, db, `SELECT COUNT(*) FROM job_runs WHERE run_id IN ('queued-old', 'running-old', 'new-success')`, 3)
	assertCount(t, db, `SELECT COUNT(*) FROM run_dispatch_events WHERE run_id IN ('old-success', 'old-failed', 'old-aborted')`, 0)
	assertCount(t, db, `SELECT COUNT(*) FROM run_dispatch_events WHERE run_id = 'queued-old'`, 1)
	assertCount(t, db, `SELECT COUNT(*) FROM run_tasks WHERE run_id IN ('old-success', 'old-failed', 'old-aborted')`, 0)
	assertCount(t, db, `SELECT COUNT(*) FROM task_attempts WHERE run_id IN ('old-success', 'old-failed', 'old-aborted')`, 0)
	assertCount(t, db, `SELECT COUNT(*) FROM run_segments WHERE run_id IN ('old-success', 'old-failed', 'old-aborted')`, 0)
	assertCount(t, db, `SELECT COUNT(*) FROM segment_executions WHERE run_id IN ('old-success', 'old-failed', 'old-aborted')`, 0)
	assertCount(t, db, `SELECT COUNT(*) FROM task_dispatch_intents WHERE run_id IN ('old-success', 'old-failed', 'old-aborted')`, 0)
	assertCount(t, db, `SELECT COUNT(*) FROM run_tasks WHERE run_id = 'queued-old'`, 1)
	assertCount(t, db, `SELECT COUNT(*) FROM task_attempts WHERE run_id = 'queued-old'`, 1)
	assertCount(t, db, `SELECT COUNT(*) FROM run_segments WHERE run_id = 'queued-old'`, 1)
	assertCount(t, db, `SELECT COUNT(*) FROM segment_executions WHERE run_id = 'queued-old'`, 1)
	assertCount(t, db, `SELECT COUNT(*) FROM task_dispatch_intents WHERE run_id = 'queued-old'`, 1)
	assertCount(t, db, `SELECT COUNT(*) FROM job_definitions WHERE job_id IN ('old-success-job', 'old-failed-job', 'old-aborted-job', 'orphan-job')`, 0)
	assertCount(t, db, `SELECT COUNT(*) FROM job_definitions WHERE job_id = 'queued-job'`, 1)
	assertCount(t, db, `SELECT COUNT(*) FROM idempotency_keys WHERE key = 'old-key'`, 0)
	assertCount(t, db, `SELECT COUNT(*) FROM idempotency_keys WHERE key = 'new-key'`, 1)
	assertCount(t, db, `SELECT COUNT(*) FROM audit_log WHERE event_type = 'old.event'`, 0)
	assertCount(t, db, `SELECT COUNT(*) FROM audit_log WHERE event_type = 'new.event'`, 1)
	assertCount(t, db, `SELECT COUNT(*) FROM audit_log WHERE event_type = 'retention.cleanup'`, 1)
}

func TestLocalRunLogCleanerPreviewAndDelete(t *testing.T) {
	dir := t.TempDir()
	runID := "run-with-log"
	path := RunLogPath(dir, runID)
	if err := os.WriteFile(path, []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "unrelated.jsonl"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}

	cleaner := LocalRunLogCleaner{Dir: dir}
	report, err := cleaner.Preview([]string{runID, "missing"})
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if report.RunLogFiles != 1 || report.RunLogBytes != 5 {
		t.Fatalf("preview report = %+v, want 1 file / 5 bytes", report)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("preview removed file: %v", err)
	}

	report, err = cleaner.Delete([]string{runID, "missing"})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if report.RunLogFiles != 1 || report.RunLogBytes != 5 {
		t.Fatalf("delete report = %+v, want 1 file / 5 bytes", report)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected log file to be removed, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "unrelated.jsonl")); err != nil {
		t.Fatalf("unrelated file should remain: %v", err)
	}
}

func seedRetentionRows(t *testing.T, db *sql.DB, now time.Time) {
	t.Helper()

	old := sqlStamp(now.Add(-40 * 24 * time.Hour))
	recent := sqlStamp(now.Add(-5 * 24 * time.Hour))
	recentIdempotency := sqlStamp(now.Add(-12 * time.Hour))

	insertRun(t, db, "old-success", "old-success-job", "succeeded", old)
	insertRun(t, db, "old-failed", "old-failed-job", "failed", old)
	insertRun(t, db, "old-aborted", "old-aborted-job", "aborted", old)
	insertRun(t, db, "queued-old", "queued-job", "queued", "")
	insertRun(t, db, "running-old", "running-job", "running", "")
	insertRun(t, db, "new-success", "new-success-job", "succeeded", recent)

	for _, runID := range []string{"old-success", "old-failed", "old-aborted", "queued-old"} {
		if _, err := db.Exec(`
			INSERT INTO run_dispatch_events (run_id, source, event_type, created_at)
			VALUES (?, 'test', 'attempt', ?)
		`, runID, now.Unix()); err != nil {
			t.Fatalf("insert dispatch event %s: %v", runID, err)
		}

		insertTaskCascadeRows(t, db, runID)
	}

	for _, jobID := range []string{"old-success-job", "old-failed-job", "old-aborted-job", "queued-job", "orphan-job"} {
		if _, err := db.Exec(`
			INSERT INTO job_definitions (job_id, version, definition_json, created_at)
			VALUES (?, 1, '{}', ?)
		`, jobID, old); err != nil {
			t.Fatalf("insert job definition %s: %v", jobID, err)
		}
	}

	if _, err := db.Exec(`
		INSERT INTO idempotency_keys (scope, key, request_hash, created_at, updated_at)
		VALUES ('test', 'old-key', 'hash', ?, ?), ('test', 'new-key', 'hash', ?, ?)
	`, old, old, recentIdempotency, recentIdempotency); err != nil {
		t.Fatalf("insert idempotency keys: %v", err)
	}

	if _, err := db.Exec(`
		INSERT INTO audit_log (event_type, metadata, created_at)
		VALUES ('old.event', '{}', ?), ('new.event', '{}', ?)
	`, old, recent); err != nil {
		t.Fatalf("insert audit rows: %v", err)
	}
}

func insertTaskCascadeRows(t *testing.T, db *sql.DB, runID string) {
	t.Helper()

	taskID := runID + ":root"
	attemptID := runID + ":attempt-1"
	segmentID := runID + ":segment"
	executionID := runID + ":execution"

	if _, err := db.Exec(`
		INSERT INTO run_tasks (task_id, run_id, task_key, name, status)
		VALUES (?, ?, 'root', 'root', 'pending')
	`, taskID, runID); err != nil {
		t.Fatalf("insert run task %s: %v", runID, err)
	}

	if _, err := db.Exec(`
		INSERT INTO task_attempts (attempt_id, task_id, run_id, cell_id, attempt, status)
		VALUES (?, ?, ?, 'local', 1, 'pending')
	`, attemptID, taskID, runID); err != nil {
		t.Fatalf("insert task attempt %s: %v", runID, err)
	}

	if _, err := db.Exec(`
		INSERT INTO run_segments (segment_id, run_id, name, status)
		VALUES (?, ?, 'root', 'pending')
	`, segmentID, runID); err != nil {
		t.Fatalf("insert run segment %s: %v", runID, err)
	}

	if _, err := db.Exec(`
		INSERT INTO segment_executions (execution_id, segment_id, run_id, task_id, task_attempt_id, cell_id, status, attempt)
		VALUES (?, ?, ?, ?, ?, 'local', 'pending', 1)
	`, executionID, segmentID, runID, taskID, attemptID); err != nil {
		t.Fatalf("insert segment execution %s: %v", runID, err)
	}

	if _, err := db.Exec(`
		INSERT INTO task_dispatch_intents (execution_id, run_id, task_id, task_attempt_id, cell_id, created_at, updated_at)
		VALUES (?, ?, ?, ?, 'local', ?, ?)
	`, executionID, runID, taskID, attemptID, fixedNow().UnixNano(), fixedNow().UnixNano()); err != nil {
		t.Fatalf("insert task dispatch intent %s: %v", runID, err)
	}
}

func insertRun(t *testing.T, db *sql.DB, runID, jobID, status, finishedAt string) {
	t.Helper()
	var finished any
	if finishedAt != "" {
		finished = finishedAt
	}

	if _, err := db.Exec(`
		INSERT INTO job_runs (run_id, job_id, run_index, status, started_at, finished_at, definition_version)
		VALUES (?, ?, 1, ?, ?, ?, 1)
	`, runID, jobID, status, sqlStamp(fixedNow().Add(-40*24*time.Hour)), finished); err != nil {
		t.Fatalf("insert run %s: %v", runID, err)
	}
}

func assertCount(t *testing.T, db *sql.DB, query string, want int64) {
	t.Helper()
	var got int64
	if err := db.QueryRow(query).Scan(&got); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	if got != want {
		t.Fatalf("count query %q: got %d want %d", query, got, want)
	}
}

func testPolicy() Policy {
	return Policy{
		TerminalRuns:    30 * 24 * time.Hour,
		JobDefinitions:  30 * 24 * time.Hour,
		IdempotencyKeys: 24 * time.Hour,
		AuditLog:        30 * 24 * time.Hour,
	}
}

func fixedNow() time.Time {
	return time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
}

func sqlStamp(t time.Time) string {
	return t.UTC().Format(sqlTimeLayout)
}
