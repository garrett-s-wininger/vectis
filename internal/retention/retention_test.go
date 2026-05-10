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
	if report.Counts.TerminalRuns != 2 {
		t.Fatalf("terminal run candidates: got %d want 2", report.Counts.TerminalRuns)
	}
	if report.Counts.RunDispatchEvents != 2 {
		t.Fatalf("dispatch event candidates: got %d want 2", report.Counts.RunDispatchEvents)
	}
	if report.Counts.JobDefinitions != 3 {
		t.Fatalf("job definition candidates: got %d want 3", report.Counts.JobDefinitions)
	}
	if report.Counts.IdempotencyKeys != 1 {
		t.Fatalf("idempotency candidates: got %d want 1", report.Counts.IdempotencyKeys)
	}
	if report.Counts.AuditLog != 1 {
		t.Fatalf("audit candidates: got %d want 1", report.Counts.AuditLog)
	}

	assertCount(t, db, `SELECT COUNT(*) FROM job_runs`, 5)
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
	if report.Counts.TerminalRuns != 2 {
		t.Fatalf("deleted terminal runs: got %d want 2", report.Counts.TerminalRuns)
	}
	if report.Counts.JobDefinitions != 3 {
		t.Fatalf("deleted job definitions: got %d want 3", report.Counts.JobDefinitions)
	}

	assertCount(t, db, `SELECT COUNT(*) FROM job_runs WHERE run_id IN ('old-success', 'old-failed')`, 0)
	assertCount(t, db, `SELECT COUNT(*) FROM job_runs WHERE run_id IN ('queued-old', 'running-old', 'new-success')`, 3)
	assertCount(t, db, `SELECT COUNT(*) FROM run_dispatch_events WHERE run_id IN ('old-success', 'old-failed')`, 0)
	assertCount(t, db, `SELECT COUNT(*) FROM run_dispatch_events WHERE run_id = 'queued-old'`, 1)
	assertCount(t, db, `SELECT COUNT(*) FROM job_definitions WHERE job_id IN ('old-success-job', 'old-failed-job', 'orphan-job')`, 0)
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
	insertRun(t, db, "queued-old", "queued-job", "queued", "")
	insertRun(t, db, "running-old", "running-job", "running", "")
	insertRun(t, db, "new-success", "new-success-job", "succeeded", recent)

	for _, runID := range []string{"old-success", "old-failed", "queued-old"} {
		if _, err := db.Exec(`
			INSERT INTO run_dispatch_events (run_id, source, event_type, created_at)
			VALUES (?, 'test', 'attempt', ?)
		`, runID, now.Unix()); err != nil {
			t.Fatalf("insert dispatch event %s: %v", runID, err)
		}
	}

	for _, jobID := range []string{"old-success-job", "old-failed-job", "queued-job", "orphan-job"} {
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
