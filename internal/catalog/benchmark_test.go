package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/migrations"

	_ "github.com/mattn/go-sqlite3"
)

const catalogBenchmarkJobDefinition = `{"id":"%s","root":{"uses":"builtins/script","with":{"script":"true"}}}`

func BenchmarkCatalog_BackfillRepairMissingRunStatuses(b *testing.B) {
	for _, tc := range catalogBenchmarkCases() {
		b.Run(catalogBenchmarkCaseName(tc.backlog, tc.limit), func(b *testing.B) {
			ctx := context.Background()
			db, repos := newCatalogBenchmarkDBAndRepos(b, "iad-a")
			seedCatalogBenchmarkRunningRuns(b, ctx, db, repos, "bench-catalog-backfill-runs", tc.backlog)

			processor := NewBackfillProcessor(
				"iad-a",
				repos.CatalogStatusBackfill(),
				cell.NewCatalogEventPublisher("iad-a", repos.CatalogEvents()),
			)
			want := min(tc.backlog, tc.limit)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				deleteCatalogBenchmarkEvents(b, ctx, db)
				b.StartTimer()

				result, err := processor.RepairMissing(ctx, tc.limit)
				if err != nil {
					b.Fatalf("repair missing run statuses: %v", err)
				}

				if result.RunEvents != want {
					b.Fatalf("run events=%d, want %d (%+v)", result.RunEvents, want, result)
				}
			}
		})
	}
}

func BenchmarkCatalog_FanInCopyPendingEvents(b *testing.B) {
	for _, tc := range catalogBenchmarkCases() {
		b.Run(catalogBenchmarkCaseName(tc.backlog, tc.limit), func(b *testing.B) {
			ctx := context.Background()
			sourceDB, sourceRepos := newCatalogBenchmarkDBAndRepos(b, "iad-a")
			targetDB, targetRepos := newCatalogBenchmarkDBAndRepos(b, dal.DefaultCellID)
			seedCatalogBenchmarkEvents(b, ctx, sourceRepos.CatalogEvents(), "iad-a", cell.CatalogEventTypeRunStatus, tc.backlog)

			processor := NewFanInProcessor(targetRepos.CatalogEvents(), []FanInSource{
				{CellID: "iad-a", Events: sourceRepos.CatalogEvents()},
			})

			want := min(tc.backlog, tc.limit)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				resetCatalogBenchmarkEventsPending(b, ctx, sourceDB, cell.CatalogEventTypeRunStatus)
				deleteCatalogBenchmarkEvents(b, ctx, targetDB)
				b.StartTimer()

				result, err := processor.IngestPending(ctx, tc.limit)
				if err != nil {
					b.Fatalf("ingest pending: %v", err)
				}

				if result.Read != want || result.Copied != want {
					b.Fatalf("fan-in read/copied=%d/%d, want %d (%+v)", result.Read, result.Copied, want, result)
				}
			}
		})
	}
}

func BenchmarkCatalog_InboxProcessRunStatusEvents(b *testing.B) {
	for _, tc := range catalogBenchmarkCases() {
		b.Run(catalogBenchmarkCaseName(tc.backlog, tc.limit), func(b *testing.B) {
			ctx := context.Background()
			db, repos := newCatalogBenchmarkDBAndRepos(b, "iad-a")
			runIDs := seedCatalogBenchmarkQueuedRuns(b, ctx, repos, "bench-catalog-inbox-runs", tc.backlog)
			seedCatalogBenchmarkRunStatusEvents(b, ctx, repos.CatalogEvents(), "iad-a", runIDs, dal.RunStatusRunning)

			processor := cell.NewCatalogInboxProcessor(repos.CatalogEvents(), repos.Runs())
			want := min(tc.backlog, tc.limit)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				resetCatalogBenchmarkRunRows(b, ctx, db, "bench-catalog-inbox-runs")
				resetCatalogBenchmarkEventsPending(b, ctx, db, cell.CatalogEventTypeRunStatus)
				b.StartTimer()

				result, err := processor.ProcessPending(ctx, tc.limit)
				if err != nil {
					b.Fatalf("process run status events: %v", err)
				}

				if result.Read != want || result.Applied != want {
					b.Fatalf("processed/applied=%d/%d, want %d (%+v)", result.Read, result.Applied, want, result)
				}
			}
		})
	}
}

func BenchmarkCatalog_InboxProcessExecutionStatusEvents(b *testing.B) {
	for _, tc := range catalogBenchmarkCases() {
		b.Run(catalogBenchmarkCaseName(tc.backlog, tc.limit), func(b *testing.B) {
			ctx := context.Background()
			db, repos := newCatalogBenchmarkDBAndRepos(b, "iad-a")
			executionIDs := seedCatalogBenchmarkPendingExecutions(b, ctx, repos, "bench-catalog-inbox-executions", tc.backlog)
			seedCatalogBenchmarkExecutionStatusEvents(b, ctx, repos.CatalogEvents(), "iad-a", executionIDs, dal.ExecutionStatusRunning)

			processor := cell.NewCatalogInboxProcessor(repos.CatalogEvents(), repos.Runs())
			want := min(tc.backlog, tc.limit)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				resetCatalogBenchmarkExecutionRows(b, ctx, db, "bench-catalog-inbox-executions")
				resetCatalogBenchmarkEventsPending(b, ctx, db, cell.CatalogEventTypeExecutionStatus)
				b.StartTimer()

				result, err := processor.ProcessPending(ctx, tc.limit)
				if err != nil {
					b.Fatalf("process execution status events: %v", err)
				}

				if result.Read != want || result.Applied != want {
					b.Fatalf("processed/applied=%d/%d, want %d (%+v)", result.Read, result.Applied, want, result)
				}
			}
		})
	}
}

type catalogBenchmarkCase struct {
	backlog int
	limit   int
}

func catalogBenchmarkCases() []catalogBenchmarkCase {
	return []catalogBenchmarkCase{
		{backlog: 100, limit: 100},
		{backlog: 1000, limit: 100},
		{backlog: 5000, limit: 100},
		{backlog: 1000, limit: 1000},
		{backlog: 5000, limit: 1000},
	}
}

func catalogBenchmarkCaseName(backlog, limit int) string {
	return fmt.Sprintf("backlog_%d/limit_%d", backlog, limit)
}

func newCatalogBenchmarkDBAndRepos(b *testing.B, cellID string) (*sql.DB, *dal.SQLRepositories) {
	b.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatalf("open benchmark db: %v", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := migrations.Run(db, "sqlite3"); err != nil {
		_ = db.Close()
		b.Fatalf("run migrations: %v", err)
	}

	b.Cleanup(func() { _ = db.Close() })
	return db, dal.NewSQLRepositoriesWithCellID(db, cellID)
}

func seedCatalogBenchmarkRunningRuns(b *testing.B, ctx context.Context, db *sql.DB, repos *dal.SQLRepositories, jobID string, count int) {
	b.Helper()

	executionIDs := seedCatalogBenchmarkPendingExecutions(b, ctx, repos, jobID, count)
	for _, executionID := range executionIDs {
		if err := repos.Runs().MarkExecutionStarted(ctx, executionID); err != nil {
			b.Fatalf("mark benchmark execution started: %v", err)
		}
	}

	execCatalogBenchmarkSQL(b, ctx, db, `
		UPDATE job_runs
		SET status = ?, started_at = COALESCE(started_at, CURRENT_TIMESTAMP)
		WHERE job_id = ?
	`, dal.RunStatusRunning, jobID)
}

func seedCatalogBenchmarkQueuedRuns(b *testing.B, ctx context.Context, repos *dal.SQLRepositories, jobID string, count int) []string {
	b.Helper()

	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, fmt.Sprintf(catalogBenchmarkJobDefinition, jobID)); err != nil {
		b.Fatalf("create benchmark job definition %s: %v", jobID, err)
	}

	runs := repos.Runs()
	runIDs := make([]string, 0, count)
	for i := 0; i < count; i++ {
		runIndex := i + 1
		runID, _, err := runs.CreateRun(ctx, jobID, &runIndex, 1)
		if err != nil {
			b.Fatalf("create benchmark run %d: %v", runIndex, err)
		}

		runIDs = append(runIDs, runID)
	}

	return runIDs
}

func seedCatalogBenchmarkPendingExecutions(b *testing.B, ctx context.Context, repos *dal.SQLRepositories, jobID string, count int) []string {
	b.Helper()

	runIDs := seedCatalogBenchmarkQueuedRuns(b, ctx, repos, jobID, count)
	executionIDs := make([]string, 0, len(runIDs))
	for _, runID := range runIDs {
		dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
		if err != nil {
			b.Fatalf("get pending benchmark execution: %v", err)
		}

		executionIDs = append(executionIDs, dispatch.ExecutionID)
	}

	return executionIDs
}

func seedCatalogBenchmarkEvents(b *testing.B, ctx context.Context, events dal.CatalogEventsRepository, sourceCell, eventType string, count int) {
	b.Helper()

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("run:bench-%06d:running", i)
		payload := []byte(fmt.Sprintf(`{"run_id":"bench-%06d","status":"running"}`, i))
		if _, _, err := events.Record(ctx, sourceCell, key, eventType, payload); err != nil {
			b.Fatalf("record benchmark catalog event %d: %v", i, err)
		}
	}
}

func seedCatalogBenchmarkRunStatusEvents(b *testing.B, ctx context.Context, events dal.CatalogEventsRepository, sourceCell string, runIDs []string, status string) {
	b.Helper()

	for _, runID := range runIDs {
		payload := []byte(fmt.Sprintf(`{"run_id":"%s","status":"%s"}`, runID, status))
		if _, _, err := events.Record(ctx, sourceCell, cell.CatalogRunStatusEventKey(runID, status), cell.CatalogEventTypeRunStatus, payload); err != nil {
			b.Fatalf("record run status catalog event: %v", err)
		}
	}
}

func seedCatalogBenchmarkExecutionStatusEvents(b *testing.B, ctx context.Context, events dal.CatalogEventsRepository, sourceCell string, executionIDs []string, status string) {
	b.Helper()

	for _, executionID := range executionIDs {
		payload := []byte(fmt.Sprintf(`{"execution_id":"%s","status":"%s"}`, executionID, status))
		if _, _, err := events.Record(ctx, sourceCell, cell.CatalogExecutionStatusEventKey(executionID, status), cell.CatalogEventTypeExecutionStatus, payload); err != nil {
			b.Fatalf("record execution status catalog event: %v", err)
		}
	}
}

func deleteCatalogBenchmarkEvents(b *testing.B, ctx context.Context, db *sql.DB) {
	b.Helper()
	execCatalogBenchmarkSQL(b, ctx, db, "DELETE FROM cell_catalog_events")
}

func resetCatalogBenchmarkEventsPending(b *testing.B, ctx context.Context, db *sql.DB, eventType string) {
	b.Helper()
	execCatalogBenchmarkSQL(b, ctx, db, `
		UPDATE cell_catalog_events
		SET status = ?, attempts = 0, last_error = NULL, applied_at = NULL, updated_at = ?
		WHERE event_type = ?
	`, dal.CatalogEventStatusPending, time.Now().UnixNano(), eventType)
}

func resetCatalogBenchmarkRunRows(b *testing.B, ctx context.Context, db *sql.DB, jobID string) {
	b.Helper()
	execCatalogBenchmarkSQL(b, ctx, db, `
		UPDATE job_runs
		SET status = ?, started_at = NULL, finished_at = NULL, failure_code = '', failure_reason = NULL, orphan_reason = ''
		WHERE job_id = ?
	`, dal.RunStatusQueued, jobID)
}

func resetCatalogBenchmarkExecutionRows(b *testing.B, ctx context.Context, db *sql.DB, jobID string) {
	b.Helper()
	execCatalogBenchmarkSQL(b, ctx, db, `
		UPDATE run_tasks
		SET status = ?
		WHERE run_id IN (SELECT run_id FROM job_runs WHERE job_id = ?)
	`, dal.TaskStatusPending, jobID)

	execCatalogBenchmarkSQL(b, ctx, db, `
		UPDATE task_attempts
		SET status = ?, accepted_at = NULL, started_at = NULL, finished_at = NULL, last_observed_at = NULL, event_sequence = 0
		WHERE run_id IN (SELECT run_id FROM job_runs WHERE job_id = ?)
	`, dal.TaskStatusPending, jobID)

	execCatalogBenchmarkSQL(b, ctx, db, `
		UPDATE run_segments
		SET status = ?
		WHERE run_id IN (SELECT run_id FROM job_runs WHERE job_id = ?)
	`, dal.SegmentStatusPending, jobID)

	execCatalogBenchmarkSQL(b, ctx, db, `
		UPDATE segment_executions
		SET status = ?, lease_owner = NULL, lease_until = NULL, claim_token = NULL,
			accepted_at = NULL, started_at = NULL, finished_at = NULL, last_observed_at = NULL, event_sequence = 0
		WHERE run_id IN (SELECT run_id FROM job_runs WHERE job_id = ?)
	`, dal.ExecutionStatusPending, jobID)
}

func execCatalogBenchmarkSQL(b *testing.B, ctx context.Context, db *sql.DB, query string, args ...any) {
	b.Helper()
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		b.Fatalf("exec benchmark sql: %v", err)
	}
}
