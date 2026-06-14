package reconciler

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/migrations"

	_ "github.com/mattn/go-sqlite3"
)

func BenchmarkReconciler_ProcessQueuedRedispatch(b *testing.B) {
	for _, queuedRuns := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("queued_%d/limit_%d", queuedRuns, QueuedRedispatchLimit), func(b *testing.B) {
			ctx := context.Background()
			db, repos := newBenchmarkReconcilerDBAndRepos(b)
			jobID := fmt.Sprintf("bench-reconciler-process-%d", queuedRuns)
			seedBenchmarkReconcilerJob(b, ctx, repos, jobID)
			seedBenchmarkReconcilerRuns(b, ctx, repos.Runs(), jobID, queuedRuns)

			clock := mocks.NewMockClock()
			clock.SetNow(time.Date(2026, 6, 14, 12, 0, 0, 0, time.UTC))
			svc := NewService(mocks.NewMockLogger(), db, mocks.NewMockQueueService(), clock)
			svc.SetMinDispatchGap(time.Nanosecond)
			svc.SetExecutionIngress(benchmarkNoopExecutionIngress{})

			resetBenchmarkReconcilerQueuedRuns(b, ctx, db, jobID)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := svc.Process(ctx); err != nil {
					b.Fatalf("process reconciler: %v", err)
				}

				b.StopTimer()
				resetBenchmarkReconcilerQueuedRuns(b, ctx, db, jobID)
				b.StartTimer()
			}
		})
	}
}

type benchmarkNoopExecutionIngress struct{}

func (benchmarkNoopExecutionIngress) SubmitExecution(context.Context, cell.ExecutionSubmission) error {
	return nil
}

func newBenchmarkReconcilerDBAndRepos(b *testing.B) (*sql.DB, *dal.SQLRepositories) {
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
	return db, dal.NewSQLRepositories(db)
}

func seedBenchmarkReconcilerJob(b *testing.B, ctx context.Context, repos *dal.SQLRepositories, jobID string) {
	b.Helper()

	if err := repos.Jobs().Create(ctx, jobID, fmt.Sprintf(benchmarkReconcilerJobDefinition, jobID), 1); err != nil {
		b.Fatalf("create benchmark job: %v", err)
	}
}

func seedBenchmarkReconcilerRuns(b *testing.B, ctx context.Context, runs dal.RunsRepository, jobID string, count int) {
	b.Helper()

	for i := 0; i < count; i++ {
		runIndex := i + 1
		if _, _, err := runs.CreateRun(ctx, jobID, &runIndex, 1); err != nil {
			b.Fatalf("create benchmark run %d: %v", runIndex, err)
		}
	}
}

func resetBenchmarkReconcilerQueuedRuns(b *testing.B, ctx context.Context, db *sql.DB, jobID string) {
	b.Helper()

	if _, err := db.ExecContext(ctx, `
		UPDATE job_runs
		SET status = ?,
			last_dispatched_at = NULL,
			orphan_reason = '',
			failure_code = '',
			failure_reason = NULL,
			finished_at = NULL
		WHERE job_id = ?
	`, dal.RunStatusQueued, jobID); err != nil {
		b.Fatalf("reset queued benchmark runs: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		DELETE FROM run_dispatch_events
		WHERE run_id IN (SELECT run_id FROM job_runs WHERE job_id = ?)
	`, jobID); err != nil {
		b.Fatalf("reset dispatch events: %v", err)
	}
}

const benchmarkReconcilerJobDefinition = `{"id":"%s","root":{"uses":"builtins/shell","with":{"command":"true"}}}`
