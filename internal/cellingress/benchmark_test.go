package cellingress

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/migrations"

	_ "github.com/mattn/go-sqlite3"
)

const cellIngressBenchmarkJobID = "bench-cell-ingress"

func BenchmarkCellIngress_AcceptExecution_NewRuns(b *testing.B) {
	ctx := context.Background()
	_, repos := newCellIngressBenchmarkDBAndRepos(b, "iad-a")
	acceptances := repos.CellExecutionAcceptances()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		created, err := acceptances.AcceptExecution(ctx, cellIngressBenchmarkAcceptance(cellIngressBenchmarkJobID, i))
		if err != nil {
			b.Fatalf("accept execution: %v", err)
		}

		if !created {
			b.Fatalf("execution %d was not newly accepted", i)
		}
	}
}

func BenchmarkCellIngress_AcceptExecution_Replay(b *testing.B) {
	ctx := context.Background()
	_, repos := newCellIngressBenchmarkDBAndRepos(b, "iad-a")
	acceptances := repos.CellExecutionAcceptances()
	acceptance := cellIngressBenchmarkAcceptance(cellIngressBenchmarkJobID, 0)
	if created, err := acceptances.AcceptExecution(ctx, acceptance); err != nil {
		b.Fatalf("seed accepted execution: %v", err)
	} else if !created {
		b.Fatal("seed acceptance was not created")
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		created, err := acceptances.AcceptExecution(ctx, acceptance)
		if err != nil {
			b.Fatalf("replay accepted execution: %v", err)
		}

		if created {
			b.Fatal("replay unexpectedly created a new acceptance")
		}
	}
}

func BenchmarkCellIngress_RepairHandoffListAndMark(b *testing.B) {
	for _, tc := range cellIngressBenchmarkCases() {
		b.Run(cellIngressBenchmarkCaseName(tc.backlog, tc.limit), func(b *testing.B) {
			ctx := context.Background()
			db, repos := newCellIngressBenchmarkDBAndRepos(b, "iad-a")
			seedCellIngressBenchmarkAcceptances(b, ctx, repos.CellExecutionAcceptances(), tc.backlog)
			acceptances := repos.CellExecutionAcceptances()
			cutoff := time.Now().UnixNano()
			want := min(tc.backlog, tc.limit)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				resetCellIngressBenchmarkHandoffs(b, ctx, db)
				b.StartTimer()

				handoffs, err := acceptances.ListPendingQueueHandoffs(ctx, cutoff, tc.limit)
				if err != nil {
					b.Fatalf("list pending queue handoffs: %v", err)
				}

				if len(handoffs) != want {
					b.Fatalf("handoffs=%d, want %d", len(handoffs), want)
				}

				enqueuedAt := time.Now().UnixNano()
				for _, handoff := range handoffs {
					if err := acceptances.MarkEnqueued(ctx, handoff.ExecutionID, enqueuedAt); err != nil {
						b.Fatalf("mark enqueued: %v", err)
					}
				}
			}
		})
	}
}

type cellIngressBenchmarkCase struct {
	backlog int
	limit   int
}

func cellIngressBenchmarkCases() []cellIngressBenchmarkCase {
	return []cellIngressBenchmarkCase{
		{backlog: 100, limit: 100},
		{backlog: 1000, limit: 100},
		{backlog: 5000, limit: 100},
		{backlog: 1000, limit: 1000},
		{backlog: 5000, limit: 1000},
	}
}

func cellIngressBenchmarkCaseName(backlog, limit int) string {
	return fmt.Sprintf("backlog_%d/limit_%d", backlog, limit)
}

func newCellIngressBenchmarkDBAndRepos(b *testing.B, cellID string) (*sql.DB, *dal.SQLRepositories) {
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

func seedCellIngressBenchmarkAcceptances(b *testing.B, ctx context.Context, acceptances dal.CellExecutionAcceptancesRepository, count int) {
	b.Helper()

	for i := 0; i < count; i++ {
		if created, err := acceptances.AcceptExecution(ctx, cellIngressBenchmarkAcceptance(cellIngressBenchmarkJobID, i)); err != nil {
			b.Fatalf("seed accepted execution %d: %v", i, err)
		} else if !created {
			b.Fatalf("seed accepted execution %d was not created", i)
		}
	}
}

func cellIngressBenchmarkAcceptance(jobID string, i int) dal.CellExecutionAcceptance {
	runID := fmt.Sprintf("run-%06d", i)
	taskID := runID + ":" + dal.RootTaskKey
	attempt := 1
	definitionJSON := fmt.Sprintf(`{"id":"%s","root":{"uses":"builtins/shell","with":{"command":"true"}}}`, jobID)
	executionID := fmt.Sprintf("execution-%06d", i)

	return dal.CellExecutionAcceptance{
		ExecutionID:        executionID,
		RunID:              runID,
		JobID:              jobID,
		RunIndex:           i + 1,
		TaskID:             taskID,
		TaskKey:            dal.RootTaskKey,
		TaskName:           dal.RootTaskKey,
		TaskAttemptID:      fmt.Sprintf("%s:attempt:%d", taskID, attempt),
		SegmentID:          fmt.Sprintf("segment-%06d", i),
		SegmentName:        dal.RootTaskKey,
		CellID:             "iad-a",
		Attempt:            attempt,
		DefinitionVersion:  1,
		DefinitionHash:     dal.DefinitionHash(definitionJSON),
		DefinitionJSON:     definitionJSON,
		RequestJSON:        fmt.Sprintf(`{"job":{"id":"%s","runId":"%s","root":{"uses":"builtins/shell","with":{"command":"true"}}},"metadata":{"benchExecution":"%s"}}`, jobID, runID, executionID),
		AcceptedAtUnixNano: int64(i + 1),
	}
}

func resetCellIngressBenchmarkHandoffs(b *testing.B, ctx context.Context, db *sql.DB) {
	b.Helper()

	if _, err := db.ExecContext(ctx, `
		UPDATE cell_execution_acceptances
		SET enqueued_at = NULL,
			last_enqueue_attempt_at = NULL,
			enqueue_attempts = 0,
			last_enqueue_error = NULL,
			updated_at = CURRENT_TIMESTAMP
	`); err != nil {
		b.Fatalf("reset benchmark handoffs: %v", err)
	}
}
