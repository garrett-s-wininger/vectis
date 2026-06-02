package dal_test

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

const benchJobDefinition = `{"id":"%s","root":{"uses":"builtins/shell","with":{"command":"true"}}}`

func newBenchmarkRepos(b *testing.B) *dal.SQLRepositories {
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
	return dal.NewSQLRepositories(db)
}

func seedBenchmarkJob(b *testing.B, ctx context.Context, repos *dal.SQLRepositories, jobID string) {
	b.Helper()

	definition := fmt.Sprintf(benchJobDefinition, jobID)
	if err := repos.Jobs().Create(ctx, jobID, definition, 1); err != nil {
		b.Fatalf("create benchmark job %s: %v", jobID, err)
	}
}

func createBenchmarkRun(b *testing.B, ctx context.Context, runs dal.RunsRepository, jobID string, runIndex int) string {
	b.Helper()

	runID, _, err := runs.CreateRun(ctx, jobID, &runIndex, 1)
	if err != nil {
		b.Fatalf("create benchmark run %d: %v", runIndex, err)
	}

	return runID
}

func claimBenchmarkExecution(b *testing.B, ctx context.Context, runs dal.RunsRepository, runID string) (string, string) {
	b.Helper()

	dispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		b.Fatalf("get pending benchmark execution: %v", err)
	}

	claim, err := runs.TryClaimExecution(ctx, dispatch.ExecutionID, "bench-worker", time.Now().Add(dal.DefaultLeaseTTL))
	if err != nil {
		b.Fatalf("claim benchmark execution: %v", err)
	}

	if !claim.Claimed {
		b.Fatalf("benchmark execution %s was not claimed", dispatch.ExecutionID)
	}

	return dispatch.ExecutionID, claim.ClaimToken
}

func BenchmarkDAL_CreateRun_AutoIndex(b *testing.B) {
	ctx := context.Background()
	repos := newBenchmarkRepos(b)
	jobID := "bench-create-run"

	seedBenchmarkJob(b, ctx, repos, jobID)
	runs := repos.Runs()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, _, err := runs.CreateRun(ctx, jobID, nil, 1); err != nil {
			b.Fatalf("create run: %v", err)
		}
	}
}

func BenchmarkDAL_Idempotency_ReserveComplete(b *testing.B) {
	ctx := context.Background()
	repos := newBenchmarkRepos(b)
	idempotency := repos.Idempotency()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		if _, created, err := idempotency.Reserve(ctx, "bench-scope", key, "hash"); err != nil {
			b.Fatalf("reserve idempotency: %v", err)
		} else if !created {
			b.Fatalf("idempotency key %s already existed", key)
		}

		if err := idempotency.Complete(ctx, "bench-scope", key, `{"ok":true}`); err != nil {
			b.Fatalf("complete idempotency: %v", err)
		}
	}
}

func BenchmarkDAL_Idempotency_ReplayCompleted(b *testing.B) {
	ctx := context.Background()
	repos := newBenchmarkRepos(b)
	idempotency := repos.Idempotency()

	if _, created, err := idempotency.Reserve(ctx, "bench-scope", "key", "hash"); err != nil {
		b.Fatalf("reserve idempotency: %v", err)
	} else if !created {
		b.Fatal("expected initial idempotency reservation")
	}

	if err := idempotency.Complete(ctx, "bench-scope", "key", `{"ok":true}`); err != nil {
		b.Fatalf("complete idempotency: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rec, created, err := idempotency.Reserve(ctx, "bench-scope", "key", "hash")
		if err != nil {
			b.Fatalf("replay idempotency: %v", err)
		}

		if created || rec.ResponseJSON == nil {
			b.Fatalf("expected completed replay, created=%v response=%v", created, rec.ResponseJSON)
		}
	}
}

func BenchmarkDAL_TouchDispatched(b *testing.B) {
	ctx := context.Background()
	repos := newBenchmarkRepos(b)
	jobID := "bench-touch-dispatched"

	seedBenchmarkJob(b, ctx, repos, jobID)
	runs := repos.Runs()
	runID := createBenchmarkRun(b, ctx, runs, jobID, 1)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := runs.TouchDispatched(ctx, runID); err != nil {
			b.Fatalf("touch dispatched: %v", err)
		}
	}
}

func BenchmarkDAL_DispatchEvents_Record(b *testing.B) {
	ctx := context.Background()
	repos := newBenchmarkRepos(b)
	jobID := "bench-dispatch-events"

	seedBenchmarkJob(b, ctx, repos, jobID)
	runID := createBenchmarkRun(b, ctx, repos.Runs(), jobID, 1)
	dispatch := repos.DispatchEvents()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := dispatch.Record(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventAttempt, nil); err != nil {
			b.Fatalf("record dispatch event: %v", err)
		}
	}
}

func BenchmarkDAL_TryClaimExecution(b *testing.B) {
	ctx := context.Background()
	repos := newBenchmarkRepos(b)
	jobID := "bench-try-claim-execution"

	seedBenchmarkJob(b, ctx, repos, jobID)
	runs := repos.Runs()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		runID := createBenchmarkRun(b, ctx, runs, jobID, i+1)
		dispatch, err := runs.GetPendingExecution(ctx, runID)
		if err != nil {
			b.Fatalf("get pending execution: %v", err)
		}
		b.StartTimer()

		claim, err := runs.TryClaimExecution(ctx, dispatch.ExecutionID, "bench-worker", time.Now().Add(dal.DefaultLeaseTTL))
		if err != nil {
			b.Fatalf("try claim execution: %v", err)
		}

		if !claim.Claimed {
			b.Fatalf("execution %s was not claimed", dispatch.ExecutionID)
		}
	}
}

func BenchmarkDAL_RenewExecutionLease(b *testing.B) {
	ctx := context.Background()
	repos := newBenchmarkRepos(b)
	jobID := "bench-renew-execution-lease"

	seedBenchmarkJob(b, ctx, repos, jobID)
	runs := repos.Runs()
	runID := createBenchmarkRun(b, ctx, runs, jobID, 1)
	executionID, claimToken := claimBenchmarkExecution(b, ctx, runs, runID)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		leaseUntil := time.Now().Add(dal.DefaultLeaseTTL + time.Duration(i)*time.Second)
		if err := runs.RenewExecutionLease(ctx, executionID, "bench-worker", claimToken, leaseUntil); err != nil {
			b.Fatalf("renew execution lease: %v", err)
		}
	}
}

func BenchmarkDAL_MarkRunSucceeded(b *testing.B) {
	ctx := context.Background()
	repos := newBenchmarkRepos(b)
	jobID := "bench-mark-succeeded"

	seedBenchmarkJob(b, ctx, repos, jobID)
	runs := repos.Runs()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		runID := createBenchmarkRun(b, ctx, runs, jobID, i+1)
		claimBenchmarkExecution(b, ctx, runs, runID)
		b.StartTimer()

		if err := runs.MarkRunSucceeded(ctx, runID); err != nil {
			b.Fatalf("mark run succeeded: %v", err)
		}
	}
}

func BenchmarkDAL_ListByJob(b *testing.B) {
	for _, rows := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("rows_%d", rows), func(b *testing.B) {
			ctx := context.Background()
			repos := newBenchmarkRepos(b)
			jobID := fmt.Sprintf("bench-list-by-job-%d", rows)

			seedBenchmarkJob(b, ctx, repos, jobID)
			runs := repos.Runs()
			for i := range rows {
				createBenchmarkRun(b, ctx, runs, jobID, i+1)
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				records, _, err := runs.ListByJob(ctx, jobID, nil, nil, dal.DefaultCellID, 0, 100)
				if err != nil {
					b.Fatalf("list by job: %v", err)
				}

				if len(records) == 0 {
					b.Fatal("expected listed records")
				}
			}
		})
	}
}

func BenchmarkDAL_ListQueuedBeforeDispatchCutoff(b *testing.B) {
	for _, rows := range []int{100, 1000} {
		b.Run(fmt.Sprintf("rows_%d", rows), func(b *testing.B) {
			ctx := context.Background()
			repos := newBenchmarkRepos(b)
			jobID := fmt.Sprintf("bench-list-queued-%d", rows)

			seedBenchmarkJob(b, ctx, repos, jobID)
			runs := repos.Runs()
			for i := range rows {
				createBenchmarkRun(b, ctx, runs, jobID, i+1)
			}

			cutoff := time.Now().Unix() + 60

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				queued, err := runs.ListQueuedBeforeDispatchCutoff(ctx, cutoff)
				if err != nil {
					b.Fatalf("list queued before dispatch cutoff: %v", err)
				}

				if len(queued) != rows {
					b.Fatalf("queued rows=%d, want %d", len(queued), rows)
				}
			}
		})
	}
}
