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
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, definition); err != nil {
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

func BenchmarkDAL_RunRead_GetRun(b *testing.B) {
	ctx := context.Background()
	repos := newBenchmarkRepos(b)
	runID := seedBenchmarkRunReadLiveTasks(b, ctx, repos, 5000)
	runs := repos.Runs()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rec, err := runs.GetRun(ctx, runID)
		if err != nil {
			b.Fatalf("get run: %v", err)
		}

		if rec.RunID != runID {
			b.Fatalf("run id=%q, want %q", rec.RunID, runID)
		}
	}
}

func BenchmarkDAL_RunRead_GetRunTaskCompletion(b *testing.B) {
	for _, mode := range []string{"live", "final_facts"} {
		for _, childTasks := range []int{10, 100, 1000, 5000} {
			b.Run(fmt.Sprintf("%s/tasks_%d", mode, childTasks), func(b *testing.B) {
				ctx := context.Background()
				repos := newBenchmarkRepos(b)
				runID := seedBenchmarkRunReadTasks(b, ctx, repos, mode, childTasks)
				runs := repos.Runs()
				wantTotal := childTasks + 1

				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					summary, err := runs.GetRunTaskCompletion(ctx, runID)
					if err != nil {
						b.Fatalf("get run task completion: %v", err)
					}

					if summary.Total != wantTotal {
						b.Fatalf("summary total=%d, want %d", summary.Total, wantTotal)
					}
				}
			})
		}
	}
}

func BenchmarkDAL_RunRead_ListRunTasks(b *testing.B) {
	for _, mode := range []string{"live", "final_facts"} {
		for _, childTasks := range []int{100, 1000, 5000} {
			for _, limit := range []int{50, 200} {
				b.Run(fmt.Sprintf("%s/tasks_%d/limit_%d", mode, childTasks, limit), func(b *testing.B) {
					ctx := context.Background()
					repos := newBenchmarkRepos(b)
					runID := seedBenchmarkRunReadTasks(b, ctx, repos, mode, childTasks)
					runs := repos.Runs()
					wantRecords := min(limit, childTasks+1)

					b.ReportAllocs()
					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						records, _, err := runs.ListRunTasks(ctx, runID, 0, limit)
						if err != nil {
							b.Fatalf("list run tasks: %v", err)
						}

						if len(records) != wantRecords {
							b.Fatalf("task records=%d, want %d", len(records), wantRecords)
						}
					}
				})
			}
		}
	}
}

func seedBenchmarkRunReadTasks(b *testing.B, ctx context.Context, repos *dal.SQLRepositories, mode string, childTasks int) string {
	b.Helper()

	switch mode {
	case "live":
		return seedBenchmarkRunReadLiveTasks(b, ctx, repos, childTasks)
	case "final_facts":
		return seedBenchmarkRunReadFinalFacts(b, ctx, repos, childTasks)
	default:
		b.Fatalf("unknown run read benchmark mode %q", mode)
		return ""
	}
}

func seedBenchmarkRunReadLiveTasks(b *testing.B, ctx context.Context, repos *dal.SQLRepositories, childTasks int) string {
	b.Helper()

	jobID := fmt.Sprintf("bench-run-read-live-%d", childTasks)
	seedBenchmarkJob(b, ctx, repos, jobID)
	runID := createBenchmarkRun(b, ctx, repos.Runs(), jobID, 1)

	for i := 0; i < childTasks; i++ {
		taskKey := benchmarkRunReadTaskKey(i)
		if _, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
			RunID:        runID,
			TaskKey:      taskKey,
			Name:         taskKey,
			SpecHash:     "bench-spec",
			TargetCellID: dal.DefaultCellID,
		}); err != nil {
			b.Fatalf("seed live task %d: %v", i, err)
		}
	}

	return runID
}

func seedBenchmarkRunReadFinalFacts(b *testing.B, ctx context.Context, repos *dal.SQLRepositories, childTasks int) string {
	b.Helper()

	jobID := fmt.Sprintf("bench-run-read-final-facts-%d", childTasks)
	seedBenchmarkJob(b, ctx, repos, jobID)
	runID := createBenchmarkRun(b, ctx, repos.Runs(), jobID, 1)

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		b.Fatalf("get root execution for final facts: %v", err)
	}

	snapshots := make([]dal.TaskExecutionSnapshot, 0, childTasks+1)
	observedAt := time.Now().UnixNano()
	snapshots = append(snapshots, dal.TaskExecutionSnapshot{
		Record: dal.TaskExecutionRecord{
			RunID:         dispatch.RunID,
			TaskID:        dispatch.TaskID,
			TaskKey:       dispatch.TaskKey,
			Name:          dispatch.TaskName,
			TaskAttemptID: dispatch.TaskAttemptID,
			SegmentID:     dispatch.SegmentID,
			ExecutionID:   dispatch.ExecutionID,
			CellID:        dispatch.CellID,
			Attempt:       dispatch.Attempt,
		},
		Status:               dal.ExecutionStatusSucceeded,
		AcceptedAtUnixNano:   observedAt,
		StartedAtUnixNano:    observedAt,
		FinishedAtUnixNano:   observedAt,
		LastObservedUnixNano: observedAt,
		EventSequence:        1,
	})

	rootTaskID := dispatch.TaskID
	for i := 0; i < childTasks; i++ {
		taskKey := benchmarkRunReadTaskKey(i)
		taskID := benchmarkRunReadTaskID(runID, taskKey)
		attemptID := benchmarkRunReadTaskAttemptID(taskID)
		snapshots = append(snapshots, dal.TaskExecutionSnapshot{
			Record: dal.TaskExecutionRecord{
				RunID:         runID,
				ParentTaskID:  rootTaskID,
				TaskKey:       taskKey,
				Name:          taskKey,
				TaskAttemptID: attemptID,
				SegmentID:     benchmarkRunReadTaskSegmentID(taskID),
				ExecutionID:   benchmarkRunReadTaskExecutionID(attemptID),
				CellID:        dal.DefaultCellID,
				Attempt:       1,
			},
			Status:               dal.ExecutionStatusSucceeded,
			AcceptedAtUnixNano:   observedAt,
			StartedAtUnixNano:    observedAt,
			FinishedAtUnixNano:   observedAt,
			LastObservedUnixNano: observedAt,
			EventSequence:        int64(i + 2),
		})
	}

	if err := repos.Runs().ApplyTerminalExecutionSnapshot(ctx, dal.TerminalExecutionSnapshotUpdate{
		RunID:      runID,
		Outcome:    dal.ExecutionFinalizationOutcomeRunSucceeded,
		Executions: snapshots,
	}); err != nil {
		b.Fatalf("apply terminal execution snapshot: %v", err)
	}

	return runID
}

func benchmarkRunReadTaskKey(i int) string {
	return fmt.Sprintf("task-%06d", i)
}

func benchmarkRunReadTaskID(runID, taskKey string) string {
	return runID + ":" + taskKey
}

func benchmarkRunReadTaskAttemptID(taskID string) string {
	return taskID + ":attempt:1"
}

func benchmarkRunReadTaskSegmentID(taskID string) string {
	return taskID + ":segment"
}

func benchmarkRunReadTaskExecutionID(attemptID string) string {
	return attemptID + ":execution"
}
