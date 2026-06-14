package api_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"vectis/internal/api"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/migrations"

	_ "github.com/mattn/go-sqlite3"
)

func BenchmarkAPIServer_RunRead_GetRun(b *testing.B) {
	for _, mode := range []string{"live", "final_facts"} {
		for _, childTasks := range []int{100, 1000, 5000} {
			b.Run(fmt.Sprintf("%s/tasks_%d", mode, childTasks), func(b *testing.B) {
				server, repos := newBenchmarkAPIServer(b)
				runID := seedBenchmarkAPIRunReadTasks(b, context.Background(), repos, mode, childTasks)

				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID, nil)
					req.SetPathValue("id", runID)
					rec := httptest.NewRecorder()

					server.GetRun(rec, req)
					if rec.Code != http.StatusOK {
						b.Fatalf("GetRun status=%d: %s", rec.Code, rec.Body.String())
					}
				}
			})
		}
	}
}

func BenchmarkAPIServer_RunRead_GetRunTasks(b *testing.B) {
	for _, mode := range []string{"live", "final_facts"} {
		for _, childTasks := range []int{100, 1000, 5000} {
			b.Run(fmt.Sprintf("%s/tasks_%d/limit_200", mode, childTasks), func(b *testing.B) {
				server, repos := newBenchmarkAPIServer(b)
				runID := seedBenchmarkAPIRunReadTasks(b, context.Background(), repos, mode, childTasks)

				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID+"/tasks?limit=200", nil)
					req.SetPathValue("id", runID)
					rec := httptest.NewRecorder()

					server.GetRunTasks(rec, req)
					if rec.Code != http.StatusOK {
						b.Fatalf("GetRunTasks status=%d: %s", rec.Code, rec.Body.String())
					}
				}
			})
		}
	}
}

func newBenchmarkAPIServer(b *testing.B) (*api.APIServer, *dal.SQLRepositories) {
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
	return api.NewAPIServer(mocks.NewMockLogger(), db), dal.NewSQLRepositories(db)
}

func seedBenchmarkAPIRunReadTasks(b *testing.B, ctx context.Context, repos *dal.SQLRepositories, mode string, childTasks int) string {
	b.Helper()

	switch mode {
	case "live":
		return seedBenchmarkAPIRunReadLiveTasks(b, ctx, repos, childTasks)
	case "final_facts":
		return seedBenchmarkAPIRunReadFinalFacts(b, ctx, repos, childTasks)
	default:
		b.Fatalf("unknown api run read benchmark mode %q", mode)
		return ""
	}
}

func seedBenchmarkAPIRunReadLiveTasks(b *testing.B, ctx context.Context, repos *dal.SQLRepositories, childTasks int) string {
	b.Helper()

	jobID := fmt.Sprintf("bench-api-run-read-live-%d", childTasks)
	if err := repos.Jobs().Create(ctx, jobID, fmt.Sprintf(benchAPIJobDefinition, jobID), 1); err != nil {
		b.Fatalf("create benchmark job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		b.Fatalf("create benchmark run: %v", err)
	}

	for i := 0; i < childTasks; i++ {
		taskKey := benchmarkAPIRunReadTaskKey(i)
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

func seedBenchmarkAPIRunReadFinalFacts(b *testing.B, ctx context.Context, repos *dal.SQLRepositories, childTasks int) string {
	b.Helper()

	jobID := fmt.Sprintf("bench-api-run-read-final-facts-%d", childTasks)
	if err := repos.Jobs().Create(ctx, jobID, fmt.Sprintf(benchAPIJobDefinition, jobID), 1); err != nil {
		b.Fatalf("create benchmark job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		b.Fatalf("create benchmark run: %v", err)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		b.Fatalf("get root execution: %v", err)
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

	for i := 0; i < childTasks; i++ {
		taskKey := benchmarkAPIRunReadTaskKey(i)
		taskID := benchmarkAPIRunReadTaskID(runID, taskKey)
		attemptID := benchmarkAPIRunReadTaskAttemptID(taskID)
		snapshots = append(snapshots, dal.TaskExecutionSnapshot{
			Record: dal.TaskExecutionRecord{
				RunID:         runID,
				ParentTaskID:  dispatch.TaskID,
				TaskKey:       taskKey,
				Name:          taskKey,
				TaskAttemptID: attemptID,
				SegmentID:     benchmarkAPIRunReadTaskSegmentID(taskID),
				ExecutionID:   benchmarkAPIRunReadTaskExecutionID(attemptID),
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

const benchAPIJobDefinition = `{"id":"%s","root":{"uses":"builtins/shell","with":{"command":"true"}}}`

func benchmarkAPIRunReadTaskKey(i int) string {
	return fmt.Sprintf("task-%06d", i)
}

func benchmarkAPIRunReadTaskID(runID, taskKey string) string {
	return runID + ":" + taskKey
}

func benchmarkAPIRunReadTaskAttemptID(taskID string) string {
	return taskID + ":attempt:1"
}

func benchmarkAPIRunReadTaskSegmentID(taskID string) string {
	return taskID + ":segment"
}

func benchmarkAPIRunReadTaskExecutionID(attemptID string) string {
	return attemptID + ":execution"
}
