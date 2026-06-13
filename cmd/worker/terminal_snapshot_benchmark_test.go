package main

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/migrations"
	"vectis/internal/orchestrator"

	_ "github.com/mattn/go-sqlite3"
)

func BenchmarkWorker_PersistTerminalExecutionSnapshot(b *testing.B) {
	for _, totalTasks := range []int{1000, 5000, 10000, 25000} {
		b.Run(fmt.Sprintf("total_tasks_%05d", totalTasks), func(b *testing.B) {
			benchmarkWorkerPersistTerminalExecutionSnapshot(b, totalTasks)
		})
	}
}

func benchmarkWorkerPersistTerminalExecutionSnapshot(b *testing.B, totalTasks int) {
	b.Helper()
	if totalTasks <= 0 {
		b.Fatal("totalTasks must be positive")
	}

	ctx := context.Background()
	db := newTerminalSnapshotBenchmarkDB(b)
	runs := dal.NewSQLRepositoriesWithCellID(db, "local").Runs()
	worker := &worker{
		runCtx:        ctx,
		logger:        interfaces.NewLogger("worker-terminal-snapshot-bench"),
		workerID:      "worker-terminal-snapshot-bench",
		store:         runs,
		choreographer: terminalSnapshotBenchmarkChoreographer{},
	}

	fixtures := make([]dal.ExecutionFinalizationResult, 0, b.N)
	for i := 0; i < b.N; i++ {
		fixtures = append(fixtures, terminalSnapshotBenchmarkFixture(b, ctx, runs, totalTasks, i))
	}

	b.ReportAllocs()
	b.ReportMetric(float64(totalTasks), "snapshot_items")
	b.ReportMetric(float64(max(0, totalTasks-1)), "materialized_children")
	b.ResetTimer()
	start := time.Now()

	for _, fixture := range fixtures {
		if err := worker.persistTerminalExecutionSnapshot(ctx, fixture, "", ""); err != nil {
			b.Fatalf("persist terminal snapshot for run %s: %v", fixture.RunID, err)
		}
	}

	elapsed := time.Since(start)
	b.StopTimer()
	if elapsed > 0 {
		totalItems := float64(totalTasks * b.N)
		b.ReportMetric(totalItems/elapsed.Seconds(), "snapshot_items/s")
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "terminal_snapshots/s")
	}
}

func newTerminalSnapshotBenchmarkDB(b *testing.B) *sql.DB {
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
	return db
}

func terminalSnapshotBenchmarkFixture(b *testing.B, ctx context.Context, runs dal.RunsRepository, totalTasks, runIndex int) dal.ExecutionFinalizationResult {
	b.Helper()

	jobID := fmt.Sprintf("job-terminal-snapshot-%d", runIndex)
	runID, _, err := runs.CreateRun(ctx, jobID, &runIndex, 1)
	if err != nil {
		b.Fatalf("create benchmark run %d: %v", runIndex, err)
	}

	root, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		b.Fatalf("get benchmark root execution %s: %v", runID, err)
	}

	executions := make([]dal.TaskExecutionSnapshot, 0, totalTasks)
	executions = append(executions, dal.TaskExecutionSnapshot{
		Record: terminalSnapshotRecordFromDispatch(root),
		Status: dal.ExecutionStatusSucceeded,
	})

	rootTaskID := root.TaskID
	for i := 1; i < totalTasks; i++ {
		taskKey := fmt.Sprintf("task-%05d", i)
		taskID := runID + ":" + taskKey
		taskAttemptID := taskID + ":attempt:1"
		executions = append(executions, dal.TaskExecutionSnapshot{
			Record: dal.TaskExecutionRecord{
				RunID:         runID,
				TaskID:        taskID,
				ParentTaskID:  rootTaskID,
				TaskKey:       taskKey,
				Name:          taskKey,
				TaskAttemptID: taskAttemptID,
				SegmentID:     taskID + ":segment",
				SegmentName:   taskKey,
				ExecutionID:   taskAttemptID + ":execution",
				CellID:        "local",
				Attempt:       1,
			},
			Status: dal.ExecutionStatusSucceeded,
		})
	}

	return dal.ExecutionFinalizationResult{
		RunID:      runID,
		Outcome:    dal.ExecutionFinalizationOutcomeRunSucceeded,
		Executions: executions,
	}
}

func terminalSnapshotRecordFromDispatch(dispatch dal.ExecutionDispatchRecord) dal.TaskExecutionRecord {
	return dal.TaskExecutionRecord{
		RunID:         dispatch.RunID,
		TaskID:        dispatch.TaskID,
		TaskKey:       dispatch.TaskKey,
		Name:          dispatch.TaskName,
		TaskAttemptID: dispatch.TaskAttemptID,
		SegmentID:     dispatch.SegmentID,
		SegmentName:   dispatch.SegmentName,
		ExecutionID:   dispatch.ExecutionID,
		CellID:        dispatch.CellID,
		Attempt:       dispatch.Attempt,
	}
}

type terminalSnapshotBenchmarkChoreographer struct{}

func (terminalSnapshotBenchmarkChoreographer) LoadRun(context.Context, *api.Job, *cell.ExecutionEnvelope, []orchestrator.TaskExecutionSnapshot) error {
	return nil
}

func (terminalSnapshotBenchmarkChoreographer) ClaimAndStartExecution(context.Context, *cell.ExecutionEnvelope, string, time.Time) (dal.ExecutionClaimResult, error) {
	return dal.ExecutionClaimResult{}, nil
}

func (terminalSnapshotBenchmarkChoreographer) RenewExecutionLease(context.Context, *cell.ExecutionEnvelope, string, string, time.Time) error {
	return nil
}

func (terminalSnapshotBenchmarkChoreographer) CompleteExecution(context.Context, *cell.ExecutionEnvelope, string, string, string, string, string) (dal.ExecutionFinalizationResult, error) {
	return dal.ExecutionFinalizationResult{}, nil
}

func (terminalSnapshotBenchmarkChoreographer) RequiresDurableTaskRows() bool {
	return false
}
