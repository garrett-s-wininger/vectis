package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"
	_ "vectis/internal/dbdrivers"
	"vectis/internal/interfaces"
	"vectis/internal/migrations"
	"vectis/internal/orchestrator"
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

	driver := os.Getenv("VECTIS_PERF_DATABASE_DRIVER")
	if driver == "" {
		driver = "sqlite3"
	}

	dsn := os.Getenv("VECTIS_PERF_DATABASE_DSN")
	if dsn == "" && driver == "sqlite3" {
		dsn = ":memory:"
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		b.Fatalf("open benchmark db: %v", err)
	}

	db.SetMaxOpenConns(terminalSnapshotBenchmarkEnvInt("VECTIS_PERF_DATABASE_MAX_OPEN_CONNS", terminalSnapshotBenchmarkDefaultMaxOpenConns(driver)))
	db.SetMaxIdleConns(terminalSnapshotBenchmarkEnvInt("VECTIS_PERF_DATABASE_MAX_IDLE_CONNS", terminalSnapshotBenchmarkDefaultMaxIdleConns(driver)))

	if err := migrations.Run(db, driver); err != nil {
		_ = db.Close()
		b.Fatalf("run migrations: %v", err)
	}

	b.Cleanup(func() { _ = db.Close() })
	return db
}

func terminalSnapshotBenchmarkDefaultMaxOpenConns(driver string) int {
	if driver == "pgx" {
		return 32
	}

	return 1
}

func terminalSnapshotBenchmarkDefaultMaxIdleConns(driver string) int {
	if driver == "pgx" {
		return 16
	}

	return 1
}

func terminalSnapshotBenchmarkEnvInt(name string, defaultValue int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return defaultValue
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return defaultValue
	}

	return value
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
