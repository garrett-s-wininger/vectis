package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
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

var terminalSnapshotBenchmarkSequence atomic.Uint64

func BenchmarkWorker_PersistTerminalExecutionSnapshot(b *testing.B) {
	for _, totalTasks := range []int{1000, 5000, 10000, 25000} {
		b.Run(fmt.Sprintf("total_tasks_%05d", totalTasks), func(b *testing.B) {
			benchmarkWorkerPersistTerminalExecutionSnapshot(b, totalTasks)
		})
	}
}

func BenchmarkWorker_PersistTerminalExecutionSnapshot_ConcurrentShallowRuns(b *testing.B) {
	for _, totalTasks := range []int{1, 5, 10, 25, 50} {
		for _, concurrency := range []int{1, 8, 32} {
			b.Run(fmt.Sprintf("total_tasks_%05d/concurrency_%03d", totalTasks, concurrency), func(b *testing.B) {
				benchmarkWorkerPersistTerminalExecutionSnapshotConcurrent(b, totalTasks, concurrency)
			})
		}
	}
}

func BenchmarkWorker_MirrorExecutionClaim(b *testing.B) {
	ctx := context.Background()

	b.Run("orchestrator_root_hot_state", func(b *testing.B) {
		db := newTerminalSnapshotBenchmarkDB(b)
		runs := dal.NewSQLRepositoriesWithCellID(db, "local").Runs()
		worker := newTerminalSnapshotBenchmarkWorker(ctx, runs, "worker-mirror-hot-state")
		env := &cell.ExecutionEnvelope{
			RunID:       "run-hot-state",
			TaskID:      "run-hot-state:root",
			TaskKey:     dal.RootTaskKey,
			ExecutionID: "execution-hot-state-root",
			CellID:      "local",
		}
		leaseUntil := time.Now().Add(dal.DefaultLeaseTTL)

		b.ReportAllocs()
		b.ResetTimer()
		start := time.Now()

		for i := 0; i < b.N; i++ {
			if err := worker.mirrorExecutionClaim(ctx, &api.Job{}, env, "claim-hot-state", leaseUntil); err != nil {
				b.Fatalf("mirror hot-state root claim: %v", err)
			}
		}

		reportBenchmarkRate(b, "claim_mirrors/s", b.N, time.Since(start))
	})

	b.Run("sql_root_mirror", func(b *testing.B) {
		db := newTerminalSnapshotBenchmarkDB(b)
		runs := dal.NewSQLRepositoriesWithCellID(db, "local").Runs()
		worker := &worker{
			runCtx:        ctx,
			logger:        interfaces.NewLogger("worker-mirror-sql"),
			workerID:      "worker-mirror-sql",
			store:         runs,
			choreographer: sqlExecutionChoreographer{runs: runs},
		}

		envs := make([]*cell.ExecutionEnvelope, 0, b.N)
		for i := 0; i < b.N; i++ {
			envs = append(envs, terminalSnapshotBenchmarkRootEnvelope(b, ctx, runs, i))
		}
		leaseUntil := time.Now().Add(dal.DefaultLeaseTTL)

		b.ReportAllocs()
		b.ResetTimer()
		start := time.Now()

		for i, env := range envs {
			if err := worker.mirrorExecutionClaim(ctx, &api.Job{}, env, fmt.Sprintf("claim-sql-%d", i), leaseUntil); err != nil {
				b.Fatalf("mirror SQL root claim: %v", err)
			}
		}

		reportBenchmarkRate(b, "claim_mirrors/s", b.N, time.Since(start))
	})
}

func BenchmarkWorker_RenewMirroredExecutionClaim(b *testing.B) {
	ctx := context.Background()

	b.Run("orchestrator_root_hot_state", func(b *testing.B) {
		db := newTerminalSnapshotBenchmarkDB(b)
		runs := dal.NewSQLRepositoriesWithCellID(db, "local").Runs()
		worker := newTerminalSnapshotBenchmarkWorker(ctx, runs, "worker-renew-hot-state")
		env := &cell.ExecutionEnvelope{
			RunID:       "run-hot-state",
			TaskID:      "run-hot-state:root",
			TaskKey:     dal.RootTaskKey,
			ExecutionID: "execution-hot-state-root",
			CellID:      "local",
		}
		leaseUntil := time.Now().Add(dal.DefaultLeaseTTL)

		b.ReportAllocs()
		b.ResetTimer()
		start := time.Now()

		for i := 0; i < b.N; i++ {
			if err := worker.renewMirroredExecutionClaim(ctx, &api.Job{}, env, "claim-hot-state", leaseUntil); err != nil {
				b.Fatalf("renew hot-state root claim: %v", err)
			}
		}

		reportBenchmarkRate(b, "claim_renews/s", b.N, time.Since(start))
	})

	b.Run("sql_root_renew", func(b *testing.B) {
		db := newTerminalSnapshotBenchmarkDB(b)
		runs := dal.NewSQLRepositoriesWithCellID(db, "local").Runs()
		worker := &worker{
			runCtx:        ctx,
			logger:        interfaces.NewLogger("worker-renew-sql"),
			workerID:      "worker-renew-sql",
			store:         runs,
			choreographer: sqlExecutionChoreographer{runs: runs},
		}

		env := terminalSnapshotBenchmarkRootEnvelope(b, ctx, runs, 0)
		claimToken := "claim-sql-renew"
		leaseUntil := time.Now().Add(dal.DefaultLeaseTTL)
		if err := worker.mirrorExecutionClaim(ctx, &api.Job{}, env, claimToken, leaseUntil); err != nil {
			b.Fatalf("seed SQL root claim: %v", err)
		}
		renewUntil := leaseUntil.Add(dal.DefaultLeaseTTL)

		b.ReportAllocs()
		b.ResetTimer()
		start := time.Now()

		for i := 0; i < b.N; i++ {
			if err := worker.renewMirroredExecutionClaim(ctx, &api.Job{}, env, claimToken, renewUntil); err != nil {
				b.Fatalf("renew SQL root claim: %v", err)
			}
		}

		reportBenchmarkRate(b, "claim_renews/s", b.N, time.Since(start))
	})
}

func benchmarkWorkerPersistTerminalExecutionSnapshot(b *testing.B, totalTasks int) {
	b.Helper()
	if totalTasks <= 0 {
		b.Fatal("totalTasks must be positive")
	}

	ctx := context.Background()
	db := newTerminalSnapshotBenchmarkDB(b)
	runs := dal.NewSQLRepositoriesWithCellID(db, "local").Runs()
	worker := newTerminalSnapshotBenchmarkWorker(ctx, runs, "worker-terminal-snapshot-bench")

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

func benchmarkWorkerPersistTerminalExecutionSnapshotConcurrent(b *testing.B, totalTasks, concurrency int) {
	b.Helper()
	if totalTasks <= 0 {
		b.Fatal("totalTasks must be positive")
	}

	if concurrency <= 0 {
		b.Fatal("concurrency must be positive")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := newTerminalSnapshotBenchmarkDB(b)
	runs := dal.NewSQLRepositoriesWithCellID(db, "local").Runs()

	fixtures := make([]dal.ExecutionFinalizationResult, 0, b.N)
	for i := 0; i < b.N; i++ {
		fixtures = append(fixtures, terminalSnapshotBenchmarkFixture(b, ctx, runs, totalTasks, i))
	}

	b.ReportAllocs()
	b.ReportMetric(float64(totalTasks), "snapshot_items")
	b.ReportMetric(float64(max(0, totalTasks-1)), "materialized_children")
	b.ReportMetric(float64(concurrency), "concurrent_workers")
	b.ResetTimer()
	start := time.Now()

	workCh := make(chan dal.ExecutionFinalizationResult)
	errCh := make(chan error, concurrency)
	var wg sync.WaitGroup
	for workerIndex := 0; workerIndex < concurrency; workerIndex++ {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()
			worker := newTerminalSnapshotBenchmarkWorker(ctx, runs, fmt.Sprintf("worker-terminal-snapshot-bench-%d", workerIndex))
			for fixture := range workCh {
				if err := worker.persistTerminalExecutionSnapshot(ctx, fixture, "", ""); err != nil {
					select {
					case errCh <- fmt.Errorf("persist terminal snapshot for run %s: %w", fixture.RunID, err):
					default:
					}
					cancel()
					return
				}
			}
		}(workerIndex)
	}

sendLoop:
	for _, fixture := range fixtures {
		select {
		case workCh <- fixture:
		case <-ctx.Done():
			break sendLoop
		}
	}

	close(workCh)
	wg.Wait()

	elapsed := time.Since(start)
	b.StopTimer()

	select {
	case err := <-errCh:
		b.Fatal(err)
	default:
	}

	if elapsed > 0 {
		totalItems := float64(totalTasks * b.N)
		b.ReportMetric(totalItems/elapsed.Seconds(), "snapshot_items/s")
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "terminal_snapshots/s")
	}
}

func newTerminalSnapshotBenchmarkWorker(ctx context.Context, runs dal.RunsRepository, workerID string) *worker {
	return &worker{
		runCtx:        ctx,
		logger:        interfaces.NewLogger(workerID),
		workerID:      workerID,
		store:         runs,
		choreographer: terminalSnapshotBenchmarkChoreographer{},
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

	jobID := terminalSnapshotBenchmarkJobID("job-terminal-snapshot", runIndex)
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

func terminalSnapshotBenchmarkRootEnvelope(b *testing.B, ctx context.Context, runs dal.RunsRepository, runIndex int) *cell.ExecutionEnvelope {
	b.Helper()

	jobID := terminalSnapshotBenchmarkJobID("job-root-claim", runIndex)
	runID, _, err := runs.CreateRun(ctx, jobID, &runIndex, 1)
	if err != nil {
		b.Fatalf("create root claim run %d: %v", runIndex, err)
	}

	root, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		b.Fatalf("get root claim execution %s: %v", runID, err)
	}

	return &cell.ExecutionEnvelope{
		RunID:       root.RunID,
		TaskID:      root.TaskID,
		TaskKey:     root.TaskKey,
		ExecutionID: root.ExecutionID,
		CellID:      root.CellID,
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

func reportBenchmarkRate(b *testing.B, name string, n int, elapsed time.Duration) {
	b.Helper()
	if elapsed <= 0 {
		return
	}

	b.ReportMetric(float64(n)/elapsed.Seconds(), name)
}

func terminalSnapshotBenchmarkJobID(prefix string, runIndex int) string {
	return fmt.Sprintf("%s-%d-%d", prefix, terminalSnapshotBenchmarkSequence.Add(1), runIndex)
}
