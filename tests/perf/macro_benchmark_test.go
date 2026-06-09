package perf_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apipb "vectis/api/gen/go"
	"vectis/internal/api"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
	"vectis/internal/logserver"
	"vectis/internal/migrations"
	"vectis/internal/observability"
	"vectis/internal/queue"

	_ "vectis/internal/dbdrivers"
)

const (
	defaultMacroTriggerClients = 4
	defaultMacroWorkers        = 4

	envMacroDatabaseDriver       = "VECTIS_PERF_DATABASE_DRIVER"
	envMacroDatabaseDSN          = "VECTIS_PERF_DATABASE_DSN"
	envMacroDatabaseMaxOpenConns = "VECTIS_PERF_DATABASE_MAX_OPEN_CONNS"
	envMacroDatabaseMaxIdleConns = "VECTIS_PERF_DATABASE_MAX_IDLE_CONNS"
)

var macroJobSequence atomic.Uint64

type noopLogClient struct{}

func (noopLogClient) StreamLogs(context.Context) (interfaces.LogStream, error) {
	return noopLogStream{}, nil
}

func (noopLogClient) Close() error {
	return nil
}

type noopLogStream struct{}

func (noopLogStream) Send(*apipb.LogChunk) error {
	return nil
}

func (noopLogStream) CloseSend() error {
	return nil
}

type macroBenchEnv struct {
	handler  http.Handler
	queue    macroWorkerQueue
	apiQueue interfaces.QueueService
	runs     dal.RunsRepository
	log      interfaces.Logger
}

type macroWorkerQueue interface {
	TryDequeue(context.Context, *apipb.Empty) (*apipb.JobRequest, error)
	Ack(context.Context, *apipb.AckRequest) (*apipb.Empty, error)
}

type macroLogBenchEnv struct {
	macroBenchEnv
	store   *logserver.LocalRunLogStore
	logSink storeLogClient
	job     storedMacroJob
}

type macroRunTimings struct {
	runID                       string
	httpAcceptedToQueueAccepted int64
	queueAcceptedToDequeued     int64
	dequeuedToClaimed           int64
	claimedToTerminal           int64
	acceptedToTerminal          int64
	triggerToTerminal           int64
	logFlush                    int64
}

type macroTriggerInfo struct {
	runID          string
	triggerStart   time.Time
	httpAcceptedAt time.Time
}

func macroBenchmarkEnvInt(b *testing.B, name string, defaultValue int) int {
	b.Helper()

	raw := os.Getenv(name)
	if raw == "" {
		return defaultValue
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		b.Fatalf("%s must be a positive integer, got %q", name, raw)
	}

	return value
}

func macroBenchmarkTriggerClients(b *testing.B) int {
	return macroBenchmarkEnvInt(b, "VECTIS_PERF_TRIGGER_CLIENTS", defaultMacroTriggerClients)
}

func macroBenchmarkWorkers(b *testing.B) int {
	return macroBenchmarkEnvInt(b, "VECTIS_PERF_WORKERS", defaultMacroWorkers)
}

type macroDatabaseConfig struct {
	driver       string
	dsn          string
	maxOpenConns int
	maxIdleConns int
}

func macroDatabaseConfigFromEnv(b *testing.B) macroDatabaseConfig {
	b.Helper()

	driver := os.Getenv(envMacroDatabaseDriver)
	if driver == "" {
		driver = "sqlite3"
	}

	switch driver {
	case "sqlite3":
		dsn := os.Getenv(envMacroDatabaseDSN)
		if dsn == "" {
			dsn = ":memory:"
		}

		maxOpen := macroBenchmarkOptionalEnvInt(b, envMacroDatabaseMaxOpenConns, 1)
		maxIdle := macroBenchmarkOptionalEnvInt(b, envMacroDatabaseMaxIdleConns, 1)
		return macroDatabaseConfig{
			driver:       driver,
			dsn:          dsn,
			maxOpenConns: maxOpen,
			maxIdleConns: min(maxIdle, maxOpen),
		}
	case "pgx":
		dsn := os.Getenv(envMacroDatabaseDSN)
		if dsn == "" {
			b.Fatalf("%s is required when %s=pgx", envMacroDatabaseDSN, envMacroDatabaseDriver)
		}

		maxOpen := macroBenchmarkOptionalEnvInt(b, envMacroDatabaseMaxOpenConns, 32)
		maxIdle := macroBenchmarkOptionalEnvInt(b, envMacroDatabaseMaxIdleConns, min(16, maxOpen))
		return macroDatabaseConfig{
			driver:       driver,
			dsn:          dsn,
			maxOpenConns: maxOpen,
			maxIdleConns: min(maxIdle, maxOpen),
		}
	default:
		b.Fatalf("%s must be sqlite3 or pgx, got %q", envMacroDatabaseDriver, driver)
		return macroDatabaseConfig{}
	}
}

func macroBenchmarkOptionalEnvInt(b *testing.B, name string, defaultValue int) int {
	b.Helper()

	raw := os.Getenv(name)
	if raw == "" {
		return defaultValue
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		b.Fatalf("%s must be a positive integer, got %q", name, raw)
	}

	return value
}

func BenchmarkMacro_APIQueueWorker_TriggerToTerminal(b *testing.B) {
	ctx := context.Background()
	macroJob := uniqueStoredMacroJob(noopMacroJob())
	env := newMacroBenchEnv(b, []storedMacroJob{macroJob})

	acceptedToQueueSamples := make([]int64, 0, b.N)
	queueToDequeuedSamples := make([]int64, 0, b.N)
	dequeuedToClaimedSamples := make([]int64, 0, b.N)
	claimedToTerminalSamples := make([]int64, 0, b.N)
	acceptedToTerminalSamples := make([]int64, 0, b.N)
	triggerToTerminalSamples := make([]int64, 0, b.N)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		timings := runMacroTriggerToTerminal(b, ctx, env, macroJob.id, noopLogClient{}, i)
		acceptedToQueueSamples = append(acceptedToQueueSamples, timings.httpAcceptedToQueueAccepted)
		queueToDequeuedSamples = append(queueToDequeuedSamples, timings.queueAcceptedToDequeued)
		dequeuedToClaimedSamples = append(dequeuedToClaimedSamples, timings.dequeuedToClaimed)
		claimedToTerminalSamples = append(claimedToTerminalSamples, timings.claimedToTerminal)
		acceptedToTerminalSamples = append(acceptedToTerminalSamples, timings.acceptedToTerminal)
		triggerToTerminalSamples = append(triggerToTerminalSamples, timings.triggerToTerminal)
	}

	b.StopTimer()

	reportLatencyMetrics(b, "accepted_to_queue", acceptedToQueueSamples)
	reportLatencyMetrics(b, "queue_to_dequeued", queueToDequeuedSamples)
	reportLatencyMetrics(b, "dequeued_to_claimed", dequeuedToClaimedSamples)
	reportLatencyMetrics(b, "claimed_to_terminal", claimedToTerminalSamples)
	reportLatencyMetrics(b, "accepted_to_terminal", acceptedToTerminalSamples)
	reportLatencyMetrics(b, "trigger_to_terminal", triggerToTerminalSamples)

	if total := sumNanoseconds(triggerToTerminalSamples); total > 0 {
		b.ReportMetric(float64(b.N)/(float64(total)/float64(time.Second)), "terminal_runs/s")
	}
}

func BenchmarkMacro_ConcurrentNoop_TriggerToTerminal(b *testing.B) {
	runMacroConcurrentNoopTriggerToTerminalBenchmark(b)
}

func runMacroConcurrentNoopTriggerToTerminalBenchmark(b *testing.B) {
	b.Helper()

	triggerClients := macroBenchmarkTriggerClients(b)
	workerCount := macroBenchmarkWorkers(b)

	ctx := context.Background()
	macroJob := uniqueStoredMacroJob(noopMacroJob())
	env := newMacroBenchEnv(b, []storedMacroJob{macroJob})
	totalRuns := b.N

	b.ReportAllocs()
	b.ResetTimer()

	triggerRegistry := newMacroTriggerRegistry(totalRuns)
	workCtx, cancel := context.WithCancel(ctx)
	resultCh := make(chan macroWorkerResult, totalRuns)
	waitWorkers := startMacroWorkers(workCtx, env, triggerRegistry, workerCount, resultCh)
	defer func() {
		cancel()
		waitWorkers()
	}()

	workStart := time.Now()
	_, triggerDuration := triggerMacroBurst(b, ctx, env.handler, macroJob.id, triggerClients, totalRuns, triggerRegistry.add)
	triggerDone := time.Now()
	results := collectMacroWorkerResults(b, resultCh, totalRuns)
	terminalDone := time.Now()

	b.StopTimer()

	acceptedToQueueSamples := make([]int64, 0, len(results))
	queueToDequeuedSamples := make([]int64, 0, len(results))
	dequeuedToClaimedSamples := make([]int64, 0, len(results))
	claimedToTerminalSamples := make([]int64, 0, len(results))
	acceptedToTerminalSamples := make([]int64, 0, len(results))
	triggerToTerminalSamples := make([]int64, 0, len(results))
	logFlushSamples := make([]int64, 0, len(results))

	for _, timings := range results {
		acceptedToQueueSamples = append(acceptedToQueueSamples, timings.httpAcceptedToQueueAccepted)
		queueToDequeuedSamples = append(queueToDequeuedSamples, timings.queueAcceptedToDequeued)
		dequeuedToClaimedSamples = append(dequeuedToClaimedSamples, timings.dequeuedToClaimed)
		claimedToTerminalSamples = append(claimedToTerminalSamples, timings.claimedToTerminal)
		acceptedToTerminalSamples = append(acceptedToTerminalSamples, timings.acceptedToTerminal)
		triggerToTerminalSamples = append(triggerToTerminalSamples, timings.triggerToTerminal)
		logFlushSamples = append(logFlushSamples, timings.logFlush)
	}

	reportLatencyMetrics(b, "accepted_to_queue", acceptedToQueueSamples)
	reportLatencyMetrics(b, "queue_to_dequeued", queueToDequeuedSamples)
	reportLatencyMetrics(b, "dequeued_to_claimed", dequeuedToClaimedSamples)
	reportLatencyMetrics(b, "claimed_to_terminal", claimedToTerminalSamples)
	reportLatencyMetrics(b, "accepted_to_terminal", acceptedToTerminalSamples)
	reportLatencyMetrics(b, "trigger_to_terminal", triggerToTerminalSamples)
	reportLatencyMetrics(b, "log_flush", logFlushSamples)

	if triggerDuration > 0 {
		b.ReportMetric(float64(totalRuns)/triggerDuration.Seconds(), "accepted_requests/s")
	}

	if terminalDuration := terminalDone.Sub(workStart); terminalDuration > 0 {
		b.ReportMetric(float64(totalRuns)/terminalDuration.Seconds(), "terminal_runs/s")
	}

	b.ReportMetric(float64(triggerDone.Sub(workStart))/float64(time.Millisecond), "trigger_burst_ms")
	b.ReportMetric(float64(terminalDone.Sub(triggerDone))/float64(time.Millisecond), "terminal_drain_ms")
	b.ReportMetric(float64(triggerClients), "trigger_clients")
	b.ReportMetric(float64(workerCount), "worker_count")
	b.ReportMetric(float64(totalRuns), "total_runs")
}

func BenchmarkMacro_APITriggerToQueued(b *testing.B) {
	runMacroAPITriggerToQueuedBenchmark(b)
}

func BenchmarkMacro_WorkerClaimAck(b *testing.B) {
	runMacroWorkerClaimAckBenchmark(b)
}

func BenchmarkMacro_WorkerClaimAckComplete(b *testing.B) {
	runMacroWorkerClaimAckCompleteBenchmark(b)
}

func BenchmarkMacro_ResultActionWorkerClaimAckComplete(b *testing.B) {
	runMacroWorkerClaimAckCompleteBenchmarkWithWorkersAndJob(b, macroBenchmarkWorkers(b), resultMacroJob())
}

func BenchmarkMacro_WorkerClaimAckFinalize(b *testing.B) {
	runMacroWorkerClaimAckFinalizeBenchmark(b)
}

func BenchmarkMacro_WorkerScale_ClaimAckComplete(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("workers_%02d", workers), func(b *testing.B) {
			runMacroWorkerClaimAckCompleteBenchmarkWithWorkers(b, workers)
		})
	}
}

func BenchmarkMacro_WorkerScale_ResultActionClaimAckComplete(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("workers_%02d", workers), func(b *testing.B) {
			runMacroWorkerClaimAckCompleteBenchmarkWithWorkersAndJob(b, workers, resultMacroJob())
		})
	}
}

func BenchmarkMacro_LogHeavy_TriggerToTerminalReplay(b *testing.B) {
	ctx := context.Background()
	env := newMacroLogBenchEnv(b, 200)

	acceptedToQueueSamples := make([]int64, 0, b.N)
	queueToDequeuedSamples := make([]int64, 0, b.N)
	dequeuedToClaimedSamples := make([]int64, 0, b.N)
	claimedToTerminalSamples := make([]int64, 0, b.N)
	acceptedToTerminalSamples := make([]int64, 0, b.N)
	triggerToTerminalSamples := make([]int64, 0, b.N)
	logFlushSamples := make([]int64, 0, b.N)
	logReplaySamples := make([]int64, 0, b.N)
	logChunkSamples := make([]int64, 0, b.N)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		timings, logTimings := runMacroLogHeavyTriggerToTerminal(b, ctx, env, i)
		acceptedToQueueSamples = append(acceptedToQueueSamples, timings.httpAcceptedToQueueAccepted)
		queueToDequeuedSamples = append(queueToDequeuedSamples, timings.queueAcceptedToDequeued)
		dequeuedToClaimedSamples = append(dequeuedToClaimedSamples, timings.dequeuedToClaimed)
		claimedToTerminalSamples = append(claimedToTerminalSamples, timings.claimedToTerminal)
		acceptedToTerminalSamples = append(acceptedToTerminalSamples, timings.acceptedToTerminal)
		triggerToTerminalSamples = append(triggerToTerminalSamples, timings.triggerToTerminal)
		logFlushSamples = append(logFlushSamples, logTimings.flush)
		logReplaySamples = append(logReplaySamples, logTimings.replay)
		logChunkSamples = append(logChunkSamples, int64(logTimings.chunks))
	}

	b.StopTimer()

	reportLatencyMetrics(b, "accepted_to_queue", acceptedToQueueSamples)
	reportLatencyMetrics(b, "queue_to_dequeued", queueToDequeuedSamples)
	reportLatencyMetrics(b, "dequeued_to_claimed", dequeuedToClaimedSamples)
	reportLatencyMetrics(b, "claimed_to_terminal", claimedToTerminalSamples)
	reportLatencyMetrics(b, "accepted_to_terminal", acceptedToTerminalSamples)
	reportLatencyMetrics(b, "trigger_to_terminal", triggerToTerminalSamples)
	reportLatencyMetrics(b, "log_flush", logFlushSamples)
	reportLatencyMetrics(b, "log_replay", logReplaySamples)
	reportCountMetrics(b, "log_chunks", logChunkSamples)

	if total := sumNanoseconds(triggerToTerminalSamples); total > 0 {
		b.ReportMetric(float64(b.N)/(float64(total)/float64(time.Second)), "terminal_runs/s")
	}
}

type storedMacroJob struct {
	id      string
	uses    string
	with    map[string]string
	command string
}

func noopMacroJob() storedMacroJob {
	return storedMacroJob{id: "macro-noop", uses: "builtins/shell", with: map[string]string{"command": "true"}, command: "true"}
}

func resultMacroJob() storedMacroJob {
	return storedMacroJob{id: "macro-result", uses: "builtins/result", with: map[string]string{"success": "true"}}
}

func logHeavyMacroJob(lines int) storedMacroJob {
	command := fmt.Sprintf(`i=0; while [ "$i" -lt %d ]; do printf 'line-%%04d\n' "$i"; i=$((i + 1)); done`, lines)
	return storedMacroJob{
		id:      "macro-log-heavy",
		uses:    "builtins/shell",
		with:    map[string]string{"command": command},
		command: command,
	}
}

func uniqueStoredMacroJob(job storedMacroJob) storedMacroJob {
	job.id = fmt.Sprintf("%s-%d", job.id, macroJobSequence.Add(1))
	return job
}

func newMacroBenchEnv(b *testing.B, jobs []storedMacroJob) macroBenchEnv {
	b.Helper()

	dbConfig := macroDatabaseConfigFromEnv(b)
	db, err := sql.Open(dbConfig.driver, dbConfig.dsn)
	if err != nil {
		b.Fatalf("open benchmark db: %v", err)
	}

	db.SetMaxOpenConns(dbConfig.maxOpenConns)
	db.SetMaxIdleConns(dbConfig.maxIdleConns)

	if err := migrations.Run(db, dbConfig.driver); err != nil {
		_ = db.Close()
		b.Fatalf("run migrations: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })

	logger := mocks.NopLogger{}
	server := api.NewAPIServer(logger, db)
	queueService := queue.NewQueueService(logger)
	server.SetQueueClient(queueService)
	repos := dal.NewSQLRepositories(db)

	handler := server.Handler()
	for _, j := range jobs {
		seedStoredMacroJob(b, repos.Jobs(), j)
	}

	job.SetLogSpoolDirForTest(b.TempDir())

	return macroBenchEnv{
		handler:  handler,
		queue:    queueService,
		apiQueue: queueService,
		runs:     repos.Runs(),
		log:      logger,
	}
}

func runMacroAPITriggerToQueuedBenchmark(b *testing.B) {
	b.Helper()

	ctx := context.Background()
	macroJob := uniqueStoredMacroJob(noopMacroJob())
	env := newMacroBenchEnv(b, []storedMacroJob{macroJob})

	acceptedToQueueSamples := make([]int64, 0, b.N)
	triggerToQueueSamples := make([]int64, 0, b.N)
	queueToDequeuedSamples := make([]int64, 0, b.N)

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()

	for i := 0; i < b.N; i++ {
		info, err := triggerMacroJob(ctx, env.handler, macroJob.id, fmt.Sprintf("%s-queued-%d", macroJob.id, i))
		if err != nil {
			b.Fatal(err)
		}

		jobReq, dequeuedAt := waitForDequeuedJob(b, ctx, env.queue)
		queuedJob := jobReq.GetJob()
		if queuedJob.GetRunId() != info.runID {
			b.Fatalf("dequeued run_id=%q, want %q", queuedJob.GetRunId(), info.runID)
		}

		queueAcceptedAt := macroQueueAcceptedAt(jobReq, info.httpAcceptedAt)
		deliveryID := queuedJob.GetDeliveryId()
		if _, err := env.queue.Ack(ctx, &apipb.AckRequest{DeliveryId: &deliveryID}); err != nil {
			b.Fatalf("ack queued delivery %s: %v", deliveryID, err)
		}

		acceptedToQueueSamples = append(acceptedToQueueSamples, max(queueAcceptedAt.Sub(info.httpAcceptedAt).Nanoseconds(), 0))
		triggerToQueueSamples = append(triggerToQueueSamples, max(queueAcceptedAt.Sub(info.triggerStart).Nanoseconds(), 0))
		queueToDequeuedSamples = append(queueToDequeuedSamples, max(dequeuedAt.Sub(queueAcceptedAt).Nanoseconds(), 0))
	}

	elapsed := time.Since(start)
	b.StopTimer()

	reportLatencyMetrics(b, "accepted_to_queue", acceptedToQueueSamples)
	reportLatencyMetrics(b, "trigger_to_queue", triggerToQueueSamples)
	reportLatencyMetrics(b, "queue_to_dequeued", queueToDequeuedSamples)
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "queued_runs/s")
	}

	b.ReportMetric(float64(b.N), "total_runs")
}

func runMacroWorkerClaimAckBenchmark(b *testing.B) {
	b.Helper()

	runMacroWorkerClaimAckBenchmarkWithWorkers(b, macroBenchmarkWorkers(b))
}

func runMacroWorkerClaimAckBenchmarkWithWorkers(b *testing.B, workerCount int) {
	b.Helper()

	ctx := context.Background()
	macroJob := uniqueStoredMacroJob(noopMacroJob())
	env := newMacroBenchEnv(b, []storedMacroJob{macroJob})
	preseedMacroQueuedRuns(b, ctx, env, macroJob, b.N)

	resultCh := make(chan macroClaimAckResult, b.N)
	workCtx, cancel := context.WithCancel(ctx)

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	waitWorkers := startMacroClaimAckWorkers(workCtx, env, workerCount, resultCh)
	defer func() {
		cancel()
		waitWorkers()
	}()

	results := collectMacroClaimAckResults(b, resultCh, b.N)
	elapsed := time.Since(start)
	b.StopTimer()

	cancel()
	waitWorkers()

	reportMacroClaimAckMetrics(b, results)
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "dispatches/s")
	}

	b.ReportMetric(float64(workerCount), "worker_count")
	b.ReportMetric(float64(b.N), "total_runs")
}

func runMacroWorkerClaimAckCompleteBenchmark(b *testing.B) {
	b.Helper()

	runMacroWorkerClaimAckCompleteBenchmarkWithWorkers(b, macroBenchmarkWorkers(b))
}

func runMacroWorkerClaimAckCompleteBenchmarkWithWorkers(b *testing.B, workerCount int) {
	b.Helper()

	runMacroWorkerClaimAckCompleteBenchmarkWithWorkersAndJob(b, workerCount, noopMacroJob())
}

func runMacroWorkerClaimAckCompleteBenchmarkWithWorkersAndJob(
	b *testing.B,
	workerCount int,
	macroJob storedMacroJob,
) {
	b.Helper()

	ctx := context.Background()
	macroJob = uniqueStoredMacroJob(macroJob)
	env := newMacroBenchEnv(b, []storedMacroJob{macroJob})
	preseedMacroQueuedRuns(b, ctx, env, macroJob, b.N)

	resultCh := make(chan macroWorkerResult, b.N)
	workCtx, cancel := context.WithCancel(ctx)

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	waitWorkers := startMacroCompleteWorkers(workCtx, env, workerCount, resultCh)
	defer func() {
		cancel()
		waitWorkers()
	}()

	results := collectMacroWorkerResults(b, resultCh, b.N)
	elapsed := time.Since(start)
	b.StopTimer()

	cancel()
	waitWorkers()

	reportMacroRunTimingMetrics(b, results)
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "terminal_runs/s")
	}

	b.ReportMetric(float64(workerCount), "worker_count")
	b.ReportMetric(float64(b.N), "total_runs")
}

func runMacroWorkerClaimAckFinalizeBenchmark(b *testing.B) {
	b.Helper()

	runMacroWorkerClaimAckFinalizeBenchmarkWithWorkers(b, macroBenchmarkWorkers(b))
}

func runMacroWorkerClaimAckFinalizeBenchmarkWithWorkers(b *testing.B, workerCount int) {
	b.Helper()

	ctx := context.Background()
	macroJob := uniqueStoredMacroJob(noopMacroJob())
	env := newMacroBenchEnv(b, []storedMacroJob{macroJob})
	preseedMacroQueuedRuns(b, ctx, env, macroJob, b.N)

	resultCh := make(chan macroClaimAckFinalizeResult, b.N)
	workCtx, cancel := context.WithCancel(ctx)

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	waitWorkers := startMacroClaimAckFinalizeWorkers(workCtx, env, workerCount, resultCh)
	defer func() {
		cancel()
		waitWorkers()
	}()

	results := collectMacroClaimAckFinalizeResults(b, resultCh, b.N)
	elapsed := time.Since(start)
	b.StopTimer()

	cancel()
	waitWorkers()

	reportMacroClaimAckFinalizeMetrics(b, results)
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "finalized_runs/s")
	}

	b.ReportMetric(float64(workerCount), "worker_count")
	b.ReportMetric(float64(b.N), "total_runs")
}

func preseedMacroQueuedRuns(b *testing.B, ctx context.Context, env macroBenchEnv, job storedMacroJob, total int) {
	b.Helper()

	for i := 0; i < total; i++ {
		runIndex := i + 1
		runID, _, err := env.runs.CreateRun(ctx, job.id, &runIndex, 1)
		if err != nil {
			b.Fatalf("create queued run %d: %v", i, err)
		}

		req := macroJobRequest(job, runID)
		if _, err := cell.AttachPendingExecutionEnvelope(ctx, env.runs, req, runID, time.Now().UnixNano()); err != nil {
			b.Fatalf("attach execution envelope for queued run %s: %v", runID, err)
		}

		if _, err := env.apiQueue.Enqueue(ctx, req); err != nil {
			b.Fatalf("enqueue queued run %s: %v", runID, err)
		}

		if err := env.runs.TouchDispatched(ctx, runID); err != nil {
			b.Fatalf("touch dispatched queued run %s: %v", runID, err)
		}
	}
}

func macroJobRequest(job storedMacroJob, runID string) *apipb.JobRequest {
	rootID := "root"
	uses := job.uses
	if uses == "" {
		uses = "builtins/shell"
	}

	with := make(map[string]string, len(job.with))
	for k, v := range job.with {
		with[k] = v
	}

	if len(with) == 0 && job.command != "" {
		with["command"] = job.command
	}

	return &apipb.JobRequest{
		Job: &apipb.Job{
			Id:    &job.id,
			RunId: &runID,
			Root: &apipb.Node{
				Id:   &rootID,
				Uses: &uses,
				With: with,
			},
		},
	}
}

type macroClaimAckTimings struct {
	queueAcceptedToDequeued int64
	dequeuedToClaimed       int64
	claimedToAcked          int64
	dequeuedToAcked         int64
}

type macroClaimAckResult struct {
	timings macroClaimAckTimings
	err     error
}

type macroClaimAckFinalizeTimings struct {
	dequeuedToClaimed   int64
	claimedToAcked      int64
	ackedToFinalized    int64
	claimedToFinalized  int64
	dequeuedToFinalized int64
}

type macroClaimAckFinalizeResult struct {
	timings macroClaimAckFinalizeTimings
	err     error
}

func startMacroClaimAckWorkers(
	ctx context.Context,
	env macroBenchEnv,
	workers int,
	resultCh chan<- macroClaimAckResult,
) func() {
	if workers <= 0 {
		workers = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		workerID := fmt.Sprintf("macro-claim-worker-%d", i)
		wg.Add(1)

		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				jobReq, err := env.queue.TryDequeue(ctx, &apipb.Empty{})
				if err != nil {
					sendMacroClaimAckResult(ctx, resultCh, macroClaimAckResult{err: fmt.Errorf("try dequeue: %w", err)})
					return
				}

				if jobReq == nil {
					time.Sleep(10 * time.Microsecond)
					continue
				}

				dequeuedAt := time.Now()
				timings, err := finishDequeuedMacroClaimAck(ctx, env, jobReq, dequeuedAt, workerID)
				sendMacroClaimAckResult(ctx, resultCh, macroClaimAckResult{timings: timings, err: err})
				if err != nil {
					return
				}
			}
		}()
	}

	return wg.Wait
}

func startMacroClaimAckFinalizeWorkers(
	ctx context.Context,
	env macroBenchEnv,
	workers int,
	resultCh chan<- macroClaimAckFinalizeResult,
) func() {
	if workers <= 0 {
		workers = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		workerID := fmt.Sprintf("macro-finalize-worker-%d", i)
		wg.Add(1)

		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				jobReq, err := env.queue.TryDequeue(ctx, &apipb.Empty{})
				if err != nil {
					sendMacroClaimAckFinalizeResult(ctx, resultCh, macroClaimAckFinalizeResult{err: fmt.Errorf("try dequeue: %w", err)})
					return
				}

				if jobReq == nil {
					time.Sleep(10 * time.Microsecond)
					continue
				}

				dequeuedAt := time.Now()
				timings, err := finishDequeuedMacroClaimAckFinalize(ctx, env, jobReq, dequeuedAt, workerID)
				sendMacroClaimAckFinalizeResult(ctx, resultCh, macroClaimAckFinalizeResult{timings: timings, err: err})

				if err != nil {
					return
				}
			}
		}()
	}

	return wg.Wait
}

func startMacroCompleteWorkers(
	ctx context.Context,
	env macroBenchEnv,
	workers int,
	resultCh chan<- macroWorkerResult,
) func() {
	if workers <= 0 {
		workers = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		workerID := fmt.Sprintf("macro-complete-worker-%d", i)
		wg.Add(1)

		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				jobReq, err := env.queue.TryDequeue(ctx, &apipb.Empty{})
				if err != nil {
					sendMacroWorkerResult(ctx, resultCh, macroWorkerResult{err: fmt.Errorf("try dequeue: %w", err)})
					return
				}

				if jobReq == nil {
					time.Sleep(10 * time.Microsecond)
					continue
				}

				dequeuedAt := time.Now()
				runID := jobReq.GetJob().GetRunId()
				queueAcceptedAt := macroQueueAcceptedAt(jobReq, dequeuedAt)
				info := macroTriggerInfo{
					runID:          runID,
					triggerStart:   queueAcceptedAt,
					httpAcceptedAt: queueAcceptedAt,
				}

				timings, err := finishDequeuedMacroJob(ctx, env, jobReq, info, dequeuedAt, workerID, noopLogClient{})
				sendMacroWorkerResult(ctx, resultCh, macroWorkerResult{timings: timings, err: err})
				if err != nil {
					return
				}
			}
		}()
	}

	return wg.Wait
}

func finishDequeuedMacroClaimAck(
	ctx context.Context,
	env macroBenchEnv,
	jobReq *apipb.JobRequest,
	dequeuedAt time.Time,
	workerID string,
) (macroClaimAckTimings, error) {
	queuedJob := jobReq.GetJob()
	runID := queuedJob.GetRunId()
	if runID == "" {
		return macroClaimAckTimings{}, fmt.Errorf("dequeued job missing run_id")
	}

	executionEnvelope, ok, err := cell.ExecutionEnvelopeFromRequest(jobReq)
	if err != nil {
		return macroClaimAckTimings{}, fmt.Errorf("decode execution envelope: %w", err)
	}

	if !ok {
		return macroClaimAckTimings{}, fmt.Errorf("missing execution envelope")
	}

	queueAcceptedAt := macroQueueAcceptedAt(jobReq, dequeuedAt)
	executionClaim, err := env.runs.TryClaimExecution(ctx, executionEnvelope.ExecutionID, workerID, time.Now().Add(dal.DefaultLeaseTTL))
	claimedAt := time.Now()
	if err != nil {
		return macroClaimAckTimings{}, fmt.Errorf("try claim execution %s: %w", executionEnvelope.ExecutionID, err)
	}
	if !executionClaim.Claimed {
		return macroClaimAckTimings{}, fmt.Errorf("execution %s was not claimed", executionEnvelope.ExecutionID)
	}

	deliveryID := queuedJob.GetDeliveryId()
	if deliveryID == "" {
		return macroClaimAckTimings{}, fmt.Errorf("run %s missing delivery id", runID)
	}

	if _, err := env.queue.Ack(ctx, &apipb.AckRequest{DeliveryId: &deliveryID}); err != nil {
		return macroClaimAckTimings{}, fmt.Errorf("ack delivery %s: %w", deliveryID, err)
	}

	ackedAt := time.Now()

	return macroClaimAckTimings{
		queueAcceptedToDequeued: max(dequeuedAt.Sub(queueAcceptedAt).Nanoseconds(), 0),
		dequeuedToClaimed:       max(claimedAt.Sub(dequeuedAt).Nanoseconds(), 0),
		claimedToAcked:          max(ackedAt.Sub(claimedAt).Nanoseconds(), 0),
		dequeuedToAcked:         max(ackedAt.Sub(dequeuedAt).Nanoseconds(), 0),
	}, nil
}

func finishDequeuedMacroClaimAckFinalize(
	ctx context.Context,
	env macroBenchEnv,
	jobReq *apipb.JobRequest,
	dequeuedAt time.Time,
	workerID string,
) (macroClaimAckFinalizeTimings, error) {
	queuedJob := jobReq.GetJob()
	runID := queuedJob.GetRunId()
	if runID == "" {
		return macroClaimAckFinalizeTimings{}, fmt.Errorf("dequeued job missing run_id")
	}

	executionEnvelope, ok, err := cell.ExecutionEnvelopeFromRequest(jobReq)
	if err != nil {
		return macroClaimAckFinalizeTimings{}, fmt.Errorf("decode execution envelope: %w", err)
	}
	if !ok {
		return macroClaimAckFinalizeTimings{}, fmt.Errorf("missing execution envelope")
	}

	executionClaim, err := env.runs.TryClaimExecution(ctx, executionEnvelope.ExecutionID, workerID, time.Now().Add(dal.DefaultLeaseTTL))
	claimedAt := time.Now()
	if err != nil {
		return macroClaimAckFinalizeTimings{}, fmt.Errorf("try claim execution %s: %w", executionEnvelope.ExecutionID, err)
	}
	if !executionClaim.Claimed {
		return macroClaimAckFinalizeTimings{}, fmt.Errorf("execution %s was not claimed", executionEnvelope.ExecutionID)
	}

	deliveryID := queuedJob.GetDeliveryId()
	if deliveryID == "" {
		return macroClaimAckFinalizeTimings{}, fmt.Errorf("run %s missing delivery id", runID)
	}

	if _, err := env.queue.Ack(ctx, &apipb.AckRequest{DeliveryId: &deliveryID}); err != nil {
		return macroClaimAckFinalizeTimings{}, fmt.Errorf("ack delivery %s: %w", deliveryID, err)
	}

	ackedAt := time.Now()

	finalized, err := env.runs.CompleteExecutionAndFinalizeRunByClaim(ctx, executionEnvelope.ExecutionID, workerID, executionClaim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
	if err != nil {
		return macroClaimAckFinalizeTimings{}, fmt.Errorf("finalize execution %s: %w", executionEnvelope.ExecutionID, err)
	}
	if finalized.Outcome != dal.ExecutionFinalizationOutcomeRunSucceeded {
		return macroClaimAckFinalizeTimings{}, fmt.Errorf("finalize execution %s outcome %q", executionEnvelope.ExecutionID, finalized.Outcome)
	}

	finalizedAt := time.Now()

	return macroClaimAckFinalizeTimings{
		dequeuedToClaimed:   max(claimedAt.Sub(dequeuedAt).Nanoseconds(), 0),
		claimedToAcked:      max(ackedAt.Sub(claimedAt).Nanoseconds(), 0),
		ackedToFinalized:    max(finalizedAt.Sub(ackedAt).Nanoseconds(), 0),
		claimedToFinalized:  max(finalizedAt.Sub(claimedAt).Nanoseconds(), 0),
		dequeuedToFinalized: max(finalizedAt.Sub(dequeuedAt).Nanoseconds(), 0),
	}, nil
}

func collectMacroClaimAckResults(
	b *testing.B,
	resultCh <-chan macroClaimAckResult,
	total int,
) []macroClaimAckTimings {
	b.Helper()

	results := make([]macroClaimAckTimings, 0, total)
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()

	for len(results) < total {
		select {
		case result := <-resultCh:
			if result.err != nil {
				b.Fatalf("drain claim/ack queue: %v", result.err)
			}
			results = append(results, result.timings)
		case <-timeout.C:
			b.Fatalf("timed out draining claim/ack queue: got %d of %d results", len(results), total)
		}
	}

	return results
}

func collectMacroClaimAckFinalizeResults(
	b *testing.B,
	resultCh <-chan macroClaimAckFinalizeResult,
	total int,
) []macroClaimAckFinalizeTimings {
	b.Helper()

	results := make([]macroClaimAckFinalizeTimings, 0, total)
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()

	for len(results) < total {
		select {
		case result := <-resultCh:
			if result.err != nil {
				b.Fatalf("drain claim/ack/finalize queue: %v", result.err)
			}
			results = append(results, result.timings)
		case <-timeout.C:
			b.Fatalf("timed out draining claim/ack/finalize queue: got %d of %d results", len(results), total)
		}
	}

	return results
}

func sendMacroClaimAckResult(ctx context.Context, ch chan<- macroClaimAckResult, result macroClaimAckResult) {
	select {
	case ch <- result:
	case <-ctx.Done():
	}
}

func sendMacroClaimAckFinalizeResult(ctx context.Context, ch chan<- macroClaimAckFinalizeResult, result macroClaimAckFinalizeResult) {
	select {
	case ch <- result:
	case <-ctx.Done():
	}
}

func reportMacroClaimAckMetrics(b *testing.B, results []macroClaimAckTimings) {
	b.Helper()

	dequeuedToClaimedSamples := make([]int64, 0, len(results))
	claimedToAckedSamples := make([]int64, 0, len(results))
	dequeuedToAckedSamples := make([]int64, 0, len(results))

	for _, timings := range results {
		dequeuedToClaimedSamples = append(dequeuedToClaimedSamples, timings.dequeuedToClaimed)
		claimedToAckedSamples = append(claimedToAckedSamples, timings.claimedToAcked)
		dequeuedToAckedSamples = append(dequeuedToAckedSamples, timings.dequeuedToAcked)
	}

	reportLatencyMetrics(b, "dequeued_to_claimed", dequeuedToClaimedSamples)
	reportLatencyMetrics(b, "claimed_to_acked", claimedToAckedSamples)
	reportLatencyMetrics(b, "dequeued_to_acked", dequeuedToAckedSamples)
}

func reportMacroClaimAckFinalizeMetrics(b *testing.B, results []macroClaimAckFinalizeTimings) {
	b.Helper()

	dequeuedToClaimedSamples := make([]int64, 0, len(results))
	claimedToAckedSamples := make([]int64, 0, len(results))
	ackedToFinalizedSamples := make([]int64, 0, len(results))
	claimedToFinalizedSamples := make([]int64, 0, len(results))
	dequeuedToFinalizedSamples := make([]int64, 0, len(results))

	for _, timings := range results {
		dequeuedToClaimedSamples = append(dequeuedToClaimedSamples, timings.dequeuedToClaimed)
		claimedToAckedSamples = append(claimedToAckedSamples, timings.claimedToAcked)
		ackedToFinalizedSamples = append(ackedToFinalizedSamples, timings.ackedToFinalized)
		claimedToFinalizedSamples = append(claimedToFinalizedSamples, timings.claimedToFinalized)
		dequeuedToFinalizedSamples = append(dequeuedToFinalizedSamples, timings.dequeuedToFinalized)
	}

	reportLatencyMetrics(b, "dequeued_to_claimed", dequeuedToClaimedSamples)
	reportLatencyMetrics(b, "claimed_to_acked", claimedToAckedSamples)
	reportLatencyMetrics(b, "acked_to_finalized", ackedToFinalizedSamples)
	reportLatencyMetrics(b, "claimed_to_finalized", claimedToFinalizedSamples)
	reportLatencyMetrics(b, "dequeued_to_finalized", dequeuedToFinalizedSamples)
}

func reportMacroRunTimingMetrics(b *testing.B, results []macroRunTimings) {
	b.Helper()

	dequeuedToClaimedSamples := make([]int64, 0, len(results))
	claimedToTerminalSamples := make([]int64, 0, len(results))
	dequeuedToTerminalSamples := make([]int64, 0, len(results))
	logFlushSamples := make([]int64, 0, len(results))

	for _, timings := range results {
		dequeuedToClaimedSamples = append(dequeuedToClaimedSamples, timings.dequeuedToClaimed)
		claimedToTerminalSamples = append(claimedToTerminalSamples, timings.claimedToTerminal)
		dequeuedToTerminalSamples = append(dequeuedToTerminalSamples, timings.dequeuedToClaimed+timings.claimedToTerminal)
		logFlushSamples = append(logFlushSamples, timings.logFlush)
	}

	reportLatencyMetrics(b, "dequeued_to_claimed", dequeuedToClaimedSamples)
	reportLatencyMetrics(b, "claimed_to_terminal", claimedToTerminalSamples)
	reportLatencyMetrics(b, "dequeued_to_terminal", dequeuedToTerminalSamples)
	reportLatencyMetrics(b, "log_flush", logFlushSamples)
}

func macroQueueAcceptedAt(req *apipb.JobRequest, fallback time.Time) time.Time {
	if raw := req.GetMetadata()[observability.JobEnqueueAcceptedUnixNanoKey]; raw != "" {
		if ns, err := parseUnixNano(raw); err == nil && ns > 0 {
			return time.Unix(0, ns)
		}
	}

	if raw := req.GetMetadata()[observability.JobEnqueuedAtUnixNanoKey]; raw != "" {
		if ns, err := parseUnixNano(raw); err == nil && ns > 0 {
			return time.Unix(0, ns)
		}
	}

	return fallback
}

func newMacroLogBenchEnv(b *testing.B, lines int) macroLogBenchEnv {
	b.Helper()

	macroJob := uniqueStoredMacroJob(logHeavyMacroJob(lines))
	env := newMacroBenchEnv(b, []storedMacroJob{macroJob})
	store, err := logserver.NewLocalRunLogStore(b.TempDir())
	if err != nil {
		b.Fatalf("create log store: %v", err)
	}

	return macroLogBenchEnv{
		macroBenchEnv: env,
		store:         store,
		logSink:       storeLogClient{store: store, stats: newStoreLogStats()},
		job:           macroJob,
	}
}

func seedStoredMacroJob(b *testing.B, jobs dal.JobsRepository, job storedMacroJob) {
	b.Helper()

	definition, err := storedMacroJobDefinition(job)
	if err != nil {
		b.Fatalf("marshal benchmark job: %v", err)
	}

	if err := jobs.Create(context.Background(), job.id, definition, 1); err != nil {
		b.Fatalf("create benchmark job %s: %v", job.id, err)
	}
}

func storedMacroJobDefinition(job storedMacroJob) (string, error) {
	uses := job.uses
	if uses == "" {
		uses = "builtins/shell"
	}

	with := make(map[string]string, len(job.with))
	for k, v := range job.with {
		with[k] = v
	}

	if len(with) == 0 && job.command != "" {
		with["command"] = job.command
	}

	body, err := json.Marshal(map[string]any{
		"id": job.id,
		"root": map[string]any{
			"id":   "root",
			"uses": uses,
			"with": with,
		},
	})
	if err != nil {
		return "", err
	}

	return string(body), nil
}

func triggerMacroBurst(
	b *testing.B,
	ctx context.Context,
	handler http.Handler,
	jobID string,
	clients int,
	total int,
	onInfo func(macroTriggerInfo),
) ([]macroTriggerInfo, time.Duration) {
	b.Helper()

	if clients <= 0 {
		clients = 1
	}

	infos := make([]macroTriggerInfo, total)
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var firstErr error
	var errMu sync.Mutex
	setErr := func(err error) {
		if err == nil {
			return
		}

		errMu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		errMu.Unlock()
	}

	start := time.Now()
	var wg sync.WaitGroup
	for client := 0; client < clients; client++ {
		client := client

		wg.Go(func() {
			for i := client; i < total; i += clients {
				if workCtx.Err() != nil {
					return
				}

				info, err := triggerMacroJob(workCtx, handler, jobID, fmt.Sprintf("%s-concurrent-%d", jobID, i))
				if err != nil {
					setErr(err)
					return
				}

				infos[i] = info
				if onInfo != nil {
					onInfo(info)
				}
			}
		})
	}

	wg.Wait()
	duration := time.Since(start)

	errMu.Lock()
	err := firstErr
	errMu.Unlock()
	if err != nil {
		b.Fatalf("trigger burst: %v", err)
	}

	return infos, duration
}

func triggerMacroJob(ctx context.Context, handler http.Handler, jobID, idempotencyKey string) (macroTriggerInfo, error) {
	triggerStart := time.Now()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/"+jobID, nil).WithContext(ctx)
	req.Header.Set("Idempotency-Key", idempotencyKey)

	handler.ServeHTTP(rec, req)
	httpAcceptedAt := time.Now()
	if rec.Code != http.StatusAccepted {
		return macroTriggerInfo{}, fmt.Errorf("trigger job: status=%d body=%s", rec.Code, rec.Body.String())
	}

	var resp struct {
		RunID string `json:"run_id"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		return macroTriggerInfo{}, fmt.Errorf("decode trigger response: %w", err)
	}

	if resp.RunID == "" {
		return macroTriggerInfo{}, fmt.Errorf("trigger response missing run_id: %s", rec.Body.String())
	}

	return macroTriggerInfo{
		runID:          resp.RunID,
		triggerStart:   triggerStart,
		httpAcceptedAt: httpAcceptedAt,
	}, nil
}

type macroTriggerRegistry struct {
	mu      sync.Mutex
	infos   map[string]macroTriggerInfo
	timeout time.Duration
}

func newMacroTriggerRegistry(capacity int) *macroTriggerRegistry {
	return &macroTriggerRegistry{
		infos:   make(map[string]macroTriggerInfo, capacity),
		timeout: 5 * time.Second,
	}
}

func (r *macroTriggerRegistry) add(info macroTriggerInfo) {
	r.mu.Lock()
	r.infos[info.runID] = info
	r.mu.Unlock()
}

func (r *macroTriggerRegistry) wait(ctx context.Context, runID string) (macroTriggerInfo, error) {
	deadline := time.NewTimer(r.timeout)
	defer deadline.Stop()

	ticker := time.NewTicker(10 * time.Microsecond)
	defer ticker.Stop()

	for {
		r.mu.Lock()
		info, ok := r.infos[runID]
		r.mu.Unlock()

		if ok {
			return info, nil
		}

		select {
		case <-ctx.Done():
			return macroTriggerInfo{}, ctx.Err()
		case <-deadline.C:
			return macroTriggerInfo{}, fmt.Errorf("timed out waiting for trigger metadata for run %q", runID)
		case <-ticker.C:
		}
	}
}

func startMacroWorkers(
	ctx context.Context,
	env macroBenchEnv,
	triggerRegistry *macroTriggerRegistry,
	workers int,
	resultCh chan<- macroWorkerResult,
) func() {
	if workers <= 0 {
		workers = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		workerID := fmt.Sprintf("macro-worker-%d", i)

		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				jobReq, err := env.queue.TryDequeue(ctx, &apipb.DequeueRequest{})
				if err != nil {
					sendMacroWorkerResult(ctx, resultCh, macroWorkerResult{err: fmt.Errorf("try dequeue: %w", err)})
					return
				}

				if jobReq == nil {
					time.Sleep(10 * time.Microsecond)
					continue
				}

				dequeuedAt := time.Now()
				runID := jobReq.GetJob().GetRunId()
				info, err := triggerRegistry.wait(ctx, runID)
				if err != nil {
					sendMacroWorkerResult(ctx, resultCh, macroWorkerResult{err: err})
					return
				}

				timings, err := finishDequeuedMacroJob(ctx, env, jobReq, info, dequeuedAt, workerID, noopLogClient{})
				sendMacroWorkerResult(ctx, resultCh, macroWorkerResult{timings: timings, err: err})

				if err != nil {
					return
				}
			}
		})
	}

	return wg.Wait
}

func collectMacroWorkerResults(
	b *testing.B,
	resultCh <-chan macroWorkerResult,
	total int,
) []macroRunTimings {
	b.Helper()

	results := make([]macroRunTimings, 0, total)
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()

	for len(results) < total {
		select {
		case result := <-resultCh:
			if result.err != nil {
				b.Fatalf("drain macro queue: %v", result.err)
			}

			results = append(results, result.timings)
		case <-timeout.C:
			b.Fatalf("timed out draining macro queue: got %d of %d results", len(results), total)
		}
	}

	return results
}

type macroWorkerResult struct {
	timings macroRunTimings
	err     error
}

func sendMacroWorkerResult(ctx context.Context, ch chan<- macroWorkerResult, result macroWorkerResult) {
	select {
	case ch <- result:
	case <-ctx.Done():
	}
}

func runMacroTriggerToTerminal(
	b *testing.B,
	ctx context.Context,
	env macroBenchEnv,
	jobID string,
	logSink interfaces.LogClient,
	i int,
) macroRunTimings {
	b.Helper()

	info, err := triggerMacroJob(ctx, env.handler, jobID, fmt.Sprintf("%s-%d", jobID, i))
	if err != nil {
		b.Fatal(err)
	}

	jobReq, dequeuedAt := waitForDequeuedJob(b, ctx, env.queue)
	timings, err := finishDequeuedMacroJob(ctx, env, jobReq, info, dequeuedAt, "macro-worker", logSink)
	if err != nil {
		b.Fatal(err)
	}

	return timings
}

func finishDequeuedMacroJob(
	ctx context.Context,
	env macroBenchEnv,
	jobReq *apipb.JobRequest,
	info macroTriggerInfo,
	dequeuedAt time.Time,
	workerID string,
	logSink interfaces.LogClient,
) (macroRunTimings, error) {
	queuedJob := jobReq.GetJob()
	if queuedJob.GetRunId() != info.runID {
		return macroRunTimings{}, fmt.Errorf("dequeued run_id=%q, want %q", queuedJob.GetRunId(), info.runID)
	}

	executionEnvelope, ok, err := cell.ExecutionEnvelopeFromRequest(jobReq)
	if err != nil {
		return macroRunTimings{}, fmt.Errorf("decode execution envelope: %w", err)
	}

	if !ok {
		return macroRunTimings{}, fmt.Errorf("missing execution envelope")
	}

	queueAcceptedAt := info.httpAcceptedAt
	if raw := jobReq.GetMetadata()[observability.JobEnqueueAcceptedUnixNanoKey]; raw != "" {
		if ns, err := parseUnixNano(raw); err == nil {
			queueAcceptedAt = time.Unix(0, ns)
		}
	}

	deliveryID := queuedJob.GetDeliveryId()
	if _, err := env.queue.Ack(ctx, &apipb.AckRequest{DeliveryId: &deliveryID}); err != nil {
		return macroRunTimings{}, fmt.Errorf("ack delivery %s: %w", deliveryID, err)
	}

	executionClaim, err := env.runs.TryClaimExecution(ctx, executionEnvelope.ExecutionID, workerID, time.Now().Add(dal.DefaultLeaseTTL))
	claimedAt := time.Now()
	if err != nil {
		return macroRunTimings{}, fmt.Errorf("try claim execution %s: %w", executionEnvelope.ExecutionID, err)
	}

	if !executionClaim.Claimed {
		return macroRunTimings{}, fmt.Errorf("execution %s was not claimed", executionEnvelope.ExecutionID)
	}

	logDone := make(chan job.LogStreamWaiter, 1)
	exec := job.NewExecutor()
	exec.TestLogStreamHook = logDone
	if err := env.runs.MarkExecutionStarted(ctx, executionEnvelope.ExecutionID); err != nil {
		return macroRunTimings{}, fmt.Errorf("mark execution started %s: %w", executionEnvelope.ExecutionID, err)
	}

	if err := exec.ExecuteTask(ctx, queuedJob, executionEnvelope.TaskKey, logSink, env.log); err != nil {
		_, _ = env.runs.CompleteExecutionAndFinalizeRunByClaim(ctx, executionEnvelope.ExecutionID, workerID, executionClaim.ClaimToken, dal.ExecutionStatusFailed, dal.FailureCodeExecution, err.Error())
		return macroRunTimings{}, fmt.Errorf("execute task %s: %w", queuedJob.GetId(), err)
	}

	finalized, err := env.runs.CompleteExecutionAndFinalizeRunByClaim(ctx, executionEnvelope.ExecutionID, workerID, executionClaim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
	if err != nil {
		return macroRunTimings{}, fmt.Errorf("finalize execution %s: %w", executionEnvelope.ExecutionID, err)
	}
	if finalized.Outcome != dal.ExecutionFinalizationOutcomeRunSucceeded {
		return macroRunTimings{}, fmt.Errorf("finalize execution %s outcome %q", executionEnvelope.ExecutionID, finalized.Outcome)
	}

	terminalAt := time.Now()
	status, found, err := env.runs.GetRunStatus(ctx, info.runID)
	if err != nil {
		return macroRunTimings{}, fmt.Errorf("get run status %s: %w", info.runID, err)
	}

	if !found || status != dal.RunStatusSucceeded {
		return macroRunTimings{}, fmt.Errorf("run %s status found=%v status=%q", info.runID, found, status)
	}

	flushStarted := time.Now()
	if err := waitForLogFlushErr(logDone); err != nil {
		return macroRunTimings{}, err
	}

	logFlush := time.Since(flushStarted).Nanoseconds()

	return macroRunTimings{
		runID:                       info.runID,
		httpAcceptedToQueueAccepted: max(queueAcceptedAt.Sub(info.httpAcceptedAt).Nanoseconds(), 0),
		queueAcceptedToDequeued:     max(dequeuedAt.Sub(queueAcceptedAt).Nanoseconds(), 0),
		dequeuedToClaimed:           max(claimedAt.Sub(dequeuedAt).Nanoseconds(), 0),
		claimedToTerminal:           max(terminalAt.Sub(claimedAt).Nanoseconds(), 0),
		acceptedToTerminal:          max(terminalAt.Sub(info.httpAcceptedAt).Nanoseconds(), 0),
		triggerToTerminal:           max(terminalAt.Sub(info.triggerStart).Nanoseconds(), 0),
		logFlush:                    max(logFlush, 0),
	}, nil
}

type macroLogTimings struct {
	flush  int64
	replay int64
	chunks int
}

func runMacroLogHeavyTriggerToTerminal(
	b *testing.B,
	ctx context.Context,
	env macroLogBenchEnv,
	i int,
) (macroRunTimings, macroLogTimings) {
	b.Helper()

	timings := runMacroTriggerToTerminal(b, ctx, env.macroBenchEnv, env.job.id, env.logSink, i)
	runID := timings.runID
	if runID == "" {
		b.Fatal("log-heavy run did not return a run id")
	}

	entries := env.logSink.entryCount(runID)
	if entries == 0 {
		b.Fatalf("log-heavy run %s did not store log entries", runID)
	}

	replayStarted := time.Now()
	replayed, err := env.store.List(runID)
	replayDone := time.Now()
	if err != nil {
		b.Fatalf("replay logs for run %s: %v", runID, err)
	}

	if len(replayed) != entries {
		b.Fatalf("replayed entries=%d, stored entries=%d", len(replayed), entries)
	}

	return timings, macroLogTimings{
		flush:  timings.logFlush,
		replay: replayDone.Sub(replayStarted).Nanoseconds(),
		chunks: len(replayed),
	}
}

type storeLogClient struct {
	store *logserver.LocalRunLogStore
	stats *storeLogStats
}

type storeLogStats struct {
	mu     sync.Mutex
	counts map[string]int
}

func newStoreLogStats() *storeLogStats {
	return &storeLogStats{counts: make(map[string]int)}
}

func (c storeLogClient) StreamLogs(context.Context) (interfaces.LogStream, error) {
	return &storeLogStream{store: c.store, stats: c.stats}, nil
}

func (c storeLogClient) Close() error {
	return nil
}

func (c storeLogClient) entryCount(runID string) int {
	c.stats.mu.Lock()
	defer c.stats.mu.Unlock()
	return c.stats.counts[runID]
}

type storeLogStream struct {
	store *logserver.LocalRunLogStore
	stats *storeLogStats
}

func (s *storeLogStream) Send(chunk *apipb.LogChunk) error {
	runID := chunk.GetRunId()
	entry := logserver.LogEntry{
		Timestamp: time.Now(),
		Stream:    chunk.GetStream(),
		Sequence:  chunk.GetSequence(),
		Data:      string(chunk.GetData()),
		Completed: chunk.GetCompleted(),
	}

	if ts := chunk.GetTimestamp(); ts != nil {
		entry.Timestamp = ts.AsTime()
	}

	if err := s.store.Append(runID, entry); err != nil {
		return err
	}

	s.stats.mu.Lock()
	s.stats.counts[runID]++
	s.stats.mu.Unlock()
	return nil
}

func (s *storeLogStream) CloseSend() error {
	return nil
}

func waitForDequeuedJob(b *testing.B, ctx context.Context, queueService macroWorkerQueue) (*apipb.JobRequest, time.Time) {
	b.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		jobReq, err := queueService.TryDequeue(ctx, &apipb.DequeueRequest{})
		if err != nil {
			b.Fatalf("try dequeue: %v", err)
		}

		if jobReq != nil {
			return jobReq, time.Now()
		}

		time.Sleep(10 * time.Microsecond)
	}

	b.Fatal("timed out waiting for async enqueue")
	return nil, time.Time{}
}

func waitForLogFlush(b *testing.B, ch <-chan job.LogStreamWaiter) {
	b.Helper()

	if err := waitForLogFlushErr(ch); err != nil {
		b.Fatal(err)
	}
}

func waitForLogFlushErr(ch <-chan job.LogStreamWaiter) error {
	select {
	case waiter := <-ch:
		if err := waiter.WaitForDone(2 * time.Second); err != nil {
			return fmt.Errorf("wait for log flush: %w", err)
		}

		return nil
	default:
		return fmt.Errorf("executor did not expose log stream waiter")
	}
}

func parseUnixNano(raw string) (int64, error) {
	return strconv.ParseInt(raw, 10, 64)
}

func reportLatencyMetrics(b *testing.B, prefix string, values []int64) {
	b.Helper()
	if len(values) == 0 {
		return
	}

	slices.Sort(values)
	b.ReportMetric(float64(quantile(values, 0.50))/float64(time.Millisecond), prefix+"_p50_ms")
	b.ReportMetric(float64(quantile(values, 0.95))/float64(time.Millisecond), prefix+"_p95_ms")
	b.ReportMetric(float64(quantile(values, 0.99))/float64(time.Millisecond), prefix+"_p99_ms")
}

func reportCountMetrics(b *testing.B, prefix string, values []int64) {
	b.Helper()
	if len(values) == 0 {
		return
	}

	slices.Sort(values)
	b.ReportMetric(float64(quantile(values, 0.50)), prefix+"_p50")
	b.ReportMetric(float64(quantile(values, 0.95)), prefix+"_p95")
	b.ReportMetric(float64(quantile(values, 0.99)), prefix+"_p99")
}

func sumNanoseconds(values []int64) int64 {
	var total int64
	for _, value := range values {
		total += value
	}

	return total
}

func quantile(values []int64, q float64) int64 {
	if len(values) == 0 {
		return 0
	}

	if q <= 0 {
		return values[0]
	}

	if q >= 1 {
		return values[len(values)-1]
	}

	return values[int(float64(len(values)-1)*q)]
}
