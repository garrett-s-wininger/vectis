package perf_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync"
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

	_ "github.com/mattn/go-sqlite3"
)

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
	handler http.Handler
	queue   apipb.QueueServiceServer
	runs    dal.RunsRepository
	log     interfaces.Logger
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

func BenchmarkMacro_APIQueueWorker_TriggerToTerminal(b *testing.B) {
	ctx := context.Background()
	env := newMacroBenchEnv(b, []storedMacroJob{noopMacroJob()})

	acceptedToQueueSamples := make([]int64, 0, b.N)
	queueToDequeuedSamples := make([]int64, 0, b.N)
	dequeuedToClaimedSamples := make([]int64, 0, b.N)
	claimedToTerminalSamples := make([]int64, 0, b.N)
	acceptedToTerminalSamples := make([]int64, 0, b.N)
	triggerToTerminalSamples := make([]int64, 0, b.N)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		timings := runMacroTriggerToTerminal(b, ctx, env, noopMacroJob().id, noopLogClient{}, i)
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
	const (
		triggerClients = 4
		workerCount    = 4
	)

	ctx := context.Background()
	env := newMacroBenchEnv(b, []storedMacroJob{noopMacroJob()})
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
	_, triggerDuration := triggerMacroBurst(b, ctx, env.handler, noopMacroJob().id, triggerClients, totalRuns, triggerRegistry.add)
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
	command string
}

func noopMacroJob() storedMacroJob {
	return storedMacroJob{id: "macro-noop", command: "true"}
}

func logHeavyMacroJob(lines int) storedMacroJob {
	return storedMacroJob{
		id:      "macro-log-heavy",
		command: fmt.Sprintf(`i=0; while [ "$i" -lt %d ]; do printf 'line-%%04d\n' "$i"; i=$((i + 1)); done`, lines),
	}
}

func newMacroBenchEnv(b *testing.B, jobs []storedMacroJob) macroBenchEnv {
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

	logger := mocks.NopLogger{}
	queueService := queue.NewQueueService(logger)
	server := api.NewAPIServer(logger, db)
	server.SetQueueClient(queueService)

	handler := server.Handler()
	for _, j := range jobs {
		createStoredMacroJob(b, handler, j)
	}

	job.SetLogSpoolDirForTest(b.TempDir())

	return macroBenchEnv{
		handler: handler,
		queue:   queueService,
		runs:    dal.NewSQLRepositories(db).Runs(),
		log:     logger,
	}
}

func newMacroLogBenchEnv(b *testing.B, lines int) macroLogBenchEnv {
	b.Helper()

	env := newMacroBenchEnv(b, []storedMacroJob{logHeavyMacroJob(lines)})
	store, err := logserver.NewLocalRunLogStore(b.TempDir())
	if err != nil {
		b.Fatalf("create log store: %v", err)
	}
	macroJob := logHeavyMacroJob(lines)

	return macroLogBenchEnv{
		macroBenchEnv: env,
		store:         store,
		logSink:       storeLogClient{store: store, stats: newStoreLogStats()},
		job:           macroJob,
	}
}

func createStoredMacroJob(b *testing.B, handler http.Handler, job storedMacroJob) {
	b.Helper()

	body, err := json.Marshal(map[string]any{
		"id": job.id,
		"root": map[string]any{
			"id":   "root",
			"uses": "builtins/shell",
			"with": map[string]string{"command": job.command},
		},
	})

	if err != nil {
		b.Fatalf("marshal benchmark job: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		b.Fatalf("create benchmark job %s: status=%d body=%s", job.id, rec.Code, rec.Body.String())
	}
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
		wg.Add(1)

		go func() {
			defer wg.Done()
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
		}()
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
		}()
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

func waitForDequeuedJob(b *testing.B, ctx context.Context, queueService apipb.QueueServiceServer) (*apipb.JobRequest, time.Time) {
	b.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		jobReq, err := queueService.TryDequeue(ctx, &apipb.Empty{})
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
