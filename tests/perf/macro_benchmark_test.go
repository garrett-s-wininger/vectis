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

func runMacroTriggerToTerminal(
	b *testing.B,
	ctx context.Context,
	env macroBenchEnv,
	jobID string,
	logSink interfaces.LogClient,
	i int,
) macroRunTimings {
	b.Helper()

	triggerStart := time.Now()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/"+jobID, nil)
	req.Header.Set("Idempotency-Key", fmt.Sprintf("%s-%d", jobID, i))

	env.handler.ServeHTTP(rec, req)
	httpAcceptedAt := time.Now()
	if rec.Code != http.StatusAccepted {
		b.Fatalf("trigger job: status=%d body=%s", rec.Code, rec.Body.String())
	}

	var resp struct {
		RunID string `json:"run_id"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		b.Fatalf("decode trigger response: %v", err)
	}

	if resp.RunID == "" {
		b.Fatalf("trigger response missing run_id: %s", rec.Body.String())
	}

	jobReq, dequeuedAt := waitForDequeuedJob(b, ctx, env.queue)
	queuedJob := jobReq.GetJob()
	if queuedJob.GetRunId() != resp.RunID {
		b.Fatalf("dequeued run_id=%q, want %q", queuedJob.GetRunId(), resp.RunID)
	}

	queueAcceptedAt := httpAcceptedAt
	if raw := jobReq.GetMetadata()[observability.JobEnqueueAcceptedUnixNanoKey]; raw != "" {
		if ns, err := parseUnixNano(raw); err == nil {
			queueAcceptedAt = time.Unix(0, ns)
		}
	}

	claimed, claimToken, err := env.runs.TryClaim(ctx, resp.RunID, "macro-worker", time.Now().Add(dal.DefaultLeaseTTL))
	claimedAt := time.Now()
	if err != nil {
		b.Fatalf("try claim run %s: %v", resp.RunID, err)
	}

	if !claimed {
		b.Fatalf("run %s was not claimed", resp.RunID)
	}

	deliveryID := queuedJob.GetDeliveryId()
	if _, err := env.queue.Ack(ctx, &apipb.AckRequest{DeliveryId: &deliveryID}); err != nil {
		b.Fatalf("ack delivery %s: %v", deliveryID, err)
	}

	logDone := make(chan job.LogStreamWaiter, 1)
	exec := job.NewExecutor()
	exec.TestLogStreamHook = logDone
	if err := exec.ExecuteJob(ctx, queuedJob, logSink, env.log); err != nil {
		b.Fatalf("execute job %s: %v", queuedJob.GetId(), err)
	}

	if err := env.runs.MarkRunSucceeded(ctx, resp.RunID, claimToken); err != nil {
		b.Fatalf("mark run succeeded %s: %v", resp.RunID, err)
	}
	terminalAt := time.Now()

	status, found, err := env.runs.GetRunStatus(ctx, resp.RunID)
	if err != nil {
		b.Fatalf("get run status %s: %v", resp.RunID, err)
	}

	if !found || status != dal.RunStatusSucceeded {
		b.Fatalf("run %s status found=%v status=%q", resp.RunID, found, status)
	}

	b.StopTimer()
	flushStarted := time.Now()
	waitForLogFlush(b, logDone)
	logFlush := time.Since(flushStarted).Nanoseconds()
	b.StartTimer()

	return macroRunTimings{
		runID:                       resp.RunID,
		httpAcceptedToQueueAccepted: max(queueAcceptedAt.Sub(httpAcceptedAt).Nanoseconds(), 0),
		queueAcceptedToDequeued:     max(dequeuedAt.Sub(queueAcceptedAt).Nanoseconds(), 0),
		dequeuedToClaimed:           max(claimedAt.Sub(dequeuedAt).Nanoseconds(), 0),
		claimedToTerminal:           max(terminalAt.Sub(claimedAt).Nanoseconds(), 0),
		acceptedToTerminal:          max(terminalAt.Sub(httpAcceptedAt).Nanoseconds(), 0),
		triggerToTerminal:           max(terminalAt.Sub(triggerStart).Nanoseconds(), 0),
		logFlush:                    max(logFlush, 0),
	}
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

	select {
	case waiter := <-ch:
		if err := waiter.WaitForDone(2 * time.Second); err != nil {
			b.Fatalf("wait for log flush: %v", err)
		}
	default:
		b.Fatal("executor did not expose log stream waiter")
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
