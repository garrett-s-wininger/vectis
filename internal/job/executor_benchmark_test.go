package job_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
	"vectis/internal/logserver"
)

type benchmarkNoopLogClient struct{}

func (benchmarkNoopLogClient) StreamLogs(context.Context) (interfaces.LogStream, error) {
	return benchmarkNoopLogStream{}, nil
}

func (benchmarkNoopLogClient) Close() error {
	return nil
}

type benchmarkNoopLogStream struct{}

func (benchmarkNoopLogStream) Send(*api.LogChunk) error {
	return nil
}

func (benchmarkNoopLogStream) CloseSend() error {
	return nil
}

type benchmarkStoreLogClient struct {
	store *logserver.LocalRunLogStore
}

func (c benchmarkStoreLogClient) StreamLogs(context.Context) (interfaces.LogStream, error) {
	return benchmarkStoreLogStream(c), nil
}

func (c benchmarkStoreLogClient) Close() error {
	return nil
}

type benchmarkStoreLogStream struct {
	store *logserver.LocalRunLogStore
}

func (s benchmarkStoreLogStream) Send(chunk *api.LogChunk) error {
	entry := logserver.LogEntry{
		Timestamp: time.Now(),
		Stream:    chunk.GetStream(),
		Sequence:  chunk.GetSequence(),
		Data:      append([]byte(nil), chunk.GetData()...),
		Completed: chunk.GetCompleted(),
	}

	if ts := chunk.GetTimestamp(); ts != nil {
		entry.Timestamp = ts.AsTime()
	}

	return s.store.Append(chunk.GetRunId(), entry)
}

func (s benchmarkStoreLogStream) CloseSend() error {
	return nil
}

func BenchmarkExecutor_ExecuteShellTrue(b *testing.B) {
	benchmarkExecutorLogSinkCases(b, benchmarkShellTrueJob)
}

func BenchmarkExecutor_ExecuteShellTrueAsyncWorkspaceCleanup(b *testing.B) {
	benchmarkExecutorLogSinkCases(b, benchmarkShellTrueJob, job.WithAsyncWorkspaceCleanup(true))
}

func BenchmarkExecutor_ExecuteResultTrue(b *testing.B) {
	benchmarkExecutorLogSinkCases(b, benchmarkResultTrueJob)
}

func BenchmarkExecutor_ExecuteResultTrueAsyncWorkspaceCleanup(b *testing.B) {
	benchmarkExecutorLogSinkCases(b, benchmarkResultTrueJob, job.WithAsyncWorkspaceCleanup(true))
}

func benchmarkExecutorLogSinkCases(b *testing.B, jobFactory func(string) *api.Job, opts ...job.ExecutorOption) {
	b.Helper()

	for _, tc := range []struct {
		name    string
		logSink func(*testing.B) interfaces.LogClient
	}{
		{name: "noop_log", logSink: func(*testing.B) interfaces.LogClient { return benchmarkNoopLogClient{} }},
		{name: "store_log", logSink: func(b *testing.B) interfaces.LogClient {
			store, err := logserver.NewLocalRunLogStore(b.TempDir())
			if err != nil {
				b.Fatalf("create log store: %v", err)
			}
			b.Cleanup(func() { _ = store.Close() })
			return benchmarkStoreLogClient{store: store}
		}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkExecutorExecuteJob(b, jobFactory, tc.logSink(b), opts...)
		})
	}
}

func benchmarkExecutorExecuteJob(b *testing.B, jobFactory func(string) *api.Job, logSink interfaces.LogClient, opts ...job.ExecutorOption) {
	b.Helper()

	ctx := context.Background()
	logger := mocks.NopLogger{}
	exec := job.NewExecutor(opts...)
	job.SetLogSpoolDirForTest(b.TempDir())

	execSamples := make([]int64, 0, b.N)
	flushSamples := make([]int64, 0, b.N)

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()

	for i := 0; i < b.N; i++ {
		runID := fmt.Sprintf("bench-exec-%d", i)
		logDone := make(chan job.LogStreamWaiter, 1)
		exec.TestLogStreamHook = logDone

		execStart := time.Now()
		if err := exec.ExecuteJob(ctx, jobFactory(runID), logSink, logger); err != nil {
			b.Fatalf("execute job %s: %v", runID, err)
		}

		execDone := time.Now()
		flushStart := time.Now()
		if err := waitForBenchmarkLogFlush(logDone); err != nil {
			b.Fatal(err)
		}

		flushDone := time.Now()
		execSamples = append(execSamples, execDone.Sub(execStart).Nanoseconds())
		flushSamples = append(flushSamples, flushDone.Sub(flushStart).Nanoseconds())
	}

	elapsed := time.Since(start)
	b.StopTimer()
	exec.TestLogStreamHook = nil
	if err := job.WaitForAsyncWorkspaceCleanupForTest(5 * time.Second); err != nil {
		b.Fatal(err)
	}

	reportBenchmarkLatencyMetrics(b, "execute", execSamples)
	reportBenchmarkLatencyMetrics(b, "log_flush", flushSamples)
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "executions/s")
	}
}

func benchmarkShellTrueJob(runID string) *api.Job {
	jobID := "bench-shell"
	nodeID := "root"
	uses := "builtins/script"

	return &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &nodeID,
			Uses: &uses,
			With: map[string]string{"script": "true"},
		},
	}
}

func benchmarkResultTrueJob(runID string) *api.Job {
	jobID := "bench-result"
	nodeID := "root"
	uses := "builtins/result"

	return &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &nodeID,
			Uses: &uses,
			With: map[string]string{"success": "true"},
		},
	}
}

func waitForBenchmarkLogFlush(ch <-chan job.LogStreamWaiter) error {
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

func reportBenchmarkLatencyMetrics(b *testing.B, prefix string, values []int64) {
	b.Helper()
	if len(values) == 0 {
		return
	}

	slices.Sort(values)
	b.ReportMetric(float64(benchmarkQuantile(values, 0.50))/float64(time.Millisecond), prefix+"_p50_ms")
	b.ReportMetric(float64(benchmarkQuantile(values, 0.95))/float64(time.Millisecond), prefix+"_p95_ms")
	b.ReportMetric(float64(benchmarkQuantile(values, 0.99))/float64(time.Millisecond), prefix+"_p99_ms")
}

func benchmarkQuantile(values []int64, q float64) int64 {
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
