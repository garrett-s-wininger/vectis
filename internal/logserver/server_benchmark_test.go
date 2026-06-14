package logserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const benchmarkLogServerBufSize = 16 * 1024 * 1024

func BenchmarkLogServer_StreamIngest(b *testing.B) {
	for _, store := range []string{"noop", "local"} {
		for _, runs := range []int{1, 100} {
			for _, payloadBytes := range []int{256, 4096} {
				b.Run(fmt.Sprintf("store_%s/runs_%03d/payload_%04d", store, runs, payloadBytes), func(b *testing.B) {
					runLogServerStreamIngestBenchmark(b, store, runs, payloadBytes)
				})
			}
		}
	}
}

func runLogServerStreamIngestBenchmark(b *testing.B, storeName string, runCount, payloadBytes int) {
	b.Helper()

	store := benchmarkRunLogStore(b, storeName)
	client, cleanup := startBenchmarkLogServer(b, store)
	defer cleanup()

	ctx := context.Background()
	stream, err := client.StreamLogs(ctx)
	if err != nil {
		b.Fatalf("open log stream: %v", err)
	}

	payload := []byte(strings.Repeat("x", payloadBytes))
	runIDs := benchmarkRunIDs(runCount)
	streamType := api.Stream_STREAM_STDOUT

	b.SetBytes(int64(payloadBytes))
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		runID := runIDs[i%len(runIDs)]
		seq := int64(i + 1)
		if err := stream.Send(&api.LogChunk{
			RunId:    &runID,
			Data:     payload,
			Sequence: &seq,
			Stream:   &streamType,
		}); err != nil {
			b.Fatalf("send log chunk %d: %v", i, err)
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		b.Fatalf("close log stream: %v", err)
	}
	elapsed := time.Since(start)
	b.StopTimer()

	reportLogThroughput(b, b.N, payloadBytes, elapsed)
	b.ReportMetric(float64(runCount), "runs")
	b.ReportMetric(float64(payloadBytes), "payload_bytes")
}

func BenchmarkLocalRunLogStore_Append(b *testing.B) {
	for _, runs := range []int{1, 100} {
		for _, payloadBytes := range []int{256, 4096} {
			b.Run(fmt.Sprintf("runs_%03d/payload_%04d", runs, payloadBytes), func(b *testing.B) {
				runLocalRunLogStoreAppendBenchmark(b, runs, payloadBytes)
			})
		}
	}
}

func runLocalRunLogStoreAppendBenchmark(b *testing.B, runCount, payloadBytes int) {
	b.Helper()

	store, err := NewLocalRunLogStore(b.TempDir())
	if err != nil {
		b.Fatalf("create local log store: %v", err)
	}
	defer func() { _ = store.Close() }()

	runIDs := benchmarkRunIDs(runCount)
	entry := benchmarkLogEntry(payloadBytes)

	b.SetBytes(int64(payloadBytes))
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		entry.Sequence = int64(i + 1)
		if err := store.Append(runIDs[i%len(runIDs)], entry); err != nil {
			b.Fatalf("append log entry %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()

	reportLogThroughput(b, b.N, payloadBytes, elapsed)
	b.ReportMetric(float64(runCount), "runs")
	b.ReportMetric(float64(payloadBytes), "payload_bytes")
}

func BenchmarkLocalRunLogStore_AppendBatch(b *testing.B) {
	for _, runs := range []int{1, 100} {
		for _, payloadBytes := range []int{256, 4096} {
			b.Run(fmt.Sprintf("runs_%03d/payload_%04d/batch_256", runs, payloadBytes), func(b *testing.B) {
				runLocalRunLogStoreAppendBatchBenchmark(b, runs, payloadBytes, 256)
			})
		}
	}
}

func runLocalRunLogStoreAppendBatchBenchmark(b *testing.B, runCount, payloadBytes, batchSize int) {
	b.Helper()

	store, err := NewLocalRunLogStore(b.TempDir())
	if err != nil {
		b.Fatalf("create local log store: %v", err)
	}
	defer func() { _ = store.Close() }()

	runIDs := benchmarkRunIDs(runCount)
	entries := make([]LogEntry, batchSize)
	for i := range entries {
		entries[i] = benchmarkLogEntry(payloadBytes)
	}

	b.SetBytes(int64(payloadBytes * batchSize))
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		for j := range entries {
			entries[j].Sequence = int64(i*batchSize + j + 1)
		}

		if err := store.AppendBatch(runIDs[i%len(runIDs)], entries); err != nil {
			b.Fatalf("append log entry batch %d: %v", i, err)
		}
	}

	elapsed := time.Since(start)
	b.StopTimer()

	reportLogThroughput(b, b.N*batchSize, payloadBytes, elapsed)
	b.ReportMetric(float64(batchSize), "batch_size")
	b.ReportMetric(float64(runCount), "runs")
	b.ReportMetric(float64(payloadBytes), "payload_bytes")
}

func BenchmarkLocalRunLogStore_AppendBatchParallel(b *testing.B) {
	for _, runs := range []int{1, 100} {
		for _, payloadBytes := range []int{256, 4096} {
			b.Run(fmt.Sprintf("runs_%03d/payload_%04d/batch_256", runs, payloadBytes), func(b *testing.B) {
				runLocalRunLogStoreAppendBatchParallelBenchmark(b, runs, payloadBytes, 256)
			})
		}
	}
}

func runLocalRunLogStoreAppendBatchParallelBenchmark(b *testing.B, runCount, payloadBytes, batchSize int) {
	b.Helper()

	store, err := NewLocalRunLogStore(b.TempDir())
	if err != nil {
		b.Fatalf("create local log store: %v", err)
	}
	defer func() { _ = store.Close() }()

	runIDs := benchmarkRunIDs(runCount)
	var op atomic.Uint64
	var seq atomic.Int64

	b.SetBytes(int64(payloadBytes * batchSize))
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		entries := make([]LogEntry, batchSize)
		for i := range entries {
			entries[i] = benchmarkLogEntry(payloadBytes)
		}

		for pb.Next() {
			runID := runIDs[int(op.Add(1)-1)%len(runIDs)]
			baseSeq := seq.Add(int64(batchSize))
			for i := range entries {
				entries[i].Sequence = baseSeq - int64(batchSize) + int64(i) + 1
			}

			if err := store.AppendBatch(runID, entries); err != nil {
				b.Fatalf("append log entry batch: %v", err)
			}
		}
	})
	elapsed := time.Since(start)
	b.StopTimer()

	reportLogThroughput(b, b.N*batchSize, payloadBytes, elapsed)
	b.ReportMetric(float64(batchSize), "batch_size")
	b.ReportMetric(float64(runCount), "runs")
	b.ReportMetric(float64(payloadBytes), "payload_bytes")
}

func BenchmarkLogEntry_JSONMarshal(b *testing.B) {
	for _, payloadBytes := range []int{256, 4096} {
		b.Run(fmt.Sprintf("payload_%04d", payloadBytes), func(b *testing.B) {
			entry := benchmarkLogEntry(payloadBytes)

			b.SetBytes(int64(payloadBytes))
			b.ReportAllocs()
			b.ResetTimer()
			start := time.Now()
			for i := 0; i < b.N; i++ {
				entry.Sequence = int64(i + 1)
				if _, err := json.Marshal(entry); err != nil {
					b.Fatalf("marshal log entry: %v", err)
				}
			}
			elapsed := time.Since(start)
			b.StopTimer()

			reportLogThroughput(b, b.N, payloadBytes, elapsed)
			b.ReportMetric(float64(payloadBytes), "payload_bytes")
		})
	}
}

func benchmarkRunLogStore(b *testing.B, storeName string) RunLogStore {
	b.Helper()

	switch storeName {
	case "noop":
		return NoopRunLogStore{}
	case "local":
		store, err := NewLocalRunLogStore(b.TempDir())
		if err != nil {
			b.Fatalf("create local log store: %v", err)
		}
		b.Cleanup(func() { _ = store.Close() })
		return store
	default:
		b.Fatalf("unknown log store %q", storeName)
		return nil
	}
}

func startBenchmarkLogServer(b *testing.B, store RunLogStore) (api.LogServiceClient, func()) {
	b.Helper()

	listener := bufconn.Listen(benchmarkLogServerBufSize)
	server := grpc.NewServer()
	api.RegisterLogServiceServer(server, NewServerWithStore(mocks.NopLogger{}, store, nil))

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = server.Serve(listener)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///log-bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		server.Stop()
		_ = listener.Close()
		<-serveDone
		b.Fatalf("dial log server: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
		<-serveDone
	}

	return api.NewLogServiceClient(conn), cleanup
}

func benchmarkRunIDs(runCount int) []string {
	if runCount <= 0 {
		runCount = 1
	}

	runIDs := make([]string, 0, runCount)
	for i := range runCount {
		runIDs = append(runIDs, fmt.Sprintf("bench-run-%05d", i))
	}

	return runIDs
}

func benchmarkLogEntry(payloadBytes int) LogEntry {
	return LogEntry{
		Timestamp: time.Unix(1, 0).UTC(),
		Stream:    api.Stream_STREAM_STDOUT,
		Sequence:  1,
		Data:      strings.Repeat("x", payloadBytes),
	}
}

func reportLogThroughput(b *testing.B, chunks, payloadBytes int, elapsed time.Duration) {
	b.Helper()
	if elapsed <= 0 {
		return
	}

	payloadTotal := float64(chunks * payloadBytes)
	b.ReportMetric(float64(chunks)/elapsed.Seconds(), "chunks/s")
	b.ReportMetric(payloadTotal/elapsed.Seconds()/(1024*1024), "MiB/s")
}
