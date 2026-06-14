package logforwarder

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/logserver"
	"vectis/internal/testutil/socktest"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

const benchmarkForwarderLogServerBufSize = 16 * 1024 * 1024

func BenchmarkLogForwarder_SocketOffload(b *testing.B) {
	for _, runs := range []int{1, 100} {
		for _, payloadBytes := range []int{256, 4096} {
			b.Run(fmt.Sprintf("runs_%03d/payload_%04d", runs, payloadBytes), func(b *testing.B) {
				sink := &benchmarkRecordingLogClient{}
				runLogForwarderSocketBenchmark(b, sink, func() {}, runs, payloadBytes)
			})
		}
	}
}

func BenchmarkLogForwarder_SocketToLogServer(b *testing.B) {
	for _, store := range []string{"noop", "local"} {
		for _, runs := range []int{1, 100} {
			b.Run(fmt.Sprintf("store_%s/runs_%03d/payload_4096", store, runs), func(b *testing.B) {
				client, cleanup := startBenchmarkForwarderLogServer(b, store)
				runLogForwarderSocketBenchmark(b, client, cleanup, runs, 4096)
			})
		}
	}
}

func runLogForwarderSocketBenchmark(
	b *testing.B,
	client interfaces.LogClient,
	cleanupClient func(),
	runCount int,
	payloadBytes int,
) {
	b.Helper()
	defer cleanupClient()

	countingClient := &benchmarkCountingLogClient{inner: client}
	socketPath := socktest.ShortPath(b, "forwarder-bench.sock")
	server, err := NewSocketServer(socketPath, 16_384)
	if err != nil {
		b.Fatalf("create socket server: %v", err)
	}

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = server.Serve()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	forwarder := NewForwarder(server.Chunks(), mocks.NopLogger{}, b.TempDir(), 256, 1_000_000)
	forwarder.SetLogClient(countingClient)
	forwarderDone := make(chan struct{})
	go func() {
		defer close(forwarderDone)
		forwarder.Run(ctx)
	}()

	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		_ = server.Close()
		<-serveDone
		b.Fatalf("dial forwarder socket: %v", err)
	}

	payload := []byte(strings.Repeat("x", payloadBytes))
	runIDs := benchmarkForwarderRunIDs(runCount)
	streamType := api.Stream_STREAM_STDOUT

	b.SetBytes(int64(payloadBytes))
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		runID := runIDs[i%len(runIDs)]
		seq := int64(i + 1)
		chunk := &api.LogChunk{
			RunId:    &runID,
			Data:     payload,
			Sequence: &seq,
			Stream:   &streamType,
		}

		if err := writeBenchmarkForwarderChunk(conn, chunk); err != nil {
			b.Fatalf("write forwarder chunk %d: %v", i, err)
		}
	}

	if err := closeBenchmarkForwarderInput(conn, server); err != nil {
		b.Fatalf("close benchmark forwarder input: %v", err)
	}

	<-serveDone
	forwarder.Shutdown()
	<-forwarderDone
	cancel()

	elapsed := time.Since(start)
	b.StopTimer()

	delivered := int(countingClient.chunks.Load())
	reportForwarderThroughput(b, "sent_", b.N, payloadBytes, elapsed)
	reportForwarderThroughput(b, "delivered_", delivered, payloadBytes, elapsed)
	if b.N > 0 {
		b.ReportMetric(float64(delivered)/float64(b.N)*100, "delivered_pct")
	}

	b.ReportMetric(float64(countingClient.streams.Load()), "streams")
	b.ReportMetric(float64(runCount), "runs")
	b.ReportMetric(float64(payloadBytes), "payload_bytes")
}

func closeBenchmarkForwarderInput(conn net.Conn, server *SocketServer) error {
	if unixConn, ok := conn.(*net.UnixConn); ok {
		if err := unixConn.CloseWrite(); err != nil {
			return err
		}
	} else {
		if err := conn.Close(); err != nil {
			return err
		}
	}

	if server.listener != nil {
		_ = server.listener.Close()
	}

	server.wg.Wait()
	close(server.chunks)
	_ = conn.Close()
	return nil
}

type benchmarkCountingLogClient struct {
	inner   interfaces.LogClient
	chunks  atomic.Int64
	bytes   atomic.Int64
	streams atomic.Int64
}

func (c *benchmarkCountingLogClient) StreamLogs(ctx context.Context) (interfaces.LogStream, error) {
	stream, err := c.inner.StreamLogs(ctx)
	if err != nil {
		return nil, err
	}

	c.streams.Add(1)
	return &benchmarkCountingLogStream{inner: stream, chunks: &c.chunks, bytes: &c.bytes}, nil
}

func (c *benchmarkCountingLogClient) StreamLogsForRun(ctx context.Context, runID string) (interfaces.LogStream, error) {
	if inner, ok := c.inner.(interfaces.RunLogClient); ok {
		stream, err := inner.StreamLogsForRun(ctx, runID)
		if err != nil {
			return nil, err
		}

		c.streams.Add(1)
		return &benchmarkCountingLogStream{inner: stream, chunks: &c.chunks, bytes: &c.bytes}, nil
	}

	return c.StreamLogs(ctx)
}

func (c *benchmarkCountingLogClient) StreamLogsForAssignedRun(ctx context.Context, runID, shardID string) (interfaces.LogStream, error) {
	if inner, ok := c.inner.(interfaces.AssignedRunLogClient); ok {
		stream, err := inner.StreamLogsForAssignedRun(ctx, runID, shardID)
		if err != nil {
			return nil, err
		}

		c.streams.Add(1)
		return &benchmarkCountingLogStream{inner: stream, chunks: &c.chunks, bytes: &c.bytes}, nil
	}

	return c.StreamLogsForRun(ctx, runID)
}

func (c *benchmarkCountingLogClient) Close() error {
	return c.inner.Close()
}

func (c *benchmarkCountingLogClient) PreferUnscopedLogStream() bool {
	if unscoped, ok := c.inner.(interface{ PreferUnscopedLogStream() bool }); ok {
		return unscoped.PreferUnscopedLogStream()
	}

	return false
}

type benchmarkCountingLogStream struct {
	inner  interfaces.LogStream
	chunks *atomic.Int64
	bytes  *atomic.Int64
}

func (s *benchmarkCountingLogStream) Send(chunk *api.LogChunk) error {
	if err := s.inner.Send(chunk); err != nil {
		return err
	}

	s.chunks.Add(1)
	s.bytes.Add(int64(len(chunk.GetData())))
	return nil
}

func (s *benchmarkCountingLogStream) CloseSend() error {
	return s.inner.CloseSend()
}

func (s *benchmarkCountingLogStream) CloseAndRecv() error {
	if closer, ok := s.inner.(interface{ CloseAndRecv() error }); ok {
		return closer.CloseAndRecv()
	}

	return s.inner.CloseSend()
}

type benchmarkRecordingLogClient struct{}

func (benchmarkRecordingLogClient) StreamLogs(context.Context) (interfaces.LogStream, error) {
	return benchmarkRecordingLogStream{}, nil
}

func (benchmarkRecordingLogClient) StreamLogsForRun(context.Context, string) (interfaces.LogStream, error) {
	return benchmarkRecordingLogStream{}, nil
}

func (benchmarkRecordingLogClient) StreamLogsForAssignedRun(context.Context, string, string) (interfaces.LogStream, error) {
	return benchmarkRecordingLogStream{}, nil
}

func (benchmarkRecordingLogClient) Close() error {
	return nil
}

func (benchmarkRecordingLogClient) PreferUnscopedLogStream() bool {
	return true
}

type benchmarkRecordingLogStream struct{}

func (benchmarkRecordingLogStream) Send(*api.LogChunk) error {
	return nil
}

func (benchmarkRecordingLogStream) CloseSend() error {
	return nil
}

func writeBenchmarkForwarderChunk(w io.Writer, chunk *api.LogChunk) error {
	data, err := proto.Marshal(chunk)
	if err != nil {
		return fmt.Errorf("marshal log chunk: %w", err)
	}

	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(data)))
	if _, err := w.Write(length[:]); err != nil {
		return err
	}

	if _, err := w.Write(data); err != nil {
		return err
	}

	return nil
}

func startBenchmarkForwarderLogServer(b *testing.B, storeName string) (interfaces.LogClient, func()) {
	b.Helper()

	store := benchmarkForwarderRunLogStore(b, storeName)
	listener := bufconn.Listen(benchmarkForwarderLogServerBufSize)
	server := grpc.NewServer()
	api.RegisterLogServiceServer(server, logserver.NewServerWithStore(mocks.NopLogger{}, store, nil))

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = server.Serve(listener)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///forwarder-log-bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		server.Stop()
		_ = listener.Close()
		<-serveDone
		b.Fatalf("dial benchmark log server: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
		<-serveDone
		if closer, ok := store.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}

	return interfaces.NewGRPCLogClient(conn), cleanup
}

func benchmarkForwarderRunLogStore(b *testing.B, storeName string) logserver.RunLogStore {
	b.Helper()

	switch storeName {
	case "noop":
		return logserver.NoopRunLogStore{}
	case "local":
		store, err := logserver.NewLocalRunLogStore(b.TempDir())
		if err != nil {
			b.Fatalf("create local log store: %v", err)
		}

		return store
	default:
		b.Fatalf("unknown log store %q", storeName)
		return nil
	}
}

func benchmarkForwarderRunIDs(runCount int) []string {
	if runCount <= 0 {
		runCount = 1
	}

	runIDs := make([]string, 0, runCount)
	for i := range runCount {
		runIDs = append(runIDs, fmt.Sprintf("bench-run-%05d", i))
	}

	return runIDs
}

func reportForwarderThroughput(b *testing.B, prefix string, chunks, payloadBytes int, elapsed time.Duration) {
	b.Helper()
	if elapsed <= 0 {
		return
	}

	payloadTotal := float64(chunks * payloadBytes)
	b.ReportMetric(float64(chunks)/elapsed.Seconds(), prefix+"chunks/s")
	b.ReportMetric(payloadTotal/elapsed.Seconds()/(1024*1024), prefix+"MiB/s")
}
