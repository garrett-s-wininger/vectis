package job

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
)

type finalizeErrLogClient struct {
	mu          sync.Mutex
	chunks      []*api.LogChunk
	finalizeErr error
}

func (c *finalizeErrLogClient) StreamLogs(context.Context) (interfaces.LogStream, error) {
	return &finalizeErrLogStream{
		parent:      c,
		finalizeErr: c.finalizeErr,
	}, nil
}

func (c *finalizeErrLogClient) Close() error { return nil }

func (c *finalizeErrLogClient) addChunk(chunk *api.LogChunk) {
	c.mu.Lock()
	c.chunks = append(c.chunks, chunk)
	c.mu.Unlock()
}

func (c *finalizeErrLogClient) chunkCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.chunks)
}

type finalizeErrLogStream struct {
	mu          sync.Mutex
	parent      *finalizeErrLogClient
	finalizeErr error
	closed      bool
}

type flakyConnectLogClient struct {
	mu           sync.Mutex
	openFailures int
	finalizeErr  error
	alwaysFail   bool
}

func (c *flakyConnectLogClient) StreamLogs(context.Context) (interfaces.LogStream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.alwaysFail {
		return nil, errors.New("log aggregator unavailable")
	}

	if c.openFailures > 0 {
		c.openFailures--
		return nil, errors.New("log aggregator unavailable")
	}

	return &finalizeErrLogStream{finalizeErr: c.finalizeErr}, nil
}

func (c *flakyConnectLogClient) Close() error { return nil }

type blockingConnectLogClient struct{}

func (blockingConnectLogClient) StreamLogs(ctx context.Context) (interfaces.LogStream, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (blockingConnectLogClient) Close() error { return nil }

type contextBoundLogClient struct {
	mu     sync.Mutex
	chunks []*api.LogChunk
}

func (c *contextBoundLogClient) StreamLogs(ctx context.Context) (interfaces.LogStream, error) {
	return &contextBoundLogStream{ctx: ctx, parent: c}, nil
}

func (c *contextBoundLogClient) Close() error { return nil }

type contextBoundLogStream struct {
	ctx    context.Context
	parent *contextBoundLogClient
}

func (s *contextBoundLogStream) Send(chunk *api.LogChunk) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}

	s.parent.mu.Lock()
	s.parent.chunks = append(s.parent.chunks, chunk)
	s.parent.mu.Unlock()
	return nil
}

func (s *contextBoundLogStream) CloseSend() error { return nil }

func (s *finalizeErrLogStream) Send(chunk *api.LogChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("stream closed")
	}

	if s.parent != nil {
		s.parent.addChunk(chunk)
	}

	return nil
}

func (s *finalizeErrLogStream) CloseSend() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *finalizeErrLogStream) CloseAndRecv() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return s.finalizeErr
}

func TestDurableLogStream_FlushesAllChunksOnClose(t *testing.T) {
	logClient := mocks.NewMockLogClient()
	logger := mocks.NewMockLogger()

	d, err := newDurableLogStream(logClient, logger, "run-test")
	if err != nil {
		t.Fatalf("new durable stream: %v", err)
	}

	const n = 500
	for i := 0; i < n; i++ {
		rid := "run-test"
		seq := int64(i + 1)
		st := api.Stream_STREAM_STDOUT
		if err := d.Send(&api.LogChunk{RunId: &rid, Sequence: &seq, Stream: &st, Data: []byte(fmt.Sprintf("line-%d", i))}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	if err := d.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}

	chunks := logClient.GetChunks()
	if len(chunks) != n {
		t.Fatalf("expected %d chunks, got %d", n, len(chunks))
	}
}

func TestDurableLogStream_FlushesAllChunksWithConcurrentSenders(t *testing.T) {
	logClient := mocks.NewMockLogClient()
	logger := mocks.NewMockLogger()

	d, err := newDurableLogStream(logClient, logger, "run-test-concurrent")
	if err != nil {
		t.Fatalf("new durable stream: %v", err)
	}

	const workers = 8
	const perWorker = 120
	var wg sync.WaitGroup
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				rid := "run-test-concurrent"
				seq := int64((worker * perWorker) + i + 1)
				st := api.Stream_STREAM_STDOUT
				_ = d.Send(&api.LogChunk{RunId: &rid, Sequence: &seq, Stream: &st, Data: []byte(fmt.Sprintf("w%d-%d", worker, i))})
			}
		}(w)
	}

	wg.Wait()
	if err := d.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}

	want := workers * perWorker
	chunks := logClient.GetChunks()
	if len(chunks) != want {
		t.Fatalf("expected %d chunks, got %d", want, len(chunks))
	}
}

func TestDurableLogStream_NoSpuriousLossAcrossRapidClose(t *testing.T) {
	for iter := 0; iter < 50; iter++ {
		logClient := mocks.NewMockLogClient()
		logger := mocks.NewMockLogger()

		d, err := newDurableLogStream(logClient, logger, fmt.Sprintf("run-%d", iter))
		if err != nil {
			t.Fatalf("iter %d new durable stream: %v", iter, err)
		}

		const n = 80
		for i := 0; i < n; i++ {
			rid := fmt.Sprintf("run-%d", iter)
			seq := int64(i + 1)
			st := api.Stream_STREAM_STDOUT
			if err := d.Send(&api.LogChunk{RunId: &rid, Sequence: &seq, Stream: &st, Data: []byte("x")}); err != nil {
				t.Fatalf("iter %d send %d: %v", iter, i, err)
			}
		}

		if err := d.CloseSend(); err != nil {
			t.Fatalf("iter %d close: %v", iter, err)
		}

		if got := len(logClient.GetChunks()); got != n {
			t.Fatalf("iter %d expected %d chunks, got %d", iter, n, got)
		}

		time.Sleep(1 * time.Millisecond)
	}
}

func TestDurableLogStream_CloseSendReturnsFinalizeError(t *testing.T) {
	logger := mocks.NewMockLogger()
	client := &finalizeErrLogClient{finalizeErr: errors.New("server did not ack stream")}

	d, err := newDurableLogStream(client, logger, "run-finalize-fail")
	if err != nil {
		t.Fatalf("new durable stream: %v", err)
	}

	runID := "run-finalize-fail"
	seq := int64(1)
	st := api.Stream_STREAM_STDOUT
	if err := d.Send(&api.LogChunk{RunId: &runID, Sequence: &seq, Stream: &st, Data: []byte("hello")}); err != nil {
		t.Fatalf("send: %v", err)
	}

	err = d.CloseSend()
	if err == nil {
		t.Fatal("expected finalize error, got nil")
	}

	if got := client.chunkCount(); got != 1 {
		t.Fatalf("expected 1 sent chunk, got %d", got)
	}
}

func TestDurableLogStream_LogsOutageAndRecoveryOnce(t *testing.T) {
	origRetryBase := LogRetryBaseForTest()
	origRetryMax := LogRetryMaxForTest()
	origFlushTimeout := LogFlushTimeoutForTest()
	SetLogRetryBaseForTest(1 * time.Millisecond)
	SetLogRetryMaxForTest(5 * time.Millisecond)
	SetLogFlushTimeoutForTest(2 * time.Second)
	t.Cleanup(func() {
		SetLogRetryBaseForTest(origRetryBase)
		SetLogRetryMaxForTest(origRetryMax)
		SetLogFlushTimeoutForTest(origFlushTimeout)
	})

	logger := mocks.NewMockLogger()
	client := &flakyConnectLogClient{
		openFailures: 3,
	}

	d, err := newDurableLogStream(client, logger, "run-reconnect-logging")
	if err != nil {
		t.Fatalf("new durable stream: %v", err)
	}

	runID := "run-reconnect-logging"
	seq := int64(1)
	st := api.Stream_STREAM_STDOUT
	if err := d.Send(&api.LogChunk{RunId: &runID, Sequence: &seq, Stream: &st, Data: []byte("hello")}); err != nil {
		t.Fatalf("send: %v", err)
	}

	if err := d.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}

	warnCalls := logger.GetWarnCalls()
	infoCalls := logger.GetInfoCalls()

	warnCount := 0
	for _, msg := range warnCalls {
		if strings.Contains(msg, "Log aggregator unavailable; spooling logs locally and retrying") {
			warnCount++
		}
	}

	infoCount := 0
	for _, msg := range infoCalls {
		if strings.Contains(msg, "Log aggregator reconnected; resumed flushing spooled logs") {
			infoCount++
		}
	}

	if warnCount != 1 {
		t.Fatalf("expected exactly one outage warning, got %d (warn calls: %v)", warnCount, warnCalls)
	}

	if infoCount != 1 {
		t.Fatalf("expected exactly one recovery info log, got %d (info calls: %v)", infoCount, infoCalls)
	}
}

func TestDurableLogStream_WarnsWhenWaitingForRecoveryDuringClose(t *testing.T) {
	origRetryBase := LogRetryBaseForTest()
	origRetryMax := LogRetryMaxForTest()
	origFlushTimeout := LogFlushTimeoutForTest()
	SetLogRetryBaseForTest(1 * time.Millisecond)
	SetLogRetryMaxForTest(5 * time.Millisecond)
	SetLogFlushTimeoutForTest(80 * time.Millisecond)
	t.Cleanup(func() {
		SetLogRetryBaseForTest(origRetryBase)
		SetLogRetryMaxForTest(origRetryMax)
		SetLogFlushTimeoutForTest(origFlushTimeout)
	})

	logger := mocks.NewMockLogger()
	client := &flakyConnectLogClient{alwaysFail: true}

	d, err := newDurableLogStream(client, logger, "run-timeout-warning")
	if err != nil {
		t.Fatalf("new durable stream: %v", err)
	}

	runID := "run-timeout-warning"
	seq := int64(1)
	st := api.Stream_STREAM_STDOUT
	if err := d.Send(&api.LogChunk{RunId: &runID, Sequence: &seq, Stream: &st, Data: []byte("hello")}); err != nil {
		t.Fatalf("send: %v", err)
	}

	err = d.CloseSend()
	if err == nil {
		t.Fatal("expected close timeout error, got nil")
	}

	warnCalls := logger.GetWarnCalls()
	waitWarn := false
	for _, msg := range warnCalls {
		if strings.Contains(msg, "waiting up to") && strings.Contains(msg, "before failing run") {
			waitWarn = true
			break
		}
	}

	if !waitWarn {
		t.Fatalf("expected recovery wait warning, got warn calls: %v", warnCalls)
	}
}

func TestDurableLogStream_BlockingConnectEmitsOutageAndWaitWarnings(t *testing.T) {
	origRetryBase := LogRetryBaseForTest()
	origRetryMax := LogRetryMaxForTest()
	origFlushTimeout := LogFlushTimeoutForTest()
	origInitialProbe := LogInitialProbeForTest()
	SetLogRetryBaseForTest(1 * time.Millisecond)
	SetLogRetryMaxForTest(5 * time.Millisecond)
	SetLogFlushTimeoutForTest(120 * time.Millisecond)
	SetLogInitialProbeForTest(20 * time.Millisecond)
	t.Cleanup(func() {
		SetLogRetryBaseForTest(origRetryBase)
		SetLogRetryMaxForTest(origRetryMax)
		SetLogFlushTimeoutForTest(origFlushTimeout)
		SetLogInitialProbeForTest(origInitialProbe)
	})

	logger := mocks.NewMockLogger()
	client := blockingConnectLogClient{}

	d, err := newDurableLogStream(client, logger, "run-blocking-connect")
	if err != nil {
		t.Fatalf("new durable stream: %v", err)
	}

	runID := "run-blocking-connect"
	seq := int64(1)
	st := api.Stream_STREAM_STDOUT
	if err := d.Send(&api.LogChunk{RunId: &runID, Sequence: &seq, Stream: &st, Data: []byte("hello")}); err != nil {
		t.Fatalf("send: %v", err)
	}

	err = d.CloseSend()
	if err == nil {
		t.Fatal("expected close timeout error, got nil")
	}

	warnCalls := logger.GetWarnCalls()
	outageWarn := false
	waitWarn := false
	for _, msg := range warnCalls {
		if strings.Contains(msg, "Log aggregator unavailable; spooling logs locally and retrying") {
			outageWarn = true
		}

		if strings.Contains(msg, "waiting up to") && strings.Contains(msg, "before failing run") {
			waitWarn = true
		}
	}

	if !outageWarn {
		t.Fatalf("expected outage warning, got warn calls: %v", warnCalls)
	}

	if !waitWarn {
		t.Fatalf("expected recovery wait warning, got warn calls: %v", warnCalls)
	}
}

func TestDurableLogStream_DoesNotFlapWhenHealthyStreamEstablished(t *testing.T) {
	origInitialProbe := LogInitialProbeForTest()
	SetLogInitialProbeForTest(5 * time.Millisecond)
	t.Cleanup(func() {
		SetLogInitialProbeForTest(origInitialProbe)
	})

	logger := mocks.NewMockLogger()
	client := &contextBoundLogClient{}

	d, err := newDurableLogStream(client, logger, "run-no-flap")
	if err != nil {
		t.Fatalf("new durable stream: %v", err)
	}

	runID := "run-no-flap"
	st := api.Stream_STREAM_STDOUT
	seq1 := int64(1)
	if err := d.Send(&api.LogChunk{RunId: &runID, Sequence: &seq1, Stream: &st, Data: []byte("line1")}); err != nil {
		t.Fatalf("send 1: %v", err)
	}

	// If stream setup accidentally ties lifetime to a short setup timeout, this second send would fail/retry.
	time.Sleep(50 * time.Millisecond)
	seq2 := int64(2)
	if err := d.Send(&api.LogChunk{RunId: &runID, Sequence: &seq2, Stream: &st, Data: []byte("line2")}); err != nil {
		t.Fatalf("send 2: %v", err)
	}

	if err := d.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}

	if got := len(logger.GetWarnCalls()); got != 0 {
		t.Fatalf("expected no warn logs for healthy stream, got %v", logger.GetWarnCalls())
	}
	if got := len(logger.GetInfoCalls()); got != 0 {
		t.Fatalf("expected no recovery info logs for healthy stream, got %v", logger.GetInfoCalls())
	}
}

func TestDurableLogStream_InitialProbeWarnsBeforeSend(t *testing.T) {
	origInitialProbe := LogInitialProbeForTest()
	SetLogInitialProbeForTest(20 * time.Millisecond)
	t.Cleanup(func() {
		SetLogInitialProbeForTest(origInitialProbe)
	})

	logger := mocks.NewMockLogger()
	client := blockingConnectLogClient{}

	d, err := newDurableLogStream(client, logger, "run-initial-probe-warn")
	if err != nil {
		t.Fatalf("new durable stream: %v", err)
	}
	defer func() { _ = d.CloseSend() }()

	// Allow the short probe timeout to elapse.
	time.Sleep(35 * time.Millisecond)

	warnCalls := logger.GetWarnCalls()
	outageWarn := false
	for _, msg := range warnCalls {
		if strings.Contains(msg, "Log aggregator unavailable; spooling logs locally and retrying") {
			outageWarn = true
			break
		}
	}
	if !outageWarn {
		t.Fatalf("expected initial outage warning before send, got warn calls: %v", warnCalls)
	}
}
