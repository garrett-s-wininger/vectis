package logserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	api "vectis/api/gen/go"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/registry"
)

const (
	MaxLogLinesPerJob = 10000
	MaxSSEClients     = 100

	sseReadHeaderTimeout = 10 * time.Second
	sseIdleTimeout       = 120 * time.Second
	sseShutdownTimeout   = 30 * time.Second
)

type LogEntry struct {
	Timestamp time.Time  `json:"timestamp"`
	Stream    api.Stream `json:"stream"`
	Sequence  int64      `json:"sequence"`
	Data      string     `json:"data"`
}

type JobBuffer struct {
	mu          sync.RWMutex
	entries     []LogEntry
	subscribers map[chan []byte]struct{}
	subMu       sync.RWMutex
	logger      interfaces.Logger
}

func NewJobBuffer(logger interfaces.Logger) *JobBuffer {
	return &JobBuffer{
		entries:     make([]LogEntry, 0, MaxLogLinesPerJob),
		subscribers: make(map[chan []byte]struct{}),
		logger:      logger,
	}
}

func (jb *JobBuffer) Add(entry LogEntry) bool {
	jb.mu.Lock()
	defer jb.mu.Unlock()

	if len(jb.entries) >= MaxLogLinesPerJob {
		return false
	}

	jb.entries = append(jb.entries, entry)
	return true
}

func (jb *JobBuffer) GetEntries() []LogEntry {
	jb.mu.RLock()
	defer jb.mu.RUnlock()

	entries := make([]LogEntry, len(jb.entries))
	copy(entries, jb.entries)
	return entries
}

func (jb *JobBuffer) Subscribe(ch chan []byte) {
	jb.subMu.Lock()
	defer jb.subMu.Unlock()
	jb.subscribers[ch] = struct{}{}
}

func (jb *JobBuffer) Unsubscribe(ch chan []byte) bool {
	jb.subMu.Lock()
	defer jb.subMu.Unlock()

	if _, ok := jb.subscribers[ch]; !ok {
		return false
	}
	delete(jb.subscribers, ch)
	close(ch)
	return true
}

func (jb *JobBuffer) Broadcast(jobID string, entry LogEntry) {
	data, err := json.Marshal(entry)
	if err != nil {
		return
	}

	jb.subMu.RLock()
	defer jb.subMu.RUnlock()

	for ch := range jb.subscribers {
		// Never drop control events: a dropped "completed" leaves SSE clients blocked forever
		// waiting for run end (HandleSSE waits on completed). Stdout/stderr may still drop under load.
		if entry.Stream == api.Stream_STREAM_CONTROL {
			ch <- data
			continue
		}
		select {
		case ch <- data:
		default:
			if jb.logger != nil {
				jb.logger.Warn("SSE buffer full for job %s; dropping log line (seq %d)", jobID, entry.Sequence)
			}
		}
	}
}

type Server struct {
	api.UnimplementedLogServiceServer
	mu      sync.RWMutex
	buffers map[string]*JobBuffer
	logger  interfaces.Logger
	store   RunLogStore
	runs    RunStatusProvider
}

func NewServer(logger interfaces.Logger) *Server {
	return NewServerWithStoreAndStatus(logger, NoopRunLogStore{}, nil)
}

func NewServerWithStore(logger interfaces.Logger, store RunLogStore) *Server {
	return NewServerWithStoreAndStatus(logger, store, nil)
}

func NewServerWithStoreAndStatus(logger interfaces.Logger, store RunLogStore, runs RunStatusProvider) *Server {
	if store == nil {
		store = NoopRunLogStore{}
	}

	return &Server{
		buffers: make(map[string]*JobBuffer),
		logger:  logger,
		store:   store,
		runs:    runs,
	}
}

func (s *Server) getOrCreateBuffer(runID string) *JobBuffer {
	s.mu.Lock()
	defer s.mu.Unlock()

	if buffer, ok := s.buffers[runID]; ok {
		return buffer
	}

	buffer := NewJobBuffer(s.logger)
	s.buffers[runID] = buffer
	return buffer
}

func (s *Server) StreamLogs(stream api.LogService_StreamLogsServer) error {
	for {
		chunk, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return stream.SendAndClose(&api.Empty{})
			}

			return err
		}

		buffer := s.getOrCreateBuffer(chunk.GetRunId())

		entry := LogEntry{
			Timestamp: time.Now(),
			Stream:    chunk.GetStream(),
			Sequence:  chunk.GetSequence(),
			Data:      string(chunk.GetData()),
		}

		// FIXME(garrett): We currently store logs in arrival order which makes it so clients would
		// need to reorder them themselves. We should reorder them, as appropriately. A secondary
		// consideration would be how to handle gaps in the sequence numbers as well as SSE
		// resumption so we don't have to re-send all the logs to the client.
		if err := s.store.Append(chunk.GetRunId(), entry); err != nil {
			return err
		}

		if !buffer.Add(entry) {
			s.logger.Warn("Log buffer full for run %s, dropping log line (seq %d)", chunk.GetRunId(), entry.Sequence)
		}

		buffer.Broadcast(chunk.GetRunId(), entry)
		s.logger.Debug("Received log from run %s (seq %d)", chunk.GetRunId(), chunk.GetSequence())
	}
}

func (s *Server) HandleSSE(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if runID == "" {
		s.logger.Error("SSE connection rejected: missing run id")
		http.Error(w, "run id is required", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// NOTE(garrett): Prevent buffering behind various proxies for lower latency.
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	buffer := s.getOrCreateBuffer(runID)
	outCh := make(chan []byte, 256)
	buffer.Subscribe(outCh)
	defer func() {
		buffer.Unsubscribe(outCh)
	}()

	s.logger.Info("SSE client connected for run: %s", runID)

	// Subscribe before any body bytes so log lines are not dropped if the client
	// or another goroutine races the first flush (same ordering as API run SSE).
	_, _ = w.Write([]byte(": connected\n\n"))
	flusher.Flush()

	ctx := r.Context()
	completed := make(chan struct{})
	var completedOnce sync.Once

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		writeEvent := func(msg []byte) bool {
			if _, err := w.Write([]byte("data: ")); err != nil {
				return false
			}
			if _, err := w.Write(msg); err != nil {
				return false
			}
			if _, err := w.Write([]byte("\n\n")); err != nil {
				return false
			}
			flusher.Flush()

			// NOTE(garrett): Close connection after sending "completed" so the client can just read until EOF.
			// Handles both live stream and replay (replay sends completed as last entry).
			var entry LogEntry
			if err := json.Unmarshal(msg, &entry); err != nil {
				return true
			}
			if entry.Stream != api.Stream_STREAM_CONTROL {
				return true
			}

			var meta struct {
				Event string `json:"event"`
			}
			if err := json.Unmarshal([]byte(entry.Data), &meta); err != nil || meta.Event != "completed" {
				return true
			}

			// NOTE(garrett): Do not stop the writer here. If we return false and exit the writer while the main
			// goroutine is still replaying entries into outCh, it deadlocks (no reader). Main waits
			// on <-completed and unsubscribes only after that; the writer must keep draining outCh.
			completedOnce.Do(func() { close(completed) })
			return true
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = w.Write([]byte(": keep-alive\n\n"))
				flusher.Flush()
			case msg, ok := <-outCh:
				if !ok {
					return
				}

				if !writeEvent(msg) {
					return
				}
			}
		}
	}()

	s.logger.Info("SSE client subscribed to run: %s", runID)

	entries, err := s.store.List(runID)
	if err != nil {
		s.logger.Warn("Failed to load persisted logs for run %s: %v; falling back to in-memory entries", runID, err)
		entries = buffer.GetEntries()
	}

	if len(entries) == 0 {
		entries = buffer.GetEntries()
	}

	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			continue
		}
		select {
		case outCh <- data:
		case <-ctx.Done():
			return
		case <-completed:
			return
		}
	}

	if s.runs == nil {
		select {
		case <-ctx.Done():
			return
		case <-completed:
			return
		}
	}

	if s.tryInjectSyntheticCompletion(ctx, runID, buffer) {
		select {
		case <-ctx.Done():
			return
		case <-completed:
			return
		}
	}

	poll := time.NewTicker(2 * time.Second)
	defer poll.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-completed:
			return
		case <-poll.C:
			_ = s.tryInjectSyntheticCompletion(ctx, runID, buffer)
		}
	}
}

func isTerminalRunStatus(status string) bool {
	return status == "succeeded" || status == "failed"
}

func completedEventStatus(runStatus string) string {
	switch runStatus {
	case "succeeded":
		return "success"
	case "failed":
		return "failure"
	default:
		return runStatus
	}
}

func nextSequence(entries []LogEntry) int64 {
	var max int64
	for _, e := range entries {
		if e.Sequence > max {
			max = e.Sequence
		}
	}

	return max + 1
}

func hasCompletedEvent(entries []LogEntry) bool {
	for _, entry := range entries {
		if entry.Stream != api.Stream_STREAM_CONTROL {
			continue
		}

		var meta struct {
			Event string `json:"event"`
		}

		if err := json.Unmarshal([]byte(entry.Data), &meta); err != nil {
			continue
		}

		if meta.Event == "completed" {
			return true
		}
	}

	return false
}

func (s *Server) tryInjectSyntheticCompletion(ctx context.Context, runID string, buffer *JobBuffer) bool {
	if s.runs == nil {
		return false
	}

	entries := buffer.GetEntries()
	if hasCompletedEvent(entries) {
		return false
	}

	status, found, err := s.runs.GetRunStatus(ctx, runID)
	if err != nil {
		s.logger.Warn("Run-status lookup failed for run %s: %v", runID, err)
		return false
	}

	if !found || !isTerminalRunStatus(status) {
		return false
	}

	completedStatus := completedEventStatus(status)
	entry := LogEntry{
		Timestamp: time.Now(),
		Stream:    api.Stream_STREAM_CONTROL,
		Sequence:  nextSequence(entries),
		Data:      fmt.Sprintf(`{"event":"completed","status":"%s","synthetic":true}`, completedStatus),
	}

	if err := s.store.Append(runID, entry); err != nil {
		s.logger.Warn("Failed to persist synthetic completion entry for run %s: %v", runID, err)
	}

	if !buffer.Add(entry) {
		s.logger.Warn("Log buffer full for run %s; dropping synthetic completion entry", runID)
		return false
	}

	s.logger.Debug("Injected synthetic completion event for run %s with status %s", runID, completedStatus)
	buffer.Broadcast(runID, entry)
	return true
}

func (s *Server) RunGRPC(ctx context.Context, port string) error {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	api.RegisterLogServiceServer(grpcServer, s)

	hs := health.NewServer()
	healthgrpc.RegisterHealthServer(grpcServer, hs)
	hs.SetServingStatus("log", healthpb.HealthCheckResponse_SERVING)

	s.logger.Info("gRPC log server listening on %s", port)

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	err = grpcServer.Serve(lis)
	return err
}

func (s *Server) RunSSE(ctx context.Context, port string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/sse/logs/{id}", s.HandleSSE)

	server := &http.Server{
		Addr:              port,
		Handler:           mux,
		ReadHeaderTimeout: sseReadHeaderTimeout,
		IdleTimeout:       sseIdleTimeout,
	}

	s.logger.Info("SSE log server listening on %s", port)

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), sseShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutCtx); err != nil {
			s.logger.Warn("SSE log server shutdown: %v", err)
		}
	}()

	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}

	return err
}

func Run(ctx context.Context, logger interfaces.Logger, store RunLogStore, runs RunStatusProvider) error {
	server := NewServerWithStoreAndStatus(logger, store, runs)

	if config.LogRegisterWithRegistry() {
		regAddr := config.LogRegistryAddress()
		if regAddr == "" {
			regAddr = config.RegistryListenAddr()
		}

		registryClient, err := registry.New(ctx, regAddr, logger, interfaces.SystemClock{})
		if err != nil {
			return err
		}

		defer registryClient.Close()

		bindGRPC := config.LogGRPCListenAddr()
		publishAddr := config.LogGRPCRegistryPublishAddress(bindGRPC)
		if err := registryClient.Register(ctx, api.Component_COMPONENT_LOG, publishAddr); err != nil {
			return err
		}

		stopHeartbeat := registry.StartRegistrationHeartbeat(
			ctx, registryClient, api.Component_COMPONENT_LOG, publishAddr,
			config.RegistryRegistrationRefresh(), logger,
		)
		defer stopHeartbeat()

		logger.Info("Registered log service with registry at %s", publishAddr)
	} else {
		logger.Info("Skipping registry registration (log.grpc.register_with_registry is false)")
	}

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return server.RunGRPC(ctx, config.LogGRPCListenAddr())
	})

	g.Go(func() error {
		return server.RunSSE(ctx, config.LogWebSocketListenAddr())
	})

	return g.Wait()
}
