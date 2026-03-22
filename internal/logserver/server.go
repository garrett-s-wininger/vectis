package logserver

import (
	"context"
	"encoding/json"
	"errors"
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
}

func NewServer(logger interfaces.Logger) *Server {
	return &Server{
		buffers: make(map[string]*JobBuffer),
		logger:  logger,
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
		if !buffer.Add(entry) {
			s.logger.Warn("Log buffer full for run %s, dropping log line (seq %d)", chunk.GetRunId(), entry.Sequence)
			continue
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

	// Send a small comment immediately so the HTTP response is started.
	_, _ = w.Write([]byte(": connected\n\n"))
	flusher.Flush()

	s.logger.Info("SSE client connected for run: %s", runID)

	buffer := s.getOrCreateBuffer(runID)
	outCh := make(chan []byte, 256)
	buffer.Subscribe(outCh)
	defer func() {
		buffer.Unsubscribe(outCh)
	}()

	ctx := r.Context()
	completed := make(chan struct{})

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

			// Close connection after sending "completed" so the client can just read until EOF.
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

			close(completed)
			return false
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

	entries := buffer.GetEntries()
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

	select {
	case <-ctx.Done():
		return
	case <-completed:
		return
	}
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
		Addr:    port,
		Handler: mux,
	}

	s.logger.Info("SSE log server listening on %s", port)

	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}

	return err
}

func Run(ctx context.Context, logger interfaces.Logger) error {
	server := NewServer(logger)

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
