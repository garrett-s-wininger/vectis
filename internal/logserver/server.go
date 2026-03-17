package logserver

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/networking"
	"vectis/internal/registry"
)

const (
	MaxLogLinesPerJob   = 10000
	MaxWebSocketClients = 100
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
	subscribers map[*websocket.Conn]chan []byte
	subMu       sync.RWMutex
	logger      interfaces.Logger
}

func NewJobBuffer(logger interfaces.Logger) *JobBuffer {
	return &JobBuffer{
		entries:     make([]LogEntry, 0, MaxLogLinesPerJob),
		subscribers: make(map[*websocket.Conn]chan []byte),
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

func (jb *JobBuffer) Subscribe(conn *websocket.Conn, ch chan []byte) {
	jb.subMu.Lock()
	defer jb.subMu.Unlock()
	jb.subscribers[conn] = ch
}

func (jb *JobBuffer) Unsubscribe(conn *websocket.Conn) (chan []byte, bool) {
	jb.subMu.Lock()
	defer jb.subMu.Unlock()

	ch, ok := jb.subscribers[conn]
	if !ok {
		return nil, false
	}

	delete(jb.subscribers, conn)
	return ch, true
}

func (jb *JobBuffer) Broadcast(jobID string, entry LogEntry) {
	data, err := json.Marshal(entry)
	if err != nil {
		return
	}

	jb.subMu.RLock()
	defer jb.subMu.RUnlock()

	for _, ch := range jb.subscribers {
		select {
		case ch <- data:
		default:
			if jb.logger != nil {
				jb.logger.Warn("WebSocket buffer full for job %s; dropping log line (seq %d)", jobID, entry.Sequence)
			}
		}
	}
}

type Server struct {
	api.UnimplementedLogServiceServer
	mu       sync.RWMutex
	buffers  map[string]*JobBuffer
	logger   interfaces.Logger
	upgrader websocket.Upgrader
}

func NewServer(logger interfaces.Logger) *Server {
	return &Server{
		buffers: make(map[string]*JobBuffer),
		logger:  logger,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				// NOTE(garrett): Allow all origins for local development.
				return true
			},
		},
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
		// consideration would be how to handle gaps in the sequence numbers as well as websocket
		// resumption so we don't have to re-send all the logs to the client.
		if !buffer.Add(entry) {
			s.logger.Warn("Log buffer full for run %s, dropping log line (seq %d)", chunk.GetRunId(), entry.Sequence)
			continue
		}

		buffer.Broadcast(chunk.GetRunId(), entry)
		s.logger.Debug("Received log from run %s (seq %d)", chunk.GetRunId(), chunk.GetSequence())
	}
}

func (s *Server) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if runID == "" {
		s.logger.Error("WebSocket connection rejected: missing run id")
		http.Error(w, "run id is required", http.StatusBadRequest)
		return
	}

	s.logger.Info("WebSocket client connected for run: %s", runID)

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("WebSocket upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	buffer := s.getOrCreateBuffer(runID)
	outCh := make(chan []byte, 256)
	buffer.Subscribe(conn, outCh)
	defer func() {
		if ch, ok := buffer.Unsubscribe(conn); ok {
			close(ch)
		}
	}()

	go func() {
		for msg := range outCh {
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
			// Close connection after sending "completed" so the client can just read until close.
			// Handles both live stream and replay (replay sends completed as last entry).
			var entry LogEntry
			if err := json.Unmarshal(msg, &entry); err != nil {
				continue
			}
			if entry.Stream != api.Stream_STREAM_CONTROL {
				continue
			}
			var meta struct {
				Event string `json:"event"`
			}
			if err := json.Unmarshal([]byte(entry.Data), &meta); err != nil || meta.Event != "completed" {
				continue
			}
			conn.Close()
			return
		}
	}()

	s.logger.Info("WebSocket client subscribed to run: %s", runID)

	entries := buffer.GetEntries()
	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			continue
		}
		outCh <- data
	}

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				s.logger.Error("WebSocket error: %v", err)
			}
			return
		}
	}
}

func (s *Server) RunGRPC(ctx context.Context, port string) error {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	api.RegisterLogServiceServer(grpcServer, s)

	s.logger.Info("gRPC log server listening on %s", port)

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	return grpcServer.Serve(lis)
}

func (s *Server) RunWebSocket(ctx context.Context, port string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws/logs/{id}", s.HandleWebSocket)

	server := &http.Server{
		Addr:    port,
		Handler: mux,
	}

	s.logger.Info("WebSocket log server listening on %s", port)

	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	return server.ListenAndServe()
}

func Run(ctx context.Context, logger interfaces.Logger) error {
	server := NewServer(logger)

	registryClient, err := registry.New(ctx, logger, interfaces.SystemClock{})
	if err != nil {
		return err
	}
	defer registryClient.Close()

	if err := registryClient.Register(ctx, api.Component_COMPONENT_LOG, networking.LogGRPCPort); err != nil {
		return err
	}

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return server.RunGRPC(ctx, networking.LogGRPCPort)
	})

	g.Go(func() error {
		return server.RunWebSocket(ctx, networking.LogWebSocketPort)
	})

	return g.Wait()
}
