package logserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	api "vectis/api/gen/go"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
	"vectis/internal/registry"
)

const (
	MaxLogLinesPerJob = 10000
	MaxRunBuffers     = 1024

	GetLogsReplayLimitMetadata = "vectis-log-replay-limit"
	GetLogsTailMetadata        = "vectis-log-tail"
)

type LogEntry struct {
	Timestamp time.Time      `json:"timestamp"`
	Stream    api.Stream     `json:"stream"`
	Sequence  int64          `json:"sequence"`
	Data      string         `json:"data"`
	Completed api.RunOutcome `json:"completed,omitempty"`
}

type JobBuffer struct {
	mu           sync.RWMutex
	entries      []LogEntry
	subscribers  map[chan []byte]struct{}
	subMu        sync.RWMutex
	logger       interfaces.Logger
	metrics      *observability.LogMetrics
	lastActivity time.Time
	terminal     bool
}

func NewJobBuffer(logger interfaces.Logger, metrics *observability.LogMetrics) *JobBuffer {
	return &JobBuffer{
		entries:      make([]LogEntry, 0, MaxLogLinesPerJob),
		subscribers:  make(map[chan []byte]struct{}),
		logger:       logger,
		metrics:      metrics,
		lastActivity: time.Now(),
	}
}

func (jb *JobBuffer) Add(entry LogEntry) bool {
	jb.mu.Lock()
	defer jb.mu.Unlock()

	jb.lastActivity = entry.Timestamp
	if isCompletedEvent(entry) {
		jb.terminal = true
	}

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
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Sequence < entries[j].Sequence
	})

	return entries
}

func (jb *JobBuffer) IsTerminal() bool {
	jb.mu.RLock()
	defer jb.mu.RUnlock()

	return jb.terminal
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

func (jb *JobBuffer) SubscriberCount() int {
	jb.subMu.RLock()
	defer jb.subMu.RUnlock()

	return len(jb.subscribers)
}

func (jb *JobBuffer) Evictable() bool {
	jb.mu.RLock()
	terminal := jb.terminal
	jb.mu.RUnlock()

	return terminal && jb.SubscriberCount() == 0
}

func (jb *JobBuffer) LastActivity() time.Time {
	jb.mu.RLock()
	defer jb.mu.RUnlock()

	return jb.lastActivity
}

func (jb *JobBuffer) Broadcast(runID string, entry LogEntry) {
	data, err := json.Marshal(entry)
	if err != nil {
		if jb.logger != nil {
			jb.logger.Warn("Failed to marshal log entry for run %s (seq %d): %v", runID, entry.Sequence, err)
		}

		return
	}

	jb.subMu.RLock()
	defer jb.subMu.RUnlock()

	for ch := range jb.subscribers {
		if entry.Stream == api.Stream_STREAM_CONTROL {
			ch <- data
			continue
		}

		select {
		case ch <- data:
		default:
			if jb.metrics != nil {
				jb.metrics.RecordChannelDrop(context.Background())
			}

			if jb.logger != nil {
				jb.logger.Warn("channel full for run %s; dropping log line (seq %d)", runID, entry.Sequence)
			}
		}
	}
}

type Server struct {
	api.UnimplementedLogServiceServer
	mu            sync.RWMutex
	buffers       map[string]*JobBuffer
	logger        interfaces.Logger
	store         RunLogStore
	metrics       *observability.LogMetrics
	maxRunBuffers int
}

func NewServer(logger interfaces.Logger) *Server {
	return NewServerWithStore(logger, NoopRunLogStore{}, nil)
}

func NewServerWithStore(logger interfaces.Logger, store RunLogStore, metrics *observability.LogMetrics) *Server {
	if store == nil {
		store = NoopRunLogStore{}
	}

	return &Server{
		buffers:       make(map[string]*JobBuffer),
		logger:        logger,
		store:         store,
		metrics:       metrics,
		maxRunBuffers: MaxRunBuffers,
	}
}

func (s *Server) getOrCreateBuffer(runID string) *JobBuffer {
	s.mu.Lock()
	defer s.mu.Unlock()

	if buffer, ok := s.buffers[runID]; ok {
		return buffer
	}

	buffer := NewJobBuffer(s.logger, s.metrics)
	s.buffers[runID] = buffer
	s.evictTerminalBuffersLocked()
	return buffer
}

func (s *Server) bufferCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.buffers)
}

func (s *Server) evictTerminalBuffers() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.evictTerminalBuffersLocked()
}

func (s *Server) evictTerminalBuffersLocked() {
	if s.maxRunBuffers <= 0 || len(s.buffers) <= s.maxRunBuffers {
		return
	}

	type candidate struct {
		runID        string
		lastActivity time.Time
	}

	candidates := make([]candidate, 0, len(s.buffers))
	for runID, buffer := range s.buffers {
		if !buffer.Evictable() {
			continue
		}
		candidates = append(candidates, candidate{runID: runID, lastActivity: buffer.LastActivity()})
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].lastActivity.Before(candidates[j].lastActivity)
	})

	for _, c := range candidates {
		if len(s.buffers) <= s.maxRunBuffers {
			return
		}

		delete(s.buffers, c.runID)
		if s.logger != nil {
			s.logger.Debug("Evicted terminal log buffer for run %s", c.runID)
		}
	}
}

func (s *Server) StreamLogs(stream api.LogService_StreamLogsServer) error {
	// Each StreamLogs connection carries logs for a single run (one worker = one
	// connection). The synthetic completion on EOF applies only to the last run
	// seen. Multi-run connections would need per-buffer tracking on EOF.
	ctx := stream.Context()
	var lastBuffer *JobBuffer
	var lastRunID string

	for {
		chunk, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Worker stream ended. Emit synthetic completion if needed.
				if lastBuffer != nil && !lastBuffer.IsTerminal() {
					s.logger.Warn("Stream ended for run %s without completion event", lastRunID)

					entries := lastBuffer.GetEntries()
					synthetic := LogEntry{
						Timestamp: time.Now(),
						Stream:    api.Stream_STREAM_CONTROL,
						Sequence:  nextSequence(entries),
						Data:      `{"event":"completed","status":"unknown","synthetic":true}`,
						Completed: api.RunOutcome_RUN_OUTCOME_UNKNOWN,
					}

					if err := s.store.Append(lastRunID, synthetic); err != nil {
						s.logger.Warn("Failed to store synthetic completion for run %s: %v", lastRunID, err)
					}

					if !lastBuffer.Add(synthetic) {
						s.logger.Warn("Failed to add synthetic completion to buffer for run %s", lastRunID)
					}

					lastBuffer.Broadcast(lastRunID, synthetic)
					s.evictTerminalBuffers()
				}
				return stream.SendAndClose(&api.Empty{})
			}

			return err
		}

		if s.metrics != nil {
			s.metrics.RecordGRPCChunk(ctx)
		}

		lastRunID = chunk.GetRunId()
		lastBuffer = s.getOrCreateBuffer(lastRunID)

		now := time.Now()
		if ts := chunk.GetTimestamp(); ts != nil {
			now = ts.AsTime()
		}

		entry := LogEntry{
			Timestamp: now,
			Stream:    chunk.GetStream(),
			Sequence:  chunk.GetSequence(),
			Data:      string(chunk.GetData()),
			Completed: chunk.GetCompleted(),
		}

		if err := s.store.Append(chunk.GetRunId(), entry); err != nil {
			if s.metrics != nil {
				s.metrics.RecordAppendFailure(ctx)
			}

			return err
		}

		if !lastBuffer.Add(entry) {
			if s.metrics != nil {
				s.metrics.RecordMemoryBufferDrop(ctx)
			}
			s.logger.Warn("Log buffer full for run %s, dropping log line (seq %d)", chunk.GetRunId(), entry.Sequence)
		}

		lastBuffer.Broadcast(chunk.GetRunId(), entry)

		if lastBuffer.IsTerminal() {
			s.evictTerminalBuffers()
		}

		s.logger.Debug("Received log from run %s (seq %d)", chunk.GetRunId(), chunk.GetSequence())
	}
}

func (s *Server) GetLogs(req *api.GetLogsRequest, stream api.LogService_GetLogsServer) error {
	runID := req.GetRunId()
	sinceSeq := req.GetSinceSequence()
	ctx := stream.Context()
	replayLimit := positiveIntMetadata(ctx, GetLogsReplayLimitMetadata)
	tail := positiveIntMetadata(ctx, GetLogsTailMetadata)

	buffer := s.getOrCreateBuffer(runID)

	// Subscribe before replay to avoid the race window between replay and
	// subscription. Entries arriving during replay are deduplicated by sequence.
	outCh := make(chan []byte, 256)
	buffer.Subscribe(outCh)
	defer func() {
		buffer.Unsubscribe(outCh)
		s.evictTerminalBuffers()
	}()

	// Phase 1: Replay historical entries
	entries, err := s.store.List(runID)
	if err != nil {
		s.logger.Warn("Failed to load persisted logs for run %s: %v", runID, err)
	}

	if len(entries) == 0 {
		entries = buffer.GetEntries()
	}

	entries, replayTruncated := boundedReplayEntries(entries, sinceSeq, tail, replayLimit)

	var maxReplayedSeq = sinceSeq
	var sawCompletionDuringReplay bool
	for _, entry := range entries {
		chunk := &api.LogChunk{
			RunId:     &runID,
			Data:      []byte(entry.Data),
			Sequence:  &entry.Sequence,
			Stream:    &entry.Stream,
			Timestamp: timestamppb.New(entry.Timestamp),
			Completed: entry.Completed.Enum(),
		}

		if err := stream.Send(chunk); err != nil {
			return err
		}

		if isCompletedEvent(entry) {
			sawCompletionDuringReplay = true
		}

		if entry.Sequence > maxReplayedSeq {
			maxReplayedSeq = entry.Sequence
		}
	}

	if replayTruncated {
		if err := stream.Send(replayTruncatedChunk(runID, replayLimit)); err != nil {
			return err
		}

		return nil
	}

	// Drain entries that arrived on the subscription channel during replay.
	// A completion event broadcast during the replay loop would otherwise be
	// missed: IsTerminal() returns true but the event is still sitting in outCh.
	for {
		select {
		case msg, ok := <-outCh:
			if !ok {
				return nil
			}

			var entry LogEntry
			if err := json.Unmarshal(msg, &entry); err != nil {
				continue
			}

			if entry.Sequence <= maxReplayedSeq {
				continue
			}

			chunk := &api.LogChunk{
				RunId:     &runID,
				Data:      []byte(entry.Data),
				Sequence:  &entry.Sequence,
				Stream:    &entry.Stream,
				Timestamp: timestamppb.New(entry.Timestamp),
				Completed: entry.Completed.Enum(),
			}

			if err := stream.Send(chunk); err != nil {
				return err
			}

			maxReplayedSeq = entry.Sequence
		default:
			goto afterDrain
		}
	}

afterDrain:
	if sawCompletionDuringReplay || buffer.IsTerminal() {
		// One final non-blocking drain: the completion event that set terminal
		// may have been broadcast after the drain loop exited but before this check.
		select {
		case msg, ok := <-outCh:
			if ok {
				var entry LogEntry
				if err := json.Unmarshal(msg, &entry); err == nil && entry.Sequence > maxReplayedSeq {
					chunk := &api.LogChunk{
						RunId:     &runID,
						Data:      []byte(entry.Data),
						Sequence:  &entry.Sequence,
						Stream:    &entry.Stream,
						Timestamp: timestamppb.New(entry.Timestamp),
						Completed: entry.Completed.Enum(),
					}

					if err := stream.Send(chunk); err != nil {
						return err
					}
				}
			}
		default:
		}

		return nil
	}

	// Phase 2: Live subscription — drain the channel, skipping entries already
	// replayed (deduplicated by sequence).
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-outCh:
			if !ok {
				return nil
			}

			var entry LogEntry
			if err := json.Unmarshal(msg, &entry); err != nil {
				continue
			}

			if entry.Sequence <= maxReplayedSeq {
				continue
			}

			chunk := &api.LogChunk{
				RunId:     &runID,
				Data:      []byte(entry.Data),
				Sequence:  &entry.Sequence,
				Stream:    &entry.Stream,
				Timestamp: timestamppb.New(entry.Timestamp),
				Completed: entry.Completed.Enum(),
			}

			if err := stream.Send(chunk); err != nil {
				return err
			}

			if buffer.IsTerminal() {
				return nil
			}
		}
	}
}

func boundedReplayEntries(entries []LogEntry, sinceSeq int64, tail, replayLimit int) ([]LogEntry, bool) {
	filtered := make([]LogEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.Sequence > sinceSeq {
			filtered = append(filtered, entry)
		}
	}

	if tail > 0 && len(filtered) > tail {
		filtered = filtered[len(filtered)-tail:]
	}

	if replayLimit > 0 && len(filtered) > replayLimit {
		return filtered[:replayLimit], true
	}

	return filtered, false
}

func replayTruncatedChunk(runID string, replayLimit int) *api.LogChunk {
	seq := int64(-1)
	stream := api.Stream_STREAM_CONTROL
	data, _ := json.Marshal(struct {
		Event string `json:"event"`
		Limit int    `json:"limit"`
	}{
		Event: "replay_truncated",
		Limit: replayLimit,
	})

	return &api.LogChunk{
		RunId:     &runID,
		Data:      data,
		Sequence:  &seq,
		Stream:    &stream,
		Timestamp: timestamppb.Now(),
	}
}

func positiveIntMetadata(ctx context.Context, key string) int {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 0
	}

	values := md.Get(key)
	if len(values) == 0 {
		return 0
	}

	n, err := strconv.Atoi(values[0])
	if err != nil || n <= 0 {
		return 0
	}

	return n
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

func isCompletedEvent(entry LogEntry) bool {
	if entry.Completed != api.RunOutcome_RUN_OUTCOME_UNSPECIFIED {
		return true
	}

	if entry.Stream != api.Stream_STREAM_CONTROL {
		return false
	}

	var meta struct {
		Event string `json:"event"`
	}

	if err := json.Unmarshal([]byte(entry.Data), &meta); err != nil {
		return false
	}

	return meta.Event == "completed"
}

func (s *Server) RunGRPC(ctx context.Context, port string) error {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}

	srvOpts, err := config.GRPCServerOptions()
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(srvOpts...)
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

func Run(ctx context.Context, logger interfaces.Logger, store RunLogStore, metrics *observability.LogMetrics) error {
	if err := config.ValidateGRPCTLSForRole(config.GRPCTLSDaemonLog); err != nil {
		return err
	}
	config.StartGRPCTLSReloadLoop(ctx)

	server := NewServerWithStore(logger, store, metrics)
	server.maxRunBuffers = config.LogMaxRunBuffers()

	if config.LogRegisterWithRegistry() {
		regAddr := config.LogRegistrationRegistryAddress()

		bindGRPC := config.LogGRPCListenAddr()
		publishAddr := config.LogGRPCRegistryPublishAddress(bindGRPC)
		stopRegistration, err := registry.RegisterWithHeartbeat(ctx, registry.RegistrationOptions{
			RegistryAddress: regAddr,
			Component:       api.Component_COMPONENT_LOG,
			PublishAddress:  publishAddr,
			RefreshInterval: config.RegistryRegistrationRefresh(),
			Logger:          logger,
		})

		if err != nil {
			return err
		}

		defer stopRegistration()
		logger.Info("Registered log service with registry at %s", publishAddr)
	} else {
		logger.Info("Skipping registry registration (log.grpc.register_with_registry is false)")
	}

	return server.RunGRPC(ctx, config.LogGRPCListenAddr())
}
