package logserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	api "vectis/api/gen/go"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/logroute"
	"vectis/internal/observability"
	"vectis/internal/registry"
)

const (
	MaxLogLinesPerJob = 10000
	MaxRunBuffers     = 1024

	defaultLogAppendBatchSize          = 256
	defaultLogAppendBatchFlushInterval = 10 * time.Millisecond

	GetLogsReplayLimitMetadata = "vectis-log-replay-limit"
	GetLogsTailMetadata        = "vectis-log-tail"
)

type RunOptions struct {
	InstanceID string
}

type newRunWritableReporter interface {
	NewRunWritable() bool
}

type streamLogEntry struct {
	runID string
	entry LogEntry
}

type streamLogReceive struct {
	chunk *api.LogChunk
	err   error
}

type runLogEntryGroup struct {
	runID   string
	entries []LogEntry
}

func DefaultInstanceID(bindAddr string) string {
	hostname, err := os.Hostname()
	if err == nil {
		hostname = strings.TrimSpace(hostname)
	}

	if hostname == "" {
		hostname = "log"
	}

	_, port, err := net.SplitHostPort(bindAddr)
	if err != nil || port == "" {
		return hostname
	}

	return hostname + "-" + port
}

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
	jb.subMu.RLock()
	hasSubscribers := len(jb.subscribers) > 0
	jb.subMu.RUnlock()
	if !hasSubscribers {
		return
	}

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
	// Synthetic completion is intended for direct worker streams and applies to
	// the last run seen. Forwarder batch streams may carry multiple runs, but do
	// not request synthetic completion.
	ctx := stream.Context()
	syntheticCompletion := boolMetadata(ctx, interfaces.LogSyntheticCompletionMetadata)
	var lastBuffer *JobBuffer
	var lastRunID string
	var route logroute.StreamRoute
	pending := make([]streamLogEntry, 0, defaultLogAppendBatchSize)

	flushPending := func() error {
		if len(pending) == 0 {
			return nil
		}

		if err := s.appendLogEntryBatch(ctx, pending); err != nil {
			return err
		}

		for _, item := range pending {
			lastRunID = item.runID
			lastBuffer = s.publishLogEntry(ctx, item.runID, item.entry)
		}

		pending = pending[:0]
		return nil
	}

	recvDone := make(chan struct{})
	recvCh := make(chan streamLogReceive, 1)
	defer close(recvDone)
	go receiveLogStream(stream, recvDone, recvCh)

	ticker := time.NewTicker(defaultLogAppendBatchFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if err := flushPending(); err != nil {
				return err
			}

			return ctx.Err()
		case <-ticker.C:
			if err := flushPending(); err != nil {
				return err
			}
		case received := <-recvCh:
			if received.err != nil {
				if err := flushPending(); err != nil {
					return err
				}

				if errors.Is(received.err, io.EOF) {
					// Worker stream ended. Emit synthetic completion if needed.
					if syntheticCompletion && lastBuffer != nil && !lastBuffer.IsTerminal() {
						s.logger.Warn("Stream ended for run %s without completion event", lastRunID)

						entries := lastBuffer.GetEntries()
						synthetic := LogEntry{
							Timestamp: time.Now(),
							Stream:    api.Stream_STREAM_CONTROL,
							Sequence:  nextSequence(entries),
							Data:      `{"event":"completed","status":"unknown","synthetic":true}`,
							Completed: api.RunOutcome_RUN_OUTCOME_UNKNOWN,
						}

						if err := s.appendLogEntryBatch(ctx, []streamLogEntry{{runID: lastRunID, entry: synthetic}}); err != nil {
							s.logger.Warn("Failed to store synthetic completion for run %s: %v", lastRunID, err)
						}

						s.publishLogEntry(ctx, lastRunID, synthetic)
					}

					return stream.SendAndClose(&api.Empty{})
				}

				return received.err
			}

			if s.metrics != nil {
				s.metrics.RecordGRPCChunk(ctx)
			}

			chunk := received.chunk
			streamRoute, err := route.Bind(chunk)
			if err != nil {
				return status.Error(codes.InvalidArgument, err.Error())
			}

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

			pending = append(pending, streamLogEntry{runID: streamRoute.RunID, entry: entry})
			if len(pending) >= defaultLogAppendBatchSize {
				if err := flushPending(); err != nil {
					return err
				}
			}
		}
	}
}

func receiveLogStream(stream api.LogService_StreamLogsServer, done <-chan struct{}, out chan<- streamLogReceive) {
	for {
		chunk, err := stream.Recv()
		select {
		case out <- streamLogReceive{chunk: chunk, err: err}:
		case <-done:
			return
		}

		if err != nil {
			return
		}
	}
}

func (s *Server) appendLogEntryBatch(ctx context.Context, entries []streamLogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	if err := s.persistLogEntryBatch(entries); err != nil {
		if s.metrics != nil {
			s.metrics.RecordAppendFailure(ctx)
		}

		if errors.Is(err, ErrLogStoreReadOnly) {
			return status.Error(codes.ResourceExhausted, err.Error())
		}

		return err
	}

	return nil
}

func (s *Server) persistLogEntryBatch(entries []streamLogEntry) error {
	if batchStore, ok := s.store.(RunLogBatchStore); ok {
		for _, group := range groupStreamLogEntries(entries) {
			if err := batchStore.AppendBatch(group.runID, group.entries); err != nil {
				return err
			}
		}

		return nil
	}

	for _, item := range entries {
		if err := s.store.Append(item.runID, item.entry); err != nil {
			return err
		}
	}

	return nil
}

func groupStreamLogEntries(entries []streamLogEntry) []runLogEntryGroup {
	groupIndex := make(map[string]int)
	groups := make([]runLogEntryGroup, 0)
	for _, item := range entries {
		idx, ok := groupIndex[item.runID]
		if !ok {
			idx = len(groups)
			groupIndex[item.runID] = idx
			groups = append(groups, runLogEntryGroup{runID: item.runID})
		}

		groups[idx].entries = append(groups[idx].entries, item.entry)
	}

	return groups
}

func (s *Server) publishLogEntry(ctx context.Context, runID string, entry LogEntry) *JobBuffer {
	buffer := s.getOrCreateBuffer(runID)

	if !buffer.Add(entry) {
		if s.metrics != nil {
			s.metrics.RecordMemoryBufferDrop(ctx)
		}

		s.logger.Warn("Log buffer full for run %s, dropping log line (seq %d)", runID, entry.Sequence)
	}

	buffer.Broadcast(runID, entry)

	if buffer.IsTerminal() {
		s.evictTerminalBuffers()
	}

	s.logger.Debug("Received log from run %s (seq %d)", runID, entry.Sequence)
	return buffer
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

	terminalSeq := terminalSequence(entries)
	terminalAlreadyConsumed := terminalSeq > 0 && terminalSeq <= sinceSeq
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
			if isCompletedEvent(entry) {
				return nil
			}
		default:
			goto afterDrain
		}
	}

afterDrain:
	if sawCompletionDuringReplay || terminalAlreadyConsumed {
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

			maxReplayedSeq = entry.Sequence
			if isCompletedEvent(entry) {
				return nil
			}
		}
	}
}

func terminalSequence(entries []LogEntry) int64 {
	var seq int64
	for _, entry := range entries {
		if isCompletedEvent(entry) && entry.Sequence > seq {
			seq = entry.Sequence
		}
	}

	return seq
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

func boolMetadata(ctx context.Context, key string) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}

	values := md.Get(key)
	if len(values) == 0 {
		return false
	}

	switch values[0] {
	case "1", "true", "TRUE", "True", "yes", "YES", "Yes", "on", "ON", "On":
		return true
	default:
		return false
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

	srvOpts, err := config.GRPCServerOptionsForRole(config.ServiceIdentityRoleLog)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(srvOpts...)
	api.RegisterLogServiceServer(grpcServer, s)

	hs := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, hs)
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
	return RunWithOptions(ctx, logger, store, metrics, RunOptions{})
}

func RunWithOptions(ctx context.Context, logger interfaces.Logger, store RunLogStore, metrics *observability.LogMetrics, opts RunOptions) error {
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
		instanceID := opts.InstanceID
		if instanceID == "" {
			instanceID = DefaultInstanceID(bindGRPC)
		}

		stopRegistration, err := registerLogWithHeartbeat(ctx, regAddr, instanceID, publishAddr, store, logger)
		if err != nil {
			return err
		}

		defer stopRegistration()
		logger.Info("Registered log service %s with registry at %s", instanceID, publishAddr)
	} else {
		logger.Info("Skipping registry registration (log.grpc.register_with_registry is false)")
	}

	return server.RunGRPC(ctx, config.LogGRPCListenAddr())
}

func registerLogWithHeartbeat(ctx context.Context, registryAddress, instanceID, publishAddress string, store RunLogStore, logger interfaces.Logger) (func(), error) {
	interval := config.RegistryRegistrationRefresh()
	if interval <= 0 {
		interval = 45 * time.Second
	}

	return registry.RegisterWithDynamicMetadataHeartbeat(ctx, registry.RegistrationOptions{
		RegistryAddress: registryAddress,
		Component:       api.Component_COMPONENT_LOG,
		InstanceID:      instanceID,
		PublishAddress:  publishAddress,
		RefreshInterval: interval,
		Logger:          logger,
	}, func() map[string]string {
		return logServiceMetadata(store)
	})
}

func logServiceMetadata(store RunLogStore) map[string]string {
	metadata := registry.DefaultServiceMetadataForCell(config.CellID())
	metadata[registry.MetadataLogWriteState] = registry.LogWriteStateWritable

	if reporter, ok := store.(newRunWritableReporter); ok && !reporter.NewRunWritable() {
		metadata[registry.MetadataLogWriteState] = registry.LogWriteStateReadOnly
	}

	return metadata
}
