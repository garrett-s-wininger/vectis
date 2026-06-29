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
	"vectis/internal/logbatch"
	"vectis/internal/logroute"
	"vectis/internal/observability"
	"vectis/internal/registry"
)

const (
	MaxLogLinesPerJob = 10000
	MaxRunBuffers     = 1024

	defaultLogAppendBatchSize          = 256
	defaultLogAppendBatchFlushInterval = 10 * time.Millisecond
	defaultGetLogsTerminalPollInterval = 100 * time.Millisecond

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
	Data      []byte         `json:"data"`
	Completed api.RunOutcome `json:"completed,omitempty"`
}

func (e LogEntry) MarshalJSON() ([]byte, error) {
	type logEntryJSON struct {
		Timestamp time.Time      `json:"timestamp"`
		Stream    api.Stream     `json:"stream"`
		Sequence  int64          `json:"sequence"`
		Data      string         `json:"data"`
		Completed api.RunOutcome `json:"completed,omitempty"`
	}

	return json.Marshal(logEntryJSON{
		Timestamp: e.Timestamp,
		Stream:    e.Stream,
		Sequence:  e.Sequence,
		Data:      string(e.Data),
		Completed: e.Completed,
	})
}

type JobBuffer struct {
	mu            sync.RWMutex
	entries       []LogEntry
	subscribers   map[chan LogEntry]struct{}
	subMu         sync.RWMutex
	logger        interfaces.Logger
	metrics       *observability.LogMetrics
	lastActivity  time.Time
	terminal      bool
	terminalEntry LogEntry
}

func NewJobBuffer(logger interfaces.Logger, metrics *observability.LogMetrics) *JobBuffer {
	return &JobBuffer{
		entries:      make([]LogEntry, 0, MaxLogLinesPerJob),
		subscribers:  make(map[chan LogEntry]struct{}),
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
		jb.recordTerminalLocked(entry)
	}

	if len(jb.entries) >= MaxLogLinesPerJob {
		return false
	}

	jb.entries = append(jb.entries, entry)
	return true
}

func (jb *JobBuffer) AddBatch(entries []LogEntry) int {
	if len(entries) == 0 {
		return 0
	}

	jb.mu.Lock()
	defer jb.mu.Unlock()

	jb.lastActivity = entries[len(entries)-1].Timestamp
	for _, entry := range entries {
		if isCompletedEvent(entry) {
			jb.recordTerminalLocked(entry)
		}
	}

	available := MaxLogLinesPerJob - len(jb.entries)
	if available <= 0 {
		return len(entries)
	}

	if available > len(entries) {
		available = len(entries)
	}

	jb.entries = append(jb.entries, entries[:available]...)
	return len(entries) - available
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

func (jb *JobBuffer) TerminalEntry() (LogEntry, bool) {
	jb.mu.RLock()
	defer jb.mu.RUnlock()

	if !jb.terminal {
		return LogEntry{}, false
	}

	return jb.terminalEntry, true
}

func (jb *JobBuffer) recordTerminalLocked(entry LogEntry) {
	if !jb.terminal || entry.Sequence >= jb.terminalEntry.Sequence {
		jb.terminalEntry = entry
	}

	jb.terminal = true
}

func (jb *JobBuffer) Subscribe(ch chan LogEntry) {
	jb.subMu.Lock()
	defer jb.subMu.Unlock()
	jb.subscribers[ch] = struct{}{}
}

func (jb *JobBuffer) Unsubscribe(ch chan LogEntry) bool {
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

func (jb *JobBuffer) Broadcast(ctx context.Context, _ string, entry LogEntry) {
	jb.subMu.RLock()
	defer jb.subMu.RUnlock()
	if len(jb.subscribers) == 0 {
		return
	}

	for ch := range jb.subscribers {
		jb.sendToSubscriber(ctx, ch, entry)
	}
}

func (jb *JobBuffer) BroadcastBatch(ctx context.Context, _ string, entries []LogEntry) {
	if len(entries) == 0 {
		return
	}

	jb.subMu.RLock()
	defer jb.subMu.RUnlock()
	if len(jb.subscribers) == 0 {
		return
	}

	for _, entry := range entries {
		for ch := range jb.subscribers {
			jb.sendToSubscriber(ctx, ch, entry)
		}
	}
}

func (jb *JobBuffer) sendToSubscriber(ctx context.Context, ch chan LogEntry, entry LogEntry) {
	select {
	case ch <- entry:
	case <-ctx.Done():
	default:
		if jb.metrics != nil {
			jb.metrics.RecordChannelDrop(ctx)
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

		groups := groupStreamLogEntries(pending)
		if err := s.appendLogEntryGroups(ctx, pending, groups); err != nil {
			return err
		}

		lastRunID, lastBuffer = s.publishLogEntryGroups(ctx, groups, pending[len(pending)-1].runID)

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
							Data:      []byte(`{"event":"completed","status":"unknown","synthetic":true}`),
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
				if flushErr := flushPending(); flushErr != nil {
					return flushErr
				}

				return status.Error(codes.InvalidArgument, err.Error())
			}

			item, err := streamLogEntryFromChunk(received.chunk)
			if err != nil {
				return err
			}
			item.runID = streamRoute.RunID

			pending = append(pending, item)
			if len(pending) >= defaultLogAppendBatchSize {
				if err := flushPending(); err != nil {
					return err
				}
			}
		}
	}
}

func (s *Server) SendLogBatch(ctx context.Context, batch *api.LogBatch) (*api.Empty, error) {
	if batch == nil {
		return nil, status.Error(codes.InvalidArgument, "log batch is required")
	}

	records := batch.GetRecords()
	if len(records) == 0 {
		return &api.Empty{}, nil
	}

	entries := make([]streamLogEntry, 0, defaultLogAppendBatchSize)
	if err := logbatch.DecodeRecords(records, func(record logbatch.Record) error {
		if s.metrics != nil {
			s.metrics.RecordGRPCChunk(ctx)
		}

		timestamp := time.Now()
		if record.HasTimestamp {
			timestamp = record.Timestamp
		}

		entries = append(entries, streamLogEntry{
			runID: record.RunID,
			entry: LogEntry{
				Timestamp: timestamp,
				Stream:    record.Stream,
				Sequence:  record.Sequence,
				Data:      record.Data,
				Completed: record.Completed,
			},
		})

		return nil
	}); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "decode log batch: %v", err)
	}

	if len(entries) == 0 {
		return &api.Empty{}, nil
	}

	groups := groupStreamLogEntries(entries)
	if err := s.appendLogEntryGroups(ctx, entries, groups); err != nil {
		return nil, err
	}

	s.publishLogEntryGroups(ctx, groups, entries[len(entries)-1].runID)
	return &api.Empty{}, nil
}

func streamLogEntryFromChunk(chunk *api.LogChunk) (streamLogEntry, error) {
	if chunk == nil {
		return streamLogEntry{}, status.Error(codes.InvalidArgument, "log chunk is required")
	}

	now := time.Now()
	if ts := chunk.GetTimestamp(); ts != nil {
		now = ts.AsTime()
	}

	return streamLogEntry{
		runID: chunk.GetRunId(),
		entry: LogEntry{
			Timestamp: now,
			Stream:    chunk.GetStream(),
			Sequence:  chunk.GetSequence(),
			Data:      chunk.GetData(),
			Completed: chunk.GetCompleted(),
		},
	}, nil
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

	return s.appendLogEntryGroups(ctx, entries, groupStreamLogEntries(entries))
}

func (s *Server) appendLogEntryGroups(ctx context.Context, entries []streamLogEntry, groups []runLogEntryGroup) error {
	if len(entries) == 0 {
		return nil
	}

	if err := s.persistLogEntryGroups(entries, groups); err != nil {
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
	return s.persistLogEntryGroups(entries, groupStreamLogEntries(entries))
}

func (s *Server) persistLogEntryGroups(entries []streamLogEntry, groups []runLogEntryGroup) error {
	if batchStore, ok := s.store.(RunLogBatchStore); ok {
		for _, group := range groups {
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

func (s *Server) publishLogEntryGroups(ctx context.Context, groups []runLogEntryGroup, lastRunID string) (string, *JobBuffer) {
	var lastBuffer *JobBuffer
	var terminalSeen bool

	for _, group := range groups {
		buffer := s.getOrCreateBuffer(group.runID)
		dropped := buffer.AddBatch(group.entries)
		for range dropped {
			if s.metrics != nil {
				s.metrics.RecordMemoryBufferDrop(ctx)
			}
		}

		if dropped > 0 {
			firstDropped := group.entries[len(group.entries)-dropped].Sequence
			lastDropped := group.entries[len(group.entries)-1].Sequence
			s.logger.Warn("Log buffer full for run %s, dropping %d log lines from flush (seq %d-%d)", group.runID, dropped, firstDropped, lastDropped)
		}

		buffer.BroadcastBatch(ctx, group.runID, group.entries)

		if buffer.IsTerminal() {
			terminalSeen = true
		}

		if group.runID == lastRunID {
			lastBuffer = buffer
		}

		firstSeq := group.entries[0].Sequence
		lastSeq := group.entries[len(group.entries)-1].Sequence
		s.logger.Debug("Received %d logs from run %s (seq %d-%d)", len(group.entries), group.runID, firstSeq, lastSeq)
	}

	if terminalSeen {
		s.evictTerminalBuffers()
	}

	return lastRunID, lastBuffer
}

func groupStreamLogEntries(entries []streamLogEntry) []runLogEntryGroup {
	if len(entries) == 0 {
		return nil
	}

	firstRunID := entries[0].runID
	sameRun := true
	for _, item := range entries[1:] {
		if item.runID != firstRunID {
			sameRun = false
			break
		}
	}

	if sameRun {
		group := runLogEntryGroup{
			runID:   firstRunID,
			entries: make([]LogEntry, len(entries)),
		}

		for i, item := range entries {
			group.entries[i] = item.entry
		}

		return []runLogEntryGroup{group}
	}

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

	buffer.Broadcast(ctx, runID, entry)

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
	outCh := make(chan LogEntry, 256)
	buffer.Subscribe(outCh)
	defer func() {
		buffer.Unsubscribe(outCh)
		s.evictTerminalBuffers()
	}()

	// Phase 1: Replay historical entries
	entries, terminalAlreadyConsumed, replayTruncated := s.replayHistoricalEntries(runID, sinceSeq, tail, replayLimit, buffer)

	var maxReplayedSeq = sinceSeq
	var sawCompletionDuringReplay bool
	for _, entry := range entries {
		if err := sendLogEntryChunk(stream, runID, entry); err != nil {
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

			if msg.Sequence <= maxReplayedSeq {
				continue
			}

			if err := sendLogEntryChunk(stream, runID, msg); err != nil {
				return err
			}

			maxReplayedSeq = msg.Sequence
			if isCompletedEvent(msg) {
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

	if done, err := sendBufferedTerminalIfReady(stream, runID, buffer, maxReplayedSeq, outCh); done {
		return err
	}

	terminalTicker := time.NewTicker(defaultGetLogsTerminalPollInterval)
	defer terminalTicker.Stop()

	// Phase 2: Live subscription — drain the channel, skipping entries already
	// replayed (deduplicated by sequence).
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-terminalTicker.C:
			if done, err := sendBufferedTerminalIfReady(stream, runID, buffer, maxReplayedSeq, outCh); done {
				return err
			}
		case msg, ok := <-outCh:
			if !ok {
				return nil
			}

			if msg.Sequence <= maxReplayedSeq {
				continue
			}

			if err := sendLogEntryChunk(stream, runID, msg); err != nil {
				return err
			}

			maxReplayedSeq = msg.Sequence
			if isCompletedEvent(msg) {
				return nil
			}

			if done, err := sendBufferedTerminalIfReady(stream, runID, buffer, maxReplayedSeq, outCh); done {
				return err
			}
		}
	}
}

func sendLogEntryChunk(stream api.LogService_GetLogsServer, runID string, entry LogEntry) error {
	return stream.Send(&api.LogChunk{
		RunId:     &runID,
		Data:      entry.Data,
		Sequence:  &entry.Sequence,
		Stream:    &entry.Stream,
		Timestamp: timestamppb.New(entry.Timestamp),
		Completed: entry.Completed.Enum(),
	})
}

func sendBufferedTerminalIfReady(stream api.LogService_GetLogsServer, runID string, buffer *JobBuffer, maxReplayedSeq int64, outCh chan LogEntry) (bool, error) {
	if len(outCh) != 0 {
		return false, nil
	}

	entry, ok := buffer.TerminalEntry()
	if !ok {
		return false, nil
	}

	if entry.Sequence > maxReplayedSeq {
		if err := sendLogEntryChunk(stream, runID, entry); err != nil {
			return true, err
		}
	}

	return true, nil
}

func (s *Server) replayHistoricalEntries(runID string, sinceSeq int64, tail, replayLimit int, buffer *JobBuffer) ([]LogEntry, bool, bool) {
	if replayStore, ok := s.store.(RunLogReplayStore); ok {
		result, err := replayStore.Replay(runID, LogReplayOptions{
			SinceSequence: sinceSeq,
			Limit:         replayLimit,
			Tail:          tail,
		})

		if err != nil {
			s.logger.Warn("Failed to replay persisted logs for run %s: %v", runID, err)
		} else if result.Found {
			return result.Entries, result.TerminalAlreadyConsumed, result.Truncated
		}
	}

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
	return entries, terminalAlreadyConsumed, replayTruncated
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

	if err := json.Unmarshal(entry.Data, &meta); err != nil {
		return false
	}

	return meta.Event == "completed"
}

func (s *Server) RunGRPC(ctx context.Context, port string) error {
	var listenConfig net.ListenConfig
	lis, err := listenConfig.Listen(ctx, "tcp", port)
	if err != nil {
		return err
	}

	srvOpts, err := config.GRPCServerOptionsForRole(config.ServiceIdentityRoleLog) //nolint:contextcheck // gRPC stream interceptors authorize each RPC via grpc.ServerStream.Context().
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
