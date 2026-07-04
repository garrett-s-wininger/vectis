package logserver

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/logbatch"
	"vectis/internal/registry"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func completedLogEntry(ts time.Time, seq int64) LogEntry {
	return LogEntry{
		Timestamp: ts,
		Stream:    api.Stream_STREAM_CONTROL,
		Sequence:  seq,
		Data:      []byte(`{"event":"completed","status":"success"}`),
	}
}

func stdoutLogEntry(ts time.Time, seq int64) LogEntry {
	return LogEntry{
		Timestamp: ts,
		Stream:    api.Stream_STREAM_STDOUT,
		Sequence:  seq,
		Data:      []byte("line"),
	}
}

type logMetadataStore struct {
	writable bool
}

func (s logMetadataStore) Append(string, LogEntry) error {
	return nil
}

func (s logMetadataStore) List(string) ([]LogEntry, error) {
	return nil, nil
}

func (s logMetadataStore) NewRunWritable() bool {
	return s.writable
}

type recordingRunLogStore struct {
	entries map[string][]LogEntry
}

func (s *recordingRunLogStore) Append(runID string, entry LogEntry) error {
	if s.entries == nil {
		s.entries = make(map[string][]LogEntry)
	}

	s.entries[runID] = append(s.entries[runID], entry)
	return nil
}

func (s *recordingRunLogStore) List(runID string) ([]LogEntry, error) {
	return append([]LogEntry(nil), s.entries[runID]...), nil
}

type fakeStreamLogsServer struct {
	ctx    context.Context
	chunks []*api.LogChunk
	closed bool
}

func (s *fakeStreamLogsServer) Recv() (*api.LogChunk, error) {
	if len(s.chunks) == 0 {
		return nil, io.EOF
	}

	chunk := s.chunks[0]
	s.chunks = s.chunks[1:]
	return chunk, nil
}

func (s *fakeStreamLogsServer) SendAndClose(*api.Empty) error {
	s.closed = true
	return nil
}

func (s *fakeStreamLogsServer) SetHeader(metadata.MD) error {
	return nil
}

func (s *fakeStreamLogsServer) SendHeader(metadata.MD) error {
	return nil
}

func (s *fakeStreamLogsServer) SetTrailer(metadata.MD) {}

func (s *fakeStreamLogsServer) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}

	return context.Background()
}

func (s *fakeStreamLogsServer) SendMsg(any) error {
	return nil
}

func (s *fakeStreamLogsServer) RecvMsg(any) error {
	return nil
}

type fakeGetLogsServer struct {
	ctx    context.Context
	chunks []*api.LogChunk
}

func (s *fakeGetLogsServer) Send(chunk *api.LogChunk) error {
	s.chunks = append(s.chunks, chunk)
	return nil
}

func (s *fakeGetLogsServer) SetHeader(metadata.MD) error {
	return nil
}

func (s *fakeGetLogsServer) SendHeader(metadata.MD) error {
	return nil
}

func (s *fakeGetLogsServer) SetTrailer(metadata.MD) {}

func (s *fakeGetLogsServer) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}

	return context.Background()
}

func (s *fakeGetLogsServer) SendMsg(any) error {
	return nil
}

func (s *fakeGetLogsServer) RecvMsg(any) error {
	return nil
}

func TestStreamLogsRejectsMixedRunStream(t *testing.T) {
	store := &recordingRunLogStore{}
	s := NewServerWithStore(mocks.NopLogger{}, store, nil)
	stream := &fakeStreamLogsServer{
		chunks: []*api.LogChunk{
			{RunId: proto.String("run-1"), Sequence: proto.Int64(1), Data: []byte("one")},
			{RunId: proto.String("run-2"), Sequence: proto.Int64(1), Data: []byte("two")},
		},
	}

	err := s.StreamLogs(stream)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("StreamLogs error = %v, want InvalidArgument", err)
	}

	if got := len(store.entries["run-1"]); got != 1 {
		t.Fatalf("stored run-1 entries = %d, want 1", got)
	}

	if got := len(store.entries["run-2"]); got != 0 {
		t.Fatalf("stored run-2 entries = %d, want 0", got)
	}

	if stream.closed {
		t.Fatal("mixed-run stream should return an error, not SendAndClose")
	}
}

func TestStreamLogsAssignsRunGlobalSequencesAcrossTaskStreams(t *testing.T) {
	store := &recordingRunLogStore{}
	s := NewServerWithStore(mocks.NopLogger{}, store, nil)
	runID := "run-task-streams"
	stdout := api.Stream_STREAM_STDOUT
	control := api.Stream_STREAM_CONTROL
	success := api.RunOutcome_RUN_OUTCOME_SUCCESS

	rootStream := &fakeStreamLogsServer{
		chunks: []*api.LogChunk{
			{RunId: proto.String(runID), Sequence: proto.Int64(1), Stream: &stdout, Data: []byte("root start")},
			{RunId: proto.String(runID), Sequence: proto.Int64(2), Stream: &control, Completed: success.Enum(), Data: []byte(`{"event":"completed","status":"success"}`)},
		},
	}

	if err := s.StreamLogs(rootStream); err != nil {
		t.Fatalf("root StreamLogs: %v", err)
	}

	fanoutStream := &fakeStreamLogsServer{
		chunks: []*api.LogChunk{
			{RunId: proto.String(runID), Sequence: proto.Int64(1), Stream: &stdout, Data: []byte("fanout after root completion")},
		},
	}

	if err := s.StreamLogs(fanoutStream); err != nil {
		t.Fatalf("fanout StreamLogs: %v", err)
	}

	entries := store.entries[runID]
	if len(entries) != 3 {
		t.Fatalf("stored entries = %d, want 3", len(entries))
	}

	for i, entry := range entries {
		wantSeq := int64(i + 1)
		if entry.Sequence != wantSeq {
			t.Fatalf("entry %d sequence = %d, want %d", i, entry.Sequence, wantSeq)
		}
	}

	replay := &fakeGetLogsServer{}
	if err := s.GetLogs(&api.GetLogsRequest{RunId: proto.String(runID), SinceSequence: proto.Int64(2)}, replay); err != nil {
		t.Fatalf("GetLogs: %v", err)
	}

	if len(replay.chunks) != 1 {
		t.Fatalf("replayed chunks = %d, want 1", len(replay.chunks))
	}

	if got := string(replay.chunks[0].GetData()); got != "fanout after root completion" {
		t.Fatalf("replayed data = %q, want fanout log", got)
	}

	if replay.chunks[0].GetSequence() != 3 {
		t.Fatalf("replayed sequence = %d, want 3", replay.chunks[0].GetSequence())
	}
}

type batchStoreCall struct {
	runID     string
	sequences []int64
}

type recordingBatchRunLogStore struct {
	appendCalls int
	batchCalls  []batchStoreCall
}

func (s *recordingBatchRunLogStore) Append(string, LogEntry) error {
	s.appendCalls++
	return nil
}

func (s *recordingBatchRunLogStore) AppendBatch(runID string, entries []LogEntry) error {
	call := batchStoreCall{runID: runID, sequences: make([]int64, 0, len(entries))}
	for _, entry := range entries {
		call.sequences = append(call.sequences, entry.Sequence)
	}

	s.batchCalls = append(s.batchCalls, call)
	return nil
}

func (s *recordingBatchRunLogStore) List(string) ([]LogEntry, error) {
	return nil, nil
}

type replayRecordingRunLogStore struct {
	replayCalls  int
	listCalls    int
	lastOptions  LogReplayOptions
	replayResult LogReplayResult
	listEntries  []LogEntry
}

func (s *replayRecordingRunLogStore) Append(string, LogEntry) error {
	return nil
}

func (s *replayRecordingRunLogStore) List(string) ([]LogEntry, error) {
	s.listCalls++
	return s.listEntries, nil
}

func (s *replayRecordingRunLogStore) Replay(_ string, opts LogReplayOptions) (LogReplayResult, error) {
	s.replayCalls++
	s.lastOptions = opts
	return s.replayResult, nil
}

func TestServer_EvictsOldestTerminalBufferOverLimit(t *testing.T) {
	s := NewServer(mocks.NopLogger{})
	s.maxRunBuffers = 2

	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	s.getOrCreateBuffer("run-1").Add(completedLogEntry(base, 1))
	s.getOrCreateBuffer("run-2").Add(completedLogEntry(base.Add(time.Second), 1))
	s.getOrCreateBuffer("run-3").Add(stdoutLogEntry(base.Add(2*time.Second), 1))

	if got := s.bufferCount(); got != 2 {
		t.Fatalf("expected 2 buffers after eviction, got %d", got)
	}

	if _, ok := s.buffers["run-1"]; ok {
		t.Fatal("expected oldest terminal buffer to be evicted")
	}

	if _, ok := s.buffers["run-2"]; !ok {
		t.Fatal("expected newer terminal buffer to remain")
	}

	if _, ok := s.buffers["run-3"]; !ok {
		t.Fatal("expected nonterminal buffer to remain")
	}
}

func TestServerPersistsStreamEntriesWithBatchStore(t *testing.T) {
	store := &recordingBatchRunLogStore{}
	s := NewServerWithStore(mocks.NopLogger{}, store, nil)
	entries := []streamLogEntry{
		{runID: "run-a", entry: LogEntry{Sequence: 1}},
		{runID: "run-b", entry: LogEntry{Sequence: 2}},
		{runID: "run-a", entry: LogEntry{Sequence: 3}},
	}

	if err := s.persistLogEntryBatch(entries); err != nil {
		t.Fatalf("persist log entry batch: %v", err)
	}

	if store.appendCalls != 0 {
		t.Fatalf("append calls = %d, want 0", store.appendCalls)
	}

	want := []batchStoreCall{
		{runID: "run-a", sequences: []int64{1, 3}},
		{runID: "run-b", sequences: []int64{2}},
	}

	if !reflect.DeepEqual(store.batchCalls, want) {
		t.Fatalf("batch calls = %+v, want %+v", store.batchCalls, want)
	}
}

func TestServerSendLogBatchPersistsAndPublishes(t *testing.T) {
	store := &recordingBatchRunLogStore{}
	s := NewServerWithStore(mocks.NopLogger{}, store, nil)
	buffer := s.getOrCreateBuffer("run-a")
	ch := make(chan LogEntry, 2)
	buffer.Subscribe(ch)

	runID := "run-a"
	streamType := api.Stream_STREAM_STDOUT
	seq1 := int64(1)
	seq2 := int64(2)
	records, err := logbatch.MarshalChunks([]*api.LogChunk{
		{RunId: &runID, Sequence: &seq1, Stream: &streamType, Data: []byte("first")},
		{RunId: &runID, Sequence: &seq2, Stream: &streamType, Data: []byte("second")},
	})

	if err != nil {
		t.Fatalf("marshal log batch: %v", err)
	}

	_, err = s.SendLogBatch(t.Context(), &api.LogBatch{Records: records})
	if err != nil {
		t.Fatalf("send log batch: %v", err)
	}

	want := []batchStoreCall{{runID: "run-a", sequences: []int64{1, 2}}}
	if !reflect.DeepEqual(store.batchCalls, want) {
		t.Fatalf("batch calls = %+v, want %+v", store.batchCalls, want)
	}

	for wantSeq := int64(1); wantSeq <= 2; wantSeq++ {
		select {
		case got := <-ch:
			if got.Sequence != wantSeq {
				t.Fatalf("subscriber sequence = %d, want %d", got.Sequence, wantSeq)
			}
		default:
			t.Fatalf("missing subscriber entry sequence %d", wantSeq)
		}
	}
}

func TestServerPublishLogEntryGroupsBatchesSubscriberFanout(t *testing.T) {
	s := NewServer(mocks.NopLogger{})
	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	buffer := s.getOrCreateBuffer("run-a")
	ch := make(chan LogEntry, 4)
	buffer.Subscribe(ch)

	groups := []runLogEntryGroup{
		{
			runID: "run-a",
			entries: []LogEntry{
				stdoutLogEntry(base, 1),
				stdoutLogEntry(base.Add(time.Second), 2),
				completedLogEntry(base.Add(2*time.Second), 3),
			},
		},
	}

	lastRunID, lastBuffer := s.publishLogEntryGroups(t.Context(), groups, "run-a")
	if lastRunID != "run-a" {
		t.Fatalf("last run id = %q, want run-a", lastRunID)
	}

	if lastBuffer != buffer {
		t.Fatal("expected publish to return the run buffer")
	}

	if !buffer.IsTerminal() {
		t.Fatal("expected batch publish to mark buffer terminal")
	}

	for wantSeq := int64(1); wantSeq <= 3; wantSeq++ {
		select {
		case got := <-ch:
			if got.Sequence != wantSeq {
				t.Fatalf("subscriber sequence = %d, want %d", got.Sequence, wantSeq)
			}
		default:
			t.Fatalf("missing subscriber entry sequence %d", wantSeq)
		}
	}
}

func TestJobBufferBroadcastControlDoesNotBlockWhenSubscriberFull(t *testing.T) {
	buffer := NewJobBuffer(mocks.NopLogger{}, nil)
	ch := make(chan LogEntry, 1)
	ch <- stdoutLogEntry(time.Now(), 1)
	buffer.Subscribe(ch)

	done := make(chan struct{})
	go func() {
		buffer.Broadcast(context.Background(), "run-a", completedLogEntry(time.Now(), 2))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("control broadcast blocked behind a full subscriber channel")
	}

	buffer.Unsubscribe(ch)
}

func TestJobBufferRecordsTerminalEntryWhenMemoryBufferFull(t *testing.T) {
	buffer := NewJobBuffer(mocks.NopLogger{}, nil)
	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	for i := 0; i < MaxLogLinesPerJob; i++ {
		if !buffer.Add(stdoutLogEntry(base.Add(time.Duration(i)*time.Millisecond), int64(i+1))) {
			t.Fatalf("stdout entry %d unexpectedly dropped", i+1)
		}
	}

	terminal := completedLogEntry(base.Add(time.Minute), MaxLogLinesPerJob+1)
	if buffer.Add(terminal) {
		t.Fatal("terminal entry unexpectedly fit in full memory buffer")
	}

	if !buffer.IsTerminal() {
		t.Fatal("expected full buffer to retain terminal state")
	}

	got, ok := buffer.TerminalEntry()
	if !ok {
		t.Fatal("expected full buffer to retain terminal entry")
	}

	if got.Sequence != terminal.Sequence || !bytes.Equal(got.Data, terminal.Data) {
		t.Fatalf("terminal entry = %+v, want sequence %d data %q", got, terminal.Sequence, terminal.Data)
	}
}

func TestServerReplayHistoricalEntriesUsesReplayStoreWithoutTail(t *testing.T) {
	store := &replayRecordingRunLogStore{
		replayResult: LogReplayResult{
			Found:                   true,
			Truncated:               true,
			TerminalAlreadyConsumed: true,
			Entries:                 []LogEntry{{Sequence: 2, Data: []byte("second")}},
		},
		listEntries: []LogEntry{{Sequence: 1, Data: []byte("first")}},
	}

	s := NewServerWithStore(mocks.NopLogger{}, store, nil)
	entries, terminalAlreadyConsumed, replayTruncated := s.replayHistoricalEntries("run-1", 1, 0, 1, NewJobBuffer(mocks.NopLogger{}, nil))
	if store.replayCalls != 1 {
		t.Fatalf("replay calls = %d, want 1", store.replayCalls)
	}

	if store.listCalls != 0 {
		t.Fatalf("list calls = %d, want 0", store.listCalls)
	}

	if len(entries) != 1 || entries[0].Sequence != 2 {
		t.Fatalf("entries = %+v, want sequence 2", entries)
	}

	if !terminalAlreadyConsumed {
		t.Fatal("expected terminalAlreadyConsumed")
	}

	if !replayTruncated {
		t.Fatal("expected replayTruncated")
	}
}

func TestServerReplayHistoricalEntriesUsesReplayStoreForTail(t *testing.T) {
	store := &replayRecordingRunLogStore{
		replayResult: LogReplayResult{
			Found:   true,
			Entries: []LogEntry{{Sequence: 2, Data: []byte("second")}},
		},
		listEntries: []LogEntry{{Sequence: 99, Data: []byte("list")}},
	}

	s := NewServerWithStore(mocks.NopLogger{}, store, nil)
	entries, _, replayTruncated := s.replayHistoricalEntries("run-1", 0, 1, 10, NewJobBuffer(mocks.NopLogger{}, nil))
	if store.replayCalls != 1 {
		t.Fatalf("replay calls = %d, want 1", store.replayCalls)
	}

	if store.listCalls != 0 {
		t.Fatalf("list calls = %d, want 0", store.listCalls)
	}

	if store.lastOptions.Tail != 1 || store.lastOptions.Limit != 10 {
		t.Fatalf("replay options = %+v, want tail=1 limit=10", store.lastOptions)
	}

	if len(entries) != 1 || entries[0].Sequence != 2 {
		t.Fatalf("entries = %+v, want replay sequence 2", entries)
	}

	if replayTruncated {
		t.Fatal("did not expect replay truncation")
	}
}

func TestServer_DoesNotEvictActiveTerminalBuffer(t *testing.T) {
	s := NewServer(mocks.NopLogger{})
	s.maxRunBuffers = 1

	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	buffer := s.getOrCreateBuffer("active-terminal")
	buffer.Add(completedLogEntry(base, 1))
	ch := make(chan LogEntry, 1)
	buffer.Subscribe(ch)

	s.getOrCreateBuffer("new-run").Add(stdoutLogEntry(base.Add(time.Second), 1))

	if got := s.bufferCount(); got != 2 {
		t.Fatalf("expected active terminal buffer to prevent eviction, got %d buffers", got)
	}

	if _, ok := s.buffers["active-terminal"]; !ok {
		t.Fatal("expected active terminal buffer to remain")
	}

	buffer.Unsubscribe(ch)
	s.evictTerminalBuffers()

	if got := s.bufferCount(); got != 1 {
		t.Fatalf("expected inactive terminal buffer to be evicted, got %d buffers", got)
	}

	if _, ok := s.buffers["active-terminal"]; ok {
		t.Fatal("expected inactive terminal buffer to be evicted")
	}
}

func TestServer_DoesNotEvictNonterminalBuffers(t *testing.T) {
	s := NewServer(mocks.NopLogger{})
	s.maxRunBuffers = 1

	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	s.getOrCreateBuffer("run-1").Add(stdoutLogEntry(base, 1))
	s.getOrCreateBuffer("run-2").Add(stdoutLogEntry(base.Add(time.Second), 1))

	if got := s.bufferCount(); got != 2 {
		t.Fatalf("expected nonterminal buffers to remain even over limit, got %d", got)
	}
}

func TestIsCompletedEvent(t *testing.T) {
	if !isCompletedEvent(completedLogEntry(time.Now(), 1)) {
		t.Fatal("expected completed control event")
	}

	if isCompletedEvent(stdoutLogEntry(time.Now(), 1)) {
		t.Fatal("stdout should not be a completed event")
	}

	entry := completedLogEntry(time.Now(), 1)
	entry.Data = []byte(`{"event":"start"}`)
	if isCompletedEvent(entry) {
		t.Fatal("non-completed control event should not be completed")
	}
}

func TestLogServiceMetadataReportsWritableState(t *testing.T) {
	got := logServiceMetadata(logMetadataStore{writable: true})
	if got[registry.MetadataLogWriteState] != registry.LogWriteStateWritable {
		t.Fatalf("expected writable metadata, got %+v", got)
	}

	if got[registry.MetadataCellID] != registry.DefaultCellID {
		t.Fatalf("expected default cell metadata, got %+v", got)
	}
}

func TestLogServiceMetadataReportsReadOnlyState(t *testing.T) {
	got := logServiceMetadata(logMetadataStore{writable: false})
	if got[registry.MetadataLogWriteState] != registry.LogWriteStateReadOnly {
		t.Fatalf("expected read-only metadata, got %+v", got)
	}
}

func TestBoundedReplayEntriesAppliesSinceTailAndLimit(t *testing.T) {
	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	entries := []LogEntry{
		stdoutLogEntry(base, 1),
		stdoutLogEntry(base, 2),
		stdoutLogEntry(base, 3),
		stdoutLogEntry(base, 4),
		stdoutLogEntry(base, 5),
	}

	got, truncated := boundedReplayEntries(entries, 1, 3, 2)
	if !truncated {
		t.Fatal("expected replay to be truncated by limit")
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].Sequence != 3 || got[1].Sequence != 4 {
		t.Fatalf("sequences = [%d %d], want [3 4]", got[0].Sequence, got[1].Sequence)
	}
}

func TestBoundedReplayEntriesTailWithoutTruncation(t *testing.T) {
	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	entries := []LogEntry{
		stdoutLogEntry(base, 1),
		stdoutLogEntry(base, 2),
		stdoutLogEntry(base, 3),
	}

	got, truncated := boundedReplayEntries(entries, 0, 2, 10)
	if truncated {
		t.Fatal("did not expect replay truncation")
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].Sequence != 2 || got[1].Sequence != 3 {
		t.Fatalf("sequences = [%d %d], want [2 3]", got[0].Sequence, got[1].Sequence)
	}
}

func TestReplayTruncatedChunk(t *testing.T) {
	chunk := replayTruncatedChunk("run-1", 100)
	if chunk.GetRunId() != "run-1" {
		t.Fatalf("run_id = %q, want run-1", chunk.GetRunId())
	}
	if chunk.GetSequence() != -1 {
		t.Fatalf("sequence = %d, want -1", chunk.GetSequence())
	}
	if chunk.GetStream() != api.Stream_STREAM_CONTROL {
		t.Fatalf("stream = %s, want control", chunk.GetStream())
	}
	if string(chunk.GetData()) != `{"event":"replay_truncated","limit":100}` {
		t.Fatalf("data = %s", chunk.GetData())
	}
}

func TestLogEntryMarshalJSONEncodesDataAsString(t *testing.T) {
	entry := LogEntry{
		Timestamp: time.Unix(1, 2).UTC(),
		Stream:    api.Stream_STREAM_STDOUT,
		Sequence:  3,
		Data:      []byte("hello"),
	}

	got, err := entry.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal log entry: %v", err)
	}

	if !bytes.Contains(got, []byte(`"data":"hello"`)) {
		t.Fatalf("marshal data = %s, want string data", got)
	}
}

func TestIsCompletedEvent_ProtoCompletedField(t *testing.T) {
	if !isCompletedEvent(LogEntry{
		Stream:    api.Stream_STREAM_STDOUT,
		Completed: api.RunOutcome_RUN_OUTCOME_SUCCESS,
	}) {
		t.Fatal("proto Completed field should mark entry as completed regardless of stream type")
	}

	if !isCompletedEvent(LogEntry{
		Stream:    api.Stream_STREAM_STDERR,
		Completed: api.RunOutcome_RUN_OUTCOME_FAILURE,
	}) {
		t.Fatal("proto Completed=FAILURE should mark entry as completed")
	}
}

func TestIsCompletedEvent_SyntheticUnknown(t *testing.T) {
	if !isCompletedEvent(LogEntry{
		Stream:    api.Stream_STREAM_CONTROL,
		Data:      []byte(`{"event":"completed","status":"unknown","synthetic":true}`),
		Completed: api.RunOutcome_RUN_OUTCOME_UNKNOWN,
	}) {
		t.Fatal("synthetic completion with Completed=UNKNOWN should be detected")
	}
}

func TestIsCompletedEvent_UnspecifiedButJSONCompleted(t *testing.T) {
	if !isCompletedEvent(LogEntry{
		Stream:    api.Stream_STREAM_CONTROL,
		Data:      []byte(`{"event":"completed","status":"success"}`),
		Completed: api.RunOutcome_RUN_OUTCOME_UNSPECIFIED,
	}) {
		t.Fatal("JSON completed event should be detected even without proto field (backward compat)")
	}
}

func TestIsCompletedEvent_UnspecifiedAndNotJSONCompleted(t *testing.T) {
	if isCompletedEvent(LogEntry{
		Stream:    api.Stream_STREAM_CONTROL,
		Data:      []byte(`{"event":"start"}`),
		Completed: api.RunOutcome_RUN_OUTCOME_UNSPECIFIED,
	}) {
		t.Fatal("non-completed event with UNSPECIFIED should not be detected")
	}
}
