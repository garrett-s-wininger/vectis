package logserver

import (
	"context"
	"io"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"
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
		Data:      `{"event":"completed","status":"success"}`,
	}
}

func stdoutLogEntry(ts time.Time, seq int64) LogEntry {
	return LogEntry{
		Timestamp: ts,
		Stream:    api.Stream_STREAM_STDOUT,
		Sequence:  seq,
		Data:      "line",
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

func TestServer_DoesNotEvictActiveTerminalBuffer(t *testing.T) {
	s := NewServer(mocks.NopLogger{})
	s.maxRunBuffers = 1

	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	buffer := s.getOrCreateBuffer("active-terminal")
	buffer.Add(completedLogEntry(base, 1))
	ch := make(chan []byte, 1)
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
	entry.Data = `{"event":"start"}`
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
		Data:      `{"event":"completed","status":"unknown","synthetic":true}`,
		Completed: api.RunOutcome_RUN_OUTCOME_UNKNOWN,
	}) {
		t.Fatal("synthetic completion with Completed=UNKNOWN should be detected")
	}
}

func TestIsCompletedEvent_UnspecifiedButJSONCompleted(t *testing.T) {
	if !isCompletedEvent(LogEntry{
		Stream:    api.Stream_STREAM_CONTROL,
		Data:      `{"event":"completed","status":"success"}`,
		Completed: api.RunOutcome_RUN_OUTCOME_UNSPECIFIED,
	}) {
		t.Fatal("JSON completed event should be detected even without proto field (backward compat)")
	}
}

func TestIsCompletedEvent_UnspecifiedAndNotJSONCompleted(t *testing.T) {
	if isCompletedEvent(LogEntry{
		Stream:    api.Stream_STREAM_CONTROL,
		Data:      `{"event":"start"}`,
		Completed: api.RunOutcome_RUN_OUTCOME_UNSPECIFIED,
	}) {
		t.Fatal("non-completed event with UNSPECIFIED should not be detected")
	}
}
