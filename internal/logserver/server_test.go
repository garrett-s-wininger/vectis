package logserver

import (
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"
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
