package logserver

import (
	"testing"
	"time"

	api "vectis/api/gen/go"
)

func TestLocalRunLogStore_AppendAndList(t *testing.T) {
	store, err := NewLocalRunLogStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local run log store: %v", err)
	}

	runID := "run-append-list"
	now := time.Now().UTC().Truncate(time.Microsecond)
	want := []LogEntry{
		{Timestamp: now, Stream: api.Stream_STREAM_STDOUT, Sequence: 1, Data: "hello"},
		{Timestamp: now.Add(time.Second), Stream: api.Stream_STREAM_STDERR, Sequence: 2, Data: "oops"},
	}

	for _, entry := range want {
		if err := store.Append(runID, entry); err != nil {
			t.Fatalf("append entry: %v", err)
		}
	}

	got, err := store.List(runID)
	if err != nil {
		t.Fatalf("list entries: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("expected %d entries, got %d", len(want), len(got))
	}

	for i := range want {
		if got[i].Sequence != want[i].Sequence || got[i].Data != want[i].Data || got[i].Stream != want[i].Stream {
			t.Fatalf("entry %d mismatch: got=%+v want=%+v", i, got[i], want[i])
		}
	}
}

func TestLocalRunLogStore_ListMissingRun(t *testing.T) {
	store, err := NewLocalRunLogStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local run log store: %v", err)
	}

	got, err := store.List("missing-run")
	if err != nil {
		t.Fatalf("list missing run: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("expected no entries, got %d", len(got))
	}
}

func TestLocalRunLogStore_ListReturnsSortedBySequence(t *testing.T) {
	store, err := NewLocalRunLogStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local run log store: %v", err)
	}

	runID := "run-out-of-order"
	entries := []LogEntry{
		{Sequence: 3, Data: "third"},
		{Sequence: 1, Data: "first"},
		{Sequence: 2, Data: "second"},
	}

	for _, entry := range entries {
		if err := store.Append(runID, entry); err != nil {
			t.Fatalf("append entry: %v", err)
		}
	}

	got, err := store.List(runID)
	if err != nil {
		t.Fatalf("list entries: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(got))
	}

	for i, wantSeq := range []int64{1, 2, 3} {
		if got[i].Sequence != wantSeq {
			t.Fatalf("entry %d: expected sequence %d, got %d", i, wantSeq, got[i].Sequence)
		}
	}
}

func TestLocalRunLogStore_SanitizesRunIDInPath(t *testing.T) {
	store, err := NewLocalRunLogStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local run log store: %v", err)
	}

	runID := "../run/with/slashes"
	if err := store.Append(runID, LogEntry{Sequence: 1, Data: "x"}); err != nil {
		t.Fatalf("append: %v", err)
	}

	got, err := store.List(runID)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(got))
	}
}
