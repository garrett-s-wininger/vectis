package logserver

import (
	"errors"
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

func TestLocalRunLogStore_AppendBatchAndList(t *testing.T) {
	store, err := NewLocalRunLogStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local run log store: %v", err)
	}

	runID := "run-append-batch"
	entries := []LogEntry{
		{Sequence: 1, Data: "first"},
		{Sequence: 2, Data: "second"},
		{Sequence: 3, Data: "third"},
	}

	if err := store.AppendBatch(runID, entries); err != nil {
		t.Fatalf("append batch: %v", err)
	}

	got, err := store.List(runID)
	if err != nil {
		t.Fatalf("list entries: %v", err)
	}

	if len(got) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(got))
	}

	for i := range entries {
		if got[i].Sequence != entries[i].Sequence || got[i].Data != entries[i].Data {
			t.Fatalf("entry %d mismatch: got=%+v want=%+v", i, got[i], entries[i])
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

func TestLocalRunLogStore_ReadOnlyThresholdRejectsNewRuns(t *testing.T) {
	store, err := NewLocalRunLogStoreWithOptions(t.TempDir(), LocalRunLogStoreOptions{
		NewRunMinFreeBytes: 100,
		statFS: func(string) (filesystemStats, error) {
			return filesystemStats{freeBytes: 99, freeInodes: 1}, nil
		},
	})

	if err != nil {
		t.Fatalf("new local run log store: %v", err)
	}

	err = store.Append("new-run", LogEntry{Sequence: 1, Data: "x"})
	if !errors.Is(err, ErrLogStoreReadOnly) {
		t.Fatalf("expected ErrLogStoreReadOnly, got %v", err)
	}
}

func TestLocalRunLogStore_ReadOnlyThresholdAllowsExistingRuns(t *testing.T) {
	freeBytes := uint64(1000)
	store, err := NewLocalRunLogStoreWithOptions(t.TempDir(), LocalRunLogStoreOptions{
		NewRunMinFreeBytes: 100,
		statFS: func(string) (filesystemStats, error) {
			return filesystemStats{freeBytes: freeBytes, freeInodes: 1}, nil
		},
	})

	if err != nil {
		t.Fatalf("new local run log store: %v", err)
	}

	if err := store.Append("existing-run", LogEntry{Sequence: 1, Data: "first"}); err != nil {
		t.Fatalf("append initial entry: %v", err)
	}

	freeBytes = 99
	if err := store.Append("existing-run", LogEntry{Sequence: 2, Data: "second"}); err != nil {
		t.Fatalf("append existing run below threshold: %v", err)
	}

	got, err := store.List("existing-run")
	if err != nil {
		t.Fatalf("list existing run: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
}

func TestLocalRunLogStore_EvictedExistingRunsStayWritableBelowThreshold(t *testing.T) {
	freeBytes := uint64(1000)
	store, err := NewLocalRunLogStoreWithOptions(t.TempDir(), LocalRunLogStoreOptions{
		NewRunMinFreeBytes: 100,
		OpenFileLimit:      1,
		statFS: func(string) (filesystemStats, error) {
			return filesystemStats{freeBytes: freeBytes, freeInodes: 1}, nil
		},
	})
	if err != nil {
		t.Fatalf("new local run log store: %v", err)
	}

	if err := store.Append("existing-run", LogEntry{Sequence: 1, Data: "first"}); err != nil {
		t.Fatalf("append initial existing entry: %v", err)
	}
	if err := store.Append("other-run", LogEntry{Sequence: 1, Data: "other"}); err != nil {
		t.Fatalf("append other run: %v", err)
	}

	freeBytes = 99
	if err := store.Append("existing-run", LogEntry{Sequence: 2, Data: "second"}); err != nil {
		t.Fatalf("append evicted existing run below threshold: %v", err)
	}

	got, err := store.List("existing-run")
	if err != nil {
		t.Fatalf("list existing run: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
	for i, wantSeq := range []int64{1, 2} {
		if got[i].Sequence != wantSeq {
			t.Fatalf("entry %d: expected sequence %d, got %d", i, wantSeq, got[i].Sequence)
		}
	}
}

func TestLocalRunLogStore_CloseReleasesCachedRunFiles(t *testing.T) {
	dir := t.TempDir()

	store, err := NewLocalRunLogStore(dir)
	if err != nil {
		t.Fatalf("new local run log store: %v", err)
	}
	if err := store.Append("cached-run", LogEntry{Sequence: 1, Data: "first"}); err != nil {
		t.Fatalf("append cached entry: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store again: %v", err)
	}

	reopened, err := NewLocalRunLogStore(dir)
	if err != nil {
		t.Fatalf("new store after close: %v", err)
	}
	defer reopened.Close()

	got, err := reopened.List("cached-run")
	if err != nil {
		t.Fatalf("list cached run after reopen: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(got))
	}
}

func TestLocalRunLogStore_LocksStorageDir(t *testing.T) {
	dir := t.TempDir()

	first, err := NewLocalRunLogStore(dir)
	if err != nil {
		t.Fatalf("new first store: %v", err)
	}
	defer first.Close()

	if _, err := NewLocalRunLogStore(dir); err == nil {
		t.Fatal("expected second store on same directory to fail")
	}

	if err := first.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}

	second, err := NewLocalRunLogStore(dir)
	if err != nil {
		t.Fatalf("new store after close: %v", err)
	}
	defer second.Close()
}
