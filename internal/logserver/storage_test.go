package logserver

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	api "vectis/api/gen/go"
)

func newLocalRunLogStoreForTest(t *testing.T) *LocalRunLogStore {
	t.Helper()

	store, err := NewLocalRunLogStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local run log store: %v", err)
	}
	
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close local run log store: %v", err)
		}
	})

	return store
}

func newLocalRunLogStoreWithOptionsForTest(t *testing.T, opts LocalRunLogStoreOptions) *LocalRunLogStore {
	t.Helper()

	store, err := NewLocalRunLogStoreWithOptions(t.TempDir(), opts)
	if err != nil {
		t.Fatalf("new local run log store: %v", err)
	}

	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close local run log store: %v", err)
		}
	})

	return store
}

func TestLocalRunLogStore_AppendAndList(t *testing.T) {
	store := newLocalRunLogStoreForTest(t)

	runID := "run-append-list"
	now := time.Now().UTC().Truncate(time.Microsecond)
	want := []LogEntry{
		{Timestamp: now, Stream: api.Stream_STREAM_STDOUT, Sequence: 1, Data: []byte("hello")},
		{Timestamp: now.Add(time.Second), Stream: api.Stream_STREAM_STDERR, Sequence: 2, Data: []byte("oops")},
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
		if got[i].Sequence != want[i].Sequence || !bytes.Equal(got[i].Data, want[i].Data) || got[i].Stream != want[i].Stream {
			t.Fatalf("entry %d mismatch: got=%+v want=%+v", i, got[i], want[i])
		}
	}
}

func TestLocalRunLogStore_ListMissingRun(t *testing.T) {
	store := newLocalRunLogStoreForTest(t)

	got, err := store.List("missing-run")
	if err != nil {
		t.Fatalf("list missing run: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("expected no entries, got %d", len(got))
	}
}

func TestLocalRunLogStore_ListPreservesAppendOrder(t *testing.T) {
	store := newLocalRunLogStoreForTest(t)

	runID := "run-out-of-order"
	entries := []LogEntry{
		{Sequence: 3, Data: []byte("third")},
		{Sequence: 1, Data: []byte("first")},
		{Sequence: 2, Data: []byte("second")},
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

	for i, wantSeq := range []int64{3, 1, 2} {
		if got[i].Sequence != wantSeq {
			t.Fatalf("entry %d: expected sequence %d, got %d", i, wantSeq, got[i].Sequence)
		}
	}
}

func TestLocalRunLogStore_AppendBatchAndList(t *testing.T) {
	store := newLocalRunLogStoreForTest(t)

	runID := "run-append-batch"
	entries := []LogEntry{
		{Sequence: 1, Data: []byte("first")},
		{Sequence: 2, Data: []byte("second")},
		{Sequence: 3, Data: []byte("third")},
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
		if got[i].Sequence != entries[i].Sequence || !bytes.Equal(got[i].Data, entries[i].Data) {
			t.Fatalf("entry %d mismatch: got=%+v want=%+v", i, got[i], entries[i])
		}
	}
}

func TestLocalRunLogStore_AppendBatchVectoredAndList(t *testing.T) {
	store := newLocalRunLogStoreForTest(t)

	runID := "run-append-batch-vectored"
	payload := bytes.Repeat([]byte{'x'}, 1024)
	entries := make([]LogEntry, maxLogEntryRecordWritevEntries+7)
	for i := range entries {
		entries[i] = LogEntry{Sequence: int64(i + 1), Data: payload}
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
		if got[i].Sequence != entries[i].Sequence || !bytes.Equal(got[i].Data, entries[i].Data) {
			t.Fatalf("entry %d mismatch: got=%+v want=%+v", i, got[i], entries[i])
		}
	}
}

func TestLocalRunLogStore_AppendBatchPreservesRawDataBytes(t *testing.T) {
	store := newLocalRunLogStoreForTest(t)

	raw := []byte{'q', '"', '\n', 0, 0xff, '\\'}
	entry := LogEntry{
		Timestamp: time.Unix(1_710_000_000, 123_456_789).UTC(),
		Stream:    api.Stream_STREAM_STDERR,
		Sequence:  7,
		Data:      raw,
		Completed: api.RunOutcome_RUN_OUTCOME_FAILURE,
	}

	if err := store.AppendBatch("run-raw-bytes", []LogEntry{entry}); err != nil {
		t.Fatalf("append batch: %v", err)
	}

	got, err := store.List("run-raw-bytes")
	if err != nil {
		t.Fatalf("list entries: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(got))
	}

	if !got[0].Timestamp.Equal(entry.Timestamp) ||
		got[0].Stream != entry.Stream ||
		got[0].Sequence != entry.Sequence ||
		got[0].Completed != entry.Completed ||
		!bytes.Equal(got[0].Data, raw) {
		t.Fatalf("entry mismatch: got=%+v want=%+v gotData=%v wantData=%v", got[0], entry, got[0].Data, raw)
	}
}

func TestLocalRunLogStore_ReplayAppliesSinceAndLimit(t *testing.T) {
	store := newLocalRunLogStoreForTest(t)

	runID := "run-replay-limit"
	entries := []LogEntry{
		{Sequence: 1, Data: []byte("first")},
		{Sequence: 2, Data: []byte("second")},
		{Sequence: 3, Data: []byte("third")},
		{Sequence: 4, Data: []byte("fourth")},
	}
	if err := store.AppendBatch(runID, entries); err != nil {
		t.Fatalf("append batch: %v", err)
	}

	got, err := store.Replay(runID, LogReplayOptions{SinceSequence: 1, Limit: 2})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !got.Found {
		t.Fatal("expected replay to find persisted run")
	}
	if !got.Truncated {
		t.Fatal("expected replay to be truncated")
	}
	if got.TerminalAlreadyConsumed {
		t.Fatal("did not expect terminal to be consumed")
	}
	if len(got.Entries) != 2 || got.Entries[0].Sequence != 2 || got.Entries[1].Sequence != 3 {
		t.Fatalf("replay entries = %+v, want sequences [2 3]", got.Entries)
	}
}

func TestLocalRunLogStore_ReplayDetectsSkippedCompletion(t *testing.T) {
	store := newLocalRunLogStoreForTest(t)

	runID := "run-replay-terminal"
	entries := []LogEntry{
		{Sequence: 1, Data: []byte("first")},
		{
			Stream:   api.Stream_STREAM_CONTROL,
			Sequence: 2,
			Data:     []byte(`{"event":"completed","status":"success"}`),
		},
	}
	if err := store.AppendBatch(runID, entries); err != nil {
		t.Fatalf("append batch: %v", err)
	}

	got, err := store.Replay(runID, LogReplayOptions{SinceSequence: 2, Limit: 10})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !got.Found {
		t.Fatal("expected replay to find persisted run")
	}
	if len(got.Entries) != 0 {
		t.Fatalf("expected no entries after since sequence, got %+v", got.Entries)
	}
	if !got.TerminalAlreadyConsumed {
		t.Fatal("expected skipped terminal event to be detected")
	}
}

func TestLocalRunLogStore_ReplayTailUsesLengthFooter(t *testing.T) {
	store := newLocalRunLogStoreForTest(t)

	runID := "run-replay-tail"
	entries := []LogEntry{
		{Sequence: 1, Data: []byte("first")},
		{Sequence: 2, Data: []byte("second")},
		{Sequence: 3, Data: []byte("third")},
		{Sequence: 4, Data: []byte("fourth")},
	}
	if err := store.AppendBatch(runID, entries); err != nil {
		t.Fatalf("append batch: %v", err)
	}

	got, err := store.Replay(runID, LogReplayOptions{Tail: 3, Limit: 2})
	if err != nil {
		t.Fatalf("replay tail: %v", err)
	}
	if !got.Found {
		t.Fatal("expected replay to find persisted run")
	}
	if !got.Truncated {
		t.Fatal("expected replay to be truncated by limit")
	}
	if len(got.Entries) != 2 || got.Entries[0].Sequence != 2 || got.Entries[1].Sequence != 3 {
		t.Fatalf("tail replay entries = %+v, want sequences [2 3]", got.Entries)
	}
}

func TestLocalRunLogStore_ReplayTailAppliesSinceBeforeTail(t *testing.T) {
	store := newLocalRunLogStoreForTest(t)

	runID := "run-replay-tail-since"
	entries := []LogEntry{
		{Sequence: 1, Data: []byte("first")},
		{Sequence: 2, Data: []byte("second")},
		{Sequence: 3, Data: []byte("third")},
		{Sequence: 4, Data: []byte("fourth")},
	}
	if err := store.AppendBatch(runID, entries); err != nil {
		t.Fatalf("append batch: %v", err)
	}

	got, err := store.Replay(runID, LogReplayOptions{SinceSequence: 1, Tail: 2, Limit: 10})
	if err != nil {
		t.Fatalf("replay tail: %v", err)
	}
	if !got.Found {
		t.Fatal("expected replay to find persisted run")
	}
	if got.Truncated {
		t.Fatal("did not expect replay to be truncated")
	}
	if len(got.Entries) != 2 || got.Entries[0].Sequence != 3 || got.Entries[1].Sequence != 4 {
		t.Fatalf("tail replay entries = %+v, want sequences [3 4]", got.Entries)
	}
}

func TestLocalRunLogStore_ReplayTailDetectsConsumedCompletion(t *testing.T) {
	store := newLocalRunLogStoreForTest(t)

	runID := "run-replay-tail-terminal"
	entries := []LogEntry{
		{Sequence: 1, Data: []byte("first")},
		{
			Stream:   api.Stream_STREAM_CONTROL,
			Sequence: 2,
			Data:     []byte(`{"event":"completed","status":"success"}`),
		},
	}
	if err := store.AppendBatch(runID, entries); err != nil {
		t.Fatalf("append batch: %v", err)
	}

	got, err := store.Replay(runID, LogReplayOptions{SinceSequence: 2, Tail: 2, Limit: 2})
	if err != nil {
		t.Fatalf("replay tail: %v", err)
	}
	if !got.Found {
		t.Fatal("expected replay to find persisted run")
	}
	if len(got.Entries) != 0 {
		t.Fatalf("expected no replay entries, got %+v", got.Entries)
	}
	if !got.TerminalAlreadyConsumed {
		t.Fatal("expected tail replay to detect consumed terminal event")
	}
}

func TestLocalRunLogStore_ReplayMissingRun(t *testing.T) {
	store := newLocalRunLogStoreForTest(t)

	got, err := store.Replay("missing-run", LogReplayOptions{Limit: 10})
	if err != nil {
		t.Fatalf("replay missing run: %v", err)
	}
	if got.Found {
		t.Fatal("did not expect missing run to be found")
	}
}

func TestLocalRunLogStore_ListRejectsTruncatedRecord(t *testing.T) {
	store := newLocalRunLogStoreForTest(t)

	runID := "run-truncated"
	if err := os.WriteFile(store.runPath(runID), []byte{logEntryRecordHeaderSize, 0, 0}, 0o644); err != nil {
		t.Fatalf("write truncated record: %v", err)
	}

	if _, err := store.List(runID); err == nil {
		t.Fatal("expected truncated record to fail")
	}
}

func TestLocalRunLogStore_ListRejectsMismatchedRecordLengthFooter(t *testing.T) {
	store := newLocalRunLogStoreForTest(t)

	runID := "run-bad-footer"
	record, err := marshalLogEntryRecords([]LogEntry{{Sequence: 1, Data: []byte("x")}})
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	record[len(record)-1] ^= 0xff
	if err := os.WriteFile(store.runPath(runID), record, 0o644); err != nil {
		t.Fatalf("write bad footer record: %v", err)
	}

	if _, err := store.List(runID); err == nil {
		t.Fatal("expected mismatched footer to fail")
	}
}

func TestLocalRunLogStore_ReplayTailRejectsMismatchedRecordLengthFooter(t *testing.T) {
	store := newLocalRunLogStoreForTest(t)

	runID := "run-bad-tail-footer"
	record, err := marshalLogEntryRecords([]LogEntry{{Sequence: 1, Data: []byte("x")}})
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	record[len(record)-1] ^= 0xff
	if err := os.WriteFile(store.runPath(runID), record, 0o644); err != nil {
		t.Fatalf("write bad footer record: %v", err)
	}

	if _, err := store.Replay(runID, LogReplayOptions{Tail: 1}); err == nil {
		t.Fatal("expected mismatched footer to fail")
	}
}

func TestLocalRunLogStore_ConcurrentAppendBatchDifferentRuns(t *testing.T) {
	store := newLocalRunLogStoreForTest(t)

	const runCount = 16
	const entriesPerRun = 32

	var wg sync.WaitGroup
	for run := range runCount {
		runID := fmt.Sprintf("run-%02d", run)
		wg.Go(func() {
			entries := make([]LogEntry, 0, entriesPerRun)
			for i := 1; i <= entriesPerRun; i++ {
				entries = append(entries, LogEntry{Sequence: int64(i), Data: []byte(fmt.Sprintf("%s-line-%02d", runID, i))})
			}

			if err := store.AppendBatch(runID, entries); err != nil {
				t.Errorf("append batch for %s: %v", runID, err)
			}
		})
	}
	wg.Wait()

	for run := range runCount {
		runID := fmt.Sprintf("run-%02d", run)
		got, err := store.List(runID)
		if err != nil {
			t.Fatalf("list %s: %v", runID, err)
		}
		if len(got) != entriesPerRun {
			t.Fatalf("%s: expected %d entries, got %d", runID, entriesPerRun, len(got))
		}
	}
}

func TestLocalRunLogStore_SanitizesRunIDInPath(t *testing.T) {
	store := newLocalRunLogStoreForTest(t)

	runID := "../run/with/slashes"
	if err := store.Append(runID, LogEntry{Sequence: 1, Data: []byte("x")}); err != nil {
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
	store := newLocalRunLogStoreWithOptionsForTest(t, LocalRunLogStoreOptions{
		NewRunMinFreeBytes: 100,
		statFS: func(string) (filesystemStats, error) {
			return filesystemStats{freeBytes: 99, freeInodes: 1}, nil
		},
	})

	err := store.Append("new-run", LogEntry{Sequence: 1, Data: []byte("x")})
	if !errors.Is(err, ErrLogStoreReadOnly) {
		t.Fatalf("expected ErrLogStoreReadOnly, got %v", err)
	}
}

func TestLocalRunLogStore_ReadOnlyThresholdAllowsExistingRuns(t *testing.T) {
	freeBytes := uint64(1000)
	store := newLocalRunLogStoreWithOptionsForTest(t, LocalRunLogStoreOptions{
		NewRunMinFreeBytes: 100,
		statFS: func(string) (filesystemStats, error) {
			return filesystemStats{freeBytes: freeBytes, freeInodes: 1}, nil
		},
	})

	if err := store.Append("existing-run", LogEntry{Sequence: 1, Data: []byte("first")}); err != nil {
		t.Fatalf("append initial entry: %v", err)
	}

	freeBytes = 99
	if err := store.Append("existing-run", LogEntry{Sequence: 2, Data: []byte("second")}); err != nil {
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
	store := newLocalRunLogStoreWithOptionsForTest(t, LocalRunLogStoreOptions{
		NewRunMinFreeBytes: 100,
		OpenFileLimit:      1,
		statFS: func(string) (filesystemStats, error) {
			return filesystemStats{freeBytes: freeBytes, freeInodes: 1}, nil
		},
	})

	if err := store.Append("existing-run", LogEntry{Sequence: 1, Data: []byte("first")}); err != nil {
		t.Fatalf("append initial existing entry: %v", err)
	}
	if err := store.Append("other-run", LogEntry{Sequence: 1, Data: []byte("other")}); err != nil {
		t.Fatalf("append other run: %v", err)
	}

	freeBytes = 99
	if err := store.Append("existing-run", LogEntry{Sequence: 2, Data: []byte("second")}); err != nil {
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
	if err := store.Append("cached-run", LogEntry{Sequence: 1, Data: []byte("first")}); err != nil {
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
