package logforwarder

import (
	"os"
	"path/filepath"
	"testing"

	api "vectis/api/gen/go"

	"google.golang.org/protobuf/proto"
)

func TestSpoolRoundtrip(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.spool")

	chunks := []*api.LogChunk{
		{RunId: proto.String("run-1"), Sequence: proto.Int64(1), Data: []byte("hello")},
		{RunId: proto.String("run-1"), Sequence: proto.Int64(2), Data: []byte("world")},
		{RunId: proto.String("run-2"), Sequence: proto.Int64(1), Data: []byte("foo")},
	}

	w, err := NewSpoolWriter(path, 100)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	for _, c := range chunks {
		if err := w.Append(c); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	r, err := NewSpoolReader(path)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	defer r.Close()

	received, err := r.ReadBatch()
	if err != nil {
		t.Fatalf("read batch: %v", err)
	}

	if len(received) != len(chunks) {
		t.Fatalf("expected %d chunks, got %d", len(chunks), len(received))
	}

	for i, want := range chunks {
		got := received[i]
		if got.GetRunId() != want.GetRunId() {
			t.Errorf("chunk %d run_id: got %q, want %q", i, got.GetRunId(), want.GetRunId())
		}

		if got.GetSequence() != want.GetSequence() {
			t.Errorf("chunk %d sequence: got %d, want %d", i, got.GetSequence(), want.GetSequence())
		}

		if string(got.GetData()) != string(want.GetData()) {
			t.Errorf("chunk %d data: got %q, want %q", i, got.GetData(), want.GetData())
		}
	}
}

func TestSpoolMultipleFiles(t *testing.T) {
	tmpDir := t.TempDir()
	path1 := filepath.Join(tmpDir, "batch1.spool")
	path2 := filepath.Join(tmpDir, "batch2.spool")

	batch1 := []*api.LogChunk{
		{RunId: proto.String("run-1"), Sequence: proto.Int64(1), Data: []byte("a")},
	}

	batch2 := []*api.LogChunk{
		{RunId: proto.String("run-1"), Sequence: proto.Int64(2), Data: []byte("b")},
	}

	w1, err := NewSpoolWriter(path1, 100)
	if err != nil {
		t.Fatalf("new writer 1: %v", err)
	}

	for _, c := range batch1 {
		if err := w1.Append(c); err != nil {
			t.Fatalf("append 1: %v", err)
		}
	}

	if err := w1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	w2, err := NewSpoolWriter(path2, 100)
	if err != nil {
		t.Fatalf("new writer 2: %v", err)
	}

	for _, c := range batch2 {
		if err := w2.Append(c); err != nil {
			t.Fatalf("append 2: %v", err)
		}
	}

	if err := w2.Close(); err != nil {
		t.Fatalf("close 2: %v", err)
	}

	r1, err := NewSpoolReader(path1)
	if err != nil {
		t.Fatalf("new reader 1: %v", err)
	}

	b1, err := r1.ReadBatch()
	if err != nil {
		t.Fatalf("read batch 1: %v", err)
	}
	r1.Close()

	if len(b1) != 1 || string(b1[0].GetData()) != "a" {
		t.Fatalf("batch 1 mismatch: %+v", b1)
	}

	r2, err := NewSpoolReader(path2)
	if err != nil {
		t.Fatalf("new reader 2: %v", err)
	}

	b2, err := r2.ReadBatch()
	if err != nil {
		t.Fatalf("read batch 2: %v", err)
	}
	r2.Close()

	if len(b2) != 1 || string(b2[0].GetData()) != "b" {
		t.Fatalf("batch 2 mismatch: %+v", b2)
	}
}

func TestSpoolCorruptionDetected(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.spool")

	w, err := NewSpoolWriter(path, 100)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	if err := w.Append(&api.LogChunk{RunId: proto.String("r"), Sequence: proto.Int64(1), Data: []byte("x")}); err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Corrupt the file by flipping a byte in the middle.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}

	if len(data) > 10 {
		data[10] ^= 0xFF
	}

	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	r, err := NewSpoolReader(path)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	defer r.Close()

	_, err = r.ReadBatch()
	if err == nil {
		t.Fatal("expected error for corrupted spool file, got nil")
	}
}

func TestSpoolEmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "empty.spool")

	// Create an empty file (no magic header).
	if err := os.WriteFile(path, []byte{}, 0o644); err != nil {
		t.Fatalf("write empty file: %v", err)
	}

	_, err := NewSpoolReader(path)
	if err == nil {
		t.Fatal("expected error for empty spool file, got nil")
	}
}

func TestSpoolInvalidMagic(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "bad.spool")
	if err := os.WriteFile(path, []byte("BAD!"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	_, err := NewSpoolReader(path)
	if err == nil {
		t.Fatal("expected error for invalid magic, got nil")
	}
}
