package logforwarder

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"testing/quick"

	api "vectis/api/gen/go"

	"google.golang.org/protobuf/proto"
)

func TestSpoolProperty_RoundTripGeneratedChunkBatches(t *testing.T) {
	skipSpoolPropertyInShort(t)

	var lastErr error
	var lastTrace []byte
	prop := func(raw []byte) bool {
		trace := raw
		if len(trace) > 48 {
			trace = trace[:48]
		}

		if len(trace) == 0 {
			return true
		}

		chunks := spoolPropertyChunks(trace)
		err := checkSpoolRoundTrip(chunks)
		if err != nil {
			lastErr = err
			lastTrace = append([]byte(nil), trace...)
			return false
		}

		return true
	}

	if err := quick.Check(prop, &quick.Config{MaxCount: 100}); err != nil {
		t.Fatalf("spool roundtrip property failed: %v\ntrace=%v\nreason=%v", err, lastTrace, lastErr)
	}
}

func skipSpoolPropertyInShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("log forwarder property tests run under mage testProperty")
	}
}

func checkSpoolRoundTrip(chunks []*api.LogChunk) error {
	dir, err := os.MkdirTemp("", "vectis-logforwarder-spool-property-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "batch.spool")
	writer, err := NewSpoolWriter(path, len(chunks))
	if err != nil {
		return fmt.Errorf("new spool writer: %w", err)
	}

	for i, chunk := range chunks {
		if err := writer.Append(chunk); err != nil {
			return fmt.Errorf("append chunk %d: %w", i, err)
		}
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("close spool writer: %w", err)
	}

	reader, err := NewSpoolReader(path)
	if err != nil {
		return fmt.Errorf("new spool reader: %w", err)
	}
	defer reader.Close()

	got, err := reader.ReadBatch()
	if err != nil {
		return fmt.Errorf("read batch: %w", err)
	}

	if len(got) != len(chunks) {
		return fmt.Errorf("roundtrip count got %d want %d", len(got), len(chunks))
	}

	for i := range chunks {
		if !proto.Equal(got[i], chunks[i]) {
			return fmt.Errorf("roundtrip chunk %d got %v want %v", i, got[i], chunks[i])
		}
	}

	if got, err := reader.ReadBatch(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("second read got (%#v, %w), want EOF", got, err)
	}

	return nil
}

func spoolPropertyChunks(raw []byte) []*api.LogChunk {
	chunks := make([]*api.LogChunk, 0, len(raw))
	for i, b := range raw {
		runID := fmt.Sprintf("run-%d", int(b)%5)
		seq := int64(i + 1)
		stream := api.Stream_STREAM_STDOUT
		if b%2 == 1 {
			stream = api.Stream_STREAM_STDERR
		}

		chunk := &api.LogChunk{
			RunId:    proto.String(runID),
			Sequence: proto.Int64(seq),
			Stream:   &stream,
			Data:     []byte{b, b ^ 0xff, byte(i)},
		}

		if b%3 == 0 {
			chunk.LogShardId = proto.String(fmt.Sprintf("shard-%d", int(b)%7))
		}

		chunks = append(chunks, chunk)
	}

	return chunks
}
