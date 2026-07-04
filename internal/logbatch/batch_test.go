package logbatch

import (
	"bytes"
	"testing"
	"time"

	api "vectis/api/gen/go"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMarshalDecodeRecordsRoundTrip(t *testing.T) {
	runID := "run-1"
	streamType := api.Stream_STREAM_STDERR
	outcome := api.RunOutcome_RUN_OUTCOME_FAILURE
	seq := int64(42)
	ts := time.Unix(1_710_000_000, 123_456_789).UTC()
	data := []byte{'h', 'i', 0, 0xff}

	payload, err := MarshalChunks([]*api.LogChunk{{
		RunId:     &runID,
		Data:      data,
		Sequence:  &seq,
		Stream:    &streamType,
		Completed: &outcome,
		Timestamp: timestamppb.New(ts),
	}})

	if err != nil {
		t.Fatalf("marshal chunks: %v", err)
	}

	var records []Record
	if err := DecodeRecords(payload, func(record Record) error {
		records = append(records, record)
		return nil
	}); err != nil {
		t.Fatalf("decode records: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}

	got := records[0]
	if got.RunID != runID || got.Sequence != seq || got.Stream != streamType || got.Completed != outcome {
		t.Fatalf("record metadata mismatch: got %+v", got)
	}

	if !got.HasTimestamp || !got.Timestamp.Equal(ts) {
		t.Fatalf("timestamp = (%v, %v), want %v", got.HasTimestamp, got.Timestamp, ts)
	}

	if !bytes.Equal(got.Data, data) {
		t.Fatalf("data = %v, want %v", got.Data, data)
	}
}
