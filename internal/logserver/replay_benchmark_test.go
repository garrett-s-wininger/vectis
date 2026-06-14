package logserver

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	api "vectis/api/gen/go"
)

func BenchmarkLocalRunLogStore_ReplayStrategies(b *testing.B) {
	for _, cfg := range []replayBenchmarkConfig{
		{name: "entries_100000/payload_0256", entryCount: 100_000, payloadSize: 256, replayLimit: 10_000, tail: 1_000},
		{name: "entries_010000/payload_4096", entryCount: 10_000, payloadSize: 4096, replayLimit: 1_000, tail: 1_000},
	} {
		cfg := cfg
		b.Run(cfg.name, func(b *testing.B) {
			fixture := newReplayBenchmarkFixture(b, cfg.entryCount, cfg.payloadSize)
			sinceNearEnd := int64(fixture.entryCount - cfg.tail)

			b.Run(fmt.Sprintf("current/list_replay_limit_%d", cfg.replayLimit), func(b *testing.B) {
				benchmarkCurrentReplay(b, fixture, 0, 0, cfg.replayLimit, cfg.replayLimit)
			})

			b.Run(fmt.Sprintf("forward/replay_limit_%d", cfg.replayLimit), func(b *testing.B) {
				benchmarkForwardReplay(b, fixture, 0, cfg.replayLimit, cfg.replayLimit)
			})

			b.Run(fmt.Sprintf("current/list_since_near_end_%d", cfg.tail), func(b *testing.B) {
				benchmarkCurrentReplay(b, fixture, sinceNearEnd, 0, cfg.tail, cfg.tail)
			})

			b.Run(fmt.Sprintf("forward/since_near_end_%d", cfg.tail), func(b *testing.B) {
				benchmarkForwardReplay(b, fixture, sinceNearEnd, cfg.tail, cfg.tail)
			})

			b.Run(fmt.Sprintf("current/list_tail_%d", cfg.tail), func(b *testing.B) {
				benchmarkCurrentReplay(b, fixture, 0, cfg.tail, cfg.tail, cfg.tail)
			})

			b.Run(fmt.Sprintf("forward/count_then_read_tail_%d", cfg.tail), func(b *testing.B) {
				benchmarkForwardCountThenReadTail(b, fixture, cfg.tail)
			})

			b.Run(fmt.Sprintf("footer/reverse_tail_%d", cfg.tail), func(b *testing.B) {
				benchmarkFooterReverseTail(b, fixture, cfg.tail)
			})

			b.Run(fmt.Sprintf("delimiter/boundary_search_only_tail_%d", cfg.tail), func(b *testing.B) {
				benchmarkDelimiterBoundarySearchTail(b, fixture, cfg.tail)
			})
		})
	}
}

type replayBenchmarkConfig struct {
	name        string
	entryCount  int
	payloadSize int
	replayLimit int
	tail        int
}

type replayBenchmarkFixture struct {
	store       *LocalRunLogStore
	runID       string
	path        string
	delimited   []byte
	entryCount  int
	payloadSize int
}

func newReplayBenchmarkFixture(b *testing.B, entryCount, payloadSize int) *replayBenchmarkFixture {
	b.Helper()

	store, err := NewLocalRunLogStore(b.TempDir())
	if err != nil {
		b.Fatalf("create local log store: %v", err)
	}

	runID := "bench-replay-run"
	writeReplayBenchmarkLog(b, store, runID, entryCount, payloadSize)
	if err := store.Close(); err != nil {
		b.Fatalf("close benchmark log store: %v", err)
	}

	return &replayBenchmarkFixture{
		store:       store,
		runID:       runID,
		path:        store.runPath(runID),
		delimited:   replayBenchmarkDelimitedBytes(entryCount, payloadSize),
		entryCount:  entryCount,
		payloadSize: payloadSize,
	}
}

func writeReplayBenchmarkLog(b *testing.B, store *LocalRunLogStore, runID string, entryCount, payloadSize int) {
	b.Helper()

	const batchSize = 512
	batch := make([]LogEntry, 0, batchSize)
	for seq := 1; seq <= entryCount; seq++ {
		entry := benchmarkLogEntry(payloadSize)
		entry.Sequence = int64(seq)
		batch = append(batch, entry)
		if len(batch) == batchSize {
			if err := store.AppendBatch(runID, batch); err != nil {
				b.Fatalf("append benchmark log batch: %v", err)
			}

			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := store.AppendBatch(runID, batch); err != nil {
			b.Fatalf("append final benchmark log batch: %v", err)
		}
	}
}

func replayBenchmarkDelimitedBytes(entryCount, payloadSize int) []byte {
	record := bytes.Repeat([]byte{'x'}, payloadSize)
	buf := make([]byte, 0, entryCount*(payloadSize+1))
	for range entryCount {
		buf = append(buf, record...)
		buf = append(buf, 0)
	}

	return buf
}

func benchmarkCurrentReplay(b *testing.B, fixture *replayBenchmarkFixture, sinceSeq int64, tail, replayLimit, want int) {
	b.Helper()
	b.SetBytes(replayBenchmarkBytes(fixture.entryCount, fixture.entryCount, fixture.payloadSize))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		entries, err := fixture.store.List(fixture.runID)
		if err != nil {
			b.Fatalf("list benchmark replay log: %v", err)
		}

		replay, _ := boundedReplayEntries(entries, sinceSeq, tail, replayLimit)
		if len(replay) != want {
			b.Fatalf("replay entries = %d, want %d", len(replay), want)
		}
	}

	reportReplayShape(b, fixture, want)
}

func benchmarkForwardReplay(b *testing.B, fixture *replayBenchmarkFixture, sinceSeq int64, replayLimit, want int) {
	b.Helper()
	scannedRecords := replayLimit
	if sinceSeq > 0 {
		scannedRecords = fixture.entryCount
	}

	b.SetBytes(replayBenchmarkBytes(scannedRecords, want, fixture.payloadSize))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		replay, err := fixture.store.Replay(fixture.runID, LogReplayOptions{
			SinceSequence: sinceSeq,
			Limit:         replayLimit,
		})

		if err != nil {
			b.Fatalf("scan benchmark replay log: %v", err)
		}

		if len(replay.Entries) != want {
			b.Fatalf("replay entries = %d, want %d", len(replay.Entries), want)
		}
	}

	reportReplayShape(b, fixture, want)
}

func benchmarkForwardCountThenReadTail(b *testing.B, fixture *replayBenchmarkFixture, tail int) {
	b.Helper()
	b.SetBytes(replayBenchmarkBytes(fixture.entryCount*2, tail, fixture.payloadSize))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		replay, err := scanTailByCounting(fixture.path, tail)
		if err != nil {
			b.Fatalf("scan benchmark tail log: %v", err)
		}

		if len(replay) != tail {
			b.Fatalf("tail entries = %d, want %d", len(replay), tail)
		}
	}

	reportReplayShape(b, fixture, tail)
}

func benchmarkFooterReverseTail(b *testing.B, fixture *replayBenchmarkFixture, tail int) {
	b.Helper()
	b.SetBytes(replayBenchmarkBytes(tail, tail, fixture.payloadSize))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		replay, err := fixture.store.Replay(fixture.runID, LogReplayOptions{
			Tail:  tail,
			Limit: tail,
		})
		if err != nil {
			b.Fatalf("read footer tail: %v", err)
		}

		if len(replay.Entries) != tail {
			b.Fatalf("tail entries = %d, want %d", len(replay.Entries), tail)
		}
	}

	reportReplayShape(b, fixture, tail)
}

func benchmarkDelimiterBoundarySearchTail(b *testing.B, fixture *replayBenchmarkFixture, tail int) {
	b.Helper()
	b.SetBytes(int64(tail * (fixture.payloadSize + 1)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if got := countDelimitedTailBoundaries(fixture.delimited, tail); got != tail {
			b.Fatalf("tail boundaries = %d, want %d", got, tail)
		}
	}

	reportReplayShape(b, fixture, tail)
}

func replayBenchmarkBytes(records, dataRecords, payloadSize int) int64 {
	return int64(records*(2*logEntryRecordLengthSize+logEntryRecordHeaderSize) + dataRecords*payloadSize)
}

func reportReplayShape(b *testing.B, fixture *replayBenchmarkFixture, replayEntries int) {
	b.Helper()
	b.ReportMetric(float64(fixture.entryCount), "entries_total")
	b.ReportMetric(float64(fixture.payloadSize), "payload_bytes")
	b.ReportMetric(float64(replayEntries), "replay_entries")
}

type benchmarkLogRecordHeader struct {
	timestamp time.Time
	stream    api.Stream
	sequence  int64
	completed api.RunOutcome
	bodyLen   int
	dataLen   int
}

func scanTailByCounting(path string, tail int) ([]LogEntry, error) {
	count, err := countLogRecords(path)
	if err != nil {
		return nil, err
	}

	skipRecords := count - tail
	if skipRecords < 0 {
		skipRecords = 0
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1<<20)
	entries := make([]LogEntry, 0, tail)
	for seen := 0; ; seen++ {
		header, err := readBenchmarkLogRecordHeader(r)
		if err != nil {
			if errorsIsEOF(err) {
				return entries, nil
			}

			return nil, err
		}

		if seen < skipRecords {
			if err := skipBenchmarkLogRecordData(r, header); err != nil {
				return nil, err
			}

			continue
		}

		entry, err := readBenchmarkLogRecordData(r, header)
		if err != nil {
			return nil, err
		}

		entries = append(entries, entry)
	}
}

func countLogRecords(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1<<20)
	var count int
	for {
		header, err := readBenchmarkLogRecordHeader(r)
		if err != nil {
			if errorsIsEOF(err) {
				return count, nil
			}

			return 0, err
		}

		if err := skipBenchmarkLogRecordData(r, header); err != nil {
			return 0, err
		}

		count++
	}
}

func readBenchmarkLogRecordHeader(r io.Reader) (benchmarkLogRecordHeader, error) {
	var lengthBuf [logEntryRecordLengthSize]byte
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {
		return benchmarkLogRecordHeader{}, err
	}

	bodyLen := binary.LittleEndian.Uint32(lengthBuf[:])
	if bodyLen < logEntryRecordHeaderSize {
		return benchmarkLogRecordHeader{}, fmt.Errorf("record body length %d is smaller than header length %d", bodyLen, logEntryRecordHeaderSize)
	}

	var headerBuf [logEntryRecordHeaderSize]byte
	if _, err := io.ReadFull(r, headerBuf[:]); err != nil {
		return benchmarkLogRecordHeader{}, err
	}

	dataLen := binary.LittleEndian.Uint32(headerBuf[28:32])
	if dataLen != bodyLen-logEntryRecordHeaderSize {
		return benchmarkLogRecordHeader{}, fmt.Errorf("record data length %d does not match body payload length %d", dataLen, bodyLen-logEntryRecordHeaderSize)
	}

	nsec := binary.LittleEndian.Uint32(headerBuf[8:12])
	if nsec > uint32(time.Second-time.Nanosecond) {
		return benchmarkLogRecordHeader{}, fmt.Errorf("record timestamp nanosecond value %d is invalid", nsec)
	}

	return benchmarkLogRecordHeader{
		timestamp: time.Unix(int64(binary.LittleEndian.Uint64(headerBuf[0:8])), int64(nsec)).UTC(),
		stream:    api.Stream(int32(binary.LittleEndian.Uint32(headerBuf[12:16]))),
		sequence:  int64(binary.LittleEndian.Uint64(headerBuf[16:24])),
		completed: api.RunOutcome(int32(binary.LittleEndian.Uint32(headerBuf[24:28]))),
		bodyLen:   int(bodyLen),
		dataLen:   int(dataLen),
	}, nil
}

func skipBenchmarkLogRecordData(r *bufio.Reader, header benchmarkLogRecordHeader) error {
	if _, err := r.Discard(header.dataLen); err != nil {
		return err
	}

	var suffix [logEntryRecordLengthSize]byte
	return readBenchmarkLogRecordSuffix(r, uint32(header.bodyLen), &suffix)
}

func readBenchmarkLogRecordData(r io.Reader, header benchmarkLogRecordHeader) (LogEntry, error) {
	data := make([]byte, header.dataLen)
	if _, err := io.ReadFull(r, data); err != nil {
		return LogEntry{}, err
	}

	var suffix [logEntryRecordLengthSize]byte
	if err := readBenchmarkLogRecordSuffix(r, uint32(header.bodyLen), &suffix); err != nil {
		return LogEntry{}, err
	}

	return LogEntry{
		Timestamp: header.timestamp,
		Stream:    header.stream,
		Sequence:  header.sequence,
		Data:      data,
		Completed: header.completed,
	}, nil
}

func readBenchmarkLogRecordSuffix(r io.Reader, bodyLen uint32, suffix *[logEntryRecordLengthSize]byte) error {
	if _, err := io.ReadFull(r, suffix[:]); err != nil {
		return err
	}

	got := binary.LittleEndian.Uint32(suffix[:])
	if got != bodyLen {
		return fmt.Errorf("record length suffix %d does not match prefix %d", got, bodyLen)
	}

	return nil
}

func countDelimitedTailBoundaries(data []byte, tail int) int {
	pos := len(data)
	count := 0
	for count < tail && pos > 0 {
		next := bytes.LastIndexByte(data[:pos-1], 0)
		count++
		if next < 0 {
			break
		}

		pos = next + 1
	}

	return count
}

func errorsIsEOF(err error) bool {
	return err == io.EOF
}
