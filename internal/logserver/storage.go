package logserver

import (
	"bufio"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	api "vectis/api/gen/go"
)

var ErrLogStoreReadOnly = errors.New("log storage is read-only for new runs")

const (
	logStorageLockFileName       = "log.lock"
	defaultLogStoreOpenFileLimit = 256

	logEntryRecordLengthSize = 4
	logEntryRecordHeaderSize = 32
	logEntryRecordFixedSize  = logEntryRecordLengthSize + logEntryRecordHeaderSize + logEntryRecordLengthSize

	maxPooledLogEntryRecordBufferSize = 4 << 20

	minVectoredLogEntryRecordWriteSize = 256 << 10
	maxLogEntryRecordWritevSegments    = 1024
	maxLogEntryRecordWritevEntries     = maxLogEntryRecordWritevSegments / 3
)

var logEntryRecordBufferPool = sync.Pool{
	New: func() any {
		return &logEntryRecordBuffer{}
	},
}

var logEntryRecordWritevBufferPool = sync.Pool{
	New: func() any {
		return &logEntryRecordWritevBuffer{}
	},
}

type RunLogStore interface {
	Append(runID string, entry LogEntry) error
	List(runID string) ([]LogEntry, error)
}

// RunLogBatchStore persists multiple entries for a run with one durable append.
type RunLogBatchStore interface {
	AppendBatch(runID string, entries []LogEntry) error
}

// RunLogReplayStore replays a bounded sequence range without materializing the
// whole run log first.
type RunLogReplayStore interface {
	Replay(runID string, opts LogReplayOptions) (LogReplayResult, error)
}

type LogReplayOptions struct {
	SinceSequence int64
	Limit         int
	Tail          int
}

type LogReplayResult struct {
	Found                   bool
	Entries                 []LogEntry
	Truncated               bool
	TerminalAlreadyConsumed bool
}

type NoopRunLogStore struct{}

func (NoopRunLogStore) Append(string, LogEntry) error {
	return nil
}

func (NoopRunLogStore) AppendBatch(string, []LogEntry) error {
	return nil
}

func (NoopRunLogStore) List(string) ([]LogEntry, error) {
	return nil, nil
}

func (NoopRunLogStore) Replay(string, LogReplayOptions) (LogReplayResult, error) {
	return LogReplayResult{}, nil
}

type LocalRunLogStore struct {
	baseDir            string
	mu                 sync.Mutex
	newRunMinFreeBytes uint64
	statFS             filesystemStatFunc
	lockFile           *os.File
	openFileLimit      int
	openFiles          map[string]*cachedRunLogFile
	openFileClock      uint64
}

type LocalRunLogStoreOptions struct {
	NewRunMinFreeBytes uint64
	OpenFileLimit      int
	statFS             filesystemStatFunc
}

type cachedRunLogFile struct {
	runID    string
	file     *os.File
	lastUsed uint64
}

type logEntryRecordBuffer struct {
	buf []byte
}

type logEntryRecordWritevBuffer struct {
	fixed []byte
	batch platformWriteBatch
}

type filesystemStats struct {
	freeBytes  uint64
	freeInodes uint64
}

type filesystemStatFunc func(path string) (filesystemStats, error)

func defaultFilesystemStats(path string) (filesystemStats, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return filesystemStats{}, err
	}

	return filesystemStats{
		freeBytes:  st.Bavail * uint64(st.Bsize),
		freeInodes: st.Ffree,
	}, nil
}

func NewLocalRunLogStore(baseDir string) (*LocalRunLogStore, error) {
	return NewLocalRunLogStoreWithOptions(baseDir, LocalRunLogStoreOptions{})
}

func NewLocalRunLogStoreWithOptions(baseDir string, opts LocalRunLogStoreOptions) (*LocalRunLogStore, error) {
	if baseDir == "" {
		return nil, fmt.Errorf("local log storage base dir is required")
	}

	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("create log storage dir: %w", err)
	}

	lockFile, err := acquireLogStorageLock(baseDir)
	if err != nil {
		return nil, err
	}

	statFS := opts.statFS
	if statFS == nil {
		statFS = defaultFilesystemStats
	}
	openFileLimit := opts.OpenFileLimit
	if openFileLimit <= 0 {
		openFileLimit = defaultLogStoreOpenFileLimit
	}

	return &LocalRunLogStore{
		baseDir:            baseDir,
		newRunMinFreeBytes: opts.NewRunMinFreeBytes,
		statFS:             statFS,
		lockFile:           lockFile,
		openFileLimit:      openFileLimit,
		openFiles:          make(map[string]*cachedRunLogFile),
	}, nil
}

func acquireLogStorageLock(dir string) (*os.File, error) {
	lockPath := filepath.Join(dir, logStorageLockFileName)
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log storage lock %s: %w", lockPath, err)
	}

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("log storage directory %s is already in use by another log process; use a distinct storage directory for each active log shard: %w", dir, err)
		}

		return nil, fmt.Errorf("lock log storage directory %s: %w", dir, err)
	}

	return f, nil
}

func (s *LocalRunLogStore) Close() error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var result error
	for runID, cached := range s.openFiles {
		if err := cached.file.Close(); err != nil && result == nil {
			result = fmt.Errorf("close log store file for run %s: %w", cached.runID, err)
		}
		delete(s.openFiles, runID)
	}

	lockFile := s.lockFile
	s.lockFile = nil
	if lockFile == nil {
		return result
	}

	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN); err != nil && result == nil {
		result = fmt.Errorf("unlock log storage directory %s: %w", s.baseDir, err)
	}

	if err := lockFile.Close(); err != nil && result == nil {
		result = fmt.Errorf("close log storage lock %s: %w", filepath.Join(s.baseDir, logStorageLockFileName), err)
	}

	return result
}

func (s *LocalRunLogStore) Append(runID string, entry LogEntry) error {
	return s.AppendBatch(runID, []LogEntry{entry})
}

func (s *LocalRunLogStore) AppendBatch(runID string, entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	if runID == "" {
		return fmt.Errorf("run id is required")
	}

	total, err := logEntryRecordsSize(entries)
	if err != nil {
		return err
	}

	path := s.runPath(runID)

	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := s.appendFileLocked(runID, path)
	if err != nil {
		return err
	}

	if useVectoredLogEntryRecordWrite(total) {
		err = writeLogEntryRecordsVectored(f, entries, total)
	} else {
		err = writeLogEntryRecordsContiguous(f, entries, total)
	}
	if err != nil {
		return fmt.Errorf("append log entry: %w", err)
	}

	return nil
}

func marshalLogEntryRecords(entries []LogEntry) ([]byte, error) {
	total, err := logEntryRecordsSize(entries)
	if err != nil {
		return nil, err
	}

	return appendLogEntryRecords(make([]byte, 0, total), entries), nil
}

func logEntryRecordsSize(entries []LogEntry) (int, error) {
	var total int
	maxInt := uint64(int(^uint(0) >> 1))
	for _, entry := range entries {
		bodyLen := uint64(logEntryRecordHeaderSize) + uint64(len(entry.Data))
		if bodyLen > uint64(^uint32(0)) {
			return 0, fmt.Errorf("marshal log entry: data length %d exceeds binary record limit", len(entry.Data))
		}

		recordLen := bodyLen + 2*logEntryRecordLengthSize
		if uint64(total) > maxInt-recordLen {
			return 0, fmt.Errorf("marshal log entry: batch exceeds addressable buffer size")
		}

		total += int(recordLen)
	}

	return total, nil
}

func borrowLogEntryRecordBuffer(total int) *logEntryRecordBuffer {
	records := logEntryRecordBufferPool.Get().(*logEntryRecordBuffer)
	if cap(records.buf) < total {
		records.buf = make([]byte, 0, total)
		return records
	}

	records.buf = records.buf[:0]
	return records
}

func releaseLogEntryRecordBuffer(records *logEntryRecordBuffer) {
	if cap(records.buf) > maxPooledLogEntryRecordBufferSize {
		records.buf = nil
	} else {
		records.buf = records.buf[:0]
	}

	logEntryRecordBufferPool.Put(records)
}

func borrowLogEntryRecordWritevBuffer(entryCount int) *logEntryRecordWritevBuffer {
	records := logEntryRecordWritevBufferPool.Get().(*logEntryRecordWritevBuffer)

	fixedSize := entryCount * logEntryRecordFixedSize
	if cap(records.fixed) < fixedSize {
		records.fixed = make([]byte, fixedSize)
	}

	records.fixed = records.fixed[:fixedSize]
	iovSize := entryCount * 3
	records.batch.ensureCapacity(iovSize)
	records.batch.reset()

	return records
}

func releaseLogEntryRecordWritevBuffer(records *logEntryRecordWritevBuffer) {
	if cap(records.fixed) > maxLogEntryRecordWritevEntries*logEntryRecordFixedSize {
		records.fixed = nil
	} else {
		records.fixed = records.fixed[:0]
	}

	records.batch.release(maxLogEntryRecordWritevSegments)

	logEntryRecordWritevBufferPool.Put(records)
}

func appendLogEntryRecords(buf []byte, entries []LogEntry) []byte {
	for _, entry := range entries {
		bodyLen := uint32(uint64(logEntryRecordHeaderSize) + uint64(len(entry.Data)))
		buf = binary.LittleEndian.AppendUint32(buf, bodyLen)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(entry.Timestamp.Unix()))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(entry.Timestamp.Nanosecond()))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(entry.Stream))
		buf = binary.LittleEndian.AppendUint64(buf, uint64(entry.Sequence))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(entry.Completed))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entry.Data)))
		buf = append(buf, entry.Data...)
		buf = binary.LittleEndian.AppendUint32(buf, bodyLen)
	}

	return buf
}

func useVectoredLogEntryRecordWrite(total int) bool {
	return platformSupportsVectoredWrite && total >= minVectoredLogEntryRecordWriteSize
}

func writeLogEntryRecordsContiguous(f *os.File, entries []LogEntry, total int) error {
	records := borrowLogEntryRecordBuffer(total)
	defer releaseLogEntryRecordBuffer(records)

	b := appendLogEntryRecords(records.buf, entries)
	n, err := f.Write(b)
	if err != nil {
		return err
	}

	if n != len(b) {
		return io.ErrShortWrite
	}

	return nil
}

func writeLogEntryRecordsVectored(f *os.File, entries []LogEntry, total int) error {
	fd := int(f.Fd())
	written := 0
	for len(entries) > 0 {
		chunkLen := len(entries)
		if chunkLen > maxLogEntryRecordWritevEntries {
			chunkLen = maxLogEntryRecordWritevEntries
		}

		records := borrowLogEntryRecordWritevBuffer(chunkLen)
		chunkBytes := appendLogEntryRecordIovs(records, entries[:chunkLen])
		err := records.batch.writeAll(fd)
		releaseLogEntryRecordWritevBuffer(records)
		if err != nil {
			return err
		}

		written += chunkBytes
		entries = entries[chunkLen:]
	}

	if written != total {
		return io.ErrShortWrite
	}

	return nil
}

func appendLogEntryRecordIovs(records *logEntryRecordWritevBuffer, entries []LogEntry) int {
	written := 0

	for i := range entries {
		entry := entries[i]
		bodyLen := uint32(uint64(logEntryRecordHeaderSize) + uint64(len(entry.Data)))
		fixedOffset := i * logEntryRecordFixedSize
		prefixHeader := records.fixed[fixedOffset : fixedOffset+logEntryRecordLengthSize+logEntryRecordHeaderSize]
		footer := records.fixed[fixedOffset+logEntryRecordLengthSize+logEntryRecordHeaderSize : fixedOffset+logEntryRecordFixedSize]

		binary.LittleEndian.PutUint32(prefixHeader[0:4], bodyLen)
		binary.LittleEndian.PutUint64(prefixHeader[4:12], uint64(entry.Timestamp.Unix()))
		binary.LittleEndian.PutUint32(prefixHeader[12:16], uint32(entry.Timestamp.Nanosecond()))
		binary.LittleEndian.PutUint32(prefixHeader[16:20], uint32(entry.Stream))
		binary.LittleEndian.PutUint64(prefixHeader[20:28], uint64(entry.Sequence))
		binary.LittleEndian.PutUint32(prefixHeader[28:32], uint32(entry.Completed))
		binary.LittleEndian.PutUint32(prefixHeader[32:36], uint32(len(entry.Data)))
		binary.LittleEndian.PutUint32(footer, bodyLen)

		records.batch.appendBytes(prefixHeader)
		if len(entry.Data) > 0 {
			records.batch.appendBytes(entry.Data)
		}

		records.batch.appendBytes(footer)
		written += len(prefixHeader) + len(entry.Data) + len(footer)
	}

	return written
}

func (s *LocalRunLogStore) appendFileLocked(runID, path string) (*os.File, error) {
	if cached := s.openFiles[runID]; cached != nil {
		cached.lastUsed = s.nextOpenFileClockLocked()
		return cached.file, nil
	}

	exists, err := fileExists(path)
	if err != nil {
		return nil, fmt.Errorf("inspect log store file %s: %w", path, err)
	}
	if !exists {
		if err := s.ensureCanCreateRunLocked(); err != nil {
			return nil, err
		}
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log store file %s: %w", path, err)
	}

	if s.openFiles == nil {
		s.openFiles = make(map[string]*cachedRunLogFile)
	}
	s.openFiles[runID] = &cachedRunLogFile{
		runID:    runID,
		file:     f,
		lastUsed: s.nextOpenFileClockLocked(),
	}
	s.evictOpenFilesLocked()

	return f, nil
}

func (s *LocalRunLogStore) nextOpenFileClockLocked() uint64 {
	s.openFileClock++
	return s.openFileClock
}

func (s *LocalRunLogStore) evictOpenFilesLocked() {
	if s.openFileLimit <= 0 {
		return
	}

	for len(s.openFiles) > s.openFileLimit {
		var victimRunID string
		var victim *cachedRunLogFile
		for runID, cached := range s.openFiles {
			if victim == nil || cached.lastUsed < victim.lastUsed {
				victimRunID = runID
				victim = cached
			}
		}
		if victim == nil {
			return
		}

		delete(s.openFiles, victimRunID)
		_ = victim.file.Close()
	}
}

func (s *LocalRunLogStore) ensureCanCreateRunLocked() error {
	return s.newRunWritable()
}

func (s *LocalRunLogStore) NewRunWritable() bool {
	return s.newRunWritable() == nil
}

func (s *LocalRunLogStore) newRunWritable() error {
	if s.newRunMinFreeBytes == 0 {
		return nil
	}

	stats, err := s.statFS(s.baseDir)
	if err != nil {
		return fmt.Errorf("inspect log storage filesystem: %w", err)
	}

	if stats.freeInodes == 0 {
		return fmt.Errorf("%w: no free inodes in %s", ErrLogStoreReadOnly, s.baseDir)
	}

	if stats.freeBytes < s.newRunMinFreeBytes {
		return fmt.Errorf("%w: %d bytes free below %d byte threshold in %s", ErrLogStoreReadOnly, stats.freeBytes, s.newRunMinFreeBytes, s.baseDir)
	}

	return nil
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}

	return false, err
}

func (s *LocalRunLogStore) List(runID string) ([]LogEntry, error) {
	if runID == "" {
		return nil, nil
	}

	path := s.runPath(runID)

	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}

		return nil, fmt.Errorf("open log store file %s: %w", path, err)
	}
	defer f.Close()

	entries := make([]LogEntry, 0, 128)
	for {
		entry, err := readLogEntryRecord(f)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, fmt.Errorf("decode log entry from %s: %w", path, err)
		}

		entries = append(entries, entry)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Sequence < entries[j].Sequence
	})

	return entries, nil
}

func readLogEntryRecord(r io.Reader) (LogEntry, error) {
	var lengthBuf [logEntryRecordLengthSize]byte
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {
		return LogEntry{}, err
	}

	bodyLen := binary.LittleEndian.Uint32(lengthBuf[:])
	if bodyLen < logEntryRecordHeaderSize {
		return LogEntry{}, fmt.Errorf("record body length %d is smaller than header length %d", bodyLen, logEntryRecordHeaderSize)
	}

	if uint64(bodyLen) > uint64(int(^uint(0)>>1)) {
		return LogEntry{}, fmt.Errorf("record body length %d exceeds addressable buffer size", bodyLen)
	}

	body := make([]byte, int(bodyLen))
	if _, err := io.ReadFull(r, body); err != nil {
		return LogEntry{}, err
	}

	var suffix [logEntryRecordLengthSize]byte
	if _, err := io.ReadFull(r, suffix[:]); err != nil {
		return LogEntry{}, err
	}

	if got := binary.LittleEndian.Uint32(suffix[:]); got != bodyLen {
		return LogEntry{}, fmt.Errorf("record length suffix %d does not match prefix %d", got, bodyLen)
	}

	return decodeLogEntryRecordBody(body)
}

func decodeLogEntryRecordBody(body []byte) (LogEntry, error) {
	if len(body) < logEntryRecordHeaderSize {
		return LogEntry{}, fmt.Errorf("record body length %d is smaller than header length %d", len(body), logEntryRecordHeaderSize)
	}

	dataLen := binary.LittleEndian.Uint32(body[28:32])
	if dataLen != uint32(len(body)-logEntryRecordHeaderSize) {
		return LogEntry{}, fmt.Errorf("record data length %d does not match body payload length %d", dataLen, len(body)-logEntryRecordHeaderSize)
	}

	nsec := binary.LittleEndian.Uint32(body[8:12])
	if nsec > uint32(time.Second-time.Nanosecond) {
		return LogEntry{}, fmt.Errorf("record timestamp nanosecond value %d is invalid", nsec)
	}

	return LogEntry{
		Timestamp: time.Unix(int64(binary.LittleEndian.Uint64(body[0:8])), int64(nsec)).UTC(),
		Stream:    api.Stream(int32(binary.LittleEndian.Uint32(body[12:16]))),
		Sequence:  int64(binary.LittleEndian.Uint64(body[16:24])),
		Data:      body[logEntryRecordHeaderSize:],
		Completed: api.RunOutcome(int32(binary.LittleEndian.Uint32(body[24:28]))),
	}, nil
}

func (s *LocalRunLogStore) Replay(runID string, opts LogReplayOptions) (LogReplayResult, error) {
	if runID == "" {
		return LogReplayResult{}, nil
	}

	path := s.runPath(runID)

	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return LogReplayResult{}, nil
		}

		return LogReplayResult{}, fmt.Errorf("open log store file %s: %w", path, err)
	}
	defer f.Close()

	if opts.Tail > 0 {
		return replayTailFromFile(f, opts)
	}

	capacity := opts.Limit
	if capacity <= 0 {
		capacity = 128
	}

	result := LogReplayResult{
		Found:   true,
		Entries: make([]LogEntry, 0, capacity),
	}

	r := bufio.NewReaderSize(f, 1<<20)
	for {
		header, err := readLogEntryRecordHeader(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return result, nil
			}

			return LogReplayResult{}, fmt.Errorf("decode log entry from %s: %w", path, err)
		}

		if header.sequence <= opts.SinceSequence {
			if err := s.skipReplayRecordData(r, header, &result); err != nil {
				return LogReplayResult{}, fmt.Errorf("decode log entry from %s: %w", path, err)
			}

			continue
		}

		if opts.Limit > 0 && len(result.Entries) >= opts.Limit {
			result.Truncated = true
			return result, nil
		}

		entry, err := readLogEntryRecordDataBuffered(r, header)
		if err != nil {
			return LogReplayResult{}, fmt.Errorf("decode log entry from %s: %w", path, err)
		}

		result.Entries = append(result.Entries, entry)
	}
}

func (s *LocalRunLogStore) skipReplayRecordData(r *bufio.Reader, header logEntryRecordHeader, result *LogReplayResult) error {
	if header.stream != api.Stream_STREAM_CONTROL && header.completed == api.RunOutcome_RUN_OUTCOME_UNSPECIFIED {
		if _, err := r.Discard(header.dataLen); err != nil {
			return err
		}

		suffix, err := r.Peek(logEntryRecordLengthSize)
		if err != nil {
			return err
		}
		if got := binary.LittleEndian.Uint32(suffix[:]); got != uint32(header.bodyLen) {
			return fmt.Errorf("record length suffix %d does not match prefix %d", got, header.bodyLen)
		}

		_, err = r.Discard(logEntryRecordLengthSize)
		return err
	}

	entry, err := readLogEntryRecordDataBuffered(r, header)
	if err != nil {
		return err
	}

	if isCompletedEvent(entry) {
		result.TerminalAlreadyConsumed = true
	}

	return nil
}

type logEntryRecordHeader struct {
	timestamp time.Time
	stream    api.Stream
	sequence  int64
	completed api.RunOutcome
	bodyLen   int
	dataLen   int
}

func readLogEntryRecordHeader(r io.Reader) (logEntryRecordHeader, error) {
	var lengthBuf [logEntryRecordLengthSize]byte
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {
		return logEntryRecordHeader{}, err
	}

	bodyLen := binary.LittleEndian.Uint32(lengthBuf[:])
	if bodyLen < logEntryRecordHeaderSize {
		return logEntryRecordHeader{}, fmt.Errorf("record body length %d is smaller than header length %d", bodyLen, logEntryRecordHeaderSize)
	}

	if uint64(bodyLen) > uint64(int(^uint(0)>>1)) {
		return logEntryRecordHeader{}, fmt.Errorf("record body length %d exceeds addressable buffer size", bodyLen)
	}

	var headerBuf [logEntryRecordHeaderSize]byte
	if _, err := io.ReadFull(r, headerBuf[:]); err != nil {
		return logEntryRecordHeader{}, err
	}

	dataLen := binary.LittleEndian.Uint32(headerBuf[28:32])
	if dataLen != bodyLen-logEntryRecordHeaderSize {
		return logEntryRecordHeader{}, fmt.Errorf("record data length %d does not match body payload length %d", dataLen, bodyLen-logEntryRecordHeaderSize)
	}

	nsec := binary.LittleEndian.Uint32(headerBuf[8:12])
	if nsec > uint32(time.Second-time.Nanosecond) {
		return logEntryRecordHeader{}, fmt.Errorf("record timestamp nanosecond value %d is invalid", nsec)
	}

	return logEntryRecordHeader{
		timestamp: time.Unix(int64(binary.LittleEndian.Uint64(headerBuf[0:8])), int64(nsec)).UTC(),
		stream:    api.Stream(int32(binary.LittleEndian.Uint32(headerBuf[12:16]))),
		sequence:  int64(binary.LittleEndian.Uint64(headerBuf[16:24])),
		completed: api.RunOutcome(int32(binary.LittleEndian.Uint32(headerBuf[24:28]))),
		bodyLen:   int(bodyLen),
		dataLen:   int(dataLen),
	}, nil
}

func readLogEntryRecordData(r io.Reader, header logEntryRecordHeader) (LogEntry, error) {
	data := make([]byte, header.dataLen)
	if _, err := io.ReadFull(r, data); err != nil {
		return LogEntry{}, err
	}

	var suffix [logEntryRecordLengthSize]byte
	if _, err := io.ReadFull(r, suffix[:]); err != nil {
		return LogEntry{}, err
	}

	if got := binary.LittleEndian.Uint32(suffix[:]); got != uint32(header.bodyLen) {
		return LogEntry{}, fmt.Errorf("record length suffix %d does not match prefix %d", got, header.bodyLen)
	}

	return LogEntry{
		Timestamp: header.timestamp,
		Stream:    header.stream,
		Sequence:  header.sequence,
		Data:      data,
		Completed: header.completed,
	}, nil
}

func readLogEntryRecordDataBuffered(r *bufio.Reader, header logEntryRecordHeader) (LogEntry, error) {
	recordTail := make([]byte, header.dataLen+logEntryRecordLengthSize)
	if _, err := io.ReadFull(r, recordTail); err != nil {
		return LogEntry{}, err
	}

	suffix := recordTail[header.dataLen:]
	if got := binary.LittleEndian.Uint32(suffix); got != uint32(header.bodyLen) {
		return LogEntry{}, fmt.Errorf("record length suffix %d does not match prefix %d", got, header.bodyLen)
	}

	return LogEntry{
		Timestamp: header.timestamp,
		Stream:    header.stream,
		Sequence:  header.sequence,
		Data:      recordTail[:header.dataLen],
		Completed: header.completed,
	}, nil
}

func replayTailFromFile(f *os.File, opts LogReplayOptions) (LogReplayResult, error) {
	st, err := f.Stat()
	if err != nil {
		return LogReplayResult{}, err
	}

	result := LogReplayResult{
		Found:   true,
		Entries: make([]LogEntry, 0, opts.Tail),
	}

	pos := st.Size()
	for pos > 0 && len(result.Entries) < opts.Tail {
		entry, nextPos, err := readLogEntryRecordBefore(f, pos)
		if err != nil {
			return LogReplayResult{}, err
		}

		pos = nextPos
		if entry.Sequence <= opts.SinceSequence {
			if isCompletedEvent(entry) {
				result.TerminalAlreadyConsumed = true
			}

			if opts.SinceSequence > 0 {
				break
			}

			continue
		}

		result.Entries = append(result.Entries, entry)
	}

	for i, j := 0, len(result.Entries)-1; i < j; i, j = i+1, j-1 {
		result.Entries[i], result.Entries[j] = result.Entries[j], result.Entries[i]
	}

	if opts.Limit > 0 && len(result.Entries) > opts.Limit {
		result.Entries = result.Entries[:opts.Limit]
		result.Truncated = true
	}

	return result, nil
}

func readLogEntryRecordBefore(f *os.File, pos int64) (LogEntry, int64, error) {
	if pos < int64(2*logEntryRecordLengthSize+logEntryRecordHeaderSize) {
		return LogEntry{}, 0, fmt.Errorf("record ending at %d is smaller than minimum record size", pos)
	}

	var suffix [logEntryRecordLengthSize]byte
	suffixOffset := pos - logEntryRecordLengthSize
	if _, err := f.ReadAt(suffix[:], suffixOffset); err != nil {
		return LogEntry{}, 0, err
	}

	bodyLen := binary.LittleEndian.Uint32(suffix[:])
	if bodyLen < logEntryRecordHeaderSize {
		return LogEntry{}, 0, fmt.Errorf("record body length %d is smaller than header length %d", bodyLen, logEntryRecordHeaderSize)
	}

	if uint64(bodyLen) > uint64(int(^uint(0)>>1)) {
		return LogEntry{}, 0, fmt.Errorf("record body length %d exceeds addressable buffer size", bodyLen)
	}

	recordStart := pos - logEntryRecordLengthSize - int64(bodyLen) - logEntryRecordLengthSize
	if recordStart < 0 {
		return LogEntry{}, 0, fmt.Errorf("record starting before beginning of file")
	}

	var prefix [logEntryRecordLengthSize]byte
	if _, err := f.ReadAt(prefix[:], recordStart); err != nil {
		return LogEntry{}, 0, err
	}

	if got := binary.LittleEndian.Uint32(prefix[:]); got != bodyLen {
		return LogEntry{}, 0, fmt.Errorf("record length prefix %d does not match suffix %d", got, bodyLen)
	}

	body := make([]byte, int(bodyLen))
	if _, err := f.ReadAt(body, recordStart+logEntryRecordLengthSize); err != nil {
		return LogEntry{}, 0, err
	}

	entry, err := decodeLogEntryRecordBody(body)
	if err != nil {
		return LogEntry{}, 0, err
	}

	return entry, recordStart, nil
}

func (s *LocalRunLogStore) runPath(runID string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(runID))
	return filepath.Join(s.baseDir, encoded+".vlog")
}
