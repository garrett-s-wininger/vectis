package logserver

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
)

var ErrLogStoreReadOnly = errors.New("log storage is read-only for new runs")

const (
	logStorageLockFileName       = "log.lock"
	defaultLogStoreOpenFileLimit = 256
)

type RunLogStore interface {
	Append(runID string, entry LogEntry) error
	List(runID string) ([]LogEntry, error)
}

// RunLogBatchStore persists multiple entries for a run with one durable append.
type RunLogBatchStore interface {
	AppendBatch(runID string, entries []LogEntry) error
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

	path := s.runPath(runID)
	b, err := marshalLogEntryLines(entries)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := s.appendFileLocked(runID, path)
	if err != nil {
		return err
	}

	n, err := f.Write(b)
	if err != nil {
		return fmt.Errorf("append log entry: %w", err)
	}

	if n != len(b) {
		return fmt.Errorf("append log entry: %w", io.ErrShortWrite)
	}

	return nil
}

func marshalLogEntryLines(entries []LogEntry) ([]byte, error) {
	var size int
	for _, entry := range entries {
		size += len(entry.Data) + 128
	}

	var buf bytes.Buffer
	buf.Grow(size)
	for _, entry := range entries {
		b, err := json.Marshal(entry)
		if err != nil {
			return nil, fmt.Errorf("marshal log entry: %w", err)
		}

		buf.Write(b)
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
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

	dec := json.NewDecoder(f)
	entries := make([]LogEntry, 0, 128)
	for {
		var entry LogEntry
		if err := dec.Decode(&entry); err != nil {
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

func (s *LocalRunLogStore) runPath(runID string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(runID))
	return filepath.Join(s.baseDir, encoded+".jsonl")
}
