package logserver

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type RunLogStore interface {
	Append(runID string, entry LogEntry) error
	List(runID string) ([]LogEntry, error)
}

type NoopRunLogStore struct{}

func (NoopRunLogStore) Append(string, LogEntry) error {
	return nil
}

func (NoopRunLogStore) List(string) ([]LogEntry, error) {
	return nil, nil
}

type LocalRunLogStore struct {
	baseDir string
	mu      sync.Mutex
}

func NewLocalRunLogStore(baseDir string) (*LocalRunLogStore, error) {
	if baseDir == "" {
		return nil, fmt.Errorf("local log storage base dir is required")
	}

	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("create log storage dir: %w", err)
	}

	return &LocalRunLogStore{baseDir: baseDir}, nil
}

func (s *LocalRunLogStore) Append(runID string, entry LogEntry) error {
	if runID == "" {
		return fmt.Errorf("run id is required")
	}

	path := s.runPath(runID)
	b, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal log entry: %w", err)
	}
	b = append(b, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open log store file %s: %w", path, err)
	}
	defer f.Close()

	if _, err := f.Write(b); err != nil {
		return fmt.Errorf("append log entry: %w", err)
	}

	return nil
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

	return entries, nil
}

func (s *LocalRunLogStore) runPath(runID string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(runID))
	return filepath.Join(s.baseDir, encoded+".jsonl")
}
