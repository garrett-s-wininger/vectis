package api

import (
	"encoding/json"
	"sync"

	"vectis/internal/interfaces"
)

type RunEvent struct {
	RunID    string `json:"run_id"`
	RunIndex int    `json:"run_index"`
}

type RunBroadcaster struct {
	mu          sync.RWMutex
	subscribers map[string]map[chan []byte]struct{}
	logger      interfaces.Logger
}

func NewRunBroadcaster(logger interfaces.Logger) *RunBroadcaster {
	return &RunBroadcaster{
		subscribers: make(map[string]map[chan []byte]struct{}),
		logger:      logger,
	}
}

func (b *RunBroadcaster) Subscribe(jobID string) chan []byte {
	ch := make(chan []byte, 32)
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.subscribers[jobID] == nil {
		b.subscribers[jobID] = make(map[chan []byte]struct{})
	}

	b.subscribers[jobID][ch] = struct{}{}
	return ch
}

func (b *RunBroadcaster) Unsubscribe(jobID string, ch chan []byte) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	m := b.subscribers[jobID]

	if m == nil {
		return false
	}

	if _, ok := m[ch]; !ok {
		return false
	}

	delete(m, ch)
	if len(m) == 0 {
		delete(b.subscribers, jobID)
	}

	close(ch)
	return true
}

func (b *RunBroadcaster) Broadcast(jobID, runID string, runIndex int) {
	payload, err := json.Marshal(RunEvent{RunID: runID, RunIndex: runIndex})
	if err != nil {
		return
	}

	b.mu.RLock()
	m := b.subscribers[jobID]
	if m != nil {
		for ch := range m {
			select {
			case ch <- payload:
			default:
				if b.logger != nil {
					b.logger.Warn("Run broadcast buffer full for job %s; dropping run event", jobID)
				}
			}
		}
	}
	b.mu.RUnlock()
}
