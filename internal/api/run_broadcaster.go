package api

import (
	"encoding/json"
	"sync"

	"vectis/internal/interfaces"

	"github.com/gorilla/websocket"
)

type RunEvent struct {
	RunID    string `json:"run_id"`
	RunIndex int    `json:"run_index"`
}

type RunBroadcaster struct {
	mu          sync.RWMutex
	subscribers map[string]map[*websocket.Conn]chan []byte
	logger      interfaces.Logger
}

func NewRunBroadcaster(logger interfaces.Logger) *RunBroadcaster {
	return &RunBroadcaster{
		subscribers: make(map[string]map[*websocket.Conn]chan []byte),
		logger:      logger,
	}
}

func (b *RunBroadcaster) Subscribe(jobID string, conn *websocket.Conn) chan []byte {
	ch := make(chan []byte, 32)
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.subscribers[jobID] == nil {
		b.subscribers[jobID] = make(map[*websocket.Conn]chan []byte)
	}

	b.subscribers[jobID][conn] = ch
	return ch
}

func (b *RunBroadcaster) Unsubscribe(jobID string, conn *websocket.Conn) (chan []byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	m := b.subscribers[jobID]

	if m == nil {
		return nil, false
	}

	ch, ok := m[conn]
	if !ok {
		return nil, false
	}

	delete(m, conn)
	if len(m) == 0 {
		delete(b.subscribers, jobID)
	}

	return ch, true
}

func (b *RunBroadcaster) Broadcast(jobID, runID string, runIndex int) {
	payload, err := json.Marshal(RunEvent{RunID: runID, RunIndex: runIndex})
	if err != nil {
		return
	}

	b.mu.RLock()
	m := b.subscribers[jobID]
	if m == nil {
		b.mu.RUnlock()
		return
	}

	chans := make([]chan []byte, 0, len(m))
	for _, ch := range m {
		chans = append(chans, ch)
	}
	b.mu.RUnlock()

	for _, ch := range chans {
		select {
		case ch <- payload:
		default:
			if b.logger != nil {
				b.logger.Warn("Run broadcast buffer full for job %s; dropping run event", jobID)
			}
		}
	}
}
