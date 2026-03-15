package mocks

import (
	"context"
	"sync"
	"time"

	"vectis/internal/interfaces"
)

type MockClock struct {
	mu          sync.Mutex
	currentTime time.Time
	sleeps      []time.Duration
	sleepErr    error
}

func NewMockClock() *MockClock {
	return &MockClock{
		currentTime: time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC),
		sleeps:      make([]time.Duration, 0),
	}
}

func (m *MockClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.currentTime
}

func (m *MockClock) SetNow(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentTime = t
}

func (m *MockClock) Sleep(ctx context.Context, d time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.sleeps = append(m.sleeps, d)

	if m.sleepErr != nil {
		return m.sleepErr
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (m *MockClock) SetSleepError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sleepErr = err
}

func (m *MockClock) GetSleeps() []time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]time.Duration, len(m.sleeps))
	copy(result, m.sleeps)
	return result
}

func (m *MockClock) ResetSleeps() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sleeps = make([]time.Duration, 0)
}

var _ interfaces.Clock = (*MockClock)(nil)
