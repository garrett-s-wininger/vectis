package mocks

import (
	"context"
	"errors"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
)

type MockLogClient struct {
	mu          sync.Mutex
	chunks      []*api.LogChunk
	streamErr   error
	closeErr    error
	closed      bool
	streamCount int
}

func NewMockLogClient() *MockLogClient {
	return &MockLogClient{
		chunks: make([]*api.LogChunk, 0),
	}
}

func (m *MockLogClient) SetStreamError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streamErr = err
}

func (m *MockLogClient) SetCloseError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeErr = err
}

func (m *MockLogClient) GetChunks() []*api.LogChunk {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*api.LogChunk, len(m.chunks))
	copy(result, m.chunks)
	return result
}

func (m *MockLogClient) GetStreamCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.streamCount
}

func (m *MockLogClient) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *MockLogClient) StreamLogs(ctx context.Context) (interfaces.LogStream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.streamErr != nil {
		return nil, m.streamErr
	}

	m.streamCount++
	return &MockLogStream{client: m}, nil
}

func (m *MockLogClient) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closeErr != nil {
		return m.closeErr
	}

	m.closed = true
	return nil
}

func (m *MockLogClient) addChunk(chunk *api.LogChunk) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.chunks = append(m.chunks, chunk)
}

type MockLogStream struct {
	mu           sync.Mutex
	client       *MockLogClient
	sendErr      error
	closeSendErr error
	sendCalled   bool
	closeCalled  bool
	closed       bool
}

func (m *MockLogStream) SetSendError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendErr = err
}

func (m *MockLogStream) SetCloseSendError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeSendErr = err
}

func (m *MockLogStream) SendCalled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sendCalled
}

func (m *MockLogStream) CloseSendCalled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeCalled
}

func (m *MockLogStream) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *MockLogStream) Send(chunk *api.LogChunk) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.sendCalled = true

	if m.sendErr != nil {
		return m.sendErr
	}

	if m.closed {
		return errors.New("stream is closed")
	}

	m.client.addChunk(chunk)
	return nil
}

func (m *MockLogStream) CloseSend() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closeCalled = true

	if m.closeSendErr != nil {
		return m.closeSendErr
	}

	m.closed = true
	return nil
}

var _ interfaces.LogClient = (*MockLogClient)(nil)
var _ interfaces.LogStream = (*MockLogStream)(nil)
