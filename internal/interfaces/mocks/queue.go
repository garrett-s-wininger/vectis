package mocks

import (
	"context"
	"errors"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
)

type MockQueueClient struct {
	mu         sync.Mutex
	reqs       []*api.JobRequest
	enqueueErr error
	dequeueErr error
	ackErr     error
	closed     bool
}

func NewMockQueueClient() *MockQueueClient {
	return &MockQueueClient{
		reqs: make([]*api.JobRequest, 0),
	}
}

func (m *MockQueueClient) SetEnqueueError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enqueueErr = err
}

func (m *MockQueueClient) SetDequeueError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dequeueErr = err
}

func (m *MockQueueClient) SetAckError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ackErr = err
}

func (m *MockQueueClient) AddJob(job *api.Job) {
	m.AddJobRequest(&api.JobRequest{Job: job})
}

func (m *MockQueueClient) AddJobRequest(req *api.JobRequest) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reqs = append(m.reqs, req)
}

func (m *MockQueueClient) GetJobs() []*api.Job {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]*api.Job, 0, len(m.reqs))
	for _, req := range m.reqs {
		result = append(result, req.GetJob())
	}

	return result
}

func (m *MockQueueClient) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *MockQueueClient) Enqueue(ctx context.Context, req *api.JobRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.enqueueErr != nil {
		return m.enqueueErr
	}

	m.reqs = append(m.reqs, req)
	return nil
}

func (m *MockQueueClient) Dequeue(ctx context.Context) (*api.JobRequest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.dequeueErr != nil {
		return nil, m.dequeueErr
	}

	if len(m.reqs) == 0 {
		m.mu.Unlock()
		<-ctx.Done()
		m.mu.Lock()
		return nil, ctx.Err()
	}

	if len(m.reqs) == 0 {
		return nil, errors.New("no jobs available")
	}

	req := m.reqs[0]
	m.reqs = m.reqs[1:]
	return req, nil
}

func (m *MockQueueClient) TryDequeue(ctx context.Context) (*api.JobRequest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.dequeueErr != nil {
		return nil, m.dequeueErr
	}

	if len(m.reqs) == 0 {
		return nil, nil
	}

	req := m.reqs[0]
	m.reqs = m.reqs[1:]
	return req, nil
}

func (m *MockQueueClient) Ack(ctx context.Context, deliveryID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ackErr != nil {
		return m.ackErr
	}

	return nil
}

func (m *MockQueueClient) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

var _ interfaces.QueueClient = (*MockQueueClient)(nil)

type MockQueueService struct {
	mu         sync.Mutex
	reqs       []*api.JobRequest
	enqueueErr error
}

func NewMockQueueService() *MockQueueService {
	return &MockQueueService{
		reqs: make([]*api.JobRequest, 0),
	}
}

func (m *MockQueueService) SetEnqueueError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enqueueErr = err
}

func (m *MockQueueService) GetJobs() []*api.Job {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]*api.Job, 0, len(m.reqs))
	for _, req := range m.reqs {
		result = append(result, req.GetJob())
	}

	return result
}

func (m *MockQueueService) Enqueue(ctx context.Context, req *api.JobRequest) (*api.Empty, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.enqueueErr != nil {
		return nil, m.enqueueErr
	}

	m.reqs = append(m.reqs, req)
	return &api.Empty{}, nil
}

var _ interfaces.QueueService = (*MockQueueService)(nil)
