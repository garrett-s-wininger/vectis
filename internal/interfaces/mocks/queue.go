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
	jobs       []*api.Job
	enqueueErr error
	dequeueErr error
	closed     bool
}

func NewMockQueueClient() *MockQueueClient {
	return &MockQueueClient{
		jobs: make([]*api.Job, 0),
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

func (m *MockQueueClient) AddJob(job *api.Job) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs = append(m.jobs, job)
}

func (m *MockQueueClient) GetJobs() []*api.Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*api.Job, len(m.jobs))
	copy(result, m.jobs)
	return result
}

func (m *MockQueueClient) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *MockQueueClient) Enqueue(ctx context.Context, job *api.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.enqueueErr != nil {
		return m.enqueueErr
	}

	m.jobs = append(m.jobs, job)
	return nil
}

func (m *MockQueueClient) Dequeue(ctx context.Context) (*api.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.dequeueErr != nil {
		return nil, m.dequeueErr
	}

	if len(m.jobs) == 0 {
		m.mu.Unlock()
		<-ctx.Done()
		m.mu.Lock()
		return nil, ctx.Err()
	}

	if len(m.jobs) == 0 {
		return nil, errors.New("no jobs available")
	}

	job := m.jobs[0]
	m.jobs = m.jobs[1:]
	return job, nil
}

func (m *MockQueueClient) TryDequeue(ctx context.Context) (*api.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.dequeueErr != nil {
		return nil, m.dequeueErr
	}

	if len(m.jobs) == 0 {
		return nil, nil
	}

	job := m.jobs[0]
	m.jobs = m.jobs[1:]
	return job, nil
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
	jobs       []*api.Job
	enqueueErr error
}

func NewMockQueueService() *MockQueueService {
	return &MockQueueService{
		jobs: make([]*api.Job, 0),
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
	result := make([]*api.Job, len(m.jobs))
	copy(result, m.jobs)
	return result
}

func (m *MockQueueService) Enqueue(ctx context.Context, job *api.Job) (*api.Empty, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.enqueueErr != nil {
		return nil, m.enqueueErr
	}

	m.jobs = append(m.jobs, job)
	return &api.Empty{}, nil
}

var _ interfaces.QueueService = (*MockQueueService)(nil)
