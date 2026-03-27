package queueclient

import (
	"context"
	"fmt"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ManagingWorkerDial struct {
	mu      sync.RWMutex
	queue   interfaces.QueueClient
	log     interfaces.LogClient
	cleanup func()
	dial    func(context.Context) (interfaces.QueueClient, interfaces.LogClient, func(), error)
	logger  interfaces.Logger
}

func NewManagingWorkerDial(ctx context.Context, logger interfaces.Logger, dial func(context.Context) (interfaces.QueueClient, interfaces.LogClient, func(), error)) (*ManagingWorkerDial, error) {
	if dial == nil {
		return nil, fmt.Errorf("dial is required")
	}

	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	m := &ManagingWorkerDial{
		dial:   dial,
		logger: logger,
	}

	if err := m.swapAll(ctx); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *ManagingWorkerDial) swapAll(ctx context.Context) error {
	queue, log, cleanup, err := m.dial(ctx)
	if err != nil {
		return err
	}

	m.mu.Lock()
	if m.cleanup != nil {
		m.cleanup()
	}

	m.queue = queue
	m.log = log
	m.cleanup = cleanup
	m.mu.Unlock()

	return nil
}

func (m *ManagingWorkerDial) reconnectAfterTransient(ctx context.Context, cause error) error {
	if !IsTransientRPCError(cause) {
		return cause
	}

	if err := m.swapAll(ctx); err != nil {
		m.logger.Warn("worker dial reconnect failed: %v", err)
		return cause
	}

	return nil
}

func (m *ManagingWorkerDial) Enqueue(ctx context.Context, job *api.Job) error {
	m.mu.RLock()
	var err error
	if m.queue != nil {
		err = m.queue.Enqueue(ctx, job)
	}
	m.mu.RUnlock()

	if err == nil || !IsTransientRPCError(err) {
		return err
	}

	if m.reconnectAfterTransient(ctx, err) != nil {
		return err
	}

	m.mu.RLock()
	if m.queue != nil {
		err = m.queue.Enqueue(ctx, job)
	} else {
		err = fmt.Errorf("queue client not available after reconnect")
	}
	m.mu.RUnlock()

	return err
}

func (m *ManagingWorkerDial) Dequeue(ctx context.Context) (*api.Job, error) {
	m.mu.RLock()
	var job *api.Job
	var err error
	if m.queue != nil {
		job, err = m.queue.Dequeue(ctx)
	}
	m.mu.RUnlock()

	if err == nil || !IsTransientRPCError(err) {
		return job, err
	}

	if status.Code(err) == codes.DeadlineExceeded {
		return job, err
	}

	if m.reconnectAfterTransient(ctx, err) != nil {
		return job, err
	}

	m.mu.RLock()
	if m.queue != nil {
		job, err = m.queue.Dequeue(ctx)
	} else {
		err = fmt.Errorf("queue client not available after reconnect")
	}
	m.mu.RUnlock()

	return job, err
}

func (m *ManagingWorkerDial) TryDequeue(ctx context.Context) (*api.Job, error) {
	m.mu.RLock()
	var job *api.Job
	var err error
	if m.queue != nil {
		job, err = m.queue.TryDequeue(ctx)
	}
	m.mu.RUnlock()

	if err == nil || !IsTransientRPCError(err) {
		return job, err
	}

	if status.Code(err) == codes.DeadlineExceeded {
		return job, err
	}

	if m.reconnectAfterTransient(ctx, err) != nil {
		return job, err
	}

	m.mu.RLock()
	if m.queue != nil {
		job, err = m.queue.TryDequeue(ctx)
	} else {
		err = fmt.Errorf("queue client not available after reconnect")
	}
	m.mu.RUnlock()

	return job, err
}

func (m *ManagingWorkerDial) Ack(ctx context.Context, deliveryID string) error {
	m.mu.RLock()
	var err error
	if m.queue != nil {
		err = m.queue.Ack(ctx, deliveryID)
	}
	m.mu.RUnlock()

	if err == nil || !IsTransientRPCError(err) {
		return err
	}

	if m.reconnectAfterTransient(ctx, err) != nil {
		return err
	}

	m.mu.RLock()
	if m.queue != nil {
		err = m.queue.Ack(ctx, deliveryID)
	} else {
		err = fmt.Errorf("queue client not available after reconnect")
	}
	m.mu.RUnlock()

	return err
}

func (m *ManagingWorkerDial) StreamLogs(ctx context.Context) (interfaces.LogStream, error) {
	m.mu.RLock()
	var stream interfaces.LogStream
	var err error
	if m.log != nil {
		stream, err = m.log.StreamLogs(ctx)
	}
	m.mu.RUnlock()

	if err == nil || !IsTransientRPCError(err) {
		return stream, err
	}

	if m.reconnectAfterTransient(ctx, err) != nil {
		return stream, err
	}

	m.mu.RLock()
	if m.log != nil {
		stream, err = m.log.StreamLogs(ctx)
	} else {
		err = fmt.Errorf("log client not available after reconnect")
	}
	m.mu.RUnlock()

	return stream, err
}

func (m *ManagingWorkerDial) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cleanup != nil {
		m.cleanup()
		m.cleanup = nil
	}

	m.queue = nil
	m.log = nil
	return nil
}
