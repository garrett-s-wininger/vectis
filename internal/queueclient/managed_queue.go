package queueclient

import (
	"context"
	"fmt"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type ManagingQueueService struct {
	mu       sync.RWMutex
	conn     *grpc.ClientConn
	cleanup  func()
	q        interfaces.QueueService
	dial     func(context.Context) (*grpc.ClientConn, func(), error)
	logger   interfaces.Logger
	cancelFn context.CancelFunc
}

func NewManagingQueueService(ctx context.Context, logger interfaces.Logger, dial func(context.Context) (*grpc.ClientConn, func(), error)) (*ManagingQueueService, error) {
	if dial == nil {
		return nil, fmt.Errorf("dial is required")
	}

	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	m := &ManagingQueueService{
		dial:   dial,
		logger: logger,
	}

	if err := m.swapConn(ctx); err != nil {
		return nil, err
	}

	watchCtx, cancel := context.WithCancel(ctx)
	m.cancelFn = cancel
	go m.watchConn(watchCtx)

	return m, nil
}

func (m *ManagingQueueService) swapConn(ctx context.Context) error {
	conn, cleanup, err := m.dial(ctx)
	if err != nil {
		return err
	}

	m.mu.Lock()
	if m.cleanup != nil {
		m.cleanup()
	}

	m.conn = conn
	m.cleanup = cleanup
	m.q = interfaces.NewQueueService(api.NewQueueServiceClient(conn))
	m.mu.Unlock()

	return nil
}

func (m *ManagingQueueService) Enqueue(ctx context.Context, job *api.Job) (*api.Empty, error) {
	m.mu.RLock()
	var empty *api.Empty
	var err error
	if m.q != nil {
		empty, err = m.q.Enqueue(ctx, job)
	}
	m.mu.RUnlock()

	if err == nil || !IsTransientRPCError(err) {
		return empty, err
	}

	if rerr := m.swapConn(ctx); rerr != nil {
		m.logger.Warn("queue reconnect after transient Enqueue error failed: %v", rerr)
		return empty, err
	}

	m.mu.RLock()
	if m.q != nil {
		empty, err = m.q.Enqueue(ctx, job)
	} else {
		err = fmt.Errorf("queue client not available after reconnect")
	}
	m.mu.RUnlock()

	return empty, err
}

func (m *ManagingQueueService) watchConn(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		m.mu.RLock()
		conn := m.conn
		m.mu.RUnlock()

		if conn == nil {
			return
		}

		if conn.GetState() == connectivity.Shutdown {
			m.logger.Warn("queue gRPC connection already shutdown; reconnecting")
			if err := m.swapConn(context.Background()); err != nil {
				m.logger.Warn("queue reconnect after shutdown failed: %v", err)
			}

			continue
		}

		state := conn.GetState()
		if !conn.WaitForStateChange(ctx, state) {
			return
		}

		if conn.GetState() != connectivity.Shutdown {
			continue
		}

		m.logger.Warn("queue gRPC connection shutdown; reconnecting")
		if err := m.swapConn(context.Background()); err != nil {
			m.logger.Warn("queue reconnect after shutdown failed: %v", err)
		}
	}
}

func (m *ManagingQueueService) GRPCConnectivityState() connectivity.State {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.conn == nil {
		return connectivity.Shutdown
	}

	return m.conn.GetState()
}

func (m *ManagingQueueService) Close() error {
	if m.cancelFn != nil {
		m.cancelFn()
		m.cancelFn = nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cleanup != nil {
		m.cleanup()
		m.cleanup = nil
	}

	m.conn = nil
	m.q = nil
	return nil
}
