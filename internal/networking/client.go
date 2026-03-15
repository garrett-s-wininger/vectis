package networking

import (
	"context"
	"fmt"
	"time"

	"vectis/internal/backoff"
	"vectis/internal/interfaces"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultMaxTries  = 5
	defaultBaseDelay = 500 * time.Millisecond
)

type Client[T any] struct {
	conn      *grpc.ClientConn
	client    T
	Logger    interfaces.Logger
	MaxTries  int
	BaseDelay time.Duration
	Clock     interfaces.Clock
}

func NewClient[T any](ctx context.Context, addr string, newClient func(grpc.ClientConnInterface) T, logger interfaces.Logger, clock interfaces.Clock) (*Client[T], error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	if clock == nil {
		clock = interfaces.SystemClock{}
	}

	conn, err := connectWithRetry(ctx, addr, logger, clock)
	if err != nil {
		return nil, err
	}

	return &Client[T]{
		conn:      conn,
		client:    newClient(conn),
		Logger:    logger,
		MaxTries:  defaultMaxTries,
		BaseDelay: defaultBaseDelay,
		Clock:     clock,
	}, nil
}

func connectWithRetry(ctx context.Context, addr string, logger interfaces.Logger, clock interfaces.Clock) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	var conn *grpc.ClientConn

	retryer := backoff.NewRetryer(backoff.RetryConfig{
		MaxTries:  defaultMaxTries,
		BaseDelay: defaultBaseDelay,
		Clock:     clock,
	})

	err := retryer.Do(ctx, func() error {
		var e error
		conn, e = grpc.NewClient(addr, opts...)
		return e
	}, func(attempt int, nextDelay time.Duration, err error) {
		logger.Warn("Failed to connect (attempt %d/%d): %v. Retrying in %v...", attempt, defaultMaxTries, err, nextDelay)
	})

	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (c *Client[T]) Client() T {
	return c.client
}

func (c *Client[T]) Close() error {
	if c.conn == nil {
		return nil
	}

	return c.conn.Close()
}
