package interfaces

import (
	"context"
	"fmt"

	api "vectis/api/gen/go"

	"google.golang.org/grpc"
)

type QueueClient interface {
	Enqueue(ctx context.Context, job *api.Job) error
	Dequeue(ctx context.Context) (*api.Job, error)
	Close() error
}

type GRPCQueueClient struct {
	conn   *grpc.ClientConn
	client api.QueueServiceClient
}

func NewGRPCQueueClient(conn *grpc.ClientConn) *GRPCQueueClient {
	return &GRPCQueueClient{
		conn:   conn,
		client: api.NewQueueServiceClient(conn),
	}
}

func (c *GRPCQueueClient) Enqueue(ctx context.Context, job *api.Job) error {
	_, err := c.client.Enqueue(ctx, job)
	if err != nil {
		return fmt.Errorf("failed to enqueue job: %w", err)
	}
	return nil
}

func (c *GRPCQueueClient) Dequeue(ctx context.Context) (*api.Job, error) {
	job, err := c.client.Dequeue(ctx, &api.Empty{})
	if err != nil {
		return nil, fmt.Errorf("failed to dequeue job: %w", err)
	}
	return job, nil
}

func (c *GRPCQueueClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
