package interfaces

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	api "vectis/api/gen/go"
)

type QueueClient interface {
	Enqueue(ctx context.Context, req *api.JobRequest) error
	Dequeue(ctx context.Context) (*api.JobRequest, error)
	TryDequeue(ctx context.Context) (*api.JobRequest, error)
	Ack(ctx context.Context, deliveryID string) error
	Close() error
}

type QueueService interface {
	Enqueue(ctx context.Context, req *api.JobRequest) (*api.Empty, error)
}

type grpcQueueService struct {
	client api.QueueServiceClient
}

func NewQueueService(client api.QueueServiceClient) QueueService {
	return &grpcQueueService{client: client}
}

func (c *grpcQueueService) Enqueue(ctx context.Context, req *api.JobRequest) (*api.Empty, error) {
	return c.client.Enqueue(ctx, req)
}

type QueueServiceClient interface {
	Enqueue(ctx context.Context, req *api.JobRequest) (*api.Empty, error)
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

func (c *GRPCQueueClient) Enqueue(ctx context.Context, req *api.JobRequest) error {
	_, err := c.client.Enqueue(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to enqueue job: %w", err)
	}
	return nil
}

func (c *GRPCQueueClient) Dequeue(ctx context.Context) (*api.JobRequest, error) {
	req, err := c.client.Dequeue(ctx, &api.Empty{})
	if err != nil {
		return nil, fmt.Errorf("failed to dequeue job: %w", err)
	}
	return req, nil
}

func (c *GRPCQueueClient) TryDequeue(ctx context.Context) (*api.JobRequest, error) {
	req, err := c.client.TryDequeue(ctx, &api.Empty{})
	if err != nil {
		return nil, fmt.Errorf("failed to try dequeue job: %w", err)
	}
	return req, nil
}

func (c *GRPCQueueClient) Ack(ctx context.Context, deliveryID string) error {
	_, err := c.client.Ack(ctx, &api.AckRequest{DeliveryId: &deliveryID})
	if err != nil {
		return fmt.Errorf("failed to ack delivery %q: %w", deliveryID, err)
	}
	return nil
}

func (c *GRPCQueueClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
