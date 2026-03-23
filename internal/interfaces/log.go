package interfaces

import (
	"context"
	"fmt"
	"sync"

	api "vectis/api/gen/go"

	"google.golang.org/grpc"
)

type LogClient interface {
	StreamLogs(ctx context.Context) (LogStream, error)
	Close() error
}

type LogStream interface {
	Send(chunk *api.LogChunk) error
	CloseSend() error
}

type GRPCLogClient struct {
	conn   *grpc.ClientConn
	client api.LogServiceClient
}

func NewGRPCLogClient(conn *grpc.ClientConn) *GRPCLogClient {
	return &GRPCLogClient{
		conn:   conn,
		client: api.NewLogServiceClient(conn),
	}
}

func (c *GRPCLogClient) StreamLogs(ctx context.Context) (LogStream, error) {
	stream, err := c.client.StreamLogs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create log stream: %w", err)
	}
	return &grpcLogStream{stream: stream}, nil
}

func (c *GRPCLogClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

type grpcLogStream struct {
	stream api.LogService_StreamLogsClient
	mu sync.Mutex
}

func (s *grpcLogStream) Send(chunk *api.LogChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stream.Send(chunk)
}

func (s *grpcLogStream) CloseSend() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stream.CloseSend()
}
