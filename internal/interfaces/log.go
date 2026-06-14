package interfaces

import (
	"context"
	"fmt"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/logbatch"

	"google.golang.org/grpc"
)

type LogClient interface {
	StreamLogs(ctx context.Context) (LogStream, error)
	Close() error
}

type RunLogClient interface {
	LogClient
	StreamLogsForRun(ctx context.Context, runID string) (LogStream, error)
}

type AssignedRunLogClient interface {
	RunLogClient
	StreamLogsForAssignedRun(ctx context.Context, runID, shardID string) (LogStream, error)
}

type LogBatchClient interface {
	LogClient
	SendLogBatch(ctx context.Context, chunks []*api.LogChunk) error
}

type RunLogBatchClient interface {
	LogBatchClient
	SendLogBatchForRun(ctx context.Context, runID string, chunks []*api.LogChunk) error
}

type AssignedRunLogBatchClient interface {
	RunLogBatchClient
	SendLogBatchForAssignedRun(ctx context.Context, runID, shardID string, chunks []*api.LogChunk) error
}

type RunLogShardAssigner interface {
	AssignLogShardForRun(ctx context.Context, runID string) (shardID string, err error)
}

type LogStream interface {
	Send(chunk *api.LogChunk) error
	CloseSend() error
}

const LogSyntheticCompletionMetadata = "vectis-log-synthetic-completion"

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

func (c *GRPCLogClient) StreamLogsForRun(ctx context.Context, _ string) (LogStream, error) {
	return c.StreamLogs(ctx)
}

func (c *GRPCLogClient) SendLogBatch(ctx context.Context, chunks []*api.LogChunk) error {
	buffer, records, err := logbatch.BorrowMarshalBuffer(chunks)
	if err != nil {
		return err
	}
	defer logbatch.ReleaseMarshalBuffer(buffer)

	_, err = c.client.SendLogBatch(ctx, &api.LogBatch{Records: records})
	if err != nil {
		return fmt.Errorf("send log batch: %w", err)
	}

	return nil
}

func (c *GRPCLogClient) SendLogBatchForRun(ctx context.Context, _ string, chunks []*api.LogChunk) error {
	return c.SendLogBatch(ctx, chunks)
}

func (c *GRPCLogClient) SendLogBatchForAssignedRun(ctx context.Context, _, _ string, chunks []*api.LogChunk) error {
	return c.SendLogBatch(ctx, chunks)
}

// PreferUnscopedLogStream reports that run-scoped streams are a no-op for this client.
func (c *GRPCLogClient) PreferUnscopedLogStream() bool {
	return true
}

func (c *GRPCLogClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

type grpcLogStream struct {
	stream api.LogService_StreamLogsClient
	mu     sync.Mutex
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

func (s *grpcLogStream) CloseAndRecv() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.stream.CloseAndRecv()
	return err
}
