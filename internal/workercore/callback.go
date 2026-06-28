package workercore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/interfaces"

	"google.golang.org/protobuf/proto"
)

const artifactCallbackChunkSize = 64 * 1024

type callbackLogClient struct {
	sessionID     string
	shellEndpoint string

	mu     sync.Mutex
	conn   io.Closer
	client api.WorkerCoreShellServiceClient
}

func newCallbackLogClient(sessionID, shellEndpoint string) *callbackLogClient {
	return &callbackLogClient{sessionID: sessionID, shellEndpoint: shellEndpoint}
}

func (c *callbackLogClient) StreamLogs(ctx context.Context) (interfaces.LogStream, error) {
	client, err := c.shellClient(ctx)
	if err != nil {
		return nil, err
	}

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		return nil, fmt.Errorf("open worker core shell log stream: %w", err)
	}

	return &callbackLogStream{sessionID: c.sessionID, stream: stream}, nil
}

func (c *callbackLogClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil
	}

	err := c.conn.Close()
	c.conn = nil
	c.client = nil

	return err
}

func (c *callbackLogClient) shellClient(ctx context.Context) (api.WorkerCoreShellServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil {
		return c.client, nil
	}

	conn, err := dialUnixShell(ctx, c.shellEndpoint)
	if err != nil {
		return nil, err
	}

	c.conn = conn
	c.client = api.NewWorkerCoreShellServiceClient(conn)
	return c.client, nil
}

var _ interfaces.LogClient = (*callbackLogClient)(nil)

type callbackLogStream struct {
	sessionID string
	stream    api.WorkerCoreShellService_StreamLogsClient
	mu        sync.Mutex
}

func (s *callbackLogStream) Send(chunk *api.LogChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.stream.Send(&api.WorkerCoreLogChunk{
		SessionId: proto.String(s.sessionID),
		Chunk:     chunk,
	})
}

func (s *callbackLogStream) CloseSend() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.stream.CloseAndRecv()
	return err
}

var _ interfaces.LogStream = (*callbackLogStream)(nil)

type callbackArtifactPublisher struct {
	sessionID     string
	shellEndpoint string
	chunkSize     int
}

func newCallbackArtifactPublisher(sessionID, shellEndpoint string) *callbackArtifactPublisher {
	return &callbackArtifactPublisher{
		sessionID:     sessionID,
		shellEndpoint: shellEndpoint,
		chunkSize:     artifactCallbackChunkSize,
	}
}

func (p *callbackArtifactPublisher) PublishArtifact(ctx context.Context, req action.ArtifactPublishRequest) (action.ArtifactPublishResult, error) {
	conn, err := dialUnixShell(ctx, p.shellEndpoint)
	if err != nil {
		return action.ArtifactPublishResult{}, err
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(conn)

	stream, err := api.NewWorkerCoreShellServiceClient(conn).PublishArtifact(ctx)
	if err != nil {
		return action.ArtifactPublishResult{}, fmt.Errorf("open worker core shell artifact stream: %w", err)
	}

	if err := stream.Send(&api.WorkerCoreArtifactChunk{Metadata: ArtifactMetadataProto(p.sessionID, req)}); err != nil {
		return action.ArtifactPublishResult{}, fmt.Errorf("send artifact metadata: %w", err)
	}

	reader := req.Reader
	if reader == nil {
		reader = bytes.NewReader(nil)
	}

	buf := make([]byte, p.chunkSize)
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			if err := stream.Send(&api.WorkerCoreArtifactChunk{Data: append([]byte(nil), buf[:n]...)}); err != nil {
				return action.ArtifactPublishResult{}, fmt.Errorf("send artifact data: %w", err)
			}
		}

		if readErr == io.EOF {
			break
		}

		if readErr != nil {
			return action.ArtifactPublishResult{}, fmt.Errorf("read artifact data: %w", readErr)
		}
	}

	result, err := stream.CloseAndRecv()
	if err != nil {
		return action.ArtifactPublishResult{}, fmt.Errorf("publish artifact via worker core shell: %w", err)
	}

	return ArtifactResultFromProto(result), nil
}

var _ action.ArtifactPublisher = (*callbackArtifactPublisher)(nil)
