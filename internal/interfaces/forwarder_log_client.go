package interfaces

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"

	api "vectis/api/gen/go"

	"google.golang.org/protobuf/proto"
)

const sendTimeout = 30 * time.Second

// ForwarderLogClient connects to the vectis-log-forwarder over a Unix domain
// socket and streams LogChunks using a length-prefixed protobuf protocol.
type ForwarderLogClient struct {
	socketPath string
}

// NewForwarderLogClient creates a client that targets the given Unix socket.
func NewForwarderLogClient(socketPath string) *ForwarderLogClient {
	return &ForwarderLogClient{socketPath: socketPath}
}

// StreamLogs dials the forwarder Unix socket and returns a LogStream.
func (c *ForwarderLogClient) StreamLogs(ctx context.Context) (LogStream, error) {
	return c.streamLogs(ctx, "", "")
}

func (c *ForwarderLogClient) streamLogs(ctx context.Context, runID, shardID string) (LogStream, error) {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "unix", c.socketPath)
	if err != nil {
		return nil, fmt.Errorf("dial forwarder socket: %w", err)
	}

	return &forwarderLogStream{conn: conn, runID: runID, logShardID: shardID}, nil
}

func (c *ForwarderLogClient) StreamLogsForRun(ctx context.Context, runID string) (LogStream, error) {
	return c.streamLogs(ctx, runID, "")
}

func (c *ForwarderLogClient) StreamLogsForAssignedRun(ctx context.Context, runID, shardID string) (LogStream, error) {
	if shardID == "" {
		return c.StreamLogsForRun(ctx, runID)
	}

	return c.streamLogs(ctx, runID, shardID)
}

// Close is a no-op for this client (connections are per-stream).
func (c *ForwarderLogClient) Close() error {
	return nil
}

type forwarderLogStream struct {
	conn       net.Conn
	runID      string
	logShardID string
	mu         sync.Mutex
}

func (s *forwarderLogStream) Send(chunk *api.LogChunk) error {
	if chunk == nil {
		return fmt.Errorf("cannot send nil chunk")
	}

	sendChunk, err := s.prepareChunk(chunk)
	if err != nil {
		return err
	}

	data, err := proto.Marshal(sendChunk)
	if err != nil {
		return fmt.Errorf("marshal chunk: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.conn.SetWriteDeadline(time.Now().Add(sendTimeout)); err != nil {
		return fmt.Errorf("set write deadline: %w", err)
	}

	length := uint32(len(data))
	if err := binary.Write(s.conn, binary.BigEndian, length); err != nil {
		return fmt.Errorf("write length: %w", err)
	}

	if _, err := s.conn.Write(data); err != nil {
		return fmt.Errorf("write data: %w", err)
	}

	return nil
}

func (s *forwarderLogStream) prepareChunk(chunk *api.LogChunk) (*api.LogChunk, error) {
	if s.runID != "" && chunk.GetRunId() != s.runID {
		return nil, fmt.Errorf("forwarder stream is for run %q, got chunk for run %q", s.runID, chunk.GetRunId())
	}

	if s.logShardID == "" {
		return chunk, nil
	}

	if existing := chunk.GetLogShardId(); existing != "" {
		if existing != s.logShardID {
			return nil, fmt.Errorf("chunk log shard %q does not match stream log shard %q", existing, s.logShardID)
		}

		return chunk, nil
	}

	cloned, ok := proto.Clone(chunk).(*api.LogChunk)
	if !ok {
		return nil, fmt.Errorf("clone log chunk")
	}

	shardID := s.logShardID
	cloned.LogShardId = &shardID
	return cloned, nil
}

func (s *forwarderLogStream) CloseSend() error {
	return s.conn.Close()
}

// PreferForwarderLogClient tries the local Unix socket forwarder first and
// falls back to the provided gRPC client if the forwarder is unreachable.
type PreferForwarderLogClient struct {
	socketPath string
	fallback   LogClient
}

// NewPreferForwarderLogClient creates a client that prefers the local
// forwarder Unix socket when it is alive.
func NewPreferForwarderLogClient(socketPath string, fallback LogClient) *PreferForwarderLogClient {
	return &PreferForwarderLogClient{socketPath: socketPath, fallback: fallback}
}

// StreamLogs attempts to dial the forwarder first and falls back to the
// gRPC client on any error.  This avoids the TOCTOU race of a separate
// liveness probe.
func (c *PreferForwarderLogClient) StreamLogs(ctx context.Context) (LogStream, error) {
	stream, err := NewForwarderLogClient(c.socketPath).StreamLogs(ctx)
	if err == nil {
		return stream, nil
	}

	return c.fallback.StreamLogs(ctx)
}

func (c *PreferForwarderLogClient) StreamLogsForRun(ctx context.Context, runID string) (LogStream, error) {
	shardID, err := c.assignLogShardForRun(ctx, runID)
	if err != nil {
		return nil, err
	}

	socketClient := NewForwarderLogClient(c.socketPath)
	var stream LogStream
	if shardID != "" {
		stream, err = socketClient.StreamLogsForAssignedRun(ctx, runID, shardID)
	} else {
		stream, err = socketClient.StreamLogsForRun(ctx, runID)
	}
	if err == nil {
		return stream, nil
	}

	if shardID != "" {
		if assigned, ok := c.fallback.(AssignedRunLogClient); ok {
			return assigned.StreamLogsForAssignedRun(ctx, runID, shardID)
		}
	}

	if scoped, ok := c.fallback.(RunLogClient); ok {
		return scoped.StreamLogsForRun(ctx, runID)
	}

	return c.fallback.StreamLogs(ctx)
}

func (c *PreferForwarderLogClient) assignLogShardForRun(ctx context.Context, runID string) (string, error) {
	if runID == "" {
		return "", nil
	}

	if assigner, ok := c.fallback.(RunLogShardAssigner); ok {
		return assigner.AssignLogShardForRun(ctx, runID)
	}

	return "", nil
}

// Close delegates to the fallback client.
func (c *PreferForwarderLogClient) Close() error {
	return c.fallback.Close()
}

var _ LogClient = (*ForwarderLogClient)(nil)
var _ RunLogClient = (*ForwarderLogClient)(nil)
var _ AssignedRunLogClient = (*ForwarderLogClient)(nil)
var _ LogStream = (*forwarderLogStream)(nil)
var _ LogClient = (*PreferForwarderLogClient)(nil)
var _ RunLogClient = (*PreferForwarderLogClient)(nil)
