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
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "unix", c.socketPath)
	if err != nil {
		return nil, fmt.Errorf("dial forwarder socket: %w", err)
	}

	return &forwarderLogStream{conn: conn}, nil
}

// Close is a no-op for this client (connections are per-stream).
func (c *ForwarderLogClient) Close() error {
	return nil
}

type forwarderLogStream struct {
	conn net.Conn
	mu   sync.Mutex
}

func (s *forwarderLogStream) Send(chunk *api.LogChunk) error {
	if chunk == nil {
		return fmt.Errorf("cannot send nil chunk")
	}

	data, err := proto.Marshal(chunk)
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

// Close delegates to the fallback client.
func (c *PreferForwarderLogClient) Close() error {
	return c.fallback.Close()
}

var _ LogClient = (*ForwarderLogClient)(nil)
var _ LogStream = (*forwarderLogStream)(nil)
var _ LogClient = (*PreferForwarderLogClient)(nil)
